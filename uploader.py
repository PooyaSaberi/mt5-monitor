"""Independent uploader: no MT5 imports, collector locks, or collector writes."""

import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import random
import sqlite3
import threading
import time
from uuid import uuid4

from upload_config import load_upload_config
from upload_contract import require, serialize_request, validate_acknowledgement
from upload_lock import UploadLock, source_lock_path
from upload_reader import read_delivery
from upload_state import UploadState, binding_for, retry_schedule
from upload_integrity import anchors_for, snapshot
from upload_status import status, format_console


class Uploader:
    def __init__(self, config, state, *, send=None, clock=time.time, monotonic=time.monotonic, jitter=random.random):
        if send is None:
            from upload_http import post
            send = post
        self.config, self.state, self.send = config, state, send
        self.clock, self.monotonic, self.jitter = clock, monotonic, jitter
        self.read_limit = 20
        progress = state.progress
        self.uncertain = False
        self.deadline = None
        remaining, uncertain = retry_schedule(progress, self.clock())
        if not progress['paused']:
            self.uncertain = uncertain
            if remaining:
                self.deadline = self.monotonic() + remaining

    def remaining(self):
        return max(0., self.deadline - self.monotonic()) if self.deadline is not None else 0.

    def step(self):
        from upload_http import DeliveryFailure
        progress = self.state.progress
        if progress['paused']:
            return 'paused'
        if self.remaining() > 0:
            return 'backoff'
        pending = self.state.pending()  # corrupt state is fatal; never send it
        try:
            data = snapshot(self.config, self.state.binding, progress,
                            pending['request'] if pending else None, limit=self.read_limit)
        except sqlite3.OperationalError as exc:
            if not any(word in str(exc).lower() for word in ('locked', 'busy', 'interrupted')):
                self.state.failure('collector_read_failed', paused=True)
                return 'paused'
            self._retry('collector_busy_or_read_budget')
            if 'interrupted' in str(exc).lower() and pending is None:
                self.read_limit = max(1, self.read_limit//2)
            return 'retrying'
        except (ValueError, OSError, sqlite3.DatabaseError, KeyError, TypeError):
            self.state.failure('collector_or_anchor_inconsistent', paused=True)
            return 'paused'
        if pending is None:
            events = list(data['events'])
            if not events:
                return 'idle'
            request = dict(contract_version=1, batch_id=str(uuid4()), source=data['source'],
                           stream_id=data['stream_id'], events=events)
            while events:
                request.update(first_sequence=events[0]['sequence'], last_sequence=events[-1]['sequence'])
                try:
                    serialize_request(request)
                    break
                except ValueError:
                    if len(events) == 1:
                        self.state.failure('event_invalid_or_oversized', paused=True)
                        return 'paused'
                    events.pop()  # shrink the request, never an individual active set
            pending = self.state.prepare(request)  # commit bytes before any HTTP
        try:
            ack = self.send(self.config, pending['payload'])  # no collector read transaction exists here
            try:
                validate_acknowledgement(ack, pending['request'])
            except (ValueError, KeyError, TypeError, OverflowError):
                raise DeliveryFailure('invalid_acknowledgement') from None
        except DeliveryFailure as exc:
            if exc.retryable:
                self._retry(exc.code, exc.retry_after)
                return 'retrying'
            self.state.failure(exc.code, paused=True)
            return 'paused'
        self.state.acknowledge(ack)  # failure is fatal, leaves pending transaction intact
        self.deadline = None
        self.uncertain = False
        return 'acknowledged'

    def _retry(self, code, retry_after=0.):
        attempts = self.state.progress['attempts']
        cap = min(self.config.retry_max, self.config.retry_base * 2**min(attempts, 16))
        delay = max(min(retry_after, 300.), cap * (.5 + .5*self.jitter()))
        started = self.clock()
        self.state.failure(code, due=started+delay, schedule=dict(started=started, delay=delay,
                           retry_after=min(retry_after, 300.)))
        self.deadline = self.monotonic()+delay
        self.uncertain = False


def execute(config, command, *, stop=None, output=print, send=None, resume=False, output_format='json'):
    require(output_format in ('json', 'text'), 'Invalid output format')
    if command in ('check', 'status'):
        require(not resume, 'Resume requires once or run')
        view = status(config)
        output(json.dumps(view, sort_keys=True) if output_format == 'json' else format_console(view))
        return 2 if command == 'check' and view['state'] not in ('ready', 'not_initialized') else 0
    require(output_format == 'json', 'Text format is for check/status only')
    config.validate()
    stop = stop or threading.Event()
    with ExitStack() as resources:
        resources.enter_context(UploadLock(source_lock_path(config.collector_db)))
        resources.enter_context(UploadLock(str(config.state_db)+'.lock'))
        data = read_delivery(config.collector_db, expected_source=config.expected_source, metadata_only=True)
        state = UploadState(config.state_db, binding_for(config, data['stream_id']))
        resources.callback(state.close)
        if resume:
            state.resume()  # Explicit operator retry; step still verifies all anchors before HTTP.
        engine = Uploader(config, state, send=send)
        while not stop.is_set():
            outcome = engine.step()
            output(json.dumps(dict(outcome=outcome, acknowledged_sequence=str(state.progress['acknowledged_sequence']),
                                   error=state.progress['error']), sort_keys=True))
            if outcome == 'paused':
                return 2
            if command == 'once' and outcome == 'backoff' and engine.uncertain:
                if stop.wait(engine.remaining()):
                    return 1
                continue
            if command == 'once':
                return 0 if outcome in ('acknowledged', 'idle') else 1
            # One request per iteration. No sleeps hold a database transaction.
            if outcome in ('backoff', 'retrying'):
                delay = max(.1, engine.remaining())
            else:
                delay = .1 if outcome == 'acknowledged' else config.poll_seconds
            stop.wait(min(delay, 300.))
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('check', 'once', 'run', 'status'))
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--resume', action='store_true', help='Explicitly retry a paused source; all anchors are rechecked')
    parser.add_argument('--format', choices=('json', 'text'), default='json', help='Offline check/status output format')
    args = parser.parse_args(argv)
    try:
        return execute(load_upload_config(args.config, validate=args.command not in ('check', 'status')), args.command, resume=args.resume, output_format=args.format)
    except KeyboardInterrupt:
        print('Uploader stopped. Pending work remains durable.')
        return 0
    except sqlite3.Error:
        code = 'local_sqlite_failure'
        message = 'Uploader stopped: local SQLite/state failure; progress not reported as acknowledged.'
    except Exception:
        # Never emit exception text that could contain a token, URL or response body.
        code = 'local_validation_failed'
        message = 'Uploader stopped: configuration, binding, state, or local data validation failed. Inspect locally.'
    if args.command in ('check', 'status') and args.format == 'json':
        print(json.dumps(dict(status_version=2,state='unavailable',configuration_valid=None,
                              error=code,current_reachability='not_checked')))
    else:
        print(message)
    return 2


if __name__ == '__main__':
    raise SystemExit(main())
