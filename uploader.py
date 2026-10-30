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
from upload_contract import require, serialize_request, validate_acknowledgement, event_digest
from upload_http import DeliveryFailure, post
from upload_lock import UploadLock, source_lock_path
from upload_reader import read_delivery
from upload_state import UploadState, binding_for, inspect_state, file_identity


def anchors_for(progress, request=None):
    anchors = {}
    if progress['anchor']:
        anchor = progress['anchor']
        require(anchor['sequence'] == progress['acknowledged_sequence'], 'Progress anchor differs')
        anchors[anchor['sequence']] = anchor['digest']
    else:
        require(progress['acknowledged_sequence'] == 0, 'Missing progress anchor')
    if request:
        require(int(request['first_sequence']) > progress['acknowledged_sequence'], 'Pending range overlaps acknowledged progress')
        anchors.update({int(e['sequence']): event_digest(e) for e in request['events']})
    return anchors


def snapshot(config, binding, progress, request=None, *, metadata_only=False, limit=20):
    require(file_identity(config.collector_db) == binding['file_identity'], 'Collector database file replaced')
    if request:
        require(request['source'] == binding['source'] and request['stream_id'] == binding['stream_id'], 'Pending identity differs')
    data = read_delivery(config.collector_db, after_sequence=progress['acknowledged_sequence'],
                         expected_source=config.expected_source, expected_stream=binding['stream_id'],
                         anchors=anchors_for(progress, request), metadata_only=metadata_only,
                         limit=len(request['events']) if request else limit)
    require(file_identity(config.collector_db) == binding['file_identity'], 'Collector database file replaced during read')
    require(data['head_sequence'] >= progress['acknowledged_sequence'], 'Collector database rolled back')
    if request and not metadata_only:
        pending_sequences = [e['sequence'] for e in request['events']]
        require([e['sequence'] for e in data['events'][:len(pending_sequences)]] == pending_sequences,
                'Pending request no longer covers the next undelivered events')
    return data


class Uploader:
    def __init__(self, config, state, *, send=post, clock=time.time, jitter=random.random):
        self.config, self.state, self.send = config, state, send
        self.clock, self.jitter = clock, jitter
        self.read_limit = 20

    def step(self):
        progress = self.state.progress
        if progress['paused']:
            return 'paused'
        if self.clock() < progress['next_retry']:
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
        return 'acknowledged'

    def _retry(self, code, retry_after=0.):
        attempts = self.state.progress['attempts']
        cap = min(self.config.retry_max, self.config.retry_base * 2**min(attempts, 16))
        delay = max(min(retry_after, 300.), cap * (.5 + .5*self.jitter()))
        self.state.failure(code, due=self.clock()+delay)


def status(config):
    """Offline, read-only and no ingestion, locks, state creation or migrations."""
    view = inspect_state(config.state_db)
    if view is None:
        data = read_delivery(config.collector_db, expected_source=config.expected_source, metadata_only=True)
        return dict(state='not_initialized', backlog=data['pending_count'], pending=None, last_ack=None, error=None)
    p = view['progress']
    error = p['error']
    try:
        require(view['binding'] == binding_for(config, view['binding']['stream_id']), 'Uploader binding changed')
        data = snapshot(config, view['binding'], p, view['pending_request'], metadata_only=True)
        backlog = data['pending_count']
        health = 'paused' if p['paused'] else 'ready'
    except (ValueError, OSError, sqlite3.Error, KeyError, TypeError):
        backlog, health, error = None, 'inconsistent', 'collector_binding_or_anchor_inconsistent'
    pending = view['pending']
    return dict(state=health, backlog=backlog,
                pending=None if pending is None else dict(batch_id=pending[0], first_sequence=str(pending[1]),
                    last_sequence=str(pending[2]), bytes=pending[4]),
                acknowledged_sequence=str(p['acknowledged_sequence']), last_ack=p['last_ack'],
                error=error, next_retry=p['next_retry'], attempts=p['attempts'])


def execute(config, command, *, stop=None, output=print, send=post, resume=False):
    config.validate()
    if command in ('check', 'status'):
        require(not resume, 'Resume requires once or run')
        view = status(config)
        output(json.dumps(view, sort_keys=True))
        return 2 if command == 'check' and view['state'] in ('paused', 'inconsistent') else 0
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
            if command == 'once':
                return 0 if outcome in ('acknowledged', 'idle') else 1
            # One request per iteration. No sleeps hold a database transaction.
            due = state.progress['next_retry']
            if outcome in ('backoff', 'retrying'):
                delay = max(.1, due-time.time())
            else:
                delay = .1 if outcome == 'acknowledged' else config.poll_seconds
            stop.wait(min(delay, 300.))
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('check', 'once', 'run', 'status'))
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--resume', action='store_true', help='Explicitly retry a paused source; all anchors are rechecked')
    args = parser.parse_args(argv)
    try:
        return execute(load_upload_config(args.config), args.command, resume=args.resume)
    except KeyboardInterrupt:
        print('Uploader stopped. Pending work remains durable.')
        return 0
    except sqlite3.Error:
        print('Uploader stopped: local SQLite/state failure; progress not reported as acknowledged.')
    except Exception:
        # Never emit exception text that could contain a token, URL or response body.
        print('Uploader stopped: configuration, binding, state, or local data validation failed. Inspect locally.')
    return 2


if __name__ == '__main__':
    raise SystemExit(main())
