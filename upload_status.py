"""Local visibility only: bounded read-only SQLite, no network/MT5 imports."""

from datetime import datetime, timezone
import math
import re
import sqlite3
import time

from upload_contract import as_utc, utc, require, source, text, shape
from upload_integrity import snapshot
from upload_state import inspect_state, binding_for

ERROR_CODES = {
    'collector_read_failed', 'collector_busy_or_read_budget', 'collector_or_anchor_inconsistent',
    'collector_binding_or_anchor_inconsistent', 'event_invalid_or_oversized',
    'invalid_acknowledgement', 'transport_timeout', 'network_error', 'tls_verification_failed',
    'request_too_large', 'response_too_large', 'redirect_rejected',
}


def safe_error(value):
    if value is None:
        return None
    if isinstance(value, str) and (value in ERROR_CODES or re.fullmatch(r'http_[1-5][0-9]{2}', value)):
        return value
    return 'unknown_error'


def sqlite_condition(exc):
    code = getattr(exc, 'sqlite_errorcode', None)
    if code is not None and code & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
        return 'busy'
    if code == sqlite3.SQLITE_INTERRUPT:
        return 'read_budget_exceeded'
    return 'unreadable'


def age(value, now):
    if value is None:
        return dict(at_utc=None, age_seconds=None, time_state='missing')
    try:
        instant = utc(value)
        seconds = (now-instant).total_seconds()
        return dict(at_utc=as_utc(value), age_seconds=seconds,
                    time_state='future_timestamp' if seconds < 0 else 'recorded')
    except (ValueError, TypeError, OverflowError):
        return dict(at_utc=None, age_seconds=None, time_state='invalid_timestamp')


def collector_metrics(path, cursor):
    """Only identities/timestamps/counts. Never reads financial sets for metrics.

    COUNT and oldest/latest timestamp queries can visit index/table rows; a
    cooperative one-second SQLite VM budget bounds work. No schema/index changes.
    """
    result = dict(schema_version=None, source=None, stream_id=None, backlog=None,
                  oldest=None, latest=None, error=None)
    if not path.is_file():
        result['error'] = 'missing'
        return result
    db = None
    try:
        db = sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=.1)
        deadline = time.monotonic()+1.
        db.set_progress_handler(lambda: int(time.monotonic()>deadline), 1000)
        db.execute('BEGIN')
        version = db.execute('PRAGMA user_version').fetchone()[0]
        result['schema_version'] = version
        if version not in (1, 2, 3, 4):
            result['error'] = 'unsupported_schema'
            return result
        rows = db.execute('SELECT fund_id,source_id,currency FROM source LIMIT 2').fetchall()
        require(len(rows) == 1, 'Invalid source')
        result['source'] = dict(zip(('fund_id','source_id','currency'), rows[0]))
        source(result['source'])
        latest = db.execute('SELECT ended_at FROM observations ORDER BY julianday(ended_at) DESC,ended_at DESC LIMIT 1').fetchone()
        result['latest'] = latest[0] if latest else None
        if version != 4:
            result['error'] = 'requires_v4'
            return result
        stream = db.execute('SELECT stream_id FROM delivery_stream WHERE singleton=1').fetchone()
        require(stream is not None and isinstance(stream[0], str), 'Missing stream')
        text(stream[0])
        result['stream_id'] = stream[0]
        if cursor is not None:
            result['backlog'] = db.execute('SELECT count(*) FROM delivery_events WHERE sequence>?',(cursor,)).fetchone()[0]
            # Collection-time oldest, not necessarily the lowest sequence: the
            # PC clock can step backwards, and migration order is transport order.
            oldest = db.execute("""SELECT coalesce(o.started_at,s.started_at) AS stamp
                FROM delivery_events e
                LEFT JOIN observations o ON e.kind='account_observation' AND o.observation_id=e.record_id
                LEFT JOIN status_events s ON e.kind='collection_status' AND s.event_id=e.record_id
                WHERE e.sequence>? ORDER BY julianday(stamp),e.sequence LIMIT 1""",(cursor,)).fetchone()
            result['oldest'] = oldest[0] if oldest else None
    except sqlite3.Error as exc:
        result['backlog'], result['oldest'] = None, None
        result['error'] = sqlite_condition(exc)
    except (OSError, ValueError, TypeError):
        result['backlog'], result['oldest'], result['error'] = None, None, 'unreadable'
    finally:
        if db is not None:
            db.close()
    return result


def valid_progress(progress):
    cursor = progress['acknowledged_sequence']
    require(type(cursor) is int and 0 <= cursor <= 2**63-1, 'Invalid cursor')
    require(type(progress['paused']) is bool, 'Invalid pause')
    require(type(progress['attempts']) is int and progress['attempts'] >= 0, 'Invalid attempts')
    retry = progress['next_retry']
    require(type(retry) in (int,float) and math.isfinite(retry) and 0 <= retry <= 253402300799,
            'Invalid retry time')
    return cursor


def ack_summary(view, configured_source):
    """Allowlist historical ack metadata; never print a persisted dict verbatim."""
    p = view['progress']
    ack = p.get('last_ack')
    if ack is None:
        require(p['acknowledged_sequence'] == 0, 'Missing acknowledgement')
        return None
    shape(ack, {'contract_version','batch_id','source_id','stream_id','accepted_through_sequence',
                'request_sha256','result','received_at_utc'})
    text(ack['batch_id'])
    require(isinstance(ack['request_sha256'],str) and re.fullmatch(r'[0-9a-f]{64}',ack['request_sha256']),
            'Invalid acknowledgement digest')
    require(type(ack.get('contract_version')) is int and ack['contract_version'] == 1
            and ack.get('result') == 'committed'
            and ack.get('source_id') == configured_source['source_id']
            and ack.get('stream_id') == view['binding']['stream_id']
            and ack.get('accepted_through_sequence') == str(p['acknowledged_sequence']), 'Invalid saved acknowledgement')
    return dict(received_at_utc=as_utc(ack['received_at_utc']),
                accepted_through_sequence=ack['accepted_through_sequence'])


def status(config, *, now=None, stale_after_seconds=30.):
    """Offline report; old top-level fields remain as sanitized compatibility aliases."""
    now = now or datetime.now(timezone.utc)
    require(now.utcoffset() is not None, 'Status clock must be aware')
    now = now.astimezone(timezone.utc)
    require(type(stale_after_seconds) in (int,float) and math.isfinite(stale_after_seconds)
            and stale_after_seconds > 0, 'Invalid freshness threshold')
    report = dict(status_version=2, observed_at_utc=as_utc(now.isoformat()), source=None, stream_id=None,
                  configuration_valid=False, binding_valid=None, binding_state='not_checked', state='invalid_configuration',
                  schema_version=None, clock=dict(basis='local_pc_utc', accuracy='unverified', ages_clock_dependent=True))
    p = dict(acknowledged_sequence=0,paused=False,attempts=0,next_retry=0.,error=None,last_ack=None)
    view, ack, state_error = None, None, None
    try:
        config.validate()
        report['configuration_valid'] = True
        report['source'] = dict(config.expected_source)
    except (ValueError, OSError, TypeError, AttributeError):
        state_error = 'invalid_configuration'
    if report['configuration_valid']:
        try:
            view = inspect_state(config.state_db)
            if view:
                p = view['progress']
                valid_progress(p)
                ack = ack_summary(view, config.expected_source)
        except sqlite3.Error as exc:
            state_error = 'state_'+sqlite_condition(exc)
        except (ValueError, OSError, TypeError, KeyError, AttributeError):
            state_error = 'state_invalid'
    cursor = p.get('acknowledged_sequence') if state_error is None else None
    data = collector_metrics(config.collector_db, cursor) if report['configuration_valid'] else dict(
        schema_version=None,stream_id=None,source=None,backlog=None,oldest=None,latest=None,error=None)
    report.update(schema_version=data['schema_version'], stream_id=data['stream_id'])
    error = state_error or ('collector_'+data['error'] if data['error'] else safe_error(p.get('error')))
    backlog, oldest = data['backlog'], data['oldest']
    if state_error:
        report['state'] = state_error
    elif data['error']:
        report['state'] = data['error'] if data['error']=='requires_v4' else 'collector_'+data['error']
        report['binding_state'] = 'requires_v4' if data['error']=='requires_v4' else 'not_verified'
    elif data['source'] != config.expected_source:
        report.update(state='inconsistent', binding_valid=False, binding_state='source_mismatch')
        error, backlog, oldest = 'collector_binding_or_anchor_inconsistent', None, None
    elif view is None:
        report.update(state='not_initialized',binding_state='not_initialized')
    else:
        try:
            require(view['binding'] == binding_for(config,view['binding']['stream_id']), 'Binding differs')
            snapshot(config,view['binding'],p,view['pending_request'],metadata_only=True)
            report.update(state='paused' if p['paused'] else 'ready', binding_valid=True,binding_state='verified')
        except sqlite3.Error as exc:
            condition = sqlite_condition(exc)
            report.update(state='collector_'+condition,binding_state='not_verified')
            error, backlog, oldest = 'collector_'+condition, None, None
        except (ValueError, OSError, TypeError, KeyError):
            report.update(state='inconsistent',binding_valid=False,binding_state='inconsistent')
            error, backlog, oldest = 'collector_binding_or_anchor_inconsistent', None, None
    latest = age(data['latest'],now)
    latest['freshness'] = ('unknown' if latest['time_state'] in ('future_timestamp','invalid_timestamp')
                           else 'missing' if latest['age_seconds'] is None
                           else 'stale' if latest['age_seconds'] > stale_after_seconds else 'fresh')
    latest['stale_after_seconds'] = stale_after_seconds
    latest['source_matches_configuration'] = data['source'] == config.expected_source if data['source'] else None
    if latest['source_matches_configuration'] is False:
        latest['freshness'] = 'unknown'
    if (not report['configuration_valid'] or data['error'] not in (None, 'requires_v4')) and data['latest'] is None:
        latest.update(time_state='unavailable',freshness='unknown')
    pending = view.get('pending') if view and state_error is None else None
    pending_summary = None if pending is None else dict(batch_id=pending[0],first_sequence=str(pending[1]),
                                                        last_sequence=str(pending[2]),bytes=pending[4])
    retry = p.get('next_retry',0) if state_error is None else 0
    retry_at = as_utc(datetime.fromtimestamp(retry,timezone.utc).isoformat()) if retry else None
    sequence = str(p['acknowledged_sequence']) if state_error is None else None
    local_ack = age(p.get('last_acknowledged_at_utc') if ack else None,now)
    oldest_age = age(oldest,now)
    if backlog is None:
        oldest_age['time_state'] = 'unavailable'
    report['collection'] = latest
    delivery_state = 'backoff' if report['state']=='ready' and retry > now.timestamp() else report['state']
    report['delivery'] = dict(state=delivery_state, unacknowledged_events=backlog,
        oldest_unacknowledged_collection=oldest_age,total_unsent_serialized_bytes=None,
        unsent_bytes_state='unknown',pending_request=pending_summary,acknowledged_sequence=sequence,
        paused=p['paused'] if state_error is None else None,next_retry_at_utc=retry_at,
        retry_in_seconds=max(0.,retry-now.timestamp()) if retry else None,
        attempts=p.get('attempts') if state_error is None else None,last_error=error)
    report['receiver'] = dict(last_successful_acknowledgement=local_ack,
        local_ack_time_available=local_ack['at_utc'] is not None,
        receiver_receipt_at_utc=ack['received_at_utc'] if ack else None,
        receiver_receipt_vs_pc=age(ack['received_at_utc'] if ack else None,now)['time_state'],
        receipt_clock_accuracy='unverified',
        acknowledged_sequence=sequence if ack else None,current_reachability='not_checked')
    # Preserve prior machine consumers without exposing arbitrary persisted text.
    report.update(backlog=backlog,pending=pending_summary,acknowledged_sequence=sequence,
                  last_ack=ack,error=error,next_retry=retry,attempts=report['delivery']['attempts'])
    return report


def format_console(report):
    src = report['source'] or {}
    collection, delivery, receiver = report['collection'],report['delivery'],report['receiver']
    def show(value):
        return 'unknown' if value is None else str(value)
    oldest = delivery['oldest_unacknowledged_collection']
    pending_text = (str(delivery['pending_request']['bytes'])+' bytes' if delivery['pending_request'] else
                    'none' if report['state'] in ('ready','paused','not_initialized') else 'unknown')
    return '\n'.join([
        f"Source: {src.get('fund_id','unknown')}/{src.get('source_id','unknown')} | stream: {show(report['stream_id'])}",
        f"Configuration: {'valid' if report['configuration_valid'] else 'invalid'} | binding: {report['binding_state']}",
        f"Collection: {collection['freshness']} | latest: {show(collection['at_utc'])} | age: {show(collection['age_seconds'])}s ({collection['time_state']})",
        f"Delivery: {delivery['state']} | backlog: {show(delivery['unacknowledged_events'])} events | oldest: {show(oldest['at_utc'])} | age: {show(oldest['age_seconds'])}s ({oldest['time_state']})",
        f"Pending: {pending_text} | total unsent bytes: unknown | retry: {show(delivery['next_retry_at_utc'])} | paused: {show(delivery['paused'])} | error: {delivery['last_error'] or 'none'}",
        f"Receiver: ack sequence {show(receiver['acknowledged_sequence'])} | local ack: {show(receiver['last_successful_acknowledgement']['at_utc'])} | receipt: {show(receiver['receiver_receipt_at_utc'])} | reachability: not checked",
        'Ages depend on the unverified PC clock; past acknowledgements do not prove current backend reachability.',
    ])
