"""Version 1 allowlisted ingestion contract; no I/O, MT5, or dashboard imports."""

import hashlib
import json
import math
import re
from datetime import datetime, timezone

CONTRACT_VERSION = 1
ACCOUNT_FIELDS = ('balance', 'equity', 'profit', 'credit', 'margin', 'margin_free', 'margin_level')
IDENTIFIERS = {'ticket', 'identifier', 'position_id', 'position_by_id'}
POSITION_NUMBERS = {'volume', 'price_open', 'sl', 'tp', 'price_current', 'profit', 'swap'}
ORDER_NUMBERS = {'volume_initial', 'volume_current', 'price_open', 'sl', 'tp', 'price_current', 'price_stoplimit'}
RAW_TIMES = {
    'positions': {'time', 'time_msc', 'time_update', 'time_update_msc'},
    'orders': {'time_setup', 'time_setup_msc', 'time_done', 'time_done_msc', 'time_expiration'},
}
STATUSES = {'saved', 'retrying', 'stopped_identity_mismatch', 'unknown'} | {
    dataset + '_' + outcome for dataset in ('positions', 'orders')
    for outcome in ('failed', 'identity_mismatch')}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def shape(value, required, optional=()):
    require(isinstance(value, dict), 'Expected an object')
    require(set(required) <= value.keys() <= set(required) | set(optional), 'Missing or disallowed contract fields')


def text(value):
    require(isinstance(value, str) and 0 < len(value) <= 256 and
            not any(ord(c) < 32 for c in value), 'Invalid contract string')


def identifier(value, positive=False):
    require(isinstance(value, str) and re.fullmatch(r'0|[1-9][0-9]*', value) is not None,
            'Identifiers must be exact decimal strings')
    require(len(value) <= 32 and (not positive or int(value) > 0), 'Invalid identifier range')


def number(value):
    require(type(value) in (int, float), 'Expected a finite financial number')
    try:
        valid = math.isfinite(value)
    except OverflowError:
        valid = False
    require(valid, 'Expected a finite financial number')


def utc(value):
    require(isinstance(value, str), 'Expected a UTC timestamp')
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        require(result.utcoffset() is not None and result.utcoffset().total_seconds() == 0,
                'Timestamp must have an explicit UTC offset')
    except (TypeError, ValueError) as exc:
        raise ValueError('Invalid UTC timestamp') from exc
    return result


def interval(value):
    require(utc(value['started_at_utc']) <= utc(value['ended_at_utc']), 'Reversed collection interval')


def as_utc(value):
    return utc(value).astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def source(value):
    shape(value, {'fund_id', 'source_id', 'currency'})
    for item in value.values():
        text(item)


def active_record(value, dataset):
    numbers = POSITION_NUMBERS if dataset == 'positions' else ORDER_NUMBERS
    ids = {'ticket', 'identifier'} if dataset == 'positions' else {'ticket', 'position_id', 'position_by_id'}
    shape(value, {'symbol', 'type', 'broker_event_times'} | numbers | ids)
    text(value['symbol'])
    require(type(value['type']) is int, 'Invalid operation type')
    for key in ids & value.keys():
        identifier(value[key])
    for key in numbers & value.keys():
        number(value[key])
    times = value['broker_event_times']
    shape(times, {'status', 'raw'})
    require(times['status'] == 'unverified', 'Broker event mapping must remain unverified')
    shape(times['raw'], RAW_TIMES[dataset])
    for item in times['raw'].values():
        identifier(item)


def serialize_active_record(raw, dataset):
    """Drop everything outside the allowlist, including nested private metadata."""
    require(isinstance(raw, dict), 'Invalid stored active record')
    require(isinstance(dataset, str) and dataset in RAW_TIMES, 'Unknown dataset')
    numbers = POSITION_NUMBERS if dataset == 'positions' else ORDER_NUMBERS
    ids = {'ticket', 'identifier'} if dataset == 'positions' else {'ticket', 'position_id', 'position_by_id'}
    result = {key: raw[key] for key in {'symbol', 'type'} if key in raw}
    for key in ids & raw.keys():
        value = raw[key]
        require(type(value) in (int, str), 'Lossy identifier in stored record')
        result[key] = str(value)
    for key in numbers & raw.keys():
        number(raw[key])
        result[key] = float(raw[key])
    times = {}
    for key in RAW_TIMES[dataset] & raw.keys():
        require(type(raw[key]) in (int, str), 'Invalid raw broker time')
        times[key] = str(raw[key])
    result['broker_event_times'] = {'status': 'unverified', 'raw': times}
    active_record(result, dataset)
    return result


def validate_event(event):
    common = {'sequence', 'kind', 'record_id', 'started_at_utc', 'ended_at_utc'}
    require(isinstance(event, dict), 'Expected event')
    kind = event.get('kind')
    if kind == 'collection_status':
        shape(event, common | {'status'})
        require(isinstance(event['status'], str) and event['status'] in STATUSES, 'Invalid sanitized status')
    else:
        require(kind == 'account_observation', 'Unknown event kind')
        shape(event, common | {'cycle', 'account', 'active_batches', 'absent_datasets'})
        shape(event['account'], set(ACCOUNT_FIELDS))
        for value in event['account'].values():
            number(value)
        cycle = event['cycle']
        if cycle is not None:
            shape(cycle, {'cycle_id', 'started_at_utc', 'ended_at_utc'})
            text(cycle['cycle_id'])
            interval(cycle)
        require(isinstance(event['active_batches'], list), 'Expected active batches')
        seen = set()
        batch_ids = set()
        for batch in event['active_batches']:
            shape(batch, {'batch_id', 'dataset', 'started_at_utc', 'ended_at_utc', 'outcome', 'item_count'},
                  {'records', 'error_code'})
            dataset = batch['dataset']
            require(isinstance(dataset, str) and dataset in RAW_TIMES and dataset not in seen, 'Invalid or duplicate dataset')
            seen.add(dataset)
            text(batch['batch_id'])
            require(batch['batch_id'] not in batch_ids, 'Duplicate batch identifier')
            batch_ids.add(batch['batch_id'])
            interval(batch)
            if batch['outcome'] == 'success':
                require('error_code' not in batch and isinstance(batch.get('records'), list), 'Invalid success batch')
                require(type(batch['item_count']) is int and batch['item_count'] == len(batch['records']), 'Invalid item count')
                tickets = set()
                for record in batch['records']:
                    active_record(record, dataset)
                    require(record['ticket'] not in tickets, 'Duplicate ticket')
                    tickets.add(record['ticket'])
            else:
                require(batch['outcome'] in ('failed', 'identity_mismatch') and batch['item_count'] is None
                        and 'records' not in batch and batch.get('error_code') == batch['outcome'], 'Invalid failure batch')
        require(event['absent_datasets'] == sorted(set(RAW_TIMES) - seen), 'Invalid absent dataset list')
        require(cycle is not None or not seen, 'Legacy observation cannot have active batches')
    identifier(event['sequence'], positive=True)
    text(event['record_id'])
    interval(event)
    return event


def validate_request(request):
    shape(request, {'contract_version', 'batch_id', 'source', 'stream_id', 'first_sequence', 'last_sequence', 'events'})
    require(type(request['contract_version']) is int and request['contract_version'] == CONTRACT_VERSION, 'Unsupported contract version')
    text(request['batch_id'])
    text(request['stream_id'])
    source(request['source'])
    require(isinstance(request['events'], list) and 1 <= len(request['events']) <= 20, 'Expected 1..20 events')
    sequences = [int(validate_event(e)['sequence']) for e in request['events']]
    require(all(a < b for a, b in zip(sequences, sequences[1:])), 'Events must be strictly ordered')
    require(len({(e['kind'], e['record_id']) for e in request['events']}) == len(sequences), 'Duplicate event')
    identifier(request['first_sequence'], positive=True)
    identifier(request['last_sequence'], positive=True)
    require(request['first_sequence'] == str(sequences[0]) and request['last_sequence'] == str(sequences[-1]), 'Sequence range differs from events')
    return request


def serialize_request(request):
    """Digest the complete canonical v1 request bytes; digest is not authentication."""
    validate_request(request)
    data = json.dumps(request, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(',', ':')).encode('utf-8')
    require(len(data) <= 512 * 1024, 'Request exceeds 512 KiB; never split an active set')
    return data, hashlib.sha256(data).hexdigest()


def event_digest(event):
    """Fingerprint an immutable event, independent of request regrouping."""
    validate_event(event)
    return hashlib.sha256(json.dumps(event, sort_keys=True, ensure_ascii=True,
                                    allow_nan=False, separators=(',', ':')).encode()).hexdigest()


def validate_acknowledgement(ack, request):
    shape(ack, {'contract_version', 'batch_id', 'source_id', 'stream_id', 'accepted_through_sequence',
                'request_sha256', 'result', 'received_at_utc'})
    _, digest = serialize_request(request)
    require(type(ack['contract_version']) is int and ack['contract_version'] == CONTRACT_VERSION
            and ack['result'] == 'committed'
            and ack['batch_id'] == request['batch_id']
            and ack['source_id'] == request['source']['source_id']
            and ack['stream_id'] == request['stream_id']
            and ack['accepted_through_sequence'] == request['last_sequence']
            and ack['request_sha256'] == digest, 'Acknowledgement does not match request')
    utc(ack['received_at_utc'])
    return ack
