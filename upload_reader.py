"""Bounded, read-only v4 delivery serialization; never imports the MT5 reader."""

import json
import sqlite3
from pathlib import Path

from upload_contract import ACCOUNT_FIELDS, STATUSES, as_utc, require, serialize_active_record, source, validate_event


def read_delivery(path, *, after_sequence=0, limit=20, expected_source=None):
    """One short SQLite snapshot. No migration, ownership lock, or networking.

    expected_source, when supplied, is the public fund_id/source_id/currency
    dictionary. M5.2 must require this binding and separately detect restores.
    """
    require(type(after_sequence) is int and 0 <= after_sequence <= 2**63 - 1, 'Invalid delivery cursor')
    require(type(limit) is int and 1 <= limit <= 20, 'Invalid event limit')
    path = Path(path)
    require(path.is_file(), 'Database does not exist')
    db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=0.1)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        require(db.execute('PRAGMA user_version').fetchone()[0] == 4, 'Delivery requires schema v4; reader never migrates')
        rows = db.execute('SELECT fund_id,source_id,currency FROM source').fetchall()
        require(len(rows) == 1, 'Invalid source binding')
        public_source = dict(rows[0])
        source(public_source)
        require(expected_source is None or public_source == expected_source, 'Delivery source differs from expected binding')
        stream = db.execute('SELECT stream_id FROM delivery_stream WHERE singleton=1').fetchone()
        require(stream is not None, 'Missing delivery stream')
        pending_count = db.execute('SELECT count(*) FROM delivery_events WHERE sequence>?', (after_sequence,)).fetchone()[0]
        entries = db.execute('SELECT * FROM delivery_events WHERE sequence>? ORDER BY sequence LIMIT ?', (after_sequence, limit)).fetchall()
        events = []
        for entry in entries:
            table, key = ('observations', 'observation_id') if entry['kind'] == 'account_observation' else ('status_events', 'event_id')
            row = db.execute(f'SELECT * FROM {table} WHERE {key}=?', (entry['record_id'],)).fetchone()
            require(row is not None, 'Dangling delivery entry')
            event = dict(sequence=str(entry['sequence']), kind=entry['kind'], record_id=entry['record_id'],
                         started_at_utc=as_utc(row['started_at']), ended_at_utc=as_utc(row['ended_at']))
            if entry['kind'] == 'collection_status':
                event['status'] = row['status'] if row['status'] in STATUSES else 'unknown'
            else:
                event.update(account={key: row[key] for key in ACCOUNT_FIELDS}, cycle=None, active_batches=[])
                if row['cycle_id'] is not None:
                    cycle = db.execute('SELECT * FROM cycles WHERE cycle_id=?', (row['cycle_id'],)).fetchone()
                    require(cycle is not None, 'Missing observation cycle')
                    event['cycle'] = dict(cycle_id=cycle['cycle_id'], started_at_utc=as_utc(cycle['started_at']), ended_at_utc=as_utc(cycle['ended_at']))
                    for batch in db.execute('SELECT * FROM active_batches WHERE cycle_id=? ORDER BY dataset', (row['cycle_id'],)):
                        require(all(batch[key] == public_source[key] for key in public_source), 'Batch source binding differs')
                        # Private account identity is checked locally, never exported.
                        require(db.execute('SELECT count(*) FROM source WHERE login=? AND server=?', (batch['login'], batch['server'])).fetchone()[0] == 1, 'Batch account binding differs')
                        result = {key: batch[key] for key in ('batch_id', 'dataset', 'outcome', 'item_count')}
                        result.update(started_at_utc=as_utc(batch['started_at']), ended_at_utc=as_utc(batch['ended_at']))
                        records = db.execute('SELECT ticket,payload_json FROM active_records WHERE batch_id=? ORDER BY ticket', (batch['batch_id'],)).fetchall()
                        if batch['outcome'] == 'success':
                            result['records'] = []
                            for item in records:
                                record = serialize_active_record(json.loads(item['payload_json']), batch['dataset'])
                                require(item['ticket'] == record['ticket'], 'Stored ticket differs from payload')
                                result['records'].append(record)
                        else:
                            require(not records, 'Failure batch contains records')
                            result['error_code'] = batch['outcome']
                        event['active_batches'].append(result)
                event['absent_datasets'] = sorted({'positions', 'orders'} - {b['dataset'] for b in event['active_batches']})
            events.append(validate_event(event))
        return {'source': public_source, 'stream_id': stream['stream_id'], 'events': events, 'pending_count': pending_count}
    finally:
        db.close()
