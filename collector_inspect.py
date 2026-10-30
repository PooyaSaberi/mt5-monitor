"""Inspect or export a collector SQLite database without MT5."""

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path

from collector_storage import SCHEMA_VERSION
from mt5_monitor import DiagnosticError
from report_time import TimeMapping, REPORT_ZONE, decorate_row, report_day_plan, aware_utc

HISTORY_TABLES = ('deals', 'deal_revisions', 'history_queries', 'history_query_members',
                  'history_jobs', 'history_state', 'history_discrepancies')


def safe_export(value):
    """Keep identifiers exact in JSON consumers as well as in CSV."""
    if isinstance(value, list):
        return [safe_export(item) for item in value]
    if isinstance(value, dict):
        return {key: str(item) if type(item) is int and
                (key in ('ticket', 'order', 'magic', 'identifier', 'login') or key.endswith(('_id', '_ticket')))
                else safe_export(item) for key, item in value.items()}
    return value


def read_history(path, table, start=None, end=None, limit=1000, mapping=None, report_date=None):
    from deal_reader import parse_utc
    mapping = mapping or TimeMapping()
    mapping.validate()
    if table not in HISTORY_TABLES or not 1 <= limit <= 100000:
        raise ValueError('Invalid history table or limit (1..100000)')
    lower = parse_utc(start) if start else None
    upper = parse_utc(end) if end else None
    if lower and upper and lower >= upper:
        raise ValueError('Invalid inspection time range')
    day_plan = None
    if report_date:
        if start or end or table not in ('deals', 'deal_revisions'):
            raise ValueError('Tehran broker-day filtering is for deals/revisions and cannot be combined with raw UTC range arguments')
        if mapping.event_basis == 'unverified':
            raise ValueError('Tehran broker-day filtering requires a verified event mapping; raw exports remain available')
        from dataclasses import replace
        day_plan = report_day_plan(report_date, replace(mapping, query_basis=mapping.event_basis))
        if day_plan['query_status'] != 'verified':
            raise ValueError('Report day is outside the verified event-mapping period')
    if not path.is_file():
        raise ValueError('Database does not exist')
    db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        if db.execute('PRAGMA user_version').fetchone()[0] != SCHEMA_VERSION:
            raise ValueError('Unsupported database schema version')
        source = dict(db.execute('SELECT * FROM source').fetchone())
        sql, clauses, params = f'SELECT t.* FROM {table} t', [], []
        if day_plan:
            ranges = []
            for segment in day_plan['query_segments']:
                ranges.append('(t.time_msc>=? AND t.time_msc<?)')
                params.extend((segment['argument_start_epoch'] * 1000, segment['argument_end_exclusive_epoch'] * 1000))
            clauses.append('(' + ' OR '.join(ranges) + ')')
        if table == 'history_query_members':
            sql += ' JOIN history_queries q ON t.query_id=q.query_id'
        for bound, operator in ((lower, '>='), (upper, '<')):
            if bound is None:
                continue
            if table in ('deals', 'deal_revisions'):
                column, value = 't.time_msc', int(bound.timestamp()) * 1000
            elif table in ('history_queries', 'history_jobs', 'history_query_members'):
                column = 'q.range_start' if table == 'history_query_members' else 't.range_start'
                value = int(bound.timestamp())
            else:
                column = 't.start_utc' if table == 'history_state' else 't.observed_at'
                value = bound.isoformat()
            clauses.append(f'{column}{operator}?')
            params.append(value)
        if clauses:
            sql += ' WHERE ' + ' AND '.join(clauses)
        rows = [dict(row) for row in db.execute(sql + ' ORDER BY t.rowid LIMIT ?', (*params, limit))]
        for row in rows:
            if 'payload_json' in row:
                row['fields'] = json.loads(row.pop('payload_json'))
            for key in ('range_start', 'range_end', 'cursor'):
                if key in row:
                    from datetime import datetime, timezone
                    from dataclasses import replace
                    row[key + '_request_argument'] = datetime.fromtimestamp(row[key], timezone.utc).isoformat()
                    decoded = replace(mapping, event_basis=mapping.query_basis).event(row[key] * 1000)
                    row[key + '_utc'] = decoded['event_utc']
                    row[key + '_tehran'] = decoded['event_tehran']
                    row[key + '_mapping_status'] = decoded['status']
            if table == 'history_state':
                from report_time import tehran
                row['configured_start_tehran'] = tehran(aware_utc(row['start_utc'])).isoformat()
        rows = [decorate_row(row, mapping) for row in rows]
        if day_plan:
            accepted = []
            for row in rows:
                event = row['broker_event_times']['time_msc']
                if event['status'] != 'verified':
                    row['report_day_membership'] = 'unverified'
                    accepted.append(row)
                elif day_plan['start_utc'] <= event['event_utc'] < day_plan['end_utc_exclusive']:
                    row['report_day_membership'] = 'verified'
                    accepted.append(row)
            rows = accepted
        return safe_export({'schema_version': SCHEMA_VERSION, 'source': source,
                            'report_timezone': REPORT_ZONE, 'event_time_basis': mapping.event_basis,
                            'time_filter_basis': 'Tehran_civil_day' if report_date else 'raw_epoch_literal',
                            'report_day': report_date,
                            'report_status': ('unverified' if any(row.get('report_day_membership') == 'unverified' for row in rows) else 'verified') if report_date else 'not_a_daily_report',
                            table: rows})
    finally:
        db.close()


def history_main(args):
    data = read_history(args.database, args.history_table, args.from_utc, args.to_utc, args.limit,
                        TimeMapping.load(args.time_mapping), args.report_date)
    rows = data[args.history_table]
    if args.format == 'json':
        content = json.dumps(data, indent=2) + '\n'
    elif args.format == 'summary':
        content = f'{args.history_table}: {len(rows)} rows (limit {args.limit}); timezone {REPORT_ZONE}; {data["report_status"]}\n'
    else:
        import io
        buffer = io.StringIO()
        rows = [{**data['source'], **row} for row in rows]
        fields = list(dict.fromkeys(list(data['source']) + [key for row in rows for key in row]))
        writer = csv.DictWriter(buffer, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, separators=(',', ':')) if isinstance(value, dict) else value for key, value in row.items()})
        content = buffer.getvalue()
    if args.output:
        args.output.write_text(content, encoding='utf-8')
    else:
        print(content, end='')
    return 0


def read_database(path, mapping=None):
    if not path.is_file():
        raise ValueError('Database does not exist')
    connection = sqlite3.connect(f'file:{path.resolve().as_posix()}?mode=ro', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        version = connection.execute('PRAGMA user_version').fetchone()[0]
        if version != SCHEMA_VERSION:
            raise ValueError(f'Unsupported database schema version {version}')
        source = dict(connection.execute('SELECT * FROM source').fetchone())
        observations = [dict(row) for row in connection.execute('SELECT * FROM observations ORDER BY started_at, observation_id')]
        events = [dict(row) for row in connection.execute('SELECT * FROM status_events ORDER BY started_at, event_id')]
        batches = [dict(row) for row in connection.execute('SELECT * FROM active_batches ORDER BY started_at, batch_id')]
        records = []
        for row in connection.execute('SELECT batch_id, ticket, payload_json FROM active_records ORDER BY batch_id, ticket'):
            records.append({'batch_id': row['batch_id'], 'ticket': row['ticket'],
                            'fields': json.loads(row['payload_json'])})
        mapping = mapping or TimeMapping()
        mapping.validate()
        return {'schema_version': version, 'source': source, 'report_timezone': REPORT_ZONE,
                'observations': [decorate_row(row, mapping) for row in observations],
                'status_events': [decorate_row(row, mapping) for row in events],
                'active_batches': [decorate_row(row, mapping) for row in batches],
                'active_records': [decorate_row(row, mapping) for row in records]}
    finally:
        connection.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('database', type=Path, nargs='?')
    parser.add_argument('--format', choices=('summary', 'json', 'csv'), default='summary')
    parser.add_argument('--csv-table', choices=('observations', 'status_events', 'active_batches', 'active_records'), default='observations')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--history-table', choices=HISTORY_TABLES)
    parser.add_argument('--from-utc')
    parser.add_argument('--to-utc')
    parser.add_argument('--limit', type=int, default=1000)
    parser.add_argument('--time-mapping', type=Path)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--report-date')
    group.add_argument('--query-plan', help='Dry-run Tehran daily boundaries; does not open MT5 or a database')
    args = parser.parse_args(argv)
    try:
        if args.query_plan:
            content = json.dumps(report_day_plan(args.query_plan, TimeMapping.load(args.time_mapping)), indent=2) + '\n'
            if args.output:
                args.output.write_text(content, encoding='utf-8')
            else:
                print(content, end='')
            return 0
        if args.database is None:
            raise ValueError('Database path is required')
        if args.report_date and not args.history_table:
            raise ValueError('--report-date requires --history-table deals or deal_revisions')
        if args.history_table:
            return history_main(args)
        data = read_database(args.database, TimeMapping.load(args.time_mapping))
        if args.format == 'summary':
            outcomes = {}
            for batch in data['active_batches']:
                key = (batch['dataset'], batch['outcome'])
                outcomes[key] = outcomes.get(key, 0) + 1
            content = (f"Source: {data['source']['source_id']} | observations: {len(data['observations'])} | "
                       f"status events: {len(data['status_events'])} | batch outcomes: {outcomes}\n")
        elif args.format == 'json':
            content = json.dumps(safe_export(data), indent=2) + '\n'
        else:
            import io
            buffer = io.StringIO()
            rows = data[args.csv_table]
            if args.csv_table == 'active_records':
                by_id = {batch['batch_id']: batch for batch in data['active_batches']}
                rows = [{**by_id[row['batch_id']], **row['fields'],
                         'batch_id': row['batch_id'], 'ticket': row['ticket']} for row in rows]
            fields = list(dict.fromkeys(
                ['fund_id', 'source_id', 'login', 'server', 'currency'] +
                [key for row in rows for key in row.keys()] +
                (['batch_id', 'cycle_id', 'dataset', 'outcome', 'item_count', 'started_at', 'ended_at']
                 if args.csv_table in ('active_batches', 'active_records') else [])))
            writer = csv.DictWriter(buffer, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: json.dumps(value, separators=(',', ':')) if isinstance(value, dict) else value
                                 for key, value in {**data['source'], **row}.items()})
            content = buffer.getvalue()
        if args.output:
            args.output.write_text(content, encoding='utf-8')
        else:
            print(content, end='')
        return 0
    except (ValueError, sqlite3.Error, OSError, TypeError, DiagnosticError) as exc:
        print(f'Inspection failed: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
