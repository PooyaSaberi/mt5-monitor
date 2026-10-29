"""Inspect or export a collector SQLite database without MT5."""

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path

from collector_storage import SCHEMA_VERSION


def read_database(path):
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
        return {'schema_version': version, 'source': source, 'observations': observations, 'status_events': events}
    finally:
        connection.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('database', type=Path)
    parser.add_argument('--format', choices=('summary', 'json', 'csv'), default='summary')
    parser.add_argument('--csv-table', choices=('observations', 'status_events'), default='observations')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    try:
        data = read_database(args.database)
        if args.format == 'summary':
            content = f"Source: {data['source']['source_id']} | observations: {len(data['observations'])} | status events: {len(data['status_events'])}\n"
        elif args.format == 'json':
            content = json.dumps(data, indent=2) + '\n'
        else:
            import io
            buffer = io.StringIO()
            rows = data[args.csv_table]
            fields = ['fund_id', 'source_id', 'login', 'server', 'currency'] + (
                ['observation_id', 'started_at', 'ended_at', 'balance', 'equity', 'profit',
                 'credit', 'margin', 'margin_free', 'margin_level']
                if args.csv_table == 'observations' else
                ['event_id', 'started_at', 'ended_at', 'status', 'detail'])
            writer = csv.DictWriter(buffer, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                writer.writerow({**data['source'], **row})
            content = buffer.getvalue()
        if args.output:
            args.output.write_text(content, encoding='utf-8')
        else:
            print(content, end='')
        return 0
    except (ValueError, sqlite3.Error, OSError, TypeError) as exc:
        print(f'Inspection failed: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
