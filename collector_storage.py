"""Versioned SQLite store for account observations and status events."""

import json
import sqlite3
from pathlib import Path
from uuid import uuid4

from mt5_monitor import DiagnosticError

SCHEMA_VERSION = 2


class StorageError(DiagnosticError):
    pass


class Storage:
    def __init__(self, path: Path, identity: tuple[str, str, int, str, str]):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=5)
        try:
            self.connection.execute('PRAGMA busy_timeout=5000')
            version = self.connection.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1, SCHEMA_VERSION):
                raise StorageError(f'Unsupported database schema version {version}; expected {SCHEMA_VERSION}')
            with self.connection:
                if version == 0:
                    if self.connection.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0]:
                        raise StorageError('Existing unversioned database is not a collector database')
                    self.connection.execute('CREATE TABLE source (fund_id TEXT NOT NULL, source_id TEXT NOT NULL, login INTEGER NOT NULL, server TEXT NOT NULL, currency TEXT NOT NULL)')
                    self.connection.execute('CREATE TABLE observations (observation_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, ended_at TEXT NOT NULL, balance REAL NOT NULL, equity REAL NOT NULL, profit REAL NOT NULL, credit REAL NOT NULL, margin REAL NOT NULL, margin_free REAL NOT NULL, margin_level REAL NOT NULL)')
                    self.connection.execute('CREATE TABLE status_events (event_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, ended_at TEXT NOT NULL, status TEXT NOT NULL, detail TEXT)')
                    self.connection.execute('INSERT INTO source VALUES (?,?,?,?,?)', identity)
                    self.connection.execute('PRAGMA user_version=1')
            rows = self.connection.execute('SELECT fund_id, source_id, login, server, currency FROM source').fetchall()
            if rows != [identity]:
                raise StorageError('Database source/account/server/currency identity does not match configuration')
            if version in (0, 1):
                with self.connection:
                    self.connection.execute('ALTER TABLE observations ADD COLUMN cycle_id TEXT')
                    self.connection.execute('CREATE TABLE cycles (cycle_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, ended_at TEXT NOT NULL)')
                    self.connection.execute('CREATE TABLE active_batches (batch_id TEXT PRIMARY KEY, cycle_id TEXT NOT NULL REFERENCES cycles(cycle_id), dataset TEXT NOT NULL CHECK(dataset IN (\'positions\',\'orders\')), fund_id TEXT NOT NULL, source_id TEXT NOT NULL, login INTEGER NOT NULL, server TEXT NOT NULL, currency TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT NOT NULL, outcome TEXT NOT NULL CHECK(outcome IN (\'success\',\'failed\',\'identity_mismatch\')), item_count INTEGER, detail TEXT, UNIQUE(cycle_id,dataset))')
                    self.connection.execute('CREATE TABLE active_records (batch_id TEXT NOT NULL REFERENCES active_batches(batch_id), ticket TEXT NOT NULL, payload_json TEXT NOT NULL, PRIMARY KEY(batch_id,ticket))')
                    self.connection.execute(f'PRAGMA user_version={SCHEMA_VERSION}')
            self.identity = identity
        except Exception:
            self.connection.close()
            raise

    def save_observation(self, observation):
        observation_id = str(uuid4())
        with self.connection:
            self.connection.execute('INSERT INTO observations (observation_id,started_at,ended_at,balance,equity,profit,credit,margin,margin_free,margin_level) VALUES (?,?,?,?,?,?,?,?,?,?)',
                (observation_id, observation.started_at, observation.ended_at,
                 observation.balance, observation.equity, observation.profit,
                 observation.credit, observation.margin, observation.margin_free,
                 observation.margin_level))
            self.connection.execute('INSERT INTO status_events VALUES (?,?,?,?,?)',
                (str(uuid4()), observation.started_at, observation.ended_at, 'saved', None))
        return observation_id

    def save_status(self, started_at, ended_at, status, detail=None):
        with self.connection:
            self.connection.execute('INSERT INTO status_events VALUES (?,?,?,?,?)',
                (str(uuid4()), started_at, ended_at, status, detail))

    def save_cycle(self, account, cycle_started, cycle_ended, batches):
        cycle_id = str(uuid4())
        observation_id = str(uuid4())
        with self.connection:
            self.connection.execute('INSERT INTO cycles VALUES (?,?,?)',
                                    (cycle_id, cycle_started, cycle_ended))
            self.connection.execute('INSERT INTO observations VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (observation_id, account.started_at, account.ended_at,
                 account.balance, account.equity, account.profit, account.credit,
                 account.margin, account.margin_free, account.margin_level, cycle_id))
            self.connection.execute('INSERT INTO status_events VALUES (?,?,?,?,?)',
                (str(uuid4()), account.started_at, account.ended_at, 'saved', None))
            for batch in batches:
                batch_id = str(uuid4())
                records = batch['records'] if batch['outcome'] == 'success' else ()
                self.connection.execute('INSERT INTO active_batches VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (batch_id, cycle_id, batch['dataset'], *self.identity,
                     batch['started_at'], batch['ended_at'], batch['outcome'],
                     len(records) if batch['outcome'] == 'success' else None,
                     batch.get('detail')))
                for row in records:
                    self.connection.execute('INSERT INTO active_records VALUES (?,?,?)',
                        (batch_id, str(row['ticket']), json.dumps(row, allow_nan=False, separators=(',', ':'))))
                if batch['outcome'] != 'success':
                    self.connection.execute('INSERT INTO status_events VALUES (?,?,?,?,?)',
                        (str(uuid4()), batch['started_at'], batch['ended_at'],
                         batch['dataset'] + '_' + batch['outcome'], batch.get('detail')))
        return cycle_id

    def close(self):
        self.connection.close()
