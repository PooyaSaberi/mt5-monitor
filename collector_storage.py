"""Versioned SQLite store for account observations and status events."""

import sqlite3
from pathlib import Path
from uuid import uuid4

from mt5_monitor import DiagnosticError

SCHEMA_VERSION = 1


class StorageError(DiagnosticError):
    pass


class Storage:
    def __init__(self, path: Path, identity: tuple[str, str, int, str, str]):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=5)
        try:
            self.connection.execute('PRAGMA busy_timeout=5000')
            version = self.connection.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, SCHEMA_VERSION):
                raise StorageError(f'Unsupported database schema version {version}; expected {SCHEMA_VERSION}')
            with self.connection:
                if version == 0:
                    if self.connection.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0]:
                        raise StorageError('Existing unversioned database is not a collector database')
                    self.connection.execute('CREATE TABLE source (fund_id TEXT NOT NULL, source_id TEXT NOT NULL, login INTEGER NOT NULL, server TEXT NOT NULL, currency TEXT NOT NULL)')
                    self.connection.execute('CREATE TABLE observations (observation_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, ended_at TEXT NOT NULL, balance REAL NOT NULL, equity REAL NOT NULL, profit REAL NOT NULL, credit REAL NOT NULL, margin REAL NOT NULL, margin_free REAL NOT NULL, margin_level REAL NOT NULL)')
                    self.connection.execute('CREATE TABLE status_events (event_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, ended_at TEXT NOT NULL, status TEXT NOT NULL, detail TEXT)')
                    self.connection.execute('INSERT INTO source VALUES (?,?,?,?,?)', identity)
                    self.connection.execute(f'PRAGMA user_version={SCHEMA_VERSION}')
            rows = self.connection.execute('SELECT fund_id, source_id, login, server, currency FROM source').fetchall()
            if rows != [identity]:
                raise StorageError('Database source/account/server/currency identity does not match configuration')
        except Exception:
            self.connection.close()
            raise

    def save_observation(self, observation):
        observation_id = str(uuid4())
        with self.connection:
            self.connection.execute('INSERT INTO observations VALUES (?,?,?,?,?,?,?,?,?,?)',
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

    def close(self):
        self.connection.close()
