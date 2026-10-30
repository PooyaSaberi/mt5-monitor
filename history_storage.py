"""SQLite history state and atomic query acceptance; no MT5 imports."""

import hashlib
import json
from uuid import uuid4

from deal_reader import parse_utc
from mt5_monitor import DiagnosticError


def create_schema(db):
    statements = (
        'CREATE TABLE history_state (source_id TEXT PRIMARY KEY, start_utc TEXT NOT NULL, turn INTEGER NOT NULL DEFAULT 0, chunk_seconds INTEGER NOT NULL, slow_count INTEGER NOT NULL DEFAULT 0, suspended INTEGER NOT NULL DEFAULT 0, next_slot_due REAL NOT NULL DEFAULT 0)',
        'CREATE TABLE history_jobs (job_id TEXT PRIMARY KEY, kind TEXT NOT NULL, range_start INTEGER NOT NULL, range_end INTEGER NOT NULL, cursor INTEGER NOT NULL, next_due REAL NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0)',
        'CREATE TABLE history_queries (query_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, source_id TEXT NOT NULL, range_start INTEGER NOT NULL, range_end INTEGER NOT NULL, started_at TEXT NOT NULL, ended_at TEXT NOT NULL, outcome TEXT NOT NULL, item_count INTEGER, detail TEXT, duration_seconds REAL NOT NULL)',
        'CREATE TABLE deals (source_id TEXT NOT NULL, ticket TEXT NOT NULL, time_msc INTEGER NOT NULL, payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL, revision INTEGER NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, missing INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(source_id,ticket))',
        'CREATE TABLE deal_revisions (source_id TEXT NOT NULL, ticket TEXT NOT NULL, revision INTEGER NOT NULL, time_msc INTEGER NOT NULL, payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL, observed_at TEXT NOT NULL, query_id TEXT NOT NULL, PRIMARY KEY(source_id,ticket,revision))',
        'CREATE TABLE history_query_members (query_id TEXT NOT NULL, ticket TEXT NOT NULL, revision INTEGER NOT NULL, PRIMARY KEY(query_id,ticket))',
        'CREATE TABLE history_discrepancies (event_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, ticket TEXT NOT NULL, query_id TEXT NOT NULL, observed_at TEXT NOT NULL, kind TEXT NOT NULL)',
        'CREATE INDEX deals_time ON deals(time_msc)',
        'CREATE INDEX history_queries_time ON history_queries(started_at)',
    )
    for statement in statements:
        db.execute(statement)


class HistoryStore:
    def __init__(self, storage, config, now):
        self.db = storage.connection
        self.source = storage.identity[1]
        self.currency = storage.identity[4]
        self.config = config
        start = int(parse_utc(config.start).timestamp())
        existing = self.db.execute('SELECT start_utc FROM history_state WHERE source_id=?', (self.source,)).fetchone()
        if existing and existing[0] != config.start:
            from collector_storage import StorageError
            raise StorageError('HISTORY_START_UTC differs from the bound history start; restore it and use explicit reconciliation for other dates.')
        with self.db:
            if not existing:
                self.db.execute('INSERT INTO history_state(source_id,start_utc,chunk_seconds) VALUES (?,?,?)',
                                (self.source, config.start, config.chunk))
                self.db.execute('INSERT INTO history_jobs(job_id,kind,range_start,range_end,cursor) VALUES (?,?,?,?,?)',
                                (str(uuid4()), 'initial', start, max(start, int(now) - config.safety_lag), start))
            else:
                self.db.execute('UPDATE history_state SET chunk_seconds=min(?,max(?,chunk_seconds)) WHERE source_id=?',
                                (config.chunk, config.min_chunk, self.source))

    def reconcile(self, start, end):
        start, end = int(start.timestamp()), int(end.timestamp())
        if start >= end:
            raise DiagnosticError('Reconciliation start must precede end')
        row = self.db.execute("SELECT job_id FROM history_jobs WHERE kind='reconcile' AND range_start=? AND range_end=? AND cursor<range_end", (start, end)).fetchone()
        if row:
            return row[0]
        job = str(uuid4())
        with self.db:
            self.db.execute('INSERT INTO history_jobs(job_id,kind,range_start,range_end,cursor) VALUES (?,?,?,?,?)',
                            (job, 'reconcile', start, end, start))
        return job

    def select(self, now):
        """Round-robin workload selection; cursors survive process restarts."""
        with self.db:
            self.db.execute("UPDATE history_jobs SET range_end=max(cursor,?) WHERE kind='initial' AND cursor>=range_end", (int(now) - self.config.safety_lag,))
            recent = self.db.execute("SELECT job_id,cursor,range_end,next_due FROM history_jobs WHERE kind='recent'").fetchone()
            end = int(now) - self.config.safety_lag
            start = max(int(parse_utc(self.config.start).timestamp()), end - self.config.lookback)
            if not recent:
                self.db.execute('INSERT INTO history_jobs(job_id,kind,range_start,range_end,cursor) VALUES (?,?,?,?,?)',
                                (str(uuid4()), 'recent', start, max(start, end), start))
            elif recent[1] >= recent[2] and recent[3] <= now:
                self.db.execute('UPDATE history_jobs SET range_start=?,range_end=?,cursor=? WHERE job_id=?', (start, max(start, end), start, recent[0]))
            turn, chunk, suspended = self.db.execute('SELECT turn,chunk_seconds,suspended FROM history_state WHERE source_id=?', (self.source,)).fetchone()
            if suspended:
                return None
            for offset in range(3):
                index = (turn + offset) % 3
                kind = ('initial', 'recent', 'reconcile')[index]
                row = self.db.execute('SELECT job_id,range_start,range_end,cursor FROM history_jobs WHERE kind=? AND cursor<range_end AND next_due<=? ORDER BY rowid LIMIT 1', (kind, now)).fetchone()
                if row:
                    self.db.execute('UPDATE history_state SET turn=? WHERE source_id=?', ((index + 1) % 3, self.source))
                    job, lower, upper, cursor = row
                    # Reserve room for overlap while bounding the entire API range.
                    overlap = min(self.config.overlap, chunk // 2)
                    query_start = max(lower, cursor - overlap)
                    query_end = min(upper, query_start + chunk)
                    return job, query_start, query_end
        return None

    def accept(self, window, rows, started, ended, duration, now):
        job, lower, upper = window
        query = str(uuid4())
        tickets = set()
        for row in rows:
            if not isinstance(row.get('ticket'), str) or not row['ticket'].isdigit() or row['ticket'] in tickets:
                raise DiagnosticError('Invalid normalized history batch')
            tickets.add(row['ticket'])
            json.dumps(row, allow_nan=False)
        with self.db:
            self._query(query, window, started, ended, 'success', len(rows), None, duration)
            tickets = {row['ticket'] for row in rows}
            for row in rows:
                ticket = row['ticket']
                payload = json.dumps(row, sort_keys=True, separators=(',', ':'), allow_nan=False)
                digest = hashlib.sha256(payload.encode()).hexdigest()
                old = self.db.execute('SELECT payload_hash,revision,missing FROM deals WHERE source_id=? AND ticket=?', (self.source, ticket)).fetchone()
                revision = 1 if old is None else old[1] + (old[0] != digest)
                if old is None or old[0] != digest:
                    self.db.execute('INSERT INTO deal_revisions VALUES (?,?,?,?,?,?,?,?)',
                                    (self.source, ticket, revision, row['time_msc'], payload, digest, ended, query))
                if old is not None and old[2]:
                    self._discrepancy(ticket, query, ended, 'reappeared')
                self.db.execute('INSERT INTO deals VALUES (?,?,?,?,?,?,?,?,0) ON CONFLICT(source_id,ticket) DO UPDATE SET time_msc=excluded.time_msc,payload_json=excluded.payload_json,payload_hash=excluded.payload_hash,revision=excluded.revision,last_seen=excluded.last_seen,missing=0',
                                (self.source, ticket, row['time_msc'], payload, digest, revision, ended, ended))
                self.db.execute('INSERT INTO history_query_members VALUES (?,?,?)', (query, ticket, revision))
            # Boundary seconds are excluded from omission conclusions.
            candidates = self.db.execute('SELECT ticket FROM deals WHERE source_id=? AND time_msc>? AND time_msc<?', (self.source, (lower + 1) * 1000, (upper - 1) * 1000)).fetchall()
            for (ticket,) in candidates:
                if ticket not in tickets:
                    self._discrepancy(ticket, query, ended, 'omitted')
                    self.db.execute('UPDATE deals SET missing=1 WHERE source_id=? AND ticket=?', (self.source, ticket))
            kind, end = self.db.execute('SELECT kind,range_end FROM history_jobs WHERE job_id=?', (job,)).fetchone()
            due = now + self.config.refresh if kind == 'recent' and upper >= end else now
            self.db.execute('UPDATE history_jobs SET cursor=max(cursor,?),failures=0,next_due=? WHERE job_id=?', (upper, due, job))
            self.db.execute('UPDATE history_state SET next_slot_due=? WHERE source_id=?', (now + self.config.slot, self.source))
            self._speed(duration)

    def fail(self, window, started, ended, duration, now, detail, outcome='failed'):
        with self.db:
            self._query(str(uuid4()), window, started, ended, outcome, None, detail, duration)
            failures = self.db.execute('SELECT failures FROM history_jobs WHERE job_id=?', (window[0],)).fetchone()[0] + 1
            delay = min(self.config.retry * 2 ** min(failures - 1, 20), self.config.retry_max)
            self.db.execute('UPDATE history_jobs SET failures=?,next_due=? WHERE job_id=?', (failures, now + delay, window[0]))
            self.db.execute('UPDATE history_state SET next_slot_due=? WHERE source_id=?', (now + self.config.slot, self.source))
            self._speed(duration)

    def _query(self, query, window, started, ended, outcome, count, detail, duration):
        self.db.execute('INSERT INTO history_queries VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                        (query, window[0], self.source, window[1], window[2], started, ended, outcome, count, detail, duration))

    def _discrepancy(self, ticket, query, ended, kind):
        self.db.execute('INSERT INTO history_discrepancies VALUES (?,?,?,?,?,?)', (str(uuid4()), self.source, ticket, query, ended, kind))

    def _speed(self, duration):
        chunk, count = self.db.execute('SELECT chunk_seconds,slow_count FROM history_state WHERE source_id=?', (self.source,)).fetchone()
        if duration > self.config.slow_seconds:
            if chunk > self.config.min_chunk:
                chunk, count = max(self.config.min_chunk, chunk // 2), 0
            else:
                count += 1
        else:
            count = 0
        self.db.execute('UPDATE history_state SET chunk_seconds=?,slow_count=?,suspended=? WHERE source_id=?',
                        (chunk, count, int(count >= self.config.slow_limit), self.source))

    def resume(self):
        with self.db:
            self.db.execute('UPDATE history_state SET suspended=0,slow_count=0 WHERE source_id=?', (self.source,))

    def shrink(self):
        with self.db:
            chunk = self.db.execute('SELECT chunk_seconds FROM history_state WHERE source_id=?', (self.source,)).fetchone()[0]
            self.db.execute('UPDATE history_state SET chunk_seconds=?,suspended=? WHERE source_id=?',
                            (max(self.config.min_chunk, chunk // 2), int(chunk <= self.config.min_chunk), self.source))
