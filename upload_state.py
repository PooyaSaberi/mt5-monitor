"""Durable uploader progress/pending bytes in a separate SQLite database."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from upload_contract import require, serialize_request, validate_acknowledgement, event_digest, as_utc


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def file_identity(path):
    stat = Path(path).stat()
    return [stat.st_dev, stat.st_ino]


def binding_for(config, stream_id):
    return dict(collector_db=str(config.collector_db.resolve()), source=config.expected_source,
                stream_id=stream_id, destination=config.endpoint,
                file_identity=file_identity(config.collector_db))


class UploadState:
    def __init__(self, path, binding):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=1)
        try:
            self.db.execute('BEGIN IMMEDIATE')
            version = self.db.execute('PRAGMA user_version').fetchone()[0]
            if version == 0:
                require(not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table'").fetchone(), 'Unrecognized state database')
                self.db.execute('CREATE TABLE control (singleton INTEGER PRIMARY KEY CHECK(singleton=1), binding TEXT NOT NULL, progress TEXT NOT NULL)')
                self.db.execute('CREATE TABLE pending (singleton INTEGER PRIMARY KEY CHECK(singleton=1), batch_id TEXT NOT NULL, first_sequence INTEGER NOT NULL, last_sequence INTEGER NOT NULL, digest TEXT NOT NULL, payload BLOB NOT NULL)')
                self.db.execute('INSERT INTO control VALUES (1,?,?)', (encode(binding), encode(dict(
                    acknowledged_sequence=0, anchor=None, last_ack=None, attempts=0,
                    next_retry=0., paused=False, error=None))))
                self.db.execute('PRAGMA user_version=1')
            else:
                require(version == 1, 'Unsupported uploader state version')
            require(self.binding == binding, 'Uploader binding changed; explicit recovery required')
            self.db.commit()
        except Exception:
            self.db.rollback(); self.db.close(); raise

    @property
    def binding(self):
        return json.loads(self.db.execute('SELECT binding FROM control WHERE singleton=1').fetchone()[0])

    @property
    def progress(self):
        return json.loads(self.db.execute('SELECT progress FROM control WHERE singleton=1').fetchone()[0])

    def pending(self):
        row = self.db.execute('SELECT batch_id,first_sequence,last_sequence,digest,payload FROM pending WHERE singleton=1').fetchone()
        if row is None:
            return None
        batch_id, first, last, digest, payload = row
        request = json.loads(payload)
        canonical, computed = serialize_request(request)
        require(payload == canonical and digest == computed and batch_id == request['batch_id']
                and first == int(request['first_sequence']) and last == int(request['last_sequence']),
                'Pending state content is inconsistent')
        return dict(request=request, payload=payload, digest=digest)

    def _progress(self, progress):
        self.db.execute('UPDATE control SET progress=? WHERE singleton=1', (encode(progress),))

    def prepare(self, request):
        payload, digest = serialize_request(request)
        require(self.pending() is None, 'Only one pending batch is allowed')
        require(request['source'] == self.binding['source'] and request['stream_id'] == self.binding['stream_id'], 'Pending source binding differs')
        require(int(request['first_sequence']) > self.progress['acknowledged_sequence'], 'Pending sequence is not after acknowledgement')
        with self.db:
            self.db.execute('INSERT INTO pending VALUES (1,?,?,?,?,?)', (request['batch_id'],
                int(request['first_sequence']), int(request['last_sequence']), digest, payload))
        return self.pending()

    def acknowledge(self, ack, *, acknowledged_at=None):
        pending = self.pending()
        require(pending is not None, 'No pending batch')
        request = pending['request']
        validate_acknowledgement(ack, request)
        last = request['events'][-1]
        progress = self.progress
        progress.update(acknowledged_sequence=int(last['sequence']),
                        anchor={'sequence': int(last['sequence']), 'digest': event_digest(last)},
                        last_ack=ack, last_acknowledged_at_utc=as_utc(acknowledged_at or datetime.now(timezone.utc).isoformat()),
                        attempts=0, next_retry=0., error=None, paused=False)
        with self.db:
            self._progress(progress)
            self.db.execute('DELETE FROM pending WHERE singleton=1')

    def failure(self, code, *, paused=False, due=0.):
        progress = self.progress
        progress.update(error=code, paused=paused, next_retry=due, attempts=progress['attempts']+1)
        with self.db:
            self._progress(progress)

    def resume(self):
        progress = self.progress
        progress.update(paused=False, error=None, next_retry=0., attempts=0)
        with self.db:
            self._progress(progress)

    def close(self):
        self.db.close()


def inspect_state(path):
    """No state creation, locks, or writes; no credentials are returned."""
    path = Path(path)
    if not path.exists():
        return None
    db = sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=.1)
    try:
        db.execute('BEGIN')
        # State inspection is bounded too; reject oversized/corrupt state blobs.
        import time
        deadline = time.monotonic()+1.
        db.set_progress_handler(lambda: int(time.monotonic()>deadline), 1000)
        require(db.execute('PRAGMA user_version').fetchone()[0] == 1, 'Unsupported uploader state')
        binding, progress = db.execute('SELECT CASE WHEN length(binding)<=1048576 THEN binding END,CASE WHEN length(progress)<=1048576 THEN progress END FROM control WHERE singleton=1').fetchone()
        require(binding is not None and progress is not None, 'State metadata exceeds limit')
        pending = db.execute('SELECT batch_id,first_sequence,last_sequence,digest,length(payload) FROM pending WHERE singleton=1').fetchone()
        require(pending is None or pending[4] <= 512*1024, 'Pending request exceeds limit')
        payload = db.execute('SELECT payload FROM pending WHERE singleton=1').fetchone()
        request = json.loads(payload[0]) if payload else None
        if request:
            canonical, digest = serialize_request(request)
            require(canonical == payload[0] and digest == pending[3] and pending[0] == request['batch_id']
                    and pending[1] == int(request['first_sequence']) and pending[2] == int(request['last_sequence']),
                    'Pending state content is inconsistent')
        return dict(binding=json.loads(binding), progress=json.loads(progress), pending=pending, pending_request=request)
    finally:
        db.close()
