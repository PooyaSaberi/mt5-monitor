"""Loopback-only test receiver fixture, NOT a production backend."""

import argparse
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
from datetime import datetime, timezone

from upload_contract import require, serialize_request
from upload_http import strict_json


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(',', ':'))


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class Conflict(Exception):
    pass


class Receiver:
    def __init__(self, path, sources):
        self.path, self.sources = Path(path), sources
        require(isinstance(sources, dict) and sources, 'Test receiver needs configured sources')
        tokens = []
        for source_id, config in sources.items():
            require(set(config) == {'token', 'fund_id', 'currency'}, 'Invalid test source configuration')
            require(all(isinstance(s, str) and s for s in [source_id, *config.values()]), 'Invalid test source values')
            require(config['token'].isascii(), 'Invalid test bearer token')
            tokens.append(config['token'])
        require(len(set(tokens)) == len(tokens), 'Use a distinct test token per source')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path)
        try:
            db.execute('BEGIN IMMEDIATE')
            version = db.execute('PRAGMA user_version').fetchone()[0]
            if version == 0:
                require(not db.execute("SELECT 1 FROM sqlite_master WHERE type='table'").fetchone(), 'Unrecognized receiver database')
                db.execute('CREATE TABLE batches (source_id TEXT, batch_id TEXT, digest TEXT NOT NULL, ack TEXT NOT NULL, PRIMARY KEY(source_id,batch_id))')
                db.execute('CREATE TABLE records (source_id TEXT, kind TEXT, record_id TEXT, digest TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(source_id,kind,record_id))')
                db.execute('CREATE TABLE sequences (source_id TEXT, stream_id TEXT, sequence TEXT, kind TEXT, record_id TEXT, digest TEXT NOT NULL, PRIMARY KEY(source_id,stream_id,sequence))')
                db.execute('CREATE TABLE identities (source_id TEXT, kind TEXT, record_id TEXT, digest TEXT NOT NULL, PRIMARY KEY(source_id,kind,record_id))')
                db.execute('CREATE TABLE source_bindings (source_id TEXT PRIMARY KEY, fund_id TEXT NOT NULL, currency TEXT NOT NULL)')
                db.execute('PRAGMA user_version=1')
            else:
                tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                require(version == 1 and tables == {'batches','records','sequences','identities','source_bindings'}, 'Unrecognized test receiver database')
            db.commit()
        except Exception:
            db.rollback(); raise
        finally:
            db.close()

    def ingest(self, payload, authorization):
        """Return an acknowledgement only after its entire transaction commits."""
        try:
            request = strict_json(payload)
            encoded, digest = serialize_request(request)
            require(encoded == payload, 'Test receiver requires canonical request bytes')
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
            return 422, None
        source = request['source']
        authority = self.sources.get(source['source_id'])
        expected = 'Bearer '+authority['token'] if authority else 'Bearer invalid'
        if not isinstance(authorization, str) or not hmac.compare_digest(authorization.encode(), expected.encode()):
            return 401, None
        if not authority or source['fund_id'] != authority['fund_id'] or source['currency'] != authority['currency']:
            return 403, None
        source_id = source['source_id']
        db = sqlite3.connect(self.path, timeout=1)
        try:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT OR IGNORE INTO source_bindings VALUES (?,?,?)', (source_id,source['fund_id'],source['currency']))
            bound = db.execute('SELECT fund_id,currency FROM source_bindings WHERE source_id=?',(source_id,)).fetchone()
            if bound != (source['fund_id'],source['currency']): raise Conflict()
            previous = db.execute('SELECT digest,ack FROM batches WHERE source_id=? AND batch_id=?',
                                  (source_id, request['batch_id'])).fetchone()
            if previous:
                if previous[0] != digest: raise Conflict()
                ack = json.loads(previous[1])
            else:
                for event in request['events']:
                    stable = {key:value for key,value in event.items() if key != 'sequence'}
                    record_digest = fingerprint(stable)
                    old = db.execute('SELECT digest FROM records WHERE source_id=? AND kind=? AND record_id=?',
                                     (source_id,event['kind'],event['record_id'])).fetchone()
                    if old and old[0] != record_digest: raise Conflict()
                    db.execute('INSERT OR IGNORE INTO records VALUES (?,?,?,?,?)',
                               (source_id,event['kind'],event['record_id'],record_digest,canonical(stable)))
                    seq_key = (source_id,request['stream_id'],event['sequence'])
                    old = db.execute('SELECT kind,record_id,digest FROM sequences WHERE source_id=? AND stream_id=? AND sequence=?',seq_key).fetchone()
                    if old and old != (event['kind'],event['record_id'],record_digest): raise Conflict()
                    db.execute('INSERT OR IGNORE INTO sequences VALUES (?,?,?,?,?,?)',
                               (*seq_key,event['kind'],event['record_id'],record_digest))
                    nested = []
                    if event['kind'] == 'account_observation' and event['cycle']:
                        nested.append(('cycle', event['cycle']['cycle_id'], event['cycle']))
                        for batch in event['active_batches']:
                            nested.append(('active_batch',batch['batch_id'],dict(cycle_id=event['cycle']['cycle_id'],batch=batch)))
                    for kind,record_id,value in nested:
                        value_digest = fingerprint(value)
                        old = db.execute('SELECT digest FROM identities WHERE source_id=? AND kind=? AND record_id=?',
                                         (source_id,kind,record_id)).fetchone()
                        if old and old[0] != value_digest: raise Conflict()
                        db.execute('INSERT OR IGNORE INTO identities VALUES (?,?,?,?)',(source_id,kind,record_id,value_digest))
                ack = dict(contract_version=1,batch_id=request['batch_id'],source_id=source_id,
                           stream_id=request['stream_id'],accepted_through_sequence=request['last_sequence'],
                           request_sha256=digest,result='committed',received_at_utc=datetime.now(timezone.utc).isoformat())
                db.execute('INSERT INTO batches VALUES (?,?,?,?)', (source_id,request['batch_id'],digest,canonical(ack)))
            db.commit()
            return 200, ack
        except Conflict:
            db.rollback()
            return 409, None
        except sqlite3.Error:
            db.rollback()
            return 503, None
        finally:
            db.close()


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(2.)

    def log_message(self, *args):
        pass  # No access logs, credentials, URLs or arbitrary request content.

    def do_POST(self):
        if self.path != '/ingest/v1/batches':
            self.respond(404, None); return
        length = self.headers.get('Content-Length', '')
        if self.headers.get('Transfer-Encoding') or not length.isdigit() or len(length)>8 or int(length)>512*1024:
            self.respond(413, None); return
        if self.headers.get('Content-Type') != 'application/json':
            self.respond(415, None); return
        try:
            payload = self.rfile.read(int(length))
            if len(payload) != int(length): return
            code, ack = self.server.receiver.ingest(payload,self.headers.get('Authorization'))
            hook = self.server.after_commit
            if code == 200 and hook is not None and hook(ack) is False:
                return  # Test-only lost acknowledgement injection, after commit.
            self.respond(code, ack)
        except (OSError, ValueError):
            return

    def respond(self, code, ack):
        payload = canonical(ack if ack is not None else {'error':'request_rejected'}).encode()
        self.send_response(code)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def create_server(receiver, port=0):
    server = ThreadingHTTPServer(('127.0.0.1',port),Handler)
    server.daemon_threads = True
    server.receiver, server.after_commit = receiver, None
    return server


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True,type=Path,help='Private JSON: database, sources, optional port')
    args = parser.parse_args(argv)
    try:
        config = strict_json(args.config.read_bytes())
        receiver = Receiver(Path(config['database']),config['sources'])
        server = create_server(receiver, config.get('port',8765))
        print('Test fixture listening on IPv4 loopback only; not a production receiver.')
        try: server.serve_forever(poll_interval=.1)
        except KeyboardInterrupt: pass
        finally: server.server_close()
        return 0
    except Exception:
        print('Test receiver stopped: configuration or local storage error.')
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
