import csv
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from collections import namedtuple
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from collector import run
from collector_inspect import main as inspect_main, read_history
from collector_storage import Storage, StorageError
from deal_reader import parse_utc, read_deals, HistoryBatchTooLarge
from history_config import HistoryConfig, load_history_config
from history_storage import HistoryStore
from history_sync import HistorySync
from mt5_monitor import AccountGuardError, DiagnosticError
from tests.test_active_sets import SetMT5, StopAfter

Deal = namedtuple('Deal', 'ticket order time time_msc type entry magic position_id reason volume price commission swap profit fee symbol comment external_id')
BIG = 9007199254740993
EPOCH = int(parse_utc('2026-01-10T00:00:00Z').timestamp())


def deal(epoch, ticket=BIG, profit=1, kind=999):
    return Deal(ticket, BIG + 1, epoch, epoch * 1000, kind, 0, BIG + 2,
                BIG + 3, 1, 0, 0, -1, 0, profit, 0, '', 'balance/correction', '')


class Clock:
    def __init__(self):
        self.epoch = EPOCH
        self.mono = 0

    def now(self):
        return self.epoch

    def monotonic(self):
        return self.mono

    def advance(self, seconds):
        self.epoch += seconds
        self.mono += seconds


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'records.sqlite3'
        from mt5_monitor import Config
        from recorder_config import RecorderConfig
        self.identity = ('fund', 'source', 12345678, 'Demo-Server', 'USD')
        self.options = HistoryConfig(True, '2026-01-01T00:00:00+00:00', chunk=3600,
                                     overlap=60, slot=1, refresh=1)
        self.config = RecorderConfig(Config(Path(self.tmp.name) / 'terminal.exe', 12345678, 'Demo-Server', .01),
                                     'fund', 'source', self.path, self.options)
        self.storage = Storage(self.path, self.identity)
        self.addCleanup(self.storage.close)
        self.clock = Clock()
        self.mt5 = SetMT5()
        self.mt5.history_deals_get = lambda *args: ()

    def sync(self, **kwargs):
        return HistorySync(self.storage, self.config, clock=self.clock.now,
                           monotonic=self.clock.monotonic, output=lambda _: None, **kwargs)

    def count(self, table):
        return self.storage.connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0]

    def rows(self, epoch=None, ticket=BIG, profit=1):
        epoch = epoch or EPOCH - 100
        self.mt5.history_deals_get = lambda *args: (deal(epoch, ticket, profit),)
        return read_deals(self.config.mt5, self.mt5,
                          datetime.fromtimestamp(epoch - 10, timezone.utc),
                          datetime.fromtimestamp(epoch + 10, timezone.utc))

    def test_configuration_optional_utc_and_validation(self):
        self.assertFalse(load_history_config({}).enabled)
        self.assertFalse(load_history_config({'HISTORY_ENABLED': 'false', 'HISTORY_START_UTC': 'bad'}).enabled)
        for value in ('', '2026-01-01', '2026-01-01T00:00:00', '2026-01-01T01:00:00+01:00'):
            with self.assertRaises(DiagnosticError):
                load_history_config({'HISTORY_ENABLED': 'true', 'HISTORY_START_UTC': value})
        enabled = load_history_config({'HISTORY_ENABLED': 'true', 'HISTORY_START_UTC': '2026-01-01T00:00:00Z'})
        self.assertEqual(enabled.chunk, 86400)
        for key, value in (('HISTORY_CHUNK_SECONDS', '0'), ('HISTORY_SLOW_SECONDS', 'nan')):
            with self.assertRaises(DiagnosticError):
                load_history_config({'HISTORY_ENABLED': 'true', 'HISTORY_START_UTC': '2026-01-01T00:00:00Z', key: value})

    def test_empty_advances_failures_do_not_and_backoff_survives_restart(self):
        sync = self.sync()
        sync.tick(self.mt5)
        db = self.storage.connection
        initial = db.execute("SELECT cursor FROM history_jobs WHERE kind='initial'").fetchone()[0]
        self.assertGreater(initial, int(parse_utc(self.options.start).timestamp()))
        self.assertEqual(db.execute('SELECT outcome,item_count FROM history_queries').fetchone(), ('success', 0))
        self.mt5.history_deals_get = lambda *args: None
        self.clock.advance(1)
        sync.tick(self.mt5)
        job = db.execute("SELECT j.job_id,j.cursor,j.failures,j.next_due FROM history_jobs j JOIN history_queries q ON j.job_id=q.job_id WHERE q.outcome='failed'").fetchone()
        self.assertEqual(job[2], 1)
        self.assertEqual(job[3], self.clock.now() + 30)
        restarted = self.sync()
        restarted.tick(self.mt5)
        self.assertEqual(db.execute('SELECT cursor FROM history_jobs WHERE job_id=?', (job[0],)).fetchone()[0], job[1])

    def test_fairness_chunks_overlap_and_restart_all_workloads(self):
        sync = self.sync(reconcile=(parse_utc('2026-01-02T00:00:00Z'), parse_utc('2026-01-04T00:00:00Z')))
        requested = []
        self.mt5.history_deals_get = lambda start, end: requested.append((int(start.timestamp()), int(end.timestamp()))) or ()
        for _ in range(12):
            sync.tick(self.mt5)
            self.clock.advance(1)
        db = self.storage.connection
        kinds = [row[0] for row in db.execute('SELECT j.kind FROM history_queries q JOIN history_jobs j ON q.job_id=j.job_id ORDER BY q.rowid')]
        self.assertEqual(kinds[:6], ['initial', 'recent', 'reconcile'] * 2)
        self.assertTrue(all(end - start <= self.options.chunk for start, end in requested))
        self.assertEqual(requested[3][0], requested[0][1] - self.options.overlap)
        cursors = dict(db.execute('SELECT kind,cursor FROM history_jobs'))
        restarted = self.sync(reconcile=(parse_utc('2026-01-02T00:00:00Z'), parse_utc('2026-01-04T00:00:00Z')))
        self.assertEqual(self.count('history_jobs'), 3)
        for _ in range(3):
            restarted.tick(self.mt5)
            self.clock.advance(1)
        for kind, cursor in db.execute('SELECT kind,cursor FROM history_jobs'):
            self.assertGreater(cursor, cursors[kind])

    def test_idempotency_same_time_corrections_omissions_and_reappearance(self):
        sync = self.sync()
        store = sync.store
        lower, upper = EPOCH - 200, EPOCH
        job = store.reconcile(datetime.fromtimestamp(lower, timezone.utc), datetime.fromtimestamp(upper, timezone.utc))
        window = job, lower, upper
        rows = self.rows()
        second = self.rows(ticket=BIG + 10)
        for _ in range(2):
            store.accept(window, rows + second, '2026-01-10T00:00:00+00:00', '2026-01-10T00:00:01+00:00', .01, EPOCH)
        self.assertEqual(self.count('deals'), 2)
        self.assertEqual(self.count('deal_revisions'), 2)
        self.assertEqual(self.count('history_query_members'), 4)
        changed = self.rows(profit=5)
        store.accept(window, changed, 'a', 'b', .01, EPOCH)
        self.assertEqual(self.count('deal_revisions'), 3)
        self.assertEqual(self.storage.connection.execute('SELECT kind FROM history_discrepancies').fetchone()[0], 'omitted')
        store.accept(window, changed + second, 'a', 'b', .01, EPOCH)
        self.assertEqual(self.count('deals'), 2)
        self.assertIn(('reappeared',), self.storage.connection.execute('SELECT kind FROM history_discrepancies').fetchall())
        original = self.storage.connection.execute('SELECT payload_json FROM deal_revisions WHERE ticket=? AND revision=1', (str(BIG),)).fetchone()[0]
        self.assertEqual(json.loads(original)['profit'], 1)

    def test_atomic_query_and_progress_rollback(self):
        store = self.sync().store
        window = store.select(EPOCH)
        rows = self.rows(epoch=window[1] + 10)
        db = self.storage.connection
        cursor = db.execute('SELECT cursor FROM history_jobs WHERE job_id=?', (window[0],)).fetchone()[0]
        db.execute("CREATE TRIGGER reject_progress BEFORE UPDATE OF cursor ON history_jobs BEGIN SELECT RAISE(ABORT,'injected failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            store.accept(window, rows, 'a', 'b', .01, EPOCH)
        self.assertEqual(self.count('deals'), 0)
        self.assertEqual(self.count('deal_revisions'), 0)
        self.assertEqual(self.count('history_queries'), 0)
        self.assertEqual(db.execute('SELECT cursor FROM history_jobs WHERE job_id=?', (window[0],)).fetchone()[0], cursor)

    def test_changed_history_start_rejected_without_reset(self):
        self.sync().tick(self.mt5)
        before = self.storage.connection.execute('SELECT * FROM history_jobs').fetchall()
        with self.assertRaises(StorageError):
            HistoryStore(self.storage, replace(self.options, start='2025-12-01T00:00:00+00:00'), EPOCH)
        self.assertEqual(self.storage.connection.execute('SELECT * FROM history_jobs').fetchall(), before)

    def test_slow_calls_shrink_suspend_resume_and_no_burst(self):
        self.config = replace(self.config, history=replace(self.options, chunk=600, min_chunk=300, slow_limit=2))
        sync = self.sync()
        def slow(*args):
            self.clock.advance(2)
            return ()
        self.mt5.history_deals_get = slow
        sync.tick(self.mt5)
        self.assertEqual(sync.store.db.execute('SELECT chunk_seconds FROM history_state').fetchone()[0], 300)
        count = self.count('history_queries')
        sync.tick(self.mt5)
        self.assertEqual(self.count('history_queries'), count)
        for _ in range(2):
            self.clock.advance(1)
            sync.tick(self.mt5)
        self.assertEqual(sync.store.db.execute('SELECT suspended FROM history_state').fetchone()[0], 1)
        restarted = self.sync()
        self.clock.advance(100)
        restarted.tick(self.mt5)
        self.assertEqual(self.count('history_queries'), 3)
        resumed = self.sync(resume=True)
        self.mt5.history_deals_get = lambda *args: ()
        resumed.tick(self.mt5)
        self.assertEqual(self.count('history_queries'), 4)

    def test_live_cycles_continue_on_history_failure_but_storage_error_is_fatal(self):
        self.mt5.history_deals_get = lambda *args: None
        with patch('history_sync.HistorySync', side_effect=lambda storage, config, **kwargs: HistorySync(storage, config, clock=self.clock.now, monotonic=self.clock.monotonic, **kwargs)):
            run(self.config, self.mt5, stop=StopAfter(3, lambda _: self.clock.advance(1)), output=lambda _: None)
        self.assertEqual(self.count('observations'), 3)
        self.assertEqual(self.count('history_queries'), 2)
        self.assertTrue(all(row[0] == 'failed' for row in self.storage.connection.execute('SELECT outcome FROM history_queries')))
        self.storage.connection.execute("CREATE TRIGGER fail_history BEFORE INSERT ON history_queries BEGIN SELECT RAISE(ABORT,'disk failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            run(self.config, self.mt5, stop=StopAfter(1), output=lambda _: None)

    def test_identity_switch_stops_history_and_keeps_prior_live_cycle(self):
        def switch(*args):
            self.mt5.account.server = 'Other'
            return ()
        self.mt5.history_deals_get = switch
        with self.assertRaises(AccountGuardError):
            run(self.config, self.mt5, stop=StopAfter(1), output=lambda _: None)
        self.assertEqual(self.count('observations'), 1)
        self.assertEqual(self.storage.connection.execute('SELECT outcome FROM history_queries').fetchone()[0], 'identity_mismatch')

    def test_offline_exports_precision_ranges_and_limits(self):
        store = self.sync().store
        job = store.reconcile(datetime.fromtimestamp(EPOCH - 200, timezone.utc), datetime.fromtimestamp(EPOCH, timezone.utc))
        store.accept((job, EPOCH - 200, EPOCH), self.rows() + self.rows(ticket=BIG + 10), '2026-01-10T00:00:00+00:00', '2026-01-10T00:00:01+00:00', .01, EPOCH)
        data = read_history(self.path, 'deals', '2026-01-09T00:00:00Z', '2026-01-11T00:00:00Z', 1)
        self.assertEqual(len(data['deals']), 1)
        self.assertEqual(data['deals'][0]['fields']['order'], str(BIG + 1))
        self.assertEqual(read_history(self.path, 'deals', end='2026-01-01T00:00:00Z')['deals'], [])
        for fmt in ('json', 'csv'):
            output = Path(self.tmp.name) / ('export.' + fmt)
            self.assertEqual(inspect_main([str(self.path), '--history-table', 'deals', '--format', fmt, '--output', str(output)]), 0)
            self.assertIn(str(BIG + 1), output.read_text())
            if fmt == 'json':
                self.assertIsInstance(json.loads(output.read_text())['deals'][0]['fields']['order'], str)
            else:
                with output.open(newline='') as stream:
                    self.assertEqual(next(csv.DictReader(stream))['ticket'], str(BIG))
        result = subprocess.run([sys.executable, '-c', "import sys; sys.modules['MetaTrader5']=None; from collector_inspect import read_history; from pathlib import Path; read_history(Path(sys.argv[1]),'deals')", str(self.path)], capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr.decode())

    def test_reader_boundaries_same_time_and_malformed_batches(self):
        start = datetime.fromtimestamp(EPOCH - 100, timezone.utc)
        end = datetime.fromtimestamp(EPOCH, timezone.utc)
        self.mt5.history_deals_get = lambda *args: (deal(EPOCH - 100), deal(EPOCH - 100, BIG + 10), deal(EPOCH, BIG + 11))
        rows = read_deals(self.config.mt5, self.mt5, start, end)
        self.assertEqual(len(rows), 3)
        for invalid in (
            (deal(EPOCH + 2),),
            (deal(EPOCH - 10)._replace(ticket=float(BIG)),),
            (deal(EPOCH - 10)._replace(profit=float('nan')),),
            (deal(EPOCH - 10), deal(EPOCH - 10)),
            (deal(EPOCH - 10)._replace(time_msc=0),),
        ):
            self.mt5.history_deals_get = lambda *args: invalid
            with self.assertRaises(DiagnosticError):
                read_deals(self.config.mt5, self.mt5, start, end)
        self.mt5.history_deals_get = lambda *args: (deal(EPOCH - 10), deal(EPOCH - 9, BIG + 1))
        with self.assertRaises(HistoryBatchTooLarge):
            read_deals(self.config.mt5, self.mt5, start, end, max_records=1)

    def test_malformed_and_exception_queries_preserve_cursor(self):
        sync = self.sync()
        def invalid(start, end):
            return (deal(int(start.timestamp()) + 10)._replace(ticket=1.0),)
        self.mt5.history_deals_get = invalid
        sync.tick(self.mt5)
        db = self.storage.connection
        self.assertEqual(db.execute("SELECT cursor=range_start FROM history_jobs WHERE kind='initial'").fetchone()[0], 1)
        self.assertEqual(self.count('deals'), 0)
        def fail(*args):
            raise RuntimeError('private exception text')
        self.mt5.history_deals_get = fail
        self.clock.advance(1)
        sync.tick(self.mt5)
        self.assertNotIn('private exception text', str(db.execute('SELECT * FROM history_queries').fetchall()))
        self.assertEqual(self.count('history_queries'), 2)

    def test_disconnection_rejects_cached_history(self):
        def disconnect(*args):
            self.mt5.connected = False
            return ()
        self.mt5.history_deals_get = disconnect
        self.sync().tick(self.mt5)
        self.assertEqual(self.count('deals'), 0)
        self.assertEqual(self.storage.connection.execute('SELECT outcome FROM history_queries').fetchone()[0], 'failed')

    def test_omission_boundaries_are_conservative(self):
        store = self.sync().store
        job = store.reconcile(datetime.fromtimestamp(EPOCH - 200, timezone.utc), datetime.fromtimestamp(EPOCH, timezone.utc))
        window = job, EPOCH - 200, EPOCH
        store.accept(window, self.rows(epoch=EPOCH - 200), 'a', 'b', 0, EPOCH)
        store.accept(window, (), 'a', 'b', 0, EPOCH)
        self.assertEqual(self.count('history_discrepancies'), 0)
        self.assertEqual(self.count('deals'), 1)

    def test_enabled_disabled_config_and_no_mt5_history_when_disabled(self):
        self.mt5.history_deals_get = lambda *args: self.fail('Disabled history was queried')
        run(replace(self.config, history=HistoryConfig()), self.mt5, stop=StopAfter(1), output=lambda _: None)
        self.assertEqual(self.count('history_queries'), 0)
        self.assertEqual(self.count('observations'), 1)

    def test_record_limit_reduces_chunk_without_advancing(self):
        self.config = replace(self.config, history=replace(self.options, max_records=1))
        def many(start, end):
            epoch = int(start.timestamp()) + 10
            return (deal(epoch), deal(epoch, BIG + 10))
        self.mt5.history_deals_get = many
        self.sync().tick(self.mt5)
        self.assertEqual(self.count('deals'), 0)
        self.assertEqual(self.storage.connection.execute("SELECT cursor=range_start FROM history_jobs WHERE kind='initial'").fetchone()[0], 1)
        self.assertEqual(self.storage.connection.execute('SELECT chunk_seconds FROM history_state').fetchone()[0], 1800)

    def test_slot_survives_restart_and_retry_resumes_same_window(self):
        self.mt5.history_deals_get = lambda *args: None
        sync = self.sync()
        sync.tick(self.mt5)
        first = self.storage.connection.execute('SELECT range_start,range_end FROM history_queries').fetchone()
        self.sync().tick(self.mt5)
        self.assertEqual(self.count('history_queries'), 1)
        self.clock.advance(301)
        self.mt5.history_deals_get = lambda *args: ()
        restarted = self.sync()
        # Recent takes the next fair turn, then initial retries its original range.
        restarted.tick(self.mt5)
        self.clock.advance(1)
        restarted.tick(self.mt5)
        retried = self.storage.connection.execute("SELECT q.range_start,q.range_end FROM history_queries q JOIN history_jobs j ON q.job_id=j.job_id WHERE j.kind='initial' AND q.outcome='success'").fetchone()
        self.assertEqual(retried, first)

    def test_bound_currency_guard(self):
        self.mt5.account.currency = 'EUR'
        with self.assertRaises(AccountGuardError):
            self.sync().tick(self.mt5)
        self.assertEqual(self.count('deals'), 0)

    def test_backward_coverage_omissions_never_delete_and_filter_metadata(self):
        store = self.sync().store
        window = store.select(EPOCH)
        store.accept(window, (), '2026-01-10T00:00:00+00:00', '2026-01-10T00:00:01+00:00', 0, EPOCH)
        data = read_history(self.path, 'history_queries', start='2026-01-01T00:00:00Z', end='2026-01-02T00:00:00Z')
        self.assertEqual(data['history_queries'][0]['item_count'], 0)
        self.assertEqual(data['history_queries'][0]['range_start_request_argument'], self.options.start)
        self.assertIsNone(data['history_queries'][0]['range_start_utc'])

    def test_config_loader_existing_file_remains_compatible(self):
        from recorder_config import load_recorder_config
        terminal = self.config.mt5.terminal_path
        terminal.touch()
        env = Path(self.tmp.name) / 'private.env'
        env.write_text(f'MT5_TERMINAL_PATH={terminal}\nMT5_EXPECTED_LOGIN=12345678\nMT5_EXPECTED_SERVER=Demo-Server\nFUND_ID=fund\nSOURCE_ID=source\nCOLLECTOR_DB_PATH={self.path}\n')
        self.assertFalse(load_recorder_config(env).history.enabled)
        with env.open('a') as stream:
            stream.write('HISTORY_ENABLED=true\n')
        with self.assertRaises(DiagnosticError):
            load_recorder_config(env)
        with env.open('a') as stream:
            stream.write('HISTORY_START_UTC=2026-01-01T00:00:00Z\n')
        self.assertTrue(load_recorder_config(env).history.enabled)


class MigrationTests(unittest.TestCase):
    def test_v2_migration_retains_all_tables_and_rollback(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'v2.sqlite3'
            identity = ('fund', 'source', 12345678, 'Demo-Server', 'USD')
            store = Storage(path, identity)
            from account_reader import read_account
            account = read_account(__import__('mt5_monitor').Config(Path('terminal.exe'), 12345678, 'Demo-Server'), SetMT5())
            store.save_cycle(account, account.started_at, account.ended_at, [dict(dataset='positions', started_at=account.started_at, ended_at=account.ended_at, outcome='success', records=())])
            db = store.connection
            for table in ('observations', 'status_events'):
                db.execute('DROP TRIGGER delivery_' + table)
            db.execute('DROP TABLE delivery_events')
            db.execute('DROP TABLE delivery_stream')
            for (table,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'history_%' OR name IN ('deals','deal_revisions'))").fetchall():
                db.execute(f'DROP TABLE {table}')
            db.execute('PRAGMA user_version=2')
            db.commit()
            before = {table: db.execute(f'SELECT * FROM {table}').fetchall() for table in ('source','observations','cycles','active_batches','active_records','status_events')}
            store.close()
            def broken(connection):
                connection.execute('CREATE TABLE history_partial (x INTEGER)')
                raise sqlite3.OperationalError('injected migration failure')
            with patch('history_storage.create_schema', side_effect=broken):
                with self.assertRaises(sqlite3.OperationalError):
                    Storage(path, identity)
            db = sqlite3.connect(path)
            self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE name='history_partial'").fetchall(), [])
            db.close()
            upgraded = Storage(path, identity)
            self.assertEqual(upgraded.connection.execute('PRAGMA user_version').fetchone()[0], 4)
            for table, rows in before.items():
                self.assertEqual(upgraded.connection.execute(f'SELECT * FROM {table}').fetchall(), rows)
            upgraded.close()
