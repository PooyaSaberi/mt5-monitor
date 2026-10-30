import json
import sqlite3
import tempfile
import unittest
from collections import namedtuple
from pathlib import Path

from account_reader import read_active_set
from collector import run
from collector_inspect import main as inspect_main, read_database
from collector_storage import Storage, StorageError
from mt5_monitor import AccountGuardError, Config, DiagnosticError
from recorder_config import RecorderConfig
from tests.test_recorder import FakeMT5


Position = namedtuple('Position', 'ticket time time_msc time_update time_update_msc type magic identifier reason volume price_open sl tp price_current swap profit symbol comment external_id')
Order = namedtuple('Order', 'ticket time_setup time_setup_msc time_done time_done_msc time_expiration type type_time type_filling state magic position_id position_by_id reason volume_initial volume_current price_open sl tp price_current price_stoplimit symbol comment external_id')
BIG = 9007199254740993


class SetMT5(FakeMT5):
    def __init__(self):
        super().__init__()
        self.positions = ()
        self.orders = ()
        self.position_error = None
        self.order_error = None

    def positions_get(self):
        if self.position_error:
            raise self.position_error
        return self.positions

    def orders_get(self):
        if self.order_error:
            raise self.order_error
        return self.orders


def position(ticket=BIG):
    return Position(ticket, 1, 1001, 2, 2002, 0, 42, BIG + 1, 3,
                    0.1, 2000.0, 1900.0, 2200.0, 2010.0, -1.2, 2.3,
                    'XAUUSD', 'test position', '')


def order(ticket=BIG + 2):
    return Order(ticket, 1, 1001, 0, 0, 0, 2, 0, 2, 1, 42, BIG,
                 0, 3, 0.1, 0.1, 2000.0, 1900.0, 2200.0, 2010.0,
                 0.0, 'XAUUSD', 'test order', '')


class StopAfter:
    def __init__(self, cycles, callback=None):
        self.cycles = cycles
        self.count = 0
        self.callback = callback

    def is_set(self):
        return self.count >= self.cycles

    def wait(self, seconds):
        self.count += 1
        if self.callback:
            self.callback(self.count)


class ActiveSetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.config = RecorderConfig(Config(root / 'terminal64.exe', 12345678, 'Demo-Server', 0.01),
                                     'fund', 'source', root / 'records.sqlite3')
        self.mt5 = SetMT5()
        self.identity = ('fund', 'source', 12345678, 'Demo-Server', 'USD')

    def inspect(self):
        return read_database(self.config.database_path)

    def test_populated_and_empty_batches_preserve_fields_and_precision(self):
        self.mt5.positions = (position(),)
        self.mt5.orders = (order(),)
        run(self.config, self.mt5, stop=StopAfter(1), output=lambda _: None)
        data = self.inspect()
        self.assertEqual(len(data['observations']), 1)
        self.assertEqual({b['dataset']: b['item_count'] for b in data['active_batches']},
                         {'positions': 1, 'orders': 1})
        self.assertEqual(len(data['active_records']), 2)
        self.assertEqual({r['ticket'] for r in data['active_records']}, {str(BIG), str(BIG + 2)})
        self.assertEqual(next(r['fields'] for r in data['active_records'] if r['ticket'] == str(BIG)),
                         position()._asdict())
        self.assertEqual(next(r['fields'] for r in data['active_records'] if r['ticket'] == str(BIG + 2)),
                         order()._asdict())
        self.assertEqual({b['cycle_id'] for b in data['active_batches']},
                         {data['observations'][0]['cycle_id']})
        self.assertEqual({b['source_id'] for b in data['active_batches']}, {'source'})

        self.mt5.positions = ()
        self.mt5.orders = ()
        run(self.config, self.mt5, stop=StopAfter(1), output=lambda _: None)
        data = self.inspect()
        self.assertEqual(len(data['observations']), 2)
        latest = data['observations'][-1]['cycle_id']
        self.assertEqual([(b['outcome'], b['item_count']) for b in data['active_batches']
                          if b['cycle_id'] == latest], [('success', 0), ('success', 0)])
        self.assertEqual(len(data['active_records']), 2)  # Earlier set remains historical.

    def test_none_and_exception_are_failed_not_empty(self):
        self.mt5.positions = None
        self.mt5.order_error = RuntimeError('private raw error')
        run(self.config, self.mt5, stop=StopAfter(1), output=lambda _: None)
        data = self.inspect()
        self.assertEqual({b['dataset']: (b['outcome'], b['item_count']) for b in data['active_batches']},
                         {'positions': ('failed', None), 'orders': ('failed', None)})
        self.assertEqual(len(data['active_records']), 0)
        self.assertNotIn('private raw error', json.dumps(data))
        self.assertEqual(len(data['observations']), 1)

    def test_one_failure_keeps_other_valid_sets_and_account(self):
        self.mt5.positions = None
        self.mt5.orders = (order(),)
        run(self.config, self.mt5, stop=StopAfter(1), output=lambda _: None)
        data = self.inspect()
        self.assertEqual(len(data['observations']), 1)
        self.assertEqual([b['outcome'] for b in data['active_batches']], ['failed', 'success'])
        self.assertEqual(len(data['active_records']), 1)
        self.assertIn('positions_failed', [e['status'] for e in data['status_events']])

    def test_identity_change_rejects_batch_and_stops(self):
        def switch():
            self.mt5.account.server = 'Other-Server'
            return (position(),)
        self.mt5.positions_get = switch
        with self.assertRaises(AccountGuardError):
            run(self.config, self.mt5, output=lambda _: None)
        data = self.inspect()
        self.assertEqual(len(data['observations']), 1)
        self.assertEqual([b['outcome'] for b in data['active_batches']], ['identity_mismatch'])
        self.assertEqual(data['active_records'], [])
        self.assertFalse(any(b['dataset'] == 'orders' for b in data['active_batches']))

    def test_v1_migration_retains_history_and_rejects_future_version(self):
        db = sqlite3.connect(self.config.database_path)
        db.executescript('''
            CREATE TABLE source (fund_id TEXT, source_id TEXT, login INTEGER, server TEXT, currency TEXT);
            CREATE TABLE observations (observation_id TEXT PRIMARY KEY, started_at TEXT, ended_at TEXT, balance REAL, equity REAL, profit REAL, credit REAL, margin REAL, margin_free REAL, margin_level REAL);
            CREATE TABLE status_events (event_id TEXT PRIMARY KEY, started_at TEXT, ended_at TEXT, status TEXT, detail TEXT);
            INSERT INTO source VALUES ('fund','source',12345678,'Demo-Server','USD');
            INSERT INTO observations VALUES ('old-id','2026-01-01T00:00:00+00:00','2026-01-01T00:00:01+00:00',100,101,1,0,0,101,0);
            INSERT INTO status_events VALUES ('old-event','2026-01-01T00:00:00+00:00','2026-01-01T00:00:01+00:00','saved',NULL);
            PRAGMA user_version=1;
        ''')
        db.close()
        store = Storage(self.config.database_path, self.identity)
        store.close()
        data = self.inspect()
        self.assertEqual(data['schema_version'], 4)
        self.assertEqual(data['observations'][0]['observation_id'], 'old-id')
        self.assertIsNone(data['observations'][0]['cycle_id'])
        self.assertEqual(data['status_events'][0]['event_id'], 'old-event')
        db = sqlite3.connect(self.config.database_path)
        db.execute('PRAGMA user_version=99')
        db.close()
        with self.assertRaisesRegex(StorageError, 'Unsupported'):
            Storage(self.config.database_path, self.identity)

    def test_offline_json_and_csv_export(self):
        self.mt5.positions = (position(),)
        self.mt5.orders = ()
        run(self.config, self.mt5, stop=StopAfter(1), output=lambda _: None)
        json_path = Path(self.tmp.name) / 'sample.json'
        csv_path = Path(self.tmp.name) / 'batches.csv'
        records_path = Path(self.tmp.name) / 'records.csv'
        self.assertEqual(inspect_main([str(self.config.database_path), '--format', 'json', '--output', str(json_path)]), 0)
        self.assertEqual(inspect_main([str(self.config.database_path), '--format', 'csv', '--csv-table', 'active_batches', '--output', str(csv_path)]), 0)
        self.assertEqual(inspect_main([str(self.config.database_path), '--format', 'csv', '--csv-table', 'active_records', '--output', str(records_path)]), 0)
        self.assertEqual(len(json.loads(json_path.read_text())['active_batches']), 2)
        self.assertIn('item_count', csv_path.read_text())
        self.assertIn(str(BIG), records_path.read_text())
        self.assertIn('identifier', records_path.read_text())


if __name__ == '__main__':
    unittest.main()
