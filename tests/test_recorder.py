import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from account_reader import read_account
from collector import run
from collector_inspect import read_database, main as inspect_main
from collector_storage import Storage, StorageError
from mt5_monitor import AccountGuardError, Config, DiagnosticError
from recorder_config import RecorderConfig
from recorder_config import load_recorder_config
from terminal_lock import TerminalLock


class FakeMT5:
    ACCOUNT_TRADE_MODE_DEMO = 0

    def __init__(self):
        self.account = SimpleNamespace(login=12345678, server='Demo-Server', trade_mode=0,
            currency='USD', balance=1000, equity=1001, profit=1, credit=0,
            margin=10, margin_free=991, margin_level=10010)
        self.connected = True
        self.calls = []
        self.reads = 0
        self.on_read = lambda count: None

    def initialize(self, **kwargs):
        self.calls.append(('initialize', kwargs))
        return True

    def shutdown(self):
        self.calls.append(('shutdown', None))

    def terminal_info(self):
        return SimpleNamespace(connected=self.connected)

    def account_info(self):
        self.reads += 1
        self.on_read(self.reads)
        return self.account

    def last_error(self):
        return (-1, 'fake')


class RecorderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.config = RecorderConfig(Config(root / 'terminal64.exe', 12345678, 'Demo-Server', 0.01),
                                     'fund', 'source', root / 'records.sqlite3')
        self.mt5 = FakeMT5()
        self.identity = ('fund', 'source', 12345678, 'Demo-Server', 'USD')

    def test_persistence_restart_binding_and_export(self):
        observation = read_account(self.config.mt5, self.mt5)
        store = Storage(self.config.database_path, self.identity)
        first = store.save_observation(observation)
        store.close()
        store = Storage(self.config.database_path, self.identity)
        second = store.save_observation(observation)
        store.close()
        data = read_database(self.config.database_path)
        self.assertEqual(len(data['observations']), 2)
        self.assertEqual({first, second}, {r['observation_id'] for r in data['observations']})
        self.assertEqual(data['source']['currency'], 'USD')
        self.assertEqual(data['observations'][0]['credit'], 0)
        self.assertEqual(len(data['status_events']), 2)
        with self.assertRaisesRegex(StorageError, 'identity'):
            Storage(self.config.database_path, ('other', *self.identity[1:]))
        output = Path(self.tmp.name) / 'export.json'
        self.assertEqual(inspect_main([str(self.config.database_path), '--format', 'json', '--output', str(output)]), 0)
        self.assertIn(first, output.read_text())
        self.assertIn('fund', output.read_text())
        output = Path(self.tmp.name) / 'export.csv'
        self.assertEqual(inspect_main([str(self.config.database_path), '--format', 'csv', '--output', str(output)]), 0)
        self.assertIn('observation_id', output.read_text())

    def test_unsupported_version(self):
        import sqlite3
        db = sqlite3.connect(self.config.database_path)
        db.execute('PRAGMA user_version=99')
        db.close()
        with self.assertRaisesRegex(StorageError, 'Unsupported'):
            Storage(self.config.database_path, self.identity)

    def test_disconnected_and_identity_change(self):
        self.mt5.connected = False
        with self.assertRaisesRegex(DiagnosticError, 'not connected'):
            read_account(self.config.mt5, self.mt5)
        self.mt5.connected = True
        self.mt5.on_read = lambda count: setattr(self.mt5.account, 'server', 'Other') if count == 2 else None
        with self.assertRaises(AccountGuardError):
            read_account(self.config.mt5, self.mt5)

    def test_storage_failure_and_shutdown(self):
        with patch('collector.Storage', side_effect=OSError('disk failure')):
            with self.assertRaisesRegex(OSError, 'disk failure'):
                run(self.config, self.mt5)
        self.assertIn(('shutdown', None), self.mt5.calls)
        lock = TerminalLock(self.config.mt5.terminal_path)
        lock.close()

    def test_check_does_not_create_database(self):
        self.assertEqual(run(self.config, self.mt5, check_only=True, output=lambda line: None), 0)
        self.assertFalse(self.config.database_path.exists())
        self.assertEqual(self.mt5.calls[0], ('initialize', {'path': str(self.config.mt5.terminal_path)}))

    def test_config_defaults_to_five_seconds(self):
        terminal = self.config.mt5.terminal_path
        terminal.touch()
        env = Path(self.tmp.name) / 'example.env'
        env.write_text(f'MT5_TERMINAL_PATH={terminal}\nMT5_EXPECTED_LOGIN=12345678\n'
                       f'MT5_EXPECTED_SERVER=Demo-Server\nFUND_ID=fund\nSOURCE_ID=source\n'
                       f'COLLECTOR_DB_PATH={self.config.database_path}\n')
        self.assertEqual(load_recorder_config(env).mt5.refresh_interval, 5)

    def test_disconnected_check_does_not_save(self):
        self.mt5.connected = False
        with self.assertRaises(DiagnosticError):
            run(self.config, self.mt5, check_only=True, output=lambda line: None)
        self.assertFalse(self.config.database_path.exists())
        self.assertIn(('shutdown', None), self.mt5.calls)

    def test_retry_status_does_not_create_snapshot(self):
        class StopAfterRetry:
            def __init__(self):
                self.calls = 0

            def is_set(self):
                return self.calls >= 2

            def wait(self, seconds):
                self.calls += 1
                if self.calls == 1:
                    self_mt5.connected = False

        self_mt5 = self.mt5
        stop = StopAfterRetry()
        self.assertEqual(run(self.config, self.mt5, stop=stop, output=lambda line: None), 0)
        data = read_database(self.config.database_path)
        self.assertEqual(len(data['observations']), 1)
        self.assertEqual([row['status'] for row in data['status_events']], ['saved', 'retrying'])

    def test_cross_process_terminal_lock(self):
        lock = TerminalLock(self.config.mt5.terminal_path)
        try:
            script = ('from terminal_lock import TerminalLock; from pathlib import Path; '
                      'TerminalLock(Path(__import__("sys").argv[1]))')
            process = subprocess.run([str(Path(__file__).parents[1] / '.venv' / 'Scripts' / 'python.exe'),
                                      '-c', script, str(self.config.mt5.terminal_path)],
                                     cwd=Path(__file__).parents[1], capture_output=True, timeout=5)
            self.assertNotEqual(process.returncode, 0)
        finally:
            lock.close()
        lock = TerminalLock(self.config.mt5.terminal_path)
        lock.close()


if __name__ == '__main__':
    unittest.main()
