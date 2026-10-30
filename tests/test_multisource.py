import hashlib
import io
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from collector import run, main as collector_main
from collector_preflight import check_configuration_set
from collector_storage import Storage
from mt5_monitor import AccountGuardError, DiagnosticError
from recorder_config import load_recorder_config
from terminal_lock import TerminalLock, DatabaseLock, DataDirectoryLock, canonical_path
from tests.test_active_sets import SetMT5, StopAfter


class MultiSourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'databases').mkdir()
        self.paths = [self.write_config(index) for index in range(1, 4)]

    def write_config(self, index, **overrides):
        install = self.root / f'install{index}'
        data = self.root / f'data{index}'
        install.mkdir(exist_ok=True)
        data.mkdir(exist_ok=True)
        (install / 'terminal64.exe').touch()
        values = dict(MT5_TERMINAL_PATH=install / 'terminal64.exe',
                      MT5_EXPECTED_LOGIN=12345677 + index,
                      MT5_EXPECTED_SERVER='Demo-Server', MT5_DATA_PATH=data,
                      FUND_ID=f'F{index}', SOURCE_ID=f'S{index}',
                      COLLECTOR_DB_PATH=self.root / 'databases' / f'source{index}.sqlite3',
                      HISTORY_ENABLED='false')
        values.update(overrides)
        path = self.root / f'source{index}.env'
        path.write_text(''.join(f'{key}={value}\n' for key, value in values.items()), encoding='utf-8')
        return path

    def fake(self, config):
        mt5 = SetMT5()
        mt5.account.login = config.mt5.expected_login
        mt5.terminal_info = lambda: SimpleNamespace(connected=True, data_path=str(config.data_path))
        return mt5

    def test_distinct_set_offline_without_creating_databases(self):
        errors, notes = check_configuration_set(self.paths)
        self.assertEqual(errors, [])
        self.assertEqual(len(notes), 3)
        self.assertEqual(list((self.root / 'databases').iterdir()), [])
        command = "import sys; sys.modules['MetaTrader5']=None; from collector_preflight import main; raise SystemExit(main())"
        args = [item for path in self.paths for item in ('--config', str(path))]
        result = subprocess.run([sys.executable, '-c', command, *args], capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr.decode())

    def test_duplicate_terminal_database_data_fund_source_and_account(self):
        first = load_recorder_config(self.paths[0])
        fields = [('MT5_TERMINAL_PATH', first.mt5.terminal_path, 'terminal path'),
                  ('COLLECTOR_DB_PATH', first.database_path, 'database path'),
                  ('MT5_DATA_PATH', first.data_path, 'data directory'),
                  ('FUND_ID', first.fund_id.lower(), 'fund_id'),
                  ('SOURCE_ID', first.source_id.lower(), 'source_id'),
                  ('MT5_EXPECTED_LOGIN', first.mt5.expected_login, 'account/server')]
        for key, value, expected in fields:
            with self.subTest(key=key):
                self.write_config(2, **{key: value})
                errors, _ = check_configuration_set(self.paths)
                self.assertTrue(any('Duplicate ' + expected in error for error in errors), errors)

    def test_unresolved_config_fails_before_importing_mt5(self):
        self.write_config(2, MT5_EXPECTED_LOGIN='UNRESOLVED_GA2_LOGIN')
        errors, _ = check_configuration_set(self.paths)
        self.assertTrue(any('Unresolved configuration value: MT5_EXPECTED_LOGIN' in error for error in errors))
        stream = io.StringIO()
        with redirect_stderr(stream):
            self.assertEqual(collector_main(['check', '--config', str(self.paths[1])]), 1)
        self.assertIn('Unresolved configuration value', stream.getvalue())
        self.assertNotIn('12345678', stream.getvalue())

    def test_duplicate_keys_rejected_and_old_config_remains_valid(self):
        path = self.paths[0]
        with path.open('a') as stream:
            stream.write('FUND_ID=duplicate\n')
        with self.assertRaisesRegex(DiagnosticError, 'Duplicate configuration key'):
            load_recorder_config(path)
        self.write_config(1)
        text = path.read_text()
        path.write_text('\n'.join(line for line in text.splitlines() if not line.startswith('MT5_DATA_PATH=')))
        self.assertIsNone(load_recorder_config(path).data_path)
        self.assertTrue(any('MT5_DATA_PATH is required' in e for e in check_configuration_set([path])[0]))

    def test_relative_terminal_and_missing_data_directory_rejected(self):
        path = self.write_config(1, MT5_TERMINAL_PATH='relative.exe')
        (self.root / 'relative.exe').touch()
        previous = Path.cwd()
        try:
            os.chdir(self.root)
            with self.assertRaisesRegex(DiagnosticError, 'must be absolute'):
                load_recorder_config(path)
        finally:
            os.chdir(previous)
        self.write_config(1, MT5_DATA_PATH=self.root / 'missing')
        with self.assertRaisesRegex(DiagnosticError, 'MT5_DATA_PATH'):
            load_recorder_config(path)

    def test_preflight_rejects_enabled_history(self):
        self.write_config(2, HISTORY_ENABLED='true', HISTORY_START_UTC='2026-01-01T00:00:00Z')
        self.assertTrue(any('HISTORY_ENABLED must be false' in e for e in check_configuration_set(self.paths)[0]))

    def test_read_only_binding_check_preserves_database(self):
        config = load_recorder_config(self.paths[0])
        store = Storage(config.database_path, (config.fund_id, config.source_id, config.mt5.expected_login, config.mt5.expected_server, 'USD'))
        store.close()
        digest = hashlib.sha256(config.database_path.read_bytes()).digest()
        self.assertEqual(check_configuration_set(self.paths)[0], [])
        self.assertEqual(hashlib.sha256(config.database_path.read_bytes()).digest(), digest)
        self.write_config(1, FUND_ID='different')
        self.assertTrue(any('binding differs' in e for e in check_configuration_set(self.paths)[0]))
        self.assertEqual(hashlib.sha256(config.database_path.read_bytes()).digest(), digest)

    def test_existing_v2_only_reports_upgrade_does_not_migrate(self):
        config = load_recorder_config(self.paths[0])
        store = Storage(config.database_path, (config.fund_id, config.source_id, config.mt5.expected_login, config.mt5.expected_server, 'USD'))
        store.connection.execute('PRAGMA user_version=2')
        store.close()
        digest = hashlib.sha256(config.database_path.read_bytes()).digest()
        errors, notes = check_configuration_set(self.paths)
        self.assertEqual(errors, [])
        self.assertTrue(any('schema v2' in note for note in notes))
        self.assertEqual(hashlib.sha256(config.database_path.read_bytes()).digest(), digest)

    def test_journal_state_refuses_offline_binding_check(self):
        config = load_recorder_config(self.paths[0])
        store = Storage(config.database_path, (config.fund_id, config.source_id, config.mt5.expected_login, config.mt5.expected_server, 'USD'))
        store.close()
        Path(str(config.database_path) + '-wal').write_bytes(b'pending')
        self.assertTrue(any('journal state' in e for e in check_configuration_set(self.paths)[0]))

    def test_future_schema_rejected_without_changes(self):
        config = load_recorder_config(self.paths[0])
        store = Storage(config.database_path, (config.fund_id, config.source_id, config.mt5.expected_login, config.mt5.expected_server, 'USD'))
        store.connection.execute('PRAGMA user_version=99')
        store.close()
        digest = hashlib.sha256(config.database_path.read_bytes()).digest()
        self.assertTrue(any('unsupported database schema' in e for e in check_configuration_set(self.paths)[0]))
        self.assertEqual(hashlib.sha256(config.database_path.read_bytes()).digest(), digest)

    def test_real_cross_process_database_ownership_and_release(self):
        path = load_recorder_config(self.paths[0]).database_path
        lock = DatabaseLock(path)
        script = 'from terminal_lock import DatabaseLock; from pathlib import Path; import sys; lock=DatabaseLock(Path(sys.argv[1])); lock.close()'
        try:
            result = subprocess.run([sys.executable, '-c', script, str(path)], capture_output=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b'Another collector owns this database', result.stderr)
        finally:
            lock.close()
        result = subprocess.run([sys.executable, '-c', script, str(path)], capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertFalse(path.exists())

    def test_shared_database_blocks_different_terminal_before_initialize(self):
        first = load_recorder_config(self.paths[0])
        second = replace(load_recorder_config(self.paths[1]), database_path=first.database_path)
        owner = DatabaseLock(first.database_path)
        mt5 = self.fake(second)
        try:
            with self.assertRaisesRegex(DiagnosticError, 'owns this database'):
                run(second, mt5, stop=StopAfter(1), output=lambda _: None)
        finally:
            owner.close()
        self.assertFalse(any(name == 'initialize' for name, _ in mt5.calls))
        lock = TerminalLock(second.mt5.terminal_path)
        lock.close()

    def test_shared_data_directory_blocks_and_releases_database_lock(self):
        config = load_recorder_config(self.paths[0])
        owner = DataDirectoryLock(config.data_path)
        mt5 = self.fake(config)
        try:
            with self.assertRaisesRegex(DiagnosticError, 'owns this MT5 data directory'):
                run(config, mt5, stop=StopAfter(1), output=lambda _: None)
        finally:
            owner.close()
        self.assertFalse(any(name == 'initialize' for name, _ in mt5.calls))
        lock = DatabaseLock(config.database_path)
        lock.close()

    def test_runtime_data_directory_mismatch_stops_before_account(self):
        config = load_recorder_config(self.paths[0])
        mt5 = self.fake(config)
        mt5.terminal_info = lambda: SimpleNamespace(connected=True, data_path=str(self.root / 'other'))
        with self.assertRaisesRegex(AccountGuardError, 'MT5_DATA_PATH'):
            run(config, mt5, stop=StopAfter(1), output=lambda _: None)
        self.assertEqual(mt5.reads, 0)
        self.assertFalse(config.database_path.exists())
        for cls, path in ((TerminalLock, config.mt5.terminal_path), (DatabaseLock, config.database_path), (DataDirectoryLock, config.data_path)):
            lock = cls(path)
            lock.close()

    def test_one_source_failure_does_not_disrupt_another(self):
        first = load_recorder_config(self.paths[0])
        second = load_recorder_config(self.paths[1])
        bad = self.fake(first)
        bad.account.server = 'Other'
        with self.assertRaises(AccountGuardError):
            run(first, bad, stop=StopAfter(1), output=lambda _: None)
        self.assertEqual(run(second, self.fake(second), stop=StopAfter(1), output=lambda _: None), 0)
        db = sqlite3.connect(second.database_path)
        try:
            self.assertEqual(db.execute('SELECT count(*) FROM observations').fetchone()[0], 1)
        finally:
            db.close()
        self.assertFalse(first.database_path.exists())

    def test_check_only_verifies_data_path_without_creating_database(self):
        config = load_recorder_config(self.paths[0])
        self.assertEqual(run(config, self.fake(config), check_only=True, output=lambda _: None), 0)
        self.assertFalse(config.database_path.exists())

    def test_terminal_lock_key_compatible_and_does_not_grow(self):
        path = load_recorder_config(self.paths[0]).mt5.terminal_path
        expected = hashlib.sha256(canonical_path(path).encode('utf-8')).hexdigest() + '.lock'
        first = TerminalLock(path)
        try:
            self.assertEqual(first.path.name, expected)
            with self.assertRaises(DiagnosticError):
                TerminalLock(path)
            self.assertEqual(first.path.stat().st_size, 1)
        finally:
            first.close()


if __name__ == '__main__':
    unittest.main()
