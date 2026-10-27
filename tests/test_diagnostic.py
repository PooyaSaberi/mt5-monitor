import unittest
from pathlib import Path
from types import SimpleNamespace

from mt5_monitor import Config, DiagnosticError, diagnose, load_config


class FakeMT5:
    ACCOUNT_TRADE_MODE_DEMO = 0

    def __init__(self):
        self.initialized = True
        self.terminal = SimpleNamespace(connected=True)
        self.account = SimpleNamespace(
            login=12345678, server="Demo-Server", trade_mode=0,
            currency="USD", balance=1000.0, equity=995.0, profit=-5.0,
            margin=10.0, margin_free=985.0,
        )
        self.positions = ()
        self.shutdown_count = 0
        self.positions_calls = 0
        self.initialize_kwargs = None

    def initialize(self, **kwargs):
        self.initialize_kwargs = kwargs
        return self.initialized

    def last_error(self):
        return (-1, "test error")

    def terminal_info(self):
        return self.terminal

    def account_info(self):
        return self.account

    def positions_get(self):
        self.positions_calls += 1
        return self.positions

    def shutdown(self):
        self.shutdown_count += 1


class DiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.mt5 = FakeMT5()
        self.config = Config(Path("C:/MT5/terminal64.exe"), 12345678, "Demo-Server")

    def test_valid_empty_positions_and_cleanup(self):
        output = diagnose(self.config, self.mt5)
        self.assertIn("Account: ****5678", output)
        self.assertIn("Open positions: 0", output)
        self.assertNotIn("12345678", output)
        self.assertEqual(self.mt5.initialize_kwargs, {"path": str(self.config.terminal_path)})
        self.assertEqual(self.mt5.shutdown_count, 1)

    def test_connection_failure_cleans_up(self):
        self.mt5.initialized = False
        with self.assertRaisesRegex(DiagnosticError, "initialization failed"):
            diagnose(self.config, self.mt5)
        self.assertEqual(self.mt5.positions_calls, 0)
        self.assertEqual(self.mt5.shutdown_count, 1)

    def test_disconnected_terminal_stops_before_positions(self):
        self.mt5.terminal.connected = False
        with self.assertRaisesRegex(DiagnosticError, "not connected"):
            diagnose(self.config, self.mt5)
        self.assertEqual(self.mt5.positions_calls, 0)
        self.assertEqual(self.mt5.shutdown_count, 1)

    def test_account_mismatch_stops_before_positions(self):
        self.mt5.account.server = "Other-Server"
        with self.assertRaisesRegex(DiagnosticError, "does not match"):
            diagnose(self.config, self.mt5)
        self.assertEqual(self.mt5.positions_calls, 0)
        self.assertEqual(self.mt5.shutdown_count, 1)

    def test_live_account_stops_before_positions(self):
        self.mt5.account.trade_mode = 2
        with self.assertRaisesRegex(DiagnosticError, "not a demo"):
            diagnose(self.config, self.mt5)
        self.assertEqual(self.mt5.positions_calls, 0)
        self.assertEqual(self.mt5.shutdown_count, 1)

    def test_none_positions_is_error(self):
        self.mt5.positions = None
        with self.assertRaisesRegex(DiagnosticError, "Could not read open positions"):
            diagnose(self.config, self.mt5)
        self.assertEqual(self.mt5.shutdown_count, 1)

    def test_missing_account_is_error(self):
        self.mt5.account = None
        with self.assertRaisesRegex(DiagnosticError, "Could not read account"):
            diagnose(self.config, self.mt5)
        self.assertEqual(self.mt5.positions_calls, 0)
        self.assertEqual(self.mt5.shutdown_count, 1)

    def test_config_validation_happens_before_connection(self):
        with self.assertRaisesRegex(DiagnosticError, "Missing .env"):
            load_config(Path("missing-test-config.env"))


if __name__ == "__main__":
    unittest.main()
