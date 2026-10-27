import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from continuous_monitor import collect, run_monitor
from mt5_monitor import Config, DiagnosticError, load_config


class FakeClock:
    def __init__(self):
        self.value = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.delays = []
        self.after_wait = lambda count: None

    def now(self):
        return self.value

    def wait(self, seconds):
        self.delays.append(seconds)
        self.value += timedelta(seconds=seconds)
        self.after_wait(len(self.delays))


class FakeMT5:
    ACCOUNT_TRADE_MODE_DEMO = 0
    POSITION_TYPE_BUY = 0
    POSITION_TYPE_SELL = 1

    def __init__(self):
        self.init_results = [True]
        self.init_calls = []
        self.shutdown_calls = 0
        self.terminal = SimpleNamespace(connected=True)
        self.account = SimpleNamespace(
            login=12345678, server="Demo-Server", trade_mode=0,
            currency="USD", balance=1000.0, equity=1000.0, profit=0.0,
            margin=0.0, margin_free=1000.0,
        )
        self.positions = ()
        self.position_calls = 0
        self.account_calls = 0
        self.on_account_call = lambda count: None

    def initialize(self, **kwargs):
        self.init_calls.append(kwargs)
        return self.init_results.pop(0) if self.init_results else True

    def shutdown(self):
        self.shutdown_calls += 1

    def terminal_info(self):
        return self.terminal

    def account_info(self):
        self.account_calls += 1
        self.on_account_call(self.account_calls)
        return self.account

    def positions_get(self):
        self.position_calls += 1
        return self.positions

    def last_error(self):
        return (-1, "fake failure")


class ContinuousMonitorTests(unittest.TestCase):
    def setUp(self):
        self.mt5 = FakeMT5()
        self.clock = FakeClock()
        self.config = Config(Path("C:/example/terminal64.exe"), 12345678, "Demo-Server", 0.5)
        self.output = []

    def run_loop(self):
        return run_monitor(
            self.config, self.mt5, now=self.clock.now,
            wait=self.clock.wait, display=self.output.append,
        )

    def test_successful_updates_and_empty_positions(self):
        position = SimpleNamespace(
            ticket=42, symbol="XAUUSD", type=0, volume=0.1,
            price_open=2500.0, price_current=2501.0, profit=1.0,
            sl=0.0, tp=0.0,
        )
        def after_wait(count):
            if count == 1:
                self.mt5.positions = (position,)
            else:
                raise KeyboardInterrupt
        self.clock.after_wait = after_wait
        self.assertEqual(self.run_loop(), 0)
        self.assertIn("Open positions: 0", self.output[0])
        self.assertIn("#42 XAUUSD BUY", self.output[1])
        self.assertIn("UTC", self.output[1])
        self.assertEqual(self.mt5.position_calls, 2)
        self.assertEqual(self.mt5.account_calls, 4)
        self.assertEqual(self.mt5.shutdown_calls, 1)

    def test_failed_read_preserves_previous_snapshot_and_reconnects(self):
        def after_wait(count):
            if count == 1:
                self.mt5.positions = None
            elif count == 2:
                self.mt5.positions = ()
            else:
                raise KeyboardInterrupt
        self.clock.after_wait = after_wait
        self.assertEqual(self.run_loop(), 0)
        self.assertIn("STALE/RETRYING", self.output[1])
        self.assertIn("Snapshot age: 0.5s", self.output[1])
        self.assertIn("Open positions: 0", self.output[1])
        self.assertIn("LIVE", self.output[2])
        self.assertEqual(len(self.mt5.init_calls), 2)
        self.assertTrue(all(call == {"path": str(self.config.terminal_path)} for call in self.mt5.init_calls))
        self.assertEqual(self.clock.delays, [0.5, 1.0, 0.5])
        self.assertGreaterEqual(self.mt5.shutdown_calls, 2)

    def test_initial_failure_is_unavailable_and_retries(self):
        self.mt5.init_results = [False, True]
        self.clock.after_wait = lambda count: (_ for _ in ()).throw(KeyboardInterrupt()) if count == 2 else None
        self.assertEqual(self.run_loop(), 0)
        self.assertIn("UNAVAILABLE/RETRYING", self.output[0])
        self.assertIn("Account and positions: unavailable", self.output[0])
        self.assertIn("LIVE", self.output[1])
        self.assertEqual(self.mt5.position_calls, 1)
        self.assertEqual(self.clock.delays, [1.0, 0.5])

    def test_account_mismatch_stops_before_positions(self):
        self.mt5.account.server = "Other-Server"
        self.assertEqual(self.run_loop(), 1)
        self.assertIn("MONITOR STOPPED", self.output[0])
        self.assertEqual(self.mt5.position_calls, 0)
        self.assertEqual(self.mt5.shutdown_calls, 1)

    def test_account_switch_during_collection_discards_result(self):
        def switch_on_second_read(count):
            if count == 2:
                self.mt5.account.login = 99999999
        self.mt5.on_account_call = switch_on_second_read
        self.assertEqual(self.run_loop(), 1)
        self.assertEqual(self.mt5.position_calls, 1)
        self.assertFalse(any("LIVE" in line for line in self.output))
        self.assertEqual(self.mt5.shutdown_calls, 1)

    def test_non_demo_stops_before_positions(self):
        self.mt5.account.trade_mode = 2
        self.assertEqual(self.run_loop(), 1)
        self.assertEqual(self.mt5.position_calls, 0)

    def test_disconnection_marks_previous_snapshot_stale(self):
        def after_wait(count):
            if count == 1:
                self.mt5.terminal.connected = False
            else:
                raise KeyboardInterrupt
        self.clock.after_wait = after_wait
        self.assertEqual(self.run_loop(), 0)
        self.assertIn("STALE/RETRYING | MT5 disconnected", self.output[1])
        self.assertIn("Open positions: 0", self.output[1])

    def test_none_account_is_error(self):
        self.mt5.account = None
        with self.assertRaisesRegex(DiagnosticError, "Could not read account"):
            collect(self.config, self.mt5, self.clock.now)

    def test_invalid_refresh_interval_is_rejected(self):
        contents = (
            "MT5_TERMINAL_PATH=C:\\example\\terminal64.exe\n"
            "MT5_EXPECTED_LOGIN=12345678\n"
            "MT5_EXPECTED_SERVER=Demo-Server\n"
            "MT5_REFRESH_INTERVAL=nan\n"
        )
        with patch.object(Path, "is_file", return_value=True), patch.object(Path, "read_text", return_value=contents):
            with self.assertRaisesRegex(DiagnosticError, "MT5_REFRESH_INTERVAL"):
                load_config(Path(".env"))


if __name__ == "__main__":
    unittest.main()
