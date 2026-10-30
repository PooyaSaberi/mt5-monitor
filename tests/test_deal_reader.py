import unittest
from collections import namedtuple
from types import SimpleNamespace
from deal_reader import parse_utc, read_deals
from mt5_monitor import AccountGuardError, DiagnosticError
from tests.test_recorder import FakeMT5


class DealReaderTests(unittest.TestCase):
    def setUp(self):
        self.mt5 = FakeMT5()
        self.config = SimpleNamespace(expected_login=12345678, expected_server='Demo-Server')
        self.start = parse_utc('2026-01-01T00:00:00Z')
        self.end = parse_utc('2026-01-02T00:00:00Z')

    def test_exact_identifiers_and_unknown_type(self):
        Deal = namedtuple('Deal', 'ticket order time time_msc type entry magic position_id reason volume price commission swap profit fee symbol comment external_id broker_id')
        epoch = int(self.start.timestamp()) + 10
        self.mt5.history_deals_get = lambda *args: (Deal(9007199254740993, 0, epoch, epoch * 1000, 999, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '', 'extra', '', 9007199254740994),)
        rows = read_deals(self.config, self.mt5, self.start, self.end)
        self.assertEqual(rows[0]['ticket'], '9007199254740993')
        self.assertEqual(rows[0]['type'], 999)
        self.assertEqual(rows[0]['broker_id'], '9007199254740994')

    def test_empty_and_failed_are_distinct(self):
        self.mt5.history_deals_get = lambda *args: ()
        self.assertEqual(read_deals(self.config, self.mt5, self.start, self.end), ())
        self.mt5.history_deals_get = lambda *args: None
        with self.assertRaises(DiagnosticError):
            read_deals(self.config, self.mt5, self.start, self.end)

    def test_identity_switch_rejected(self):
        def query(*args):
            self.mt5.account.server = 'Other'
            return ()
        self.mt5.history_deals_get = query
        with self.assertRaises(AccountGuardError):
            read_deals(self.config, self.mt5, self.start, self.end)
