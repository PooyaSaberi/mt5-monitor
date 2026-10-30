import copy
import csv
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from collector_inspect import read_history, main as inspect_main
from collector_storage import Storage
from report_time import (EPOCH, TimeMapping, aware_utc, decorate_row, report_day_plan,
                         tehran, tehran_day_bounds, zone)


def milliseconds(value):
    return int((aware_utc(value) - EPOCH).total_seconds() * 1000)


def policy(basis='utc', query=None):
    # Synthetic fixture only; Europe/Helsinki is not an assertion about Alpari.
    return TimeMapping(basis, query or basis, 'Europe/Helsinki', 'synthetic test evidence',
                       '2025-01-01T00:00:00Z', '2027-01-01T00:00:00Z')


class ReportTimeTests(unittest.TestCase):
    def test_windows_timezone_data_and_no_local_timezone_dependency(self):
        import tzdata
        self.assertTrue(tzdata.__version__)
        self.assertEqual(zone('Asia/Tehran').key, 'Asia/Tehran')
        instant = aware_utc('2026-07-01T21:00:00Z')
        with patch.dict(os.environ, {'TZ': 'America/Los_Angeles'}):
            self.assertEqual(tehran(instant).isoformat(), '2026-07-02T00:30:00+03:30')
        self.assertEqual(tehran(tehran(instant)), tehran(instant))

    def test_server_wall_winter_summer_uses_event_date(self):
        winter = policy('server_wall').event(milliseconds('2026-01-15T12:00:00Z'))
        summer = policy('server_wall').event(milliseconds('2026-07-15T12:00:00Z'))
        self.assertEqual(winter['event_utc'], '2026-01-15T10:00:00+00:00')
        self.assertEqual(winter['event_tehran'], '2026-01-15T13:30:00+03:30')
        self.assertEqual(summer['event_utc'], '2026-07-15T09:00:00+00:00')
        self.assertEqual(summer['event_tehran'], '2026-07-15T12:30:00+03:30')

    def test_utc_epoch_never_subtracts_server_offset(self):
        result = policy('utc').event(milliseconds('2026-07-15T12:00:00Z'))
        self.assertEqual(result['event_utc'], '2026-07-15T12:00:00+00:00')
        self.assertEqual(result['event_tehran'], '2026-07-15T15:30:00+03:30')
        self.assertEqual(result['status'], 'verified')

    def test_unverified_out_of_coverage_and_double_conversion(self):
        raw = milliseconds('2026-07-15T12:00:00Z')
        unverified = TimeMapping().event(raw)
        self.assertEqual(unverified['status'], 'unverified')
        self.assertIsNone(unverified['event_utc'])
        self.assertIsNone(unverified['event_tehran'])
        self.assertEqual(unverified['raw_time_msc'], raw)
        outside = policy().event(milliseconds('2024-01-01T12:00:00Z'))
        self.assertEqual(outside['status'], 'outside_verified_period')
        self.assertIsNone(outside['event_utc'])
        with self.assertRaises(ValueError):
            policy().event(aware_utc('2026-01-01T00:00:00Z'))
        with self.assertRaises(ValueError):
            tehran(datetime(2026, 1, 1))

    def test_raw_payload_unchanged_and_decoration_is_idempotent(self):
        row = {'started_at': '2026-07-01T21:00:00Z',
               'fields': {'ticket': '9007199254740993', 'time_msc': milliseconds('2026-07-01T21:00:00Z')}}
        original = copy.deepcopy(row)
        decorated = decorate_row(row, policy())
        self.assertEqual(row, original)
        self.assertEqual(decorated['fields'], original['fields'])
        self.assertEqual(decorate_row(decorated, policy()), decorated)
        self.assertEqual(decorated['started_at_tehran'], '2026-07-02T00:30:00+03:30')
        unknown = decorate_row(row, TimeMapping())
        self.assertIsNotNone(unknown['started_at_tehran'])
        self.assertIsNone(unknown['broker_event_times']['time_msc']['event_tehran'])

    def test_tehran_midnight_boundaries_and_historical_tehran_dst(self):
        start, end = tehran_day_bounds('2026-07-02')
        self.assertEqual(start.isoformat(), '2026-07-01T20:30:00+00:00')
        self.assertEqual(end.isoformat(), '2026-07-02T20:30:00+00:00')
        self.assertEqual(tehran(aware_utc('2020-07-01T12:00:00Z')).utcoffset(), timedelta(hours=4, minutes=30))
        self.assertEqual(tehran(aware_utc('2020-01-01T12:00:00Z')).utcoffset(), timedelta(hours=3, minutes=30))
        historical_start, historical_end = tehran_day_bounds('2021-03-22')
        self.assertEqual((historical_end - historical_start).total_seconds(), 23 * 3600)

    def test_daily_query_basis_is_separate_and_date_dependent(self):
        winter = report_day_plan('2026-01-02', policy('utc', 'server_wall'))
        summer = report_day_plan('2026-07-02', policy('utc', 'server_wall'))
        self.assertEqual(winter['query_segments'][0]['argument_start'], '2026-01-01T22:30:00+00:00')
        self.assertEqual(summer['query_segments'][0]['argument_start'], '2026-07-01T23:30:00+00:00')
        utc = report_day_plan('2026-07-02', policy())
        self.assertEqual(utc['query_segments'][0]['argument_start'], utc['start_utc'])
        unknown = report_day_plan('2026-07-02', TimeMapping())
        self.assertEqual(unknown['query_status'], 'unverified')
        self.assertEqual(unknown['query_segments'], [])

    def test_broker_dst_fold_gap_and_query_transition_splitting(self):
        ambiguous = policy('server_wall').event(milliseconds('2026-10-25T03:30:00Z'))
        nonexistent = policy('server_wall').event(milliseconds('2026-03-29T03:30:00Z'))
        self.assertEqual(ambiguous['status'], 'ambiguous')
        self.assertEqual(nonexistent['status'], 'nonexistent')
        self.assertIsNone(ambiguous['event_utc'])
        self.assertIsNone(nonexistent['event_tehran'])
        spring = report_day_plan('2026-03-29', policy('server_wall'))
        fall = report_day_plan('2026-10-25', policy('server_wall'))
        self.assertEqual([r['broker_offset_seconds'] for r in spring['query_segments']], [7200,10800])
        self.assertEqual([r['broker_offset_seconds'] for r in fall['query_segments']], [10800,7200])
        self.assertEqual(len(spring['query_segments']), 2)

    def test_verified_policy_requires_evidence_and_scope(self):
        with self.assertRaises(ValueError):
            TimeMapping(event_basis='utc').validate()
        with self.assertRaises(ValueError):
            replace(policy('server_wall'), server_zone=None).validate()
        with self.assertRaises(ValueError):
            replace(policy(), valid_to_utc='2024-01-01T00:00:00Z').validate()


class InspectorTimeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / 'records.sqlite3'
        self.storage = Storage(self.path, ('fund','source',12345678,'Demo-Server','USD'))
        self.addCleanup(self.storage.close)

    def insert(self, ticket, stamp):
        raw = milliseconds(stamp)
        fields = {'ticket': str(ticket), 'time':raw//1000, 'time_msc':raw, 'type':2,'profit':1000}
        self.storage.connection.execute('INSERT INTO deals VALUES (?,?,?,?,?,?,?,?,?)',
            ('source',str(ticket),raw,json.dumps(fields),'fixture',1,'2026-01-01T21:00:00+00:00','2026-01-01T21:00:00+00:00',0))
        self.storage.connection.commit()

    def test_tehran_day_half_open_filter_and_raw_retention(self):
        self.insert(1,'2026-01-01T20:29:59Z')
        self.insert(2,'2026-01-01T20:30:00Z')
        self.insert(3,'2026-01-02T20:30:00Z')
        with self.assertRaises(ValueError):
            read_history(self.path,'deals',report_date='2026-01-02')
        data = read_history(self.path,'deals',mapping=policy(),report_date='2026-01-02')
        self.assertEqual([row['ticket'] for row in data['deals']], ['2'])
        event = data['deals'][0]['broker_event_times']['time_msc']
        self.assertEqual(event['event_tehran'], '2026-01-02T00:00:00+03:30')
        self.assertEqual(data['deals'][0]['time_msc'], milliseconds('2026-01-01T20:30:00Z'))

    def test_server_wall_day_filter_uses_event_not_query_basis(self):
        self.insert(1,'2026-01-01T22:29:59Z')
        self.insert(2,'2026-01-01T22:30:00Z')
        data = read_history(self.path,'deals',mapping=policy('server_wall','utc'),report_date='2026-01-02')
        self.assertEqual([row['ticket'] for row in data['deals']], ['2'])
        self.assertEqual(data['deals'][0]['broker_event_times']['time_msc']['event_utc'], '2026-01-01T20:30:00+00:00')

    def test_offline_json_csv_and_query_plan(self):
        self.insert(9007199254740993,'2026-01-01T21:00:00Z')
        mapping_path = self.root / 'mapping.json'
        from dataclasses import asdict
        mapping_path.write_text(json.dumps(asdict(policy())))
        for fmt in ('json','csv'):
            output = self.root / ('sample.'+fmt)
            self.assertEqual(inspect_main([str(self.path),'--history-table','deals','--format',fmt,
                '--time-mapping',str(mapping_path),'--output',str(output)]),0)
            self.assertIn('2026-01-02T00:30:00+03:30',output.read_text())
            self.assertIn('9007199254740993',output.read_text())
        raw = read_history(self.path,'deals')
        self.assertIsNone(raw['deals'][0]['broker_event_times']['time_msc']['event_utc'])
        self.assertEqual(raw['deals'][0]['first_seen_tehran'],'2026-01-02T00:30:00+03:30')
        output = self.root / 'plan.json'
        self.assertEqual(inspect_main(['--query-plan','2026-01-02','--output',str(output)]),0)
        self.assertEqual(json.loads(output.read_text())['query_status'],'unverified')
        with redirect_stderr(io.StringIO()):
            self.assertEqual(inspect_main([str(self.path),'--history-table','deals','--report-date','2026-01-02']),1)

    def test_collector_console_uses_tehran_and_retains_collector_utc(self):
        from account_reader import AccountObservation
        from collector import run
        from mt5_monitor import Config
        from recorder_config import RecorderConfig
        from tests.test_active_sets import SetMT5, StopAfter
        config = RecorderConfig(Config(self.root/'terminal.exe',12345678,'Demo-Server'), 'fund','source',self.path)
        observation = AccountObservation('2026-01-01T21:00:00+00:00','2026-01-01T21:00:01+00:00','USD',1,1,0,0,0,1,0)
        output = []
        with patch('collector.read_account',return_value=observation):
            run(config,SetMT5(),stop=StopAfter(1),output=output.append)
        self.assertTrue(any('2026-01-02T00:30:01+03:30 Asia/Tehran' in line and 'collector UTC 2026-01-01T21:00:01+00:00' in line for line in output))
