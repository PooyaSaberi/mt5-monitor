from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from collector_storage import Storage
from tests.test_delivery import account, START, END, IDENTITY
from tests import test_delivery as delivery_fixtures
from upload_config import UploadConfig
from upload_contract import serialize_request
from upload_reader import read_delivery
from upload_state import UploadState, binding_for
from upload_status import status, format_console, collector_metrics
from uploader import execute

NOW = datetime(2026,1,1,0,1,tzinfo=timezone.utc)


class UploadStatusTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name); self.path=self.root/'collector.sqlite3'
        self.store=Storage(self.path,IDENTITY); self.addCleanup(self.store.close)
        self.cfg=UploadConfig(self.path,self.root/'uploader.sqlite3','https://example.invalid/ingest',
                              'private-test-token',dict(fund_id=IDENTITY[0],source_id=IDENTITY[1],currency='USD'))

    def state(self):
        data=read_delivery(self.path,metadata_only=True)
        state=UploadState(self.cfg.state_db,binding_for(self.cfg,data['stream_id']))
        self.addCleanup(state.close)
        return state

    def prepare(self,state):
        data=read_delivery(self.path,after_sequence=state.progress['acknowledged_sequence'])
        request=dict(contract_version=1,batch_id='test-pending',source=data['source'],stream_id=data['stream_id'],
                     events=data['events'],first_sequence=data['events'][0]['sequence'],last_sequence=data['events'][-1]['sequence'])
        state.prepare(request)
        return dict(contract_version=1,batch_id=request['batch_id'],source_id=IDENTITY[1],stream_id=data['stream_id'],
                    accepted_through_sequence=request['last_sequence'],request_sha256=serialize_request(request)[1],
                    result='committed',received_at_utc=END)

    def hashes(self):
        return {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.iterdir() if p.is_file()}

    def test_missing_state_and_empty_backlog_without_creation(self):
        before=self.hashes(); report=status(self.cfg,now=NOW)
        self.assertEqual(report['state'],'not_initialized')
        self.assertTrue(report['configuration_valid']); self.assertIsNone(report['binding_valid'])
        self.assertEqual(report['backlog'],0)
        self.assertEqual(report['collection']['freshness'],'missing')
        self.assertIsNone(report['delivery']['oldest_unacknowledged_collection']['at_utc'])
        self.assertIsNone(report['delivery']['total_unsent_serialized_bytes'])
        self.assertEqual(report['receiver']['current_reachability'],'not_checked')
        self.assertEqual(self.hashes(),before)

    def test_sequence_gaps_actual_counts_oldest_and_latest_times(self):
        self.store.save_observation(account())
        self.store.connection.execute("UPDATE sqlite_sequence SET seq=99 WHERE name='delivery_events'"); self.store.connection.commit()
        self.store.save_status('2025-12-31T23:59:00Z','2025-12-31T23:59:01Z','retrying')
        before=self.hashes(); report=status(self.cfg,now=NOW)
        self.assertEqual(report['backlog'],3)
        self.assertEqual(report['delivery']['oldest_unacknowledged_collection']['age_seconds'],120.)
        self.assertEqual(report['collection']['at_utc'],'2026-01-01T00:00:01.000000Z')
        self.assertEqual(report['collection']['age_seconds'],59.)
        self.assertEqual(report['collection']['freshness'],'stale')
        self.assertEqual(self.hashes(),before)

    def test_pending_bytes_retry_pause_and_sanitization(self):
        self.store.save_observation(account()); state=self.state(); self.prepare(state)
        due=NOW.timestamp()+15
        state.failure('http_429',due=due)
        before=self.hashes(); report=status(self.cfg,now=NOW)
        self.assertTrue(report['binding_valid'])
        self.assertEqual(report['pending']['bytes'],len(state.pending()['payload']))
        self.assertEqual(report['delivery']['retry_in_seconds'],15.)
        self.assertEqual(report['delivery']['next_retry_at_utc'],'2026-01-01T00:01:15.000000Z')
        self.assertEqual(report['delivery']['last_error'],'http_429')
        self.assertFalse(report['delivery']['paused'])
        state.failure('PRIVATE arbitrary diagnostic text',paused=True)
        after_pause=self.hashes(); report=status(self.cfg,now=NOW)
        self.assertEqual(report['state'],'paused'); self.assertTrue(report['delivery']['paused'])
        self.assertEqual(report['error'],'unknown_error')
        rendered=json.dumps(report)+format_console(report)
        for secret in ['PRIVATE','private-test-token','Demo-Server','12345678',str(self.path)]: self.assertNotIn(secret,rendered)
        self.assertEqual(self.hashes(),after_pause)
        self.assertNotEqual(before,after_pause)  # only the explicit state failure wrote data

    def test_acknowledged_data_local_time_and_receiver_receipt_separate(self):
        self.store.save_observation(account()); state=self.state(); ack=self.prepare(state)
        state.acknowledge(ack,acknowledged_at='2026-01-01T00:00:55Z')
        self.store.save_status('2026-01-01T00:00:58Z','2026-01-01T00:00:59Z','saved')
        before=self.hashes(); report=status(self.cfg,now=NOW)
        self.assertEqual(report['backlog'],1)
        self.assertEqual(report['acknowledged_sequence'],'2')
        self.assertIsNone(report['pending'])
        receiver=report['receiver']
        self.assertEqual(receiver['last_successful_acknowledgement']['age_seconds'],5.)
        self.assertEqual(receiver['receiver_receipt_at_utc'],'2026-01-01T00:00:01.000000Z')
        self.assertEqual(receiver['current_reachability'],'not_checked')
        self.assertEqual(self.hashes(),before)

    def test_old_ack_without_local_time_stays_unknown(self):
        self.store.save_observation(account()); state=self.state(); ack=self.prepare(state); state.acknowledge(ack)
        progress=state.progress; progress.pop('last_acknowledged_at_utc')
        with state.db: state._progress(progress)
        before=self.hashes(); report=status(self.cfg,now=NOW)
        self.assertEqual(report['backlog'],0)
        self.assertFalse(report['receiver']['local_ack_time_available'])
        self.assertIsNone(report['receiver']['last_successful_acknowledgement']['at_utc'])
        self.assertIsNotNone(report['receiver']['receiver_receipt_at_utc'])
        self.assertEqual(self.hashes(),before)

    def test_future_times_are_signed_and_not_fresh(self):
        future=account(); future.started_at='2026-01-01T00:01:05Z'; future.ended_at='2026-01-01T00:01:06Z'
        self.store.save_observation(future)
        report=status(self.cfg,now=NOW)
        self.assertEqual(report['collection']['time_state'],'future_timestamp')
        self.assertEqual(report['collection']['age_seconds'],-6.)
        self.assertEqual(report['collection']['freshness'],'unknown')
        self.assertEqual(report['delivery']['oldest_unacknowledged_collection']['age_seconds'],-5.)
        self.assertEqual(report['delivery']['oldest_unacknowledged_collection']['time_state'],'future_timestamp')
        self.assertTrue(report['clock']['ages_clock_dependent'])
        state=self.state(); ack=self.prepare(state); state.acknowledge(ack,acknowledged_at='2026-01-01T00:01:10Z')
        self.assertEqual(status(self.cfg,now=NOW)['receiver']['last_successful_acknowledgement']['time_state'],'future_timestamp')

    def test_recent_collection_is_independent_of_delivery_pause(self):
        self.store.save_observation(account()); state=self.state(); state.failure('http_401',paused=True)
        report=status(self.cfg,now=datetime(2026,1,1,0,0,6,tzinfo=timezone.utc))
        self.assertEqual(report['collection']['freshness'],'fresh')
        self.assertEqual(report['delivery']['state'],'paused')
        self.assertEqual(report['receiver']['current_reachability'],'not_checked')

    def test_pre_v4_reads_freshness_but_never_invents_backlog_or_migrates(self):
        for version in (1,2,3):
            with self.subTest(version=version),tempfile.TemporaryDirectory() as root:
                path,_=delivery_fixtures.DeliveryMigrationTests().prepare(root,version)
                cfg=replace(self.cfg,collector_db=path,state_db=Path(root)/'missing-state.sqlite3')
                before={p.name:p.read_bytes() for p in Path(root).iterdir() if p.is_file()}
                report=status(cfg,now=NOW)
                self.assertEqual(report['state'],'requires_v4'); self.assertIsNone(report['backlog'])
                self.assertEqual(report['schema_version'],version)
                self.assertEqual(report['collection']['age_seconds'],59.)
                self.assertEqual({p.name:p.read_bytes() for p in Path(root).iterdir() if p.is_file()},before)
                lines=[]; self.assertEqual(execute(cfg,'check',output=lines.append),2)
                self.assertEqual(json.loads(lines[0])['state'],'requires_v4')

    def test_collector_and_state_lock_contention_is_clear_and_read_only(self):
        self.store.save_observation(account()); before=self.hashes()
        self.store.connection.execute('BEGIN EXCLUSIVE')
        try:
            report=status(self.cfg,now=NOW)
            self.assertEqual(report['state'],'collector_busy'); self.assertIsNone(report['backlog'])
            self.assertEqual(report['collection']['freshness'],'unknown')
        finally: self.store.connection.rollback()
        self.assertEqual(self.hashes(),before)
        state=self.state(); state.db.execute('BEGIN EXCLUSIVE')
        try:
            report=status(self.cfg,now=NOW)
            self.assertEqual(report['state'],'state_busy'); self.assertIsNone(report['backlog'])
            self.assertIsNotNone(report['collection']['at_utc'])
        finally: state.db.rollback()

    def test_content_inconsistency_does_not_erase_collection_visibility(self):
        self.store.save_observation(account()); state=self.state(); self.prepare(state)
        self.store.connection.execute('UPDATE observations SET equity=3000'); self.store.connection.commit()
        report=status(self.cfg,now=NOW)
        self.assertEqual(report['state'],'inconsistent'); self.assertFalse(report['binding_valid'])
        self.assertIsNone(report['backlog']); self.assertIsNotNone(report['pending']['bytes'])
        self.assertIsNotNone(report['collection']['at_utc'])

    def test_configuration_and_source_binding_validity(self):
        self.store.save_observation(account())
        cfg=replace(self.cfg,endpoint='http://example.invalid/not-loopback')
        report=status(cfg,now=NOW)
        self.assertFalse(report['configuration_valid']); self.assertEqual(report['state'],'invalid_configuration')
        self.assertIsNone(report['backlog']); self.assertEqual(report['collection']['freshness'],'unknown')
        cfg=replace(self.cfg,expected_source={**self.cfg.expected_source,'fund_id':'another-alias'})
        report=status(cfg,now=NOW)
        self.assertTrue(report['configuration_valid']); self.assertFalse(report['binding_valid'])
        self.assertEqual(report['binding_state'],'source_mismatch')
        self.assertEqual(report['source']['fund_id'],'another-alias')
        self.assertEqual(report['collection']['freshness'],'unknown')

    def test_ack_visibility_metadata_rolls_back_with_failed_progress_write(self):
        self.store.save_observation(account()); state=self.state(); ack=self.prepare(state)
        state.db.execute("CREATE TRIGGER fail_update BEFORE UPDATE ON control BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(sqlite3.Error): state.acknowledge(ack,acknowledged_at='2026-01-01T00:00:55Z')
        self.assertNotIn('last_acknowledged_at_utc',state.progress)
        self.assertIsNone(status(self.cfg,now=NOW)['receiver']['last_successful_acknowledgement']['at_utc'])
        self.assertIsNotNone(state.pending())

    def test_no_backlog_serialization_for_metrics_and_budget_failure(self):
        self.store.save_observation(account())
        with patch('upload_reader.serialize_active_record',side_effect=AssertionError('serialized backlog')):
            self.assertEqual(status(self.cfg,now=NOW)['backlog'],2)
        with patch('upload_status.time.monotonic',side_effect=[0.]+[5.]*10000):
            # Enough rows to exercise SQLite's cooperative progress callback.
            with self.store.connection:
                self.store.connection.executemany('INSERT INTO status_events VALUES (?,?,?,?,?)',
                    [(f'e{i}',START,END,'saved',None) for i in range(1000)])
            report=status(self.cfg,now=NOW)
            self.assertEqual(report['state'],'collector_read_budget_exceeded')
            self.assertIsNone(report['backlog'])

    def test_console_json_and_cli_without_network_or_mt5_imports(self):
        self.store.save_observation(account()); before=self.hashes()
        rendered=[]
        self.assertEqual(execute(self.cfg,'status',output=rendered.append,output_format='text'),0)
        self.assertIn('Collection:',rendered[0]); self.assertIn('Delivery:',rendered[0]); self.assertIn('Receiver:',rendered[0])
        self.assertIn('reachability: not checked',rendered[0])
        output=[]; execute(self.cfg,'status',output=output.append)
        self.assertEqual(json.loads(output[0])['status_version'],2)
        self.assertEqual(self.hashes(),before)

        cfgfile=self.root/'status-config.json'
        cfgfile.write_text(json.dumps({**self.cfg.__dict__,'collector_db':str(self.path),'state_db':str(self.cfg.state_db)}))
        before=self.hashes()
        code="import sys; sys.modules.update({n:None for n in ['socket','ssl','http.client','urllib.request','upload_http','MetaTrader5','mt5_monitor','collector','terminal_lock','account_reader','dashboard','fastapi','httpx']}); from uploader import main; raise SystemExit(main(['status','--config',sys.argv[1],'--format','json']))"
        result=subprocess.run([sys.executable,'-c',code,str(cfgfile)],capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads(result.stdout)['receiver']['current_reachability'],'not_checked')
        self.assertEqual(self.hashes(),before)

    def test_malformed_configuration_returns_sanitized_machine_error(self):
        cfgfile=self.root/'bad.json'; cfgfile.write_text('{PRIVATE INVALID CONFIG')
        before=self.hashes()
        result=subprocess.run([sys.executable,'uploader.py','status','--config',str(cfgfile)],
                              capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode,2)
        report=json.loads(result.stdout)
        self.assertEqual(report['state'],'unavailable')
        self.assertIsNone(report['configuration_valid'])
        self.assertEqual(report['error'],'local_validation_failed')
        self.assertNotIn('PRIVATE',result.stdout)
        self.assertEqual(self.hashes(),before)

    def test_concurrent_recorder_activity_uses_consistent_read_only_snapshots(self):
        done=threading.Event(); failures=[]
        def writer():
            try:
                store=Storage(self.path,IDENTITY)
                try:
                    for _ in range(20): store.save_observation(account())
                finally: store.close()
            except Exception as exc: failures.append(type(exc).__name__)
            finally: done.set()
        thread=threading.Thread(target=writer,daemon=True); thread.start()
        counts=[]
        for _ in range(10):
            report=status(self.cfg,now=NOW)
            if report['backlog'] is not None: counts.append(report['backlog']); self.assertEqual(report['backlog']%2,0)
        self.assertTrue(done.wait(3)); thread.join(3)
        self.assertEqual(failures,[])
        self.assertEqual(counts,sorted(counts))
        self.assertEqual(status(self.cfg,now=NOW)['backlog'],40)


if __name__=='__main__': unittest.main()
