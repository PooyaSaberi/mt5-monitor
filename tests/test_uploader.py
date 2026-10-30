import copy
from dataclasses import replace
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from collector_storage import Storage
from tests.test_delivery import account, batch, position, START, END, IDENTITY, BIG
from upload_config import UploadConfig, endpoint_parts, load_upload_config
from upload_contract import serialize_request
from upload_http import DeliveryFailure, post
from upload_lock import UploadLock, source_lock_path
from upload_reader import read_delivery
from upload_state import UploadState, binding_for, inspect_state
from upload_test_receiver import Receiver, create_server
from uploader import Uploader, execute, status


def acknowledgement(payload):
    request=json.loads(payload)
    return dict(contract_version=1,batch_id=request['batch_id'],source_id=request['source']['source_id'],
                stream_id=request['stream_id'],accepted_through_sequence=request['last_sequence'],
                request_sha256=hashlib.sha256(payload).hexdigest(),result='committed',received_at_utc=END)


class UploaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name); self.path=self.root/'collector.sqlite3'
        self.store=Storage(self.path,IDENTITY); self.addCleanup(self.store.close)
        self.store.save_cycle(account(),START,END,[batch(records=[position()]),batch('orders')])
        self.cfg=UploadConfig(self.path,self.root/'upload.sqlite3','http://127.0.0.1:1/ingest/v1/batches',
            'test-token-A',dict(fund_id=IDENTITY[0],source_id=IDENTITY[1],currency='USD'),
            local_test=True,timeout=.3,attempt_timeout=.6,retry_base=.01,retry_max=.1).validate()
        self.now=1000.

    def state(self,cfg=None):
        cfg=cfg or self.cfg
        stream=read_delivery(cfg.collector_db,metadata_only=True)['stream_id']
        state=UploadState(cfg.state_db,binding_for(cfg,stream)); self.addCleanup(state.close)
        return state

    def engine(self,state,send=None,cfg=None):
        return Uploader(cfg or self.cfg,state,send=send or (lambda c,p:acknowledgement(p)),
                        clock=lambda:self.now,jitter=lambda:.5)

    def server(self,receiver=None):
        receiver=receiver or Receiver(self.root/'receiver.sqlite3',{IDENTITY[1]:dict(token='test-token-A',fund_id=IDENTITY[0],currency='USD')})
        server=create_server(receiver)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01},daemon=True); thread.start()
        def cleanup(): server.shutdown(); server.server_close(); thread.join(2)
        self.addCleanup(cleanup)
        return server,replace(self.cfg,endpoint=f'http://127.0.0.1:{server.server_port}/ingest/v1/batches')

    def test_success_gaps_and_reader_transaction_closed_before_fake_http(self):
        self.store.connection.execute("UPDATE sqlite_sequence SET seq=100 WHERE name='delivery_events'"); self.store.connection.commit()
        self.store.save_status(START,END,'retrying')
        state=self.state()
        def send(config,payload):
            # Would fail under a retained rollback-journal read transaction.
            other=sqlite3.connect(self.path,timeout=.1)
            try:
                other.execute('INSERT INTO status_events VALUES (?,?,?,?,?)',('during-http',START,END,'saved',None)); other.commit()
            finally: other.close()
            return acknowledgement(payload)
        engine=self.engine(state,send)
        self.assertEqual(engine.step(),'acknowledged')
        self.assertEqual(state.progress['acknowledged_sequence'],101)
        self.assertEqual(status(self.cfg)['backlog'],1)
        self.assertIsNone(state.pending())

    def test_read_budget_retry_reduces_batch_without_progress_or_network(self):
        state=self.state(); called=[]; original=read_delivery
        def budget(*args,**kwargs):
            if kwargs.get('limit')==20: raise sqlite3.OperationalError('interrupted')
            return original(*args,**kwargs)
        engine=self.engine(state,lambda c,p:called.append(p) or acknowledgement(p))
        with patch('upload_integrity.read_delivery',side_effect=budget):
            self.assertEqual(engine.step(),'retrying')
            self.assertEqual(engine.read_limit,10); self.assertEqual(called,[])
            self.assertEqual(state.progress['acknowledged_sequence'],0)
            self.now+=1
            self.assertEqual(engine.step(),'acknowledged')

    def test_run_clean_stop_releases_uploader_ownership(self):
        class Stop:
            stopped=False
            def is_set(self): return self.stopped
            def wait(self,delay): self.stopped=True
        self.assertEqual(execute(self.cfg,'run',stop=Stop(),output=lambda _:None,
                                 send=lambda c,p:acknowledgement(p)),0)
        with UploadLock(source_lock_path(self.path)): pass
        with UploadLock(str(self.cfg.state_db)+'.lock'): pass
        self.assertIsNone(inspect_state(self.cfg.state_db)['pending'])

    def test_fake_retry_backoff_and_exact_bytes_after_reopen(self):
        state=self.state(); sent=[]
        def fail(c,p): sent.append(p); raise DeliveryFailure('transport_timeout',True)
        engine=self.engine(state,fail)
        self.assertEqual(engine.step(),'retrying'); pending=state.pending()
        self.assertEqual(state.progress['acknowledged_sequence'],0)
        self.assertEqual(engine.step(),'backoff'); self.assertEqual(len(sent),1)
        reopened=self.state(); self.now+=1
        def succeed(c,p): sent.append(p); return acknowledgement(p)
        self.assertEqual(self.engine(reopened,succeed).step(),'acknowledged')
        self.assertEqual(sent,[pending['payload'],pending['payload']])
        self.assertEqual(reopened.progress['attempts'],0)

    def test_invalid_acknowledgements_pause_keep_pending(self):
        state=self.state()
        for mutate in [lambda a:a.pop('request_sha256'),lambda a:a.update(result='queued'),
                       lambda a:a.update(accepted_through_sequence='1'),lambda a:a.update(source_id='other'),
                       lambda a:a.update(stream_id='wrong'),lambda a:a.update(batch_id='wrong'),
                       lambda a:a.update(request_sha256='0'*64),lambda a:a.update(extra='private')]:
            state.resume()
            def send(c,p):
                a=acknowledgement(p); mutate(a); return a
            self.assertEqual(self.engine(state,send).step(),'paused')
            self.assertEqual(state.progress['acknowledged_sequence'],0)
            self.assertIsNotNone(state.pending())
            self.assertEqual(state.progress['error'],'invalid_acknowledgement')

    def test_restore_same_file_id_same_stream_anchor_detects_rollback(self):
        state=self.state(); self.assertEqual(self.engine(state).step(),'acknowledged')
        # Simulate old content restored in-place: file identity and stream unchanged.
        self.store.connection.execute('DELETE FROM delivery_events'); self.store.connection.commit()
        called=[]
        self.assertEqual(self.engine(state,lambda c,p:called.append(p)).step(),'paused')
        self.assertEqual(called,[])
        self.assertEqual(state.progress['acknowledged_sequence'],2)

    def test_changed_acknowledged_content_and_pending_content_detected(self):
        for pending in (False,True):
            with self.subTest(pending=pending):
                state_path=self.root/('pending.sqlite3' if pending else 'ack.sqlite3')
                cfg=replace(self.cfg,state_db=state_path); state=self.state(cfg)
                if pending:
                    def fail(c,p): raise DeliveryFailure('network_error',True)
                    self.engine(state,fail,cfg).step(); self.now+=1
                    self.store.connection.execute('UPDATE observations SET equity=2000')
                else:
                    self.engine(state,cfg=cfg).step()
                    self.store.connection.execute("UPDATE status_events SET status='retrying'")
                self.store.connection.commit()
                self.assertEqual(self.engine(state,cfg=cfg).step(),'paused')
                self.assertEqual(state.progress['error'],'collector_or_anchor_inconsistent')

    def test_replacement_stream_destination_source_binding_changes(self):
        state=self.state(); self.engine(state).step()
        for config in [replace(self.cfg,endpoint='http://127.0.0.1:2/ingest/v1/batches'),
                       replace(self.cfg,expected_source={**self.cfg.expected_source,'fund_id':'other'})]:
            with self.assertRaises(ValueError): UploadState(config.state_db,binding_for(config,state.binding['stream_id']))
        self.store.connection.execute("UPDATE delivery_stream SET stream_id='new-stream'"); self.store.connection.commit()
        self.assertEqual(self.engine(state).step(),'paused')

    def test_file_replacement_detection_even_if_contents_identical(self):
        state=self.state(); self.engine(state).step()
        state.binding  # persisted file identity
        self.store.close()
        clone=self.root/'clone.sqlite3'; shutil.copyfile(self.path,clone); clone.replace(self.path)
        self.assertEqual(self.engine(state).step(),'paused')

    def test_state_write_failure_before_send_and_after_remote_commit(self):
        state=self.state(); called=[]
        state.db.execute("CREATE TRIGGER fail_prepare BEFORE INSERT ON pending BEGIN SELECT RAISE(ABORT,'disk failure'); END")
        with self.assertRaises(sqlite3.Error): self.engine(state,lambda c,p:called.append(p)).step()
        self.assertEqual(called,[]); self.assertEqual(state.progress['acknowledged_sequence'],0)
        state.db.execute('DROP TRIGGER fail_prepare')
        def send(c,p):
            called.append(p)
            state.db.execute("CREATE TRIGGER fail_ack BEFORE UPDATE ON control BEGIN SELECT RAISE(ABORT,'disk failure'); END")
            return acknowledgement(p)
        with self.assertRaises(sqlite3.Error): self.engine(state,send).step()
        self.assertEqual(state.progress['acknowledged_sequence'],0); self.assertIsNotNone(state.pending())
        state.db.execute('DROP TRIGGER fail_ack'); self.now+=1
        self.assertEqual(self.engine(state).step(),'acknowledged')

    def test_duplicate_uploader_real_process_and_different_state_same_source(self):
        lock=UploadLock(source_lock_path(self.path))
        try:
            script='from pathlib import Path; from upload_lock import UploadLock,source_lock_path; import sys; lock=UploadLock(source_lock_path(Path(sys.argv[1])))'
            child=subprocess.run([sys.executable,'-c',script,str(self.path)],capture_output=True,timeout=5)
            self.assertNotEqual(child.returncode,0); self.assertIn(b'Another uploader',child.stderr)
            with self.assertRaises(ValueError): execute(replace(self.cfg,state_db=self.root/'other-state.sqlite3'),'once',output=lambda _:None)
        finally: lock.close()
        self.assertEqual(execute(self.cfg,'once',send=lambda c,p:acknowledgement(p),output=lambda _:None),0)

    def test_check_status_are_offline_and_do_not_create_state_or_locks(self):
        before=hashlib.sha256(self.path.read_bytes()).hexdigest()
        def forbidden(*args): self.fail('check/status performed ingestion')
        for command in ('check','status'):
            self.assertEqual(execute(self.cfg,command,output=lambda _:None,send=forbidden),0)
        self.assertFalse(self.cfg.state_db.exists()); self.assertFalse(Path(str(self.cfg.state_db)+'.lock').exists())
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(),before)
        self.assertEqual(status(self.cfg)['backlog'],2)

    def test_actual_loopback_concurrent_recording_reading_and_uploading(self):
        server,cfg=self.server(); state=self.state(cfg); engine=self.engine(state,post,cfg)
        start=threading.Event(); done=threading.Event(); failures=[]
        def writer():
            try:
                start.wait(2)
                store=Storage(self.path,IDENTITY)
                try:
                    for _ in range(30): store.save_status(START,END,'saved')
                finally: store.close()
            except Exception as exc: failures.append(type(exc).__name__)
            finally: done.set()
        thread=threading.Thread(target=writer,daemon=True); thread.start(); start.set()
        try:
            for _ in range(15):
                self.now+=1
                result=engine.step()
                self.assertNotEqual(result,'paused')
                if done.is_set() and read_delivery(self.path,after_sequence=state.progress['acknowledged_sequence'])['pending_count']==0: break
            self.assertTrue(done.wait(2)); self.assertEqual(failures,[])
            for _ in range(5): self.now+=1; engine.step()
            self.assertEqual(status(cfg)['backlog'],0)
            db=sqlite3.connect(server.receiver.path)
            self.assertEqual(db.execute('SELECT count(*) FROM records').fetchone()[0],32); db.close()
        finally: thread.join(2)

    def test_large_multi_event_batch_shrinks_without_skipping(self):
        rows=[{**position(),'ticket':BIG+i} for i in range(800)]
        self.store.save_cycle(account(),START,END,[batch(records=rows)])
        self.store.save_cycle(account(),START,END,[batch(records=rows)])
        state=self.state(); seen=[]
        def send(c,p): seen.extend(e['sequence'] for e in json.loads(p)['events']); return acknowledgement(p)
        engine=self.engine(state,send)
        for _ in range(4):
            self.assertNotEqual(engine.step(),'paused')
        expected=[e['sequence'] for e in read_delivery(self.path)['events']]
        self.assertEqual(seen,expected)
        self.assertEqual(state.progress['acknowledged_sequence'],6)

    def test_pending_retry_not_blocked_by_later_invalid_record(self):
        state=self.state()
        def lost(c,p): raise DeliveryFailure('transport_timeout',True)
        self.engine(state,lost).step()
        bad=account(); bad.equity=float('inf'); self.store.save_observation(bad)
        self.now+=1
        self.assertEqual(self.engine(state).step(),'acknowledged')
        self.assertEqual(state.progress['acknowledged_sequence'],2)
        self.assertEqual(self.engine(state).step(),'paused')

    def test_https_certificate_verification_and_request_size_without_network(self):
        import ssl
        cfg=replace(self.cfg,endpoint='https://example.invalid/ingest',local_test=False)
        calls=[]
        class Connection:
            def __init__(self,host,port,**kwargs): calls.append(kwargs); self.sock=SimpleNamespace(shutdown=lambda _:None)
            def connect(self): pass
            def request(self,*args,**kwargs): pass
            def getresponse(self): return SimpleNamespace(status=200,getheader=lambda _:None,read=lambda _:b'{}')
            def close(self): pass
        with patch('upload_http.http.client.HTTPSConnection',Connection):
            self.assertEqual(post(cfg,b'{}'),{})
        self.assertEqual(calls[0]['context'].verify_mode,ssl.CERT_REQUIRED)
        self.assertTrue(calls[0]['context'].check_hostname)
        with self.assertRaises(DeliveryFailure) as caught: post(cfg,b'x'*(512*1024+1))
        self.assertEqual(caught.exception.code,'request_too_large')

    def test_paused_status_visibility_and_explicit_resume(self):
        state=self.state()
        self.assertEqual(self.engine(state,lambda c,p:{}).step(),'paused')
        view=status(self.cfg)
        self.assertEqual(view['state'],'paused'); self.assertEqual(view['error'],'invalid_acknowledgement')
        self.assertEqual(execute(self.cfg,'check',output=lambda _:None),2)
        self.assertEqual(execute(self.cfg,'once',send=lambda c,p:acknowledgement(p),resume=True,output=lambda _:None),0)
        self.store.connection.execute('DELETE FROM delivery_events'); self.store.connection.commit()
        view=status(self.cfg)
        self.assertEqual(view['state'],'inconsistent'); self.assertIsNone(view['backlog'])

    def test_oversized_individual_event_pauses_without_sending(self):
        rows=[{**position(),'ticket':BIG+i} for i in range(2000)]
        self.store.save_cycle(account(),START,END,[batch(records=rows)])
        state=self.state(); self.assertEqual(self.engine(state).step(),'acknowledged')
        self.assertEqual(state.progress['acknowledged_sequence'],2)
        called=[]
        self.assertEqual(self.engine(state,lambda c,p:called.append(p)).step(),'paused')
        self.assertEqual(called,[]); self.assertIsNone(state.pending())

    def test_configuration_security_and_no_mt5_dashboard_import(self):
        for endpoint,local in [('http://example.com/ingest',True),('http://localhost/ingest',True),
                              ('http://127.0.0.1/ingest',False),('https://token@example.com/',False),
                              ('https://example.com/?token=x',False)]:
            with self.assertRaises(ValueError): endpoint_parts(endpoint,local)
        self.assertEqual(endpoint_parts('https://example.invalid/ingest').scheme,'https')
        config_file=self.root/'uploader.json'; config_file.write_text(json.dumps(self.config_json(self.cfg)))
        self.assertEqual(load_upload_config(config_file),self.cfg)
        script="import sys; sys.modules.update({n:None for n in ['MetaTrader5','mt5_monitor','collector','terminal_lock','account_reader','dashboard','fastapi','httpx']}); from uploader import main; raise SystemExit(main(['check','--config',sys.argv[1]]))"
        r=subprocess.run([sys.executable,'-c',script,str(config_file)],capture_output=True,text=True,timeout=5)
        self.assertEqual(r.returncode,0,r.stderr)
        with self.assertRaises(ValueError): replace(self.cfg,state_db=self.path).validate()

    @staticmethod
    def config_json(cfg):
        return {**cfg.__dict__,'collector_db':str(cfg.collector_db),'state_db':str(cfg.state_db)}

    def test_actual_loopback_delivery_and_identical_replay(self):
        server,cfg=self.server(); state=self.state(cfg)
        captured=[]; original=server.receiver.ingest
        def capture(p,a): captured.append(p); return original(p,a)
        server.receiver.ingest=capture
        self.assertEqual(self.engine(state,post,cfg).step(),'acknowledged')
        first_ack=state.progress['last_ack']
        self.assertEqual(post(cfg,captured[0]),first_ack)
        db=sqlite3.connect(server.receiver.path)
        self.assertEqual(db.execute('SELECT count(*) FROM records').fetchone()[0],2)
        self.assertEqual(db.execute('SELECT count(*) FROM batches').fetchone()[0],1); db.close()

    def test_actual_loopback_lost_ack_and_process_restart_pending_bytes(self):
        server,cfg=self.server(); captured=[]; original=server.receiver.ingest
        def capture(p,a): captured.append(p); return original(p,a)
        server.receiver.ingest=capture
        def drop(ack): server.after_commit=None; return False
        server.after_commit=drop
        config_file=self.root/'private.json'; config_file.write_text(json.dumps(self.config_json(cfg)))
        command=[sys.executable,'uploader.py','once','--config',str(config_file)]
        first=subprocess.run(command,capture_output=True,text=True,timeout=5)
        self.assertEqual(first.returncode,1,first.stdout)
        view=inspect_state(cfg.state_db); self.assertIsNotNone(view['pending']); self.assertEqual(view['progress']['acknowledged_sequence'],0)
        second=subprocess.run(command,capture_output=True,text=True,timeout=5)
        self.assertEqual(second.returncode,0,second.stdout)
        self.assertEqual(len(captured),2); self.assertEqual(captured[0],captured[1])
        self.assertIsNone(inspect_state(cfg.state_db)['pending'])

    def test_actual_loopback_timeout_after_commit(self):
        server,cfg=self.server(); cfg=replace(cfg,timeout=.04,attempt_timeout=.1)
        done=threading.Event()
        def delay(ack):
            server.after_commit=None
            done.wait(.12)
            return True
        server.after_commit=delay
        state=self.state(cfg); engine=self.engine(state,post,cfg)
        self.assertEqual(engine.step(),'retrying'); pending=state.pending()['payload']
        db=sqlite3.connect(server.receiver.path); self.assertEqual(db.execute('SELECT count(*) FROM batches').fetchone()[0],1); db.close()
        self.now+=1; self.assertEqual(engine.step(),'acknowledged')
        self.assertEqual(state.progress['last_ack']['request_sha256'],hashlib.sha256(pending).hexdigest())
        done.set()

    def test_actual_loopback_auth_conflicts_and_source_isolation(self):
        sources={IDENTITY[1]:dict(token='test-token-A',fund_id=IDENTITY[0],currency='USD'),
                 'other-source':dict(token='test-token-B',fund_id='other-fund',currency='USD')}
        receiver=Receiver(self.root/'receiver.sqlite3',sources); server,cfg=self.server(receiver)
        state=self.state(cfg)
        self.assertEqual(self.engine(state,post,replace(cfg,token='wrong-test-token')).step(),'paused')
        self.assertEqual(state.progress['error'],'http_401'); payload=state.pending()['payload']
        request=json.loads(payload)
        wrong_fund=copy.deepcopy(request); wrong_fund['source']['fund_id']='other-fund'
        with self.assertRaises(DeliveryFailure) as caught: post(cfg,serialize_request(wrong_fund)[0])
        self.assertEqual(caught.exception.code,'http_403')
        other=copy.deepcopy(request); other['source'].update(source_id='other-source',fund_id='other-fund')
        with self.assertRaises(DeliveryFailure): post(cfg,serialize_request(other)[0])
        ack=post(cfg,payload)
        conflict=copy.deepcopy(request); conflict['events'][0]['account']['equity']=999
        with self.assertRaises(DeliveryFailure) as caught: post(cfg,serialize_request(conflict)[0])
        self.assertEqual(caught.exception.code,'http_409')
        conflict['batch_id']='different-envelope'
        with self.assertRaises(DeliveryFailure): post(cfg,serialize_request(conflict)[0])
        other_cfg=replace(cfg,token='test-token-B')
        self.assertEqual(post(other_cfg,serialize_request(other)[0])['source_id'],'other-source')
        # Regrouped identical records deduplicate independently of envelope ID.
        request['batch_id']='new-envelope'; post(cfg,serialize_request(request)[0])
        db=sqlite3.connect(receiver.path); self.assertEqual(db.execute('SELECT count(*) FROM records').fetchone()[0],4); db.close()
        # Restart/reconfigure cannot silently repurpose a persisted source alias.
        rebound=Receiver(receiver.path,{IDENTITY[1]:dict(token='test-token-A',fund_id='other-fund',currency='USD')})
        self.assertEqual(rebound.ingest(serialize_request(wrong_fund)[0],'Bearer test-token-A')[0],409)

    def test_actual_loopback_redirect_oversized_and_malformed_responses(self):
        class Adversarial(BaseHTTPRequestHandler):
            mode='redirect'
            def log_message(self,*args): pass
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                if self.mode=='redirect':
                    self.send_response(302); self.send_header('Location','http://127.0.0.1:1/credential-sink'); body=b''
                elif self.mode in ('large','large_unknown'): self.send_response(200); body=b'x'*(65537)
                elif self.mode=='partial': self.send_response(200); body=b'{"result":"committed"}'
                elif self.mode=='retry': self.send_response(429); self.send_header('Retry-After','2'); body=b'private body'
                else: self.send_response(200); body=b'{not JSON private body'
                if self.mode!='large_unknown': self.send_header('Content-Length',str(len(body)))
                self.end_headers()
                try:self.wfile.write(body)
                except OSError:pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Adversarial); server.daemon_threads=True
        t=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01},daemon=True); t.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),t.join(2)))
        cfg=replace(self.cfg,endpoint=f'http://127.0.0.1:{server.server_port}/ingest/v1/batches')
        state=self.state(cfg)
        for mode,code in [('redirect','redirect_rejected'),('large','response_too_large'),('large_unknown','response_too_large'),
                          ('partial','invalid_acknowledgement'),('bad','invalid_acknowledgement'),('retry','http_429')]:
            Adversarial.mode=mode; state.resume()
            result=self.engine(state,post,cfg).step()
            self.assertEqual(result,'retrying' if mode=='retry' else 'paused')
            self.assertEqual(state.progress['error'],code)
            self.assertEqual(state.progress['acknowledged_sequence'],0)
            self.assertIsNotNone(state.pending())

    def test_actual_loopback_receiver_commit_failure_has_no_ack(self):
        server,cfg=self.server(); state=self.state(cfg)
        db=sqlite3.connect(server.receiver.path)
        db.execute("CREATE TRIGGER fail_commit BEFORE INSERT ON batches BEGIN SELECT RAISE(ABORT,'injected'); END"); db.close()
        self.assertEqual(self.engine(state,post,cfg).step(),'retrying')
        db=sqlite3.connect(server.receiver.path)
        self.assertEqual(db.execute('SELECT count(*) FROM records').fetchone()[0],0)
        db.close(); self.assertEqual(state.progress['acknowledged_sequence'],0)


if __name__=='__main__': unittest.main()
