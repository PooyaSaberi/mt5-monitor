import copy
import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from collector_storage import Storage, StorageError
from upload_contract import serialize_request, validate_request, validate_acknowledgement
from upload_reader import read_delivery


START = '2026-01-01T00:00:00+00:00'
END = '2026-01-01T00:00:01+00:00'
BIG = 9007199254740993
IDENTITY = ('example-fund', 'example-source', 12345678, 'Demo-Server', 'USD')


def account():
    return SimpleNamespace(started_at=START, ended_at=END, currency='USD', balance=1000., equity=1001.,
                           profit=1., credit=0., margin=10., margin_free=991., margin_level=10010.)


def batch(dataset='positions', records=(), outcome='success'):
    return dict(dataset=dataset, records=records, outcome=outcome, started_at=START, ended_at=END,
                detail='PRIVATE diagnostic text and account number')


def position():
    return dict(ticket=BIG, identifier=BIG+1, symbol='XAUUSD', type=0, volume=.1,
                price_open=2000., price_current=2001., sl=0., tp=0., profit=1., swap=-.1,
                time=1, time_msc=1000, time_update=2, time_update_msc=2000,
                comment='PRIVATE comment', magic=99, reason=3, external_id='PRIVATE external',
                metadata={'login': 12345678, 'ticket': BIG, 'password': 'PRIVATE credential'})


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'source.sqlite3'
        self.store = Storage(self.path, IDENTITY)
        self.addCleanup(self.store.close)

    def request(self):
        data = read_delivery(self.path)
        events = data['events']
        return dict(contract_version=1, batch_id='batch-example', source=data['source'],
                    stream_id=data['stream_id'], first_sequence=events[0]['sequence'],
                    last_sequence=events[-1]['sequence'], events=events)

    def test_populated_records_exact_nested_identifiers_and_allowlist(self):
        order = dict(ticket=BIG+2, position_id=BIG+1, position_by_id=0, symbol='XAUUSD', type=2,
                     volume_initial=.1, volume_current=.1, price_open=2000., price_current=2001.,
                     sl=0., tp=0., price_stoplimit=0., time_setup=1, time_setup_msc=1000,
                     time_done=0, time_done_msc=0, time_expiration=0,
                     comment='PRIVATE order', magic=42)
        self.store.save_cycle(account(), START, END, [batch(records=[position()]), batch('orders', [order])])
        request = self.request()
        payload, _ = serialize_request(request)
        for private in (b'PRIVATE', b'login', b'server', b'comment', b'magic', b'reason', b'password', b'path', b'external_id'):
            self.assertNotIn(private, payload)
        events = request['events']
        obs = next(e for e in events if e['kind']=='account_observation')
        rows = {b['dataset']: b['records'][0] for b in obs['active_batches']}
        self.assertEqual(rows['positions']['ticket'], str(BIG))
        self.assertEqual(rows['positions']['identifier'], str(BIG+1))
        self.assertEqual(rows['orders']['position_id'], str(BIG+1))
        self.assertEqual(rows['orders']['position_by_id'], '0')
        self.assertEqual(rows['positions']['broker_event_times'],
                         {'status':'unverified','raw':{'time':'1','time_msc':'1000','time_update':'2','time_update_msc':'2000'}})
        self.assertNotIn('event_utc', payload.decode())
        self.assertNotIn('tehran', payload.decode())

    def test_empty_failure_absent_and_legacy(self):
        self.store.save_cycle(account(), START, END, [batch(), batch('orders', outcome='failed')])
        self.store.save_cycle(account(), START, END, [batch(outcome='identity_mismatch')])
        self.store.save_observation(account())
        self.store.save_status(START, END, 'PRIVATE unknown status', 'PRIVATE detail')
        events = read_delivery(self.path)['events']
        observations = [e for e in events if e['kind']=='account_observation']
        empty = observations[0]['active_batches'][1]  # ordered by dataset
        self.assertEqual((empty['dataset'],empty['item_count'],empty['records']), ('positions',0,[]))
        failed = observations[0]['active_batches'][0]
        self.assertIsNone(failed['item_count'])
        self.assertNotIn('records',failed)
        self.assertEqual(observations[1]['absent_datasets'],['orders'])
        self.assertIsNone(observations[2]['cycle'])
        self.assertEqual(observations[2]['absent_datasets'], ['orders','positions'])
        self.assertEqual(observations[2]['active_batches'], [])
        self.assertEqual(events[-1]['status'], 'unknown')

    def test_deterministic_bytes_digest_and_acknowledgement(self):
        self.store.save_observation(account())
        request = self.request()
        data, digest = serialize_request(request)
        reversed_keys = dict(reversed(list(request.items())))
        self.assertEqual(serialize_request(reversed_keys), (data,digest))
        self.assertEqual(digest, hashlib.sha256(data).hexdigest())
        self.assertEqual(serialize_request(self.request()), (data,digest))
        ack = dict(contract_version=1,batch_id=request['batch_id'],source_id=IDENTITY[1],
                   stream_id=request['stream_id'], accepted_through_sequence=request['last_sequence'],
                   request_sha256=digest,result='committed',received_at_utc=END)
        self.assertEqual(validate_acknowledgement(ack,request),ack)
        for key,value in [('source_id','other'),('stream_id','other'),('accepted_through_sequence','999'),
                          ('request_sha256','0'*64),('result','queued'),('batch_id','other')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_acknowledgement({**ack,key:value},request)
        changed=copy.deepcopy(request); changed['events'][0]['account']['profit']=2.
        self.assertNotEqual(serialize_request(changed)[1],digest)
        with self.assertRaises(ValueError): validate_acknowledgement({**ack,'login':'123'},request)

    def test_reject_nonfinite_and_invalid_contract_shapes(self):
        self.store.save_cycle(account(),START,END,[batch(records=[position()])])
        request=self.request()
        for value in [float('nan'),float('inf'),float('-inf'),True,'100']:
            r=copy.deepcopy(request); r['events'][0]['account']['equity']=value
            with self.subTest(value=value),self.assertRaises(ValueError): serialize_request(r)
        for mutate in [
            lambda r:r.update(login='123'),
            lambda r:r['events'][0]['account'].update(password='secret'),
            lambda r:r['events'][0]['active_batches'][0].update(item_count=0),
            lambda r:r['events'][0]['active_batches'][0].update(outcome='failed'),
            lambda r:r['events'][0].update(absent_datasets=[]),
            lambda r:r['events'][0].update(ended_at_utc='2025-01-01T00:00:00Z'),
            lambda r:r['events'][0].update(started_at_utc='2026-01-01T00:00:00'),
            lambda r:r['events'][0]['active_batches'][0]['records'][0].update(ticket=float(BIG)),
            lambda r:r['events'][0]['active_batches'][0]['records'][0].update(profit=float('nan')),
            lambda r:r['events'][0]['active_batches'][0]['records'][0].update(comment='private'),
            lambda r:r['events'][0]['active_batches'][0]['records'][0]['broker_event_times'].update(status='verified'),
            lambda r:r['events'][0]['active_batches'][0]['records'][0]['broker_event_times'].update(event_utc=END),
            lambda r:r.update(contract_version=2),
        ]:
            r=copy.deepcopy(request); mutate(r)
            with self.assertRaises(ValueError): validate_request(r)

    def test_invalid_stored_values_cannot_be_serialized(self):
        self.store.save_cycle(account(),START,END,[batch(records=[position()])])
        db=self.store.connection
        db.execute('UPDATE observations SET equity=?',(float('inf'),)); db.commit()
        with self.assertRaises(ValueError): read_delivery(self.path)
        db.execute('UPDATE observations SET equity=1001')
        db.execute('UPDATE active_batches SET item_count=0'); db.commit()
        with self.assertRaises(ValueError): read_delivery(self.path)

    def test_documented_example_and_contract_limits(self):
        document=(Path(__file__).parents[1]/'docs'/'upload-contract.md').read_text()
        examples=[json.loads(s.split('```',1)[0]) for s in document.split('```json\n')[1:]]
        request,ack=examples
        _,digest=serialize_request(request)
        ack['request_sha256']=digest
        validate_acknowledgement(ack,request)
        self.store.save_cycle(account(),START,END,[batch(records=[position()])])
        oversized=self.request()
        active=oversized['events'][0]['active_batches'][0]
        original=active['records'][0]
        active['records']=[{**original,'ticket':str(BIG+i)} for i in range(2000)]
        active['item_count']=len(active['records'])
        with self.assertRaisesRegex(ValueError,'512 KiB'): serialize_request(oversized)
        invalid=copy.deepcopy(request); invalid['events']=invalid['events']*21
        with self.assertRaisesRegex(ValueError,'1..20'): validate_request(invalid)

    def test_read_only_inspector_preserves_v3_compatibility(self):
        from collector_inspect import read_database,read_history
        self.store.connection.execute('PRAGMA user_version=3')
        self.store.connection.commit()
        digest=self.path.read_bytes()
        self.assertEqual(read_database(self.path)['schema_version'],3)
        self.assertEqual(read_history(self.path,'deals')['schema_version'],3)
        self.assertEqual(self.path.read_bytes(),digest)
        with self.assertRaises(ValueError): read_delivery(self.path)

    def test_atomic_visibility_and_source_write_rollback(self):
        observer=sqlite3.connect(self.path)
        self.addCleanup(observer.close)
        # A trigger inspects the other connection before the enclosing save commits.
        visible=[]
        self.store.connection.create_function('observe_uncommitted',0,
            lambda: visible.append(observer.execute('SELECT count(*) FROM delivery_events').fetchone()[0]) or 0)
        self.store.connection.execute('CREATE TRIGGER check_visibility AFTER INSERT ON active_batches BEGIN SELECT observe_uncommitted(); END')
        self.store.save_cycle(account(),START,END,[batch(),batch('orders')])
        self.assertEqual(visible,[0,0])
        self.assertEqual(len(read_delivery(self.path)['events']),2)
        before={t: self.store.connection.execute('SELECT count(*) FROM '+t).fetchone()[0]
                for t in ['observations','status_events','cycles','active_batches','delivery_events']}
        self.store.connection.execute("CREATE TRIGGER fail_delivery BEFORE INSERT ON delivery_events WHEN NEW.kind='collection_status' BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.store.save_cycle(account(),START,END,[batch()])
        self.assertEqual(before,{t:self.store.connection.execute('SELECT count(*) FROM '+t).fetchone()[0] for t in before})
        with self.assertRaises(sqlite3.IntegrityError): self.store.save_status(START,END,'retrying')
        with self.assertRaises(sqlite3.IntegrityError): self.store.save_observation(account())
        self.assertEqual(read_delivery(self.path)['pending_count'],2)

    def test_batch_failure_rolls_back_earlier_delivery_entry(self):
        self.store.connection.execute("CREATE TRIGGER fail_batch BEFORE INSERT ON active_batches BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.store.save_cycle(account(),START,END,[batch()])
        self.assertEqual(read_delivery(self.path)['events'],[])
        self.assertEqual(self.store.connection.execute('SELECT count(*) FROM cycles').fetchone()[0],0)

    def test_sequence_gaps_and_source_isolation(self):
        self.store.save_observation(account())
        self.store.connection.execute("UPDATE sqlite_sequence SET seq=100 WHERE name='delivery_events'")
        self.store.connection.commit()
        self.store.save_status(START,END,'retrying')
        data=read_delivery(self.path,after_sequence=2)
        self.assertEqual(data['pending_count'],1)
        self.assertEqual(data['events'][0]['sequence'],'101')
        self.assertEqual(read_delivery(self.path,after_sequence=101)['pending_count'],0)
        validate_request(self.request())  # gaps in the request are legal
        with self.assertRaisesRegex(ValueError,'binding'):
            read_delivery(self.path,expected_source=dict(fund_id='other',source_id=IDENTITY[1],currency='USD'))
        other=Storage(Path(self.tmp.name)/'other.sqlite3',('other','other',12345679,'Demo-Server','USD'))
        try: self.assertNotEqual(data['stream_id'],read_delivery(Path(self.tmp.name)/'other.sqlite3')['stream_id'])
        finally: other.close()

    def test_offline_no_mt5_dashboard_and_no_mutation(self):
        self.store.save_observation(account())
        digest=hashlib.sha256(self.path.read_bytes()).hexdigest()
        script="import sys; sys.modules.update({n:None for n in ['MetaTrader5','account_reader','collector','dashboard','fastapi','httpx','uvicorn']}); from upload_reader import read_delivery; from upload_contract import serialize_request; from pathlib import Path; assert len(read_delivery(Path(sys.argv[1]))['events'])==2"
        r=subprocess.run([sys.executable,'-c',script,str(self.path)],capture_output=True,text=True,timeout=5)
        self.assertEqual(r.returncode,0,r.stderr)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(),digest)
        with self.assertRaises(ValueError): read_delivery(self.path,limit=21)


class DeliveryMigrationTests(unittest.TestCase):
    def prepare(self,root,version):
        path=Path(root)/'old.sqlite3'; store=Storage(path,IDENTITY)
        store.save_observation(account())
        store.save_status(START,END,'retrying','private detail')
        if version>=2:
            store.save_cycle(account(),START,END,[batch(records=[position()]),batch('orders')])
        db=store.connection
        if version==3:
            # Preserve existing M3 history as well as M1/M2 data.
            db.execute('INSERT INTO deals VALUES (?,?,?,?,?,?,?,?,?)',
                       (IDENTITY[1],str(BIG),1000,'{"ticket":"9007199254740993"}','digest',1,START,END,0))
            db.execute('INSERT INTO deal_revisions VALUES (?,?,?,?,?,?,?,?)',
                       (IDENTITY[1],str(BIG),1,1000,'{"ticket":"9007199254740993"}','digest',START,'query-old'))
        for table in ('observations','status_events'): db.execute('DROP TRIGGER delivery_'+table)
        db.execute('DROP TABLE delivery_events'); db.execute('DROP TABLE delivery_stream')
        if version<3:
            for (table,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'history_%' OR name IN ('deals','deal_revisions'))").fetchall(): db.execute('DROP TABLE '+table)
        if version==1:
            db.execute('ALTER TABLE observations DROP COLUMN cycle_id')
            for table in ('active_records','active_batches','cycles'): db.execute('DROP TABLE '+table)
        db.execute('PRAGMA user_version='+str(version)); db.commit()
        before={t:db.execute('SELECT * FROM '+t).fetchall() for (t,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        store.close(); return path,before

    def test_supported_migrations_backfill_preservation_and_repeat_open(self):
        for version in (1,2,3):
            with self.subTest(version=version),tempfile.TemporaryDirectory() as root:
                path,before=self.prepare(root,version)
                store=Storage(path,IDENTITY)
                db=store.connection
                self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0],4)
                for table,rows in before.items():
                    actual=db.execute('SELECT * FROM '+table).fetchall()
                    if version==1 and table=='observations': actual=[r[:-1] for r in actual]
                    self.assertEqual(actual,rows)
                delivery=db.execute('SELECT * FROM delivery_events ORDER BY sequence').fetchall()
                expected=db.execute("SELECT kind,record_id FROM (SELECT 'account_observation' kind,observation_id record_id,ended_at FROM observations UNION ALL SELECT 'collection_status',event_id,ended_at FROM status_events) ORDER BY ended_at,kind,record_id").fetchall()
                self.assertEqual([(r[1],r[2]) for r in delivery],expected)
                stream=read_delivery(path)['stream_id']; store.close()
                store=Storage(path,IDENTITY)
                self.assertEqual(store.connection.execute('SELECT * FROM delivery_events ORDER BY sequence').fetchall(),delivery)
                self.assertEqual(read_delivery(path)['stream_id'],stream)
                store.save_status(START,END,'saved')
                self.assertEqual(read_delivery(path)['pending_count'],len(delivery)+1)
                store.close()

    def test_migration_failure_rolls_back_every_change(self):
        import delivery_stream
        original=delivery_stream.create_schema
        for version in (1,2,3):
            with self.subTest(version=version),tempfile.TemporaryDirectory() as root:
                path,before=self.prepare(root,version)
                baseline=path.read_bytes()
                def broken(db):
                    original(db)
                    raise sqlite3.OperationalError('injected after backfill and trigger creation')
                with patch('delivery_stream.create_schema',side_effect=broken):
                    with self.assertRaises(sqlite3.OperationalError): Storage(path,IDENTITY)
                self.assertEqual(path.read_bytes(),baseline)
                db=sqlite3.connect(path)
                self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0],version)
                for t,rows in before.items(): self.assertEqual(db.execute('SELECT * FROM '+t).fetchall(),rows)
                self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE name LIKE 'delivery_%'").fetchall(),[])
                db.close()
                with self.assertRaises(ValueError): read_delivery(path)

    def test_newer_schema_and_wrong_source_rejected_without_mutation(self):
        with tempfile.TemporaryDirectory() as root:
            path,_=self.prepare(root,3)
            baseline=path.read_bytes()
            with self.assertRaises(StorageError): Storage(path,('wrong',*IDENTITY[1:]))
            self.assertEqual(path.read_bytes(),baseline)
            db=sqlite3.connect(path); db.execute('PRAGMA user_version=99'); db.close()
            baseline=path.read_bytes()
            with self.assertRaises(StorageError): Storage(path,IDENTITY)
            self.assertEqual(path.read_bytes(),baseline)


if __name__=='__main__': unittest.main()
