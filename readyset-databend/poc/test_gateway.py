import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest

from gateway import Replica, load_declaration, safe_session
from m2 import Engine
from unittest.mock import Mock


class SessionTest(unittest.TestCase):
    def test_plain_autocommit_allowed(self):
        self.assertTrue(safe_session({'sql': 'select 1'}))
        self.assertTrue(safe_session({'sql': 'select 1', 'session': {'database': 'poc', 'txn_state': 'AutoCommit', 'settings': {}}}))

    def test_opaque_state_and_special_options_bypass(self):
        for s in [{'txn_state': 'Active'}, {'settings': {'timezone': 'Asia/Shanghai'}},
                  {'role': 'other'}, {'internal': '{}'}, {'need_sticky': True}, {'catalog': 'other'}, None]:
            self.assertFalse(safe_session({'sql': 'select 1', 'session': s}), str(s))
        for body in [{'sql': 'select 1', 'stage_attachment': {}}, {'sql': 'select 1', 'pagination': {'max_rows_per_page': 0}},
                     {'sql': 'select 1', 'pagination': {'unknown': 1}}]:
            self.assertFalse(safe_session(body))

    def test_malformed_session_fields_never_use_local_results(self):
        for s in [{'settings':[]},{'settings':None},{'need_sticky':0},{'internal':''},{'internal':{}}, {'internal':123}]:
            self.assertFalse(safe_session({'sql':'select 1','session':s}),str(s))

    def test_known_default_role_and_routing_metadata_are_safe(self):
        s={'database':'poc','role':'account_admin','internal':json.dumps({'last_node_id':'n','last_query_ids':['q']})}
        self.assertTrue(safe_session({'sql':'select 1','session':s},'account_admin'))
        self.assertFalse(safe_session({'sql':'select 1','session':s},'other'))
        s['internal']=json.dumps({'temp_tables':['secret']})
        self.assertFalse(safe_session({'sql':'select 1','session':s},'account_admin'))

    def test_invalid_declarations_fail_before_source_calls(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'config.json'
            for d in [dict(database='business', table='users', queries=[]),
                      dict(database='readyset_gateway_poc', table='bad;name', queries=[]),
                      dict(database='readyset_gateway_poc', table='users', queries=[{'name': 'x', 'sql':'SELECT 1'}]*2)]:
                p.write_text(json.dumps(d))
                with self.assertRaises(ValueError):
                    load_declaration(p)


class FakeEngine:
    def __init__(self, matched=True):
        self.calls = []
        self.matched = matched

    def request(self, message):
        self.calls.append(message)
        return {'ok': True, 'matched': self.matched, 'template': 'by_id', 'schema': [{'name':'id','type':'Int64'}], 'data': []}


def replica():
    r = Replica.__new__(Replica)
    r.lock = threading.RLock()
    r.args = SimpleNamespace(max_staleness=10)
    r.ready, r.failed, r.uncertain = True, None, False
    r.default_role = 'account_admin'
    r.d = {'database': 'poc'}
    r.last_sync = time.monotonic()
    r.inflight = set()
    r.local_results = {}
    r.engine = FakeEngine()
    r.counters = {'readyset':0, 'databend':0, 'empty_local':0, 'sync_batches':0}
    r.forwarded = []
    def forward(method, path, raw, headers):
        r.forwarded.append((method,path,raw,headers))
        return 200, json.dumps({'id':'upstream-id','state':'Succeeded','data':[['original']]}).encode(), {}
    r.forward = forward
    return r


class CleanupTest(unittest.TestCase):
    def test_declaration_is_frozen_for_capture_and_engine(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp)
            original=directory/'original.json'
            original.write_text('{}')
            args=SimpleNamespace(endpoint='http://127.0.0.1:8000',user='root')
            d={'database':'readyset_gateway_test','table':'source_rows','queries':[]}
            r=Replica(args,d,directory)
            original.write_text('{"database":"another"}')
            self.assertEqual(json.loads(r.declaration_path.read_text()),d)

    def test_dead_engine_broken_pipe_does_not_prevent_cleanup(self):
        engine=Engine.__new__(Engine)
        engine.process=Mock()
        engine.process.poll.return_value=1
        engine.process.stdin.closed=False
        engine.process.stdin.close.side_effect=BrokenPipeError('dead child')
        engine.selector=Mock()
        engine.stderr=Mock()
        engine.close()
        engine.process.stdout.close.assert_called_once()
        engine.stderr.close.assert_called_once()


class RouterTest(unittest.TestCase):
    def test_local_empty_does_not_fallback_and_lifecycle_is_local(self):
        r = replica()
        status, data, headers = r.handle('POST','/v1/query',b'{"sql":"select x"}',{})
        self.assertEqual(status,200)
        self.assertEqual(headers['X-ReadySet-Route'],'readyset')
        self.assertEqual(json.loads(data)['data'],[])
        self.assertEqual(r.forwarded,[])
        result=json.loads(data)
        self.assertEqual(r.handle('GET',result['stats_uri'],None,{})[0],200)
        self.assertEqual(r.handle('GET',result['final_uri'],None,{})[0],200)
        self.assertEqual(r.handle('GET',result['stats_uri'],None,{})[0],404)
        self.assertEqual(r.forwarded,[])

    def test_native_correlation_headers_preserved_for_local_lifecycle(self):
        r=replica()
        qid='01234567-1234-1234-1234-123456789abc'
        status,data,headers=r.handle('POST','/v1/query',b'{"sql":"select x"}',{'x-databend-query-id':qid,'x-databend-route-hint':'n'})
        self.assertEqual(headers['X-ReadySet-Route'],'readyset')
        self.assertEqual(json.loads(data)['id'],qid)
        self.assertEqual(r.handle('GET',f'/v1/query/{qid}/final',None,{})[0],200)
        self.assertEqual(r.handle('GET',f'/v1/query/{qid}/final',None,{})[0],404)
        self.assertEqual(r.forwarded,[])

    def test_tenant_or_warehouse_headers_never_read_local(self):
        for header in ['X-Databend-Tenant','X-Databend-Warehouse']:
            r=replica()
            r.handle('POST','/v1/query',b'{"sql":"select x"}',{header:'other'})
            self.assertEqual(r.engine.calls,[])
            self.assertEqual(len(r.forwarded),1)

    def test_unmatched_and_special_session_forward_original_body(self):
        for body in [b'{"sql":"select unknown"}', b'{"sql":"select x", "session":{"txn_state":"Active"}}']:
            r=replica();r.engine.matched=False
            self.assertEqual(r.handle('POST','/v1/query',body,{})[2]['X-ReadySet-Route'],'databend')
            self.assertEqual(r.forwarded[0][2],body)
            self.assertFalse(r.ready)

    def test_unready_stale_or_inflight_never_calls_engine(self):
        for mode in ('unready','stale','inflight','failed','uncertain'):
            r=replica()
            if mode=='unready':r.ready=False
            if mode=='stale':r.last_sync-=20
            if mode=='inflight':r.inflight.add('pending')
            if mode=='failed':r.failed='capture failed'
            if mode=='uncertain':r.uncertain=True
            r.handle('POST','/v1/query',b'{"sql":"select x"}',{})
            self.assertEqual(r.engine.calls,[],mode)
            self.assertEqual(len(r.forwarded),1)

    def test_proxy_errors_do_not_retry_and_disable_local_reads(self):
        r=replica();r.engine.matched=False
        calls=[]
        def fail(*args):
            calls.append(args)
            raise TimeoutError('unknown submission outcome')
        r.forward=fail
        with self.assertRaises(TimeoutError):r.handle('POST','/v1/query',b'{"sql":"UPDATE x SET a=1"}',{})
        self.assertEqual(len(calls),1)
        self.assertTrue(r.uncertain)
        self.assertFalse(r.ready)

    def test_native_upstream_pages_are_forwarded_and_finish_invalidates(self):
        r=replica();r.inflight.add('upstream-id')
        status, data, headers=r.handle('GET','/v1/query/upstream-id/page/1',None,{'X-DATABEND-STICKY-NODE':'n'})
        self.assertEqual(status,200)
        self.assertEqual(r.forwarded[0][1],'/v1/query/upstream-id/page/1')
        self.assertEqual(r.forwarded[0][3]['X-DATABEND-STICKY-NODE'],'n')
        self.assertFalse(r.inflight)
        self.assertFalse(r.ready)

    def test_final_without_terminal_observation_fails_closed(self):
        r=replica();r.inflight.add('upstream-id')
        r.forward=lambda *args:(200,b'{}',{})
        r.handle('GET','/v1/query/upstream-id/final',None,{})
        self.assertTrue(r.uncertain)

    def test_external_continuation_urls_rejected(self):
        r=replica()
        for path in ['https://evil/v1/query/abc/page/1','//evil/v1/query/abc','/v1/query/abc?x=1','/v1/query/../final']:
            self.assertEqual(r.handle('GET',path,None,{})[0],404)
        self.assertEqual(r.forwarded,[])


if __name__ == '__main__':
    unittest.main()
