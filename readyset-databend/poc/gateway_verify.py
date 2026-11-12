#!/usr/bin/env python3
"""Bounded live compatibility/routing test. Creates a NEW synthetic POC database only."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

from m0 import Client, NoRedirect


class Verification:
    def __init__(self, args):
        self.args=args
        self.database='readyset_gateway_'+datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_')+uuid.uuid4().hex[:8]
        self.output=Path(args.output)/self.database
        self.output.mkdir(parents=True,exist_ok=False)
        self.checks=[]
        self.requests=[]
        self.child=None
        self.origin=None
        self.auth='Basic '+base64.b64encode((args.user+':'+os.environ.get('DATABEND_PASSWORD','')).encode()).decode()
        self.opener=urllib.request.build_opener(NoRedirect())
        self.source=Client(args.endpoint,args.user,os.environ.get('DATABEND_PASSWORD',''),self.output/'setup_queries.jsonl','setup')

    def check(self,label,actual,expected):
        ok=actual==expected
        self.checks.append(dict(label=label,passed=ok,actual=actual,expected=expected))
        print(('PASS ' if ok else 'FAIL ')+label,flush=True)
        if not ok:raise AssertionError(f'{label}: {actual!r} != {expected!r}')

    def http(self,method,path,body=None,auth=True):
        raw=None if body is None else json.dumps(body,ensure_ascii=False).encode()
        headers={'Content-Type':'application/json'}
        if auth:headers['Authorization']=self.auth
        request=urllib.request.Request(self.origin+path,data=raw,method=method,headers=headers)
        try:response=self.opener.open(request,timeout=45)
        except urllib.error.HTTPError as e:response=e
        with response:
            raw=response.read(16*1024*1024)
            result=json.loads(raw) if raw else {}
            record={'method':method,'path':path,'body':body,'status':response.code,
                    'route':response.headers.get('X-ReadySet-Route'),'template':response.headers.get('X-ReadySet-Template'),'response':result}
            self.requests.append(record)
            return record

    def wait_ready(self,after=0):
        deadline=time.monotonic()+45
        while time.monotonic()<deadline:
            r=self.http('GET','/health')['response']
            if r.get('failed'):raise RuntimeError(r)
            if r.get('ready') and r['checkpoint']>after:return r
            time.sleep(.1)
        raise TimeoutError('replica did not become ready')

    def query(self,sql,session=None,route=None,pagination=None):
        body={'sql':sql}
        if session is not None:body['session']=session
        if pagination:body['pagination']=pagination
        first=self.http('POST','/v1/query',body)
        if route:self.check('query route: '+sql[:65],first['route'],route)
        r=first['response'];rows=list(r.get('data',[]));schema=r.get('schema',[])
        qid=r.get('id');deadline=time.monotonic()+45
        while r.get('next_uri'):
            if time.monotonic()>deadline:raise TimeoutError('query paging deadline')
            path=r['next_uri']
            response=self.http('GET',path)
            if path.endswith('/final'):break
            r=response['response'];rows+=r.get('data',[]);schema=r.get('schema') or schema
            if r.get('error'):break
        if qid and (not r.get('next_uri') or not str(r.get('next_uri')).endswith('/final')):
            self.http('GET',f'/v1/query/{qid}/final')
        if r.get('error'):raise RuntimeError(r['error'])
        return rows,schema,first

    def point(self,key,route='readyset'):
        return self.query(f'SELECT id, payload, score, note FROM {self.database}.source_rows WHERE id = {key}',route=route)

    def run(self):
        error=None
        try:
            db=self.database
            self.source.sql(f'CREATE DATABASE {db}')
            self.source.sql(f'CREATE TABLE {db}.source_rows (id BIGINT NOT NULL,payload STRING NOT NULL,score BIGINT NOT NULL,note STRING NULL)')
            self.source.sql(f"INSERT INTO {db}.source_rows SELECT CAST(number*17+7 AS BIGINT),concat('p-',to_string(number)),CAST(number AS BIGINT),IF(number%2=0,NULL,'中文') FROM numbers(100)")
            self.source.sql(f'CREATE TABLE {db}.other_rows (id BIGINT, val STRING)')
            self.source.sql(f"INSERT INTO {db}.other_rows VALUES (1,'not-cached')")
            config={'database':db,'table':'source_rows','queries':[
                {'name':'gateway_by_id','sql':f'SELECT id, payload, score, note FROM {db}.source_rows WHERE id = ?'},
                {'name':'gateway_payload','sql':f'SELECT payload, note FROM {db}.source_rows WHERE id = ?'}]}
            declaration=self.output/'declarations.json';declaration.write_text(json.dumps(config,indent=2)+'\n')
            log=(self.output/'gateway_stderr.log').open('w')
            stdout=(self.output/'gateway_stdout.log').open('w')
            self.child=subprocess.Popen([sys.executable,'-B',str(Path(__file__).with_name('gateway.py')),
                '--endpoint',self.args.endpoint,'--user',self.args.user,'--declarations',str(declaration),
                '--output',str(self.output/'service'),'--binary',self.args.binary,'--port','0','--interval','1','--confirm-create'],stdout=stdout,stderr=log)
            deadline=time.monotonic()+75
            while not (self.output/'service/listening.json').exists():
                if self.child.poll() is not None:raise RuntimeError('gateway exited; inspect gateway_stderr.log')
                if time.monotonic()>deadline:raise TimeoutError('gateway bootstrap timeout')
                time.sleep(.1)
            info=json.loads((self.output/'service/listening.json').read_text());self.origin=info['origin']
            self.wait_ready()
            self.check('HTTP auth required',self.http('POST','/v1/query',{'sql':'SELECT 1'},auth=False)['status'],401)
            # All keys first-touch, not just one hot-key test.
            for n in range(100):
                key=n*17+7
                rows,schema,_=self.point(key)
                self.check(f'first touch key {key}',rows,[[str(key),f'p-{n}',str(n),None if n%2==0 else '中文']])
            self.check('native result schema',schema,[{'name':'id','type':'Int64'},{'name':'payload','type':'String'},{'name':'score','type':'Int64'},{'name':'note','type':'Nullable(String)'}])
            before=self.http('GET','/health')['response']['counters']['databend']
            rows,_,local=self.point(-9223372036854775808)
            self.check('missing key local empty',rows,[])
            after=self.http('GET','/health')['response']['counters']['databend']
            self.check('empty result causes no fallback',after,before)
            self.check('local stats lifecycle',self.http('GET',local['response']['stats_uri'])['status'],404) # query() finalized it
            rows,_,_=self.query(f'select payload,note from {db}.source_rows where id=7;',route='readyset')
            self.check('second declared projection',rows,[['p-0',None]])
            rows,_,_=self.query('SELECT payload, note FROM source_rows WHERE id=24',session={'database':db},route='readyset')
            self.check('unqualified in declared database',rows,[['p-1','中文']])
            for sql in [f'SELECT count(*) FROM {db}.source_rows',f'SELECT * FROM {db}.source_rows WHERE id=7',
                        f'SELECT id, payload, score, note FROM {db}.source_rows WHERE id=7 LIMIT 0',
                        f'SELECT val FROM {db}.other_rows WHERE id=1',
                        f'SELECT payload,note FROM {db}.source_rows WHERE id=7 OR id=24']:
                self.wait_ready();self.query(sql,route='databend')
            self.wait_ready()
            rows,_,_=self.query(f'SELECT payload, note FROM {db}.source_rows WHERE id=7',session={'settings':{'max_threads':'1'}},route='databend')
            self.check('session-sensitive query kept correct',rows,[['p-0',None]])
            self.wait_ready()
            paged_rows,_,_=self.query('SELECT number FROM numbers(25)',route='databend',pagination={'max_rows_per_page':3,'max_rows_in_buffer':3,'wait_time_secs':1})
            self.check('all upstream pages preserve rows',paged_rows,[[str(i)] for i in range(25)])
            pages=[r for r in self.requests if '/page/' in r['path']]
            self.check('upstream pagination actually proxied',len(pages)>1,True)
            self.check('all upstream pages retain route',all(p['route']=='databend' for p in pages),True)
            self.wait_ready()
            # Native client, not a custom replacement protocol.
            import databend_driver
            port=self.origin.rsplit(':',1)[1]
            conn=databend_driver.BlockingDatabendClient(f'databend://root@127.0.0.1:{port}/{db}?sslmode=disable').get_conn()
            before_native=self.http('GET','/health')['response']['counters']['readyset']
            row=conn.query_row(f'SELECT id, payload, score, note FROM {db}.source_rows WHERE id=7')
            self.check('native databend_driver returns correct values',tuple(row.values()),(7,'p-0',0,None))
            after_native=self.http('GET','/health')['response']['counters']['readyset']
            self.check('native driver actually uses ReadySet',after_native,before_native+1)
            self.check('native driver local query ID',conn.last_query_id() is not None,True)
            self.wait_ready()
            self.check('native driver unmatched query passthrough',tuple(conn.query_row(f'SELECT val FROM {db}.other_rows WHERE id=1').values()),('not-cached',))
            self.wait_ready()
            before_mixed=self.http('GET','/health')['response']['counters']['readyset']
            self.check('native driver returns to declared cache',tuple(conn.query_row(f'SELECT payload, note FROM {db}.source_rows WHERE id=7').values()),('p-0',None))
            self.check('mixed native connection actually reenters ReadySet',self.http('GET','/health')['response']['counters']['readyset'],before_mixed+1)
            conn.close()
            self.wait_ready()
            # Writes through gateway and external source writes are both consumed automatically.
            checkpoint=self.http('GET','/health')['response']['checkpoint']
            self.query(f"UPDATE {db}.source_rows SET payload='via-gateway', note='' WHERE id=7",route='databend')
            self.wait_ready(after=checkpoint)
            rows,_,_=self.point(7);self.check('gateway write synchronized',rows,[['7','via-gateway','0','']])
            checkpoint=self.http('GET','/health')['response']['checkpoint']
            self.source.sql(f'DELETE FROM {db}.source_rows WHERE id=24')
            self.source.sql(f"INSERT INTO {db}.source_rows VALUES (99999,'never-seen',-9223372036854775808,NULL)")
            # Wait for a capture AFTER both writes, not just any racing timer capture.
            checkpoint=self.http('GET','/health')['response']['checkpoint']
            self.wait_ready(after=checkpoint)
            rows,_,_=self.point(24);self.check('external delete synchronized',rows,[])
            rows,_,_=self.point(99999);self.check('new never-before-accessed key synchronized',rows,[['99999','never-seen','-9223372036854775808',None]])
            self.wait_ready()
            with ThreadPoolExecutor(max_workers=4) as pool:
                outcomes=list(pool.map(lambda _:self.point(7)[0],range(12)))
            self.check('concurrent local queries correct',outcomes,[[['7','via-gateway','0','']]]*12)
            # Explicit transactions always use source session state; rollback must not leak.
            transaction=Client(self.origin,self.args.user,os.environ.get('DATABEND_PASSWORD',''),self.output/'transaction_queries.jsonl','transaction')
            self.wait_ready()
            before_tx=self.http('GET','/health')['response']['counters']['readyset']
            transaction.sql('BEGIN')
            transaction.sql(f"UPDATE {db}.source_rows SET payload='uncommitted' WHERE id=7")
            self.check('transaction sees own upstream write',transaction.sql(f'SELECT payload FROM {db}.source_rows WHERE id=7'),[['uncommitted']])
            transaction.sql('ROLLBACK')
            after_tx=self.http('GET','/health')['response']['counters']['readyset']
            self.check('transaction never used local cache',after_tx,before_tx)
            self.wait_ready()
            rows,_,_=self.point(7);self.check('rollback preserved replicated value',rows,[['7','via-gateway','0','']])
            healthy=self.http('GET','/health')['response']
            self.check('sync had no failure before fault injection',healthy['failed'],None)
            # Terminate ONLY the engine child owned by this newly started test gateway.
            # This tests fail-closed routing, NOT crash recovery or state restoration.
            os.kill(healthy['engine_pid'],signal.SIGTERM)
            time.sleep(.1)
            rows,_,_=self.point(7,route='databend')
            self.check('engine unavailable falls back to correct source result',rows,[['7','via-gateway','0','']])
            failed=self.http('GET','/health')['response']
            self.check('engine failure disables local routing',failed['ready'],False)
            self.check('engine failure is observable',bool(failed['failed']),True)
            self.child.send_signal(signal.SIGINT)
            self.child.wait(timeout=60)
            self.check('gateway clean exit',self.child.returncode,0)
            final=json.loads((self.output/'service/final_status.json').read_text())
            self.check('injected failure remains fail-closed',final['ready'],False)
            self.check('sync advanced automatically',final['checkpoint']>3,True)
            self.facts={'database':db,'gateway':info,'final_status':final,'upstream_page_requests':len(pages)}
        except Exception as exc:
            error=f'{type(exc).__name__}: {exc}'
            print(error,flush=True)
        finally:
            if self.child and self.child.poll() is None:
                self.child.send_signal(signal.SIGINT)
                try:self.child.wait(timeout=60)
                except subprocess.TimeoutExpired:self.child.kill();self.child.wait(timeout=10)
            report={'passed':error is None,'error':error,'checks':self.checks,'facts':getattr(self,'facts',{}),
                    'database':self.database,'raw_directory':str(self.output)}
            (self.output/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
            (self.output/'http_requests.jsonl').write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in self.requests))
            print('Report: '+str(self.output/'report.json'),flush=True)
        return 1 if error else 0


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint',required=True)
    p.add_argument('--user',default='root')
    p.add_argument('--binary',default='target/debug/databend-m2')
    p.add_argument('--output',default='/tmp/readyset-databend-gateway')
    p.add_argument('--confirm-create',action='store_true')
    args=p.parse_args()
    if not args.confirm_create:p.error('--confirm-create required')
    return Verification(args).run()


if __name__=='__main__':raise SystemExit(main())
