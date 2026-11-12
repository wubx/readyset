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

    def cache_names(self):
        rows,_,_=self.query('SHOW CACHES',route='readyset')
        return [row[0] for row in rows]

    def rejected_cache_sql(self, sql):
        self.wait_ready()
        before=self.http('GET','/health')['response']['counters']['databend']
        r=self.http('POST','/v1/query',{'sql':sql})
        self.check('cache SQL rejected locally: '+sql[:65],(r['route'],r['response'].get('state')),('readyset','Failed'))
        self.check('rejected DDL does not disable replica',self.http('GET','/health')['response']['ready'],True)
        self.check('rejected DDL not forwarded',self.http('GET','/health')['response']['counters']['databend'],before)
        if r['response'].get('final_uri'):self.http('GET',r['response']['final_uri'])

    def verify_sql_declarations(self, config):
        self.check('SQL mode starts with zero caches',self.cache_names(),[])
        self.point(7,route='databend')
        self.wait_ready()
        for query in config['queries']:
            self.query(f"CREATE CACHE {query['name']} FROM {query['sql']}",route='readyset')
        self.check('two SQL-declared caches on one source',self.cache_names(),['gateway_by_id','gateway_payload'])
        db=self.database
        for sql in [
            f'CREATE CACHE gateway_by_id FROM SELECT note FROM {db}.source_rows WHERE id=?',
            f'CREATE CACHE duplicate_shape FROM SELECT payload,note FROM {db}.source_rows WHERE id=?',
            f'CREATE SHALLOW CACHE bad FROM SELECT note FROM {db}.source_rows WHERE id=?',
            f'CREATE CACHE CONCURRENTLY bad FROM SELECT note FROM {db}.source_rows WHERE id=?',
            f'CREATE CACHE bad FROM SELECT note FROM {db}.other_rows WHERE id=?',
            'CREATE CACHE bad FROM SELECT note FROM another.source_rows WHERE id=?',
            f'CREATE CACHE bad FROM SELECT note FROM {db}.source_rows WHERE score!=?',
            f'CREATE CACHE bad FROM SELECT note FROM {db}.source_rows WHERE id=? LIMIT 1',
            f'CREATE CACHE bad FROM SELECT note FROM {db}.source_rows WHERE id=?; DROP CACHE gateway_by_id',
            'CREATE CACHE broken FROM', 'DROP CACHE m2_by_id', 'DROP CACHE missing']:
            self.rejected_cache_sql(sql)
        for session in [{'txn_state':'Active'},{'settings':{'max_threads':'1'}},{'role':'another'}]:
            self.check('unsafe session cache DDL rejected',self.http('POST','/v1/query',{'sql':'DROP CACHE gateway_by_id','session':session})['status'],400)
        # Requests serialize under one replica lock; exactly one conflicting CREATE can publish.
        sql=f'CREATE CACHE racing FROM SELECT payload,score FROM {db}.source_rows WHERE id=?'
        with ThreadPoolExecutor(max_workers=2) as pool:
            responses=list(pool.map(lambda _:self.http('POST','/v1/query',{'sql':sql}),range(2)))
        self.check('concurrent duplicate CREATE publishes once',sorted(r['response']['state'] for r in responses),['Failed','Succeeded'])
        for r in responses:self.http('GET',r['response']['final_uri'])
        self.query('DROP CACHE racing',route='readyset')
        self.check('failed requests leave catalog intact',self.cache_names(),['gateway_by_id','gateway_payload'])

    def verify_runtime_cache_changes(self, info):
        db=self.database
        original=self.http('GET','/health')['response']
        self.query(f'CREATE DEEP CACHE late_score FROM SELECT score,note FROM {db}.source_rows WHERE id=?',route='readyset')
        rows,_,_=self.query(f'SELECT score,note FROM {db}.source_rows WHERE id=99999',route='readyset')
        self.check('late cache contains never-read inserted key',rows,[['-9223372036854775808',None]])
        rows,_,_=self.query(f'SELECT score,note FROM {db}.source_rows WHERE id=24',route='readyset')
        self.check('late cache excludes deleted key',rows,[])
        # Exercise the bounded public catalog without creating additional source replicas.
        extra=[('only_id','id'),('only_payload','payload'),('only_note','note'),('only_score','score'),('reverse_payload','note,payload')]
        for name,columns in extra:self.query(f'CREATE CACHE {name} FROM SELECT {columns} FROM {db}.source_rows WHERE id=?',route='readyset')
        self.check('eight public caches supported',len(self.cache_names()),8)
        self.rejected_cache_sql(f'CREATE CACHE overflow FROM SELECT id,note FROM {db}.source_rows WHERE id=?')
        for name,_ in extra:self.query(f'DROP CACHE {name}',route='readyset')
        self.query('DROP CACHE gateway_payload',route='readyset')
        rows,_,_=self.query(f'SELECT payload,note FROM {db}.source_rows WHERE id=7',route='databend')
        self.check('dropped template falls back',rows,[['via-gateway','']])
        self.wait_ready()
        self.point(7)
        self.query('DROP CACHE gateway_by_id',route='readyset')
        # Dropping a full-row public route must not drop the internal replication barrier reader.
        self.source.sql(f"UPDATE {db}.source_rows SET score=12345 WHERE id=7")
        checkpoint=self.http('GET','/health')['response']['checkpoint']
        self.wait_ready(after=checkpoint)
        rows,_,_=self.query(f'SELECT score,note FROM {db}.source_rows WHERE id=7',route='readyset')
        self.check('surviving cache receives updates after drops',rows,[['12345','']])
        self.source.sql(f'UPDATE {db}.source_rows SET score=0 WHERE id=7')
        checkpoint=self.http('GET','/health')['response']['checkpoint'];self.wait_ready(after=checkpoint)
        self.query('DROP CACHE late_score',route='readyset')
        self.check('last public cache can be dropped',self.cache_names(),[])
        self.point(7,route='databend');self.wait_ready()
        for name,columns in [('gateway_by_id','id,payload,score,note'),('gateway_payload','payload,note')]:
            self.query(f'CREATE CACHE {name} FROM SELECT {columns} FROM {db}.source_rows WHERE id=?',route='readyset')
        self.point(7)
        now=self.http('GET','/health')['response']
        self.check('cache DDL does not restart engine',now['engine_pid'],original['engine_pid'])
        self.check('cache DDL reuses generation',now['generation'],info['generation'])
        source_log=[json.loads(line) for line in (self.output/'service/source_queries.jsonl').read_text().splitlines()]
        self.check('one source stream for all caches',sum(r.get('sql','').startswith('CREATE STREAM ') for r in source_log),1)

    @staticmethod
    def sql_string(value):
        return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"

    def native_compare(self, sql, route='readyset', pagination=None):
        self.wait_ready()
        expected=self.source.sql(sql)
        actual,schema,first=self.query(sql,route=route,pagination=pagination)
        order=lambda rows:sorted(rows,key=lambda row:json.dumps(row,ensure_ascii=False))
        self.check('native rows agree with Databend: '+sql[:90],order(actual),order(expected))
        return actual,schema,first

    def verify_native_queries(self, info):
        db=self.database
        baseline=self.http('GET','/health')['response']
        specs=[('native_score','id,payload,score,note','score=?'),
               ('native_bag','payload,note','score=?'),
               ('native_pair','id,payload','score=? AND note=?'),
               ('native_three','note','payload=? AND score=? AND id=?'),
               ('native_text','id,score','payload=?'),
               ('native_note','id,payload','note=?')]
        for name,columns,where in specs:
            self.query(f'CREATE CACHE {name} FROM SELECT {columns} FROM {db}.source_rows WHERE {where}',route='readyset')
        self.check('native shapes share bounded catalog',len(self.cache_names()),8)
        strings=['Case','case','é','e\u0301','中文😀','', 'tail', 'tail ', "O'Reilly", 'back\\slash']
        records=[(200001,'duplicate',777,'same'),(200002,'duplicate',777,'same'),
                 (200003,'other',777,None),(200004,'other',777,'different'),(200005,'different',777,'same')]
        records += [(201000+i,value,888,'matrix') for i,value in enumerate(strings)]
        records += [(-9223372036854775808,'low',9223372036854775807,None),
                    (9223372036854775807,'high',-9223372036854775808,'limits')]
        values=','.join(f'({key},{self.sql_string(payload)},{score},{"NULL" if note is None else self.sql_string(note)})' for key,payload,score,note in records)
        self.source.sql(f'INSERT INTO {db}.source_rows VALUES {values}')
        checkpoint=self.http('GET','/health')['response']['checkpoint'];self.wait_ready(after=checkpoint)
        full=f'SELECT id,payload,score,note FROM {db}.source_rows WHERE score=777'
        self.native_compare(full)
        self.native_compare(f'SELECT id,payload,score,note FROM {db}.source_rows WHERE score=-9223372036854775808')
        self.native_compare(f'SELECT id,payload,score,note FROM {db}.source_rows WHERE score=9223372036854775807')
        self.point(-9223372036854775808)
        self.point(9223372036854775807)
        bag=f'SELECT payload,note FROM {db}.source_rows WHERE score=777'
        rows,_,first=self.native_compare(bag,pagination={'max_rows_per_page':2,'max_rows_in_buffer':2,'wait_time_secs':1})
        self.check('duplicate projected rows retain multiplicity',rows.count(['duplicate','same']),2)
        self.check('local first page honors row bound',len(first['response']['data']),2)
        self.check('local continuation is a page', '/page/' in first['response']['next_uri'],True)
        pages=[r for r in self.requests if r['path'].startswith('/v1/query/'+first['response']['id']+'/page/')]
        self.check('all native continuation pages are local',len(pages)>=2 and all(p['route']=='readyset' for p in pages),True)
        self.native_compare(f"SELECT id,payload FROM {db}.source_rows WHERE score=777 AND note='same'")
        self.native_compare(f"SELECT note FROM {db}.source_rows WHERE payload='duplicate' AND score=777 AND id=200001")
        # Native rewrite alone decides whether alternate predicate order has the same identity.
        # If not, exact-shape routing must miss safely rather than reorder parameters ourselves.
        _,_,reordered=self.native_compare(f"SELECT note FROM {db}.source_rows WHERE id=200001 AND score=777 AND payload='duplicate'",route=None)
        self.native_reordered_route=reordered['route']
        for value in strings+['not-present']:
            self.native_compare(f'SELECT id,score FROM {db}.source_rows WHERE payload={self.sql_string(value)}')
        self.native_compare(f"SELECT id,payload FROM {db}.source_rows WHERE note='same'")
        self.native_compare(f'SELECT id,payload FROM {db}.source_rows WHERE note=NULL',route='databend')
        self.native_compare(f'SELECT id,payload FROM {db}.source_rows WHERE note IS NULL',route='databend')
        self.native_compare(f"SELECT id,payload FROM {db}.source_rows WHERE score='777' AND note='same'",route='databend')
        # One update changes every component, vacating old simple and composite index keys.
        self.source.sql(f"UPDATE {db}.source_rows SET payload='moved',score=778,note='new-note' WHERE id=200001")
        self.source.sql(f'DELETE FROM {db}.source_rows WHERE id=200002')
        checkpoint=self.http('GET','/health')['response']['checkpoint'];self.wait_ready(after=checkpoint)
        for sql in [full,bag,
                    f"SELECT id,payload FROM {db}.source_rows WHERE score=777 AND note='same'",
                    f"SELECT id,payload FROM {db}.source_rows WHERE score=778 AND note='new-note'",
                    f"SELECT note FROM {db}.source_rows WHERE payload='duplicate' AND score=777 AND id=200001",
                    f"SELECT note FROM {db}.source_rows WHERE payload='moved' AND score=778 AND id=200001",
                    f"SELECT id,score FROM {db}.source_rows WHERE payload='duplicate'",
                    f"SELECT id,score FROM {db}.source_rows WHERE payload='moved'"]:
            self.native_compare(sql)
        self.query('DROP CACHE native_three',route='readyset')
        self.query(f'CREATE CACHE native_four FROM SELECT payload FROM {db}.source_rows WHERE note=? AND score=? AND id=? AND payload=?',route='readyset')
        self.native_compare(f"SELECT payload FROM {db}.source_rows WHERE note='new-note' AND score=778 AND id=200001 AND payload='moved'")
        self.query('DROP CACHE native_four',route='readyset')
        self.query(f'CREATE CACHE native_late FROM SELECT id FROM {db}.source_rows WHERE score=? AND payload=?',route='readyset')
        self.native_compare(f"SELECT id FROM {db}.source_rows WHERE score=778 AND payload='moved'")
        self.native_compare(f"SELECT id FROM {db}.source_rows WHERE score=777 AND payload='duplicate'")
        # NULL -> non-NULL and the reverse must move equality membership, without NULL = NULL.
        self.source.sql(f"UPDATE {db}.source_rows SET note='same' WHERE id=200003")
        self.source.sql(f'UPDATE {db}.source_rows SET note=NULL WHERE id=200005')
        self.source.sql(f'DELETE FROM {db}.source_rows WHERE id=200001')
        checkpoint=self.http('GET','/health')['response']['checkpoint'];self.wait_ready(after=checkpoint)
        self.native_compare(f"SELECT id,payload FROM {db}.source_rows WHERE score=777 AND note='same'")
        self.native_compare(f"SELECT id FROM {db}.source_rows WHERE score=778 AND payload='moved'")
        self.native_compare(f"SELECT id,payload FROM {db}.source_rows WHERE note='same'")
        self.native_compare(f"SELECT id,payload FROM {db}.source_rows WHERE note='new-note'")
        self.query('DROP CACHE native_score',route='readyset')
        self.native_compare(bag)
        self.point(7)  # All-column score DROP must not delete/alias the internal id barrier.
        self.query(f'CREATE CACHE native_score FROM SELECT id,payload,score,note FROM {db}.source_rows WHERE score=?',route='readyset')
        self.native_compare(full)
        import databend_driver
        port=self.origin.rsplit(':',1)[1]
        conn=databend_driver.BlockingDatabendClient(f'databend://root@127.0.0.1:{port}/{db}?sslmode=disable').get_conn()
        before=self.http('GET','/health')['response']['counters']['readyset']
        driver_rows=[tuple(row.values()) for row in conn.query_all(f"SELECT id,payload FROM {db}.source_rows WHERE score=777 AND note='same'")]
        self.check('native driver composite lookup',driver_rows,[(200003,'other')])
        self.check('native driver composite read is local',self.http('GET','/health')['response']['counters']['readyset'],before+1)
        conn.close()
        for name in ['native_score','native_bag','native_pair','native_late','native_text','native_note']:
            self.query('DROP CACHE '+name,route='readyset')
        self.source.sql(f'DELETE FROM {db}.source_rows WHERE id>=200000 OR id=-9223372036854775808')
        checkpoint=self.http('GET','/health')['response']['checkpoint'];self.wait_ready(after=checkpoint)
        now=self.http('GET','/health')['response']
        self.check('native cache changes preserve engine',now['engine_pid'],baseline['engine_pid'])
        self.check('native cache changes preserve generation',now['generation'],info['generation'])
        source_log=[json.loads(line) for line in (self.output/'service/source_queries.jsonl').read_text().splitlines()]
        self.check('native caches still share one stream',sum(r.get('sql','').startswith('CREATE STREAM ') for r in source_log),1)

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
            binding = ['--database', db, '--table', 'source_rows'] if self.args.sql_declarations else ['--declarations', str(declaration)]
            self.child=subprocess.Popen([sys.executable,'-B',str(Path(__file__).with_name('gateway.py')),
                '--endpoint',self.args.endpoint,'--user',self.args.user,*binding,
                '--output',str(self.output/'service'),'--binary',self.args.binary,'--port','0','--interval','1','--confirm-create'],stdout=stdout,stderr=log)
            deadline=time.monotonic()+75
            while not (self.output/'service/listening.json').exists():
                if self.child.poll() is not None:raise RuntimeError('gateway exited; inspect gateway_stderr.log')
                if time.monotonic()>deadline:raise TimeoutError('gateway bootstrap timeout')
                time.sleep(.1)
            info=json.loads((self.output/'service/listening.json').read_text());self.origin=info['origin']
            self.wait_ready()
            if self.args.sql_declarations:
                self.verify_sql_declarations(config)
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
            if self.args.sql_declarations:
                conn.query_row('CREATE CACHE driver_note FROM SELECT note FROM source_rows WHERE id = ?')
                before_note=self.http('GET','/health')['response']['counters']['readyset']
                self.check('native driver SQL-created cache value',tuple(conn.query_row(f'SELECT note FROM {db}.source_rows WHERE id=24').values()),('中文',))
                self.check('native driver SQL-created cache route',self.http('GET','/health')['response']['counters']['readyset'],before_note+1)
                conn.query_row('DROP CACHE driver_note')
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
            if self.args.sql_declarations:
                self.verify_runtime_cache_changes(info)
            if self.args.native_queries:
                self.verify_native_queries(info)
            if self.args.in_range_queries:
                from in_range_verify import verify
                verify(self, info)
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
            if self.args.sql_declarations:
                before_ddl=failed['counters']['databend']
                self.check('DDL on dead engine is rejected locally',self.http('POST','/v1/query',{'sql':'SHOW CACHES'})['status'],503)
                self.check('dead engine DDL never forwarded',self.http('GET','/health')['response']['counters']['databend'],before_ddl)
            self.child.send_signal(signal.SIGINT)
            self.child.wait(timeout=60)
            self.check('gateway clean exit',self.child.returncode,0)
            final=json.loads((self.output/'service/final_status.json').read_text())
            self.check('injected failure remains fail-closed',final['ready'],False)
            self.check('sync advanced automatically',final['checkpoint']>3,True)
            self.facts={'database':db,'gateway':info,'final_status':final,'upstream_page_requests':len(pages),'native_reordered_route':getattr(self,'native_reordered_route',None)}
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
    p.add_argument('--sql-declarations',action='store_true',help='start without JSON queries; exercise runtime CREATE/SHOW/DROP CACHE')
    p.add_argument('--native-queries',action='store_true',help='exercise native multi-column equality, key migration, strings and local pagination')
    p.add_argument('--in-range-queries',action='store_true',help='exercise native IN layouts, numeric ranges, mixed equality/ranges and index withdrawal')
    args=p.parse_args()
    if not args.confirm_create:p.error('--confirm-create required')
    return Verification(args).run()


if __name__=='__main__':raise SystemExit(main())
