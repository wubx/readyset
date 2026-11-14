#!/usr/bin/env python3
"""Shared-runtime scale test: TWO heterogeneous 10M-row sources, ONE memory budget.

Only new readyset_gateway_* synthetic databases; no cleanup, service replacement, helper
metadata tables, or forced native eviction. Reuses rs_scale_bench's existing HTTP client.
Records bounded query summaries, live health, RSS/HWM, exact bags, real automatic eviction
and post-eviction replay, skewed heating, and cross-table impact of a 2M-row UPDATE.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time
import traceback
import urllib.request
import uuid

import rs_scale_bench as b

BASE = 100000
A_COLUMNS = 'id,bucket,amount,active,payload'
B_COLUMNS = 'id,grp,label,revision'
MAX_ID = 18446744073709551615


def bag(rows):
    return Counter(tuple(row) for row in rows)


def percentile(values, fraction):
    values = sorted(values)
    return round(values[min(len(values)-1, int(len(values)*fraction))], 3) if values else None


class Scale:
    def __init__(self, args):
        self.args = args
        self.out = Path(args.output)
        self.out.mkdir(parents=True, exist_ok=False)
        self.db = 'readyset_gateway_multiscale_' + time.strftime('%Y%m%d_%H%M%S_') + uuid.uuid4().hex[:6]
        self.a, self.b = self.db + '_a', self.db + '_b'
        self.ids = [self.a + '.items', self.b + '.items']
        self.service = self.out / 'service'
        self.child = self.rss = self.origin = None
        self.stop_monitor = threading.Event()
        self.samples = []
        self.monitor_errors = []
        self.phase = 'setup'
        self.lock = threading.Lock()
        self.query_log = (self.out / 'queries.jsonl').open('w')
        self.report = {'passed': False, 'smoke': args.smoke, 'rows_per_table': args.rows,
                       'databases': [self.a, self.b], 'memory_limit_mb': args.memory_limit_mb,
                       'max_staleness_secs': args.max_staleness, 'checks': [], 'stages': [],
                       'binary': args.binary, 'binary_sha256': hashlib.file_digest(open(args.binary, 'rb'), 'sha256').hexdigest()}
        b.DB, b.OUT = self.a, str(self.out)
        b.ENDPOINT = args.endpoint
        self.save()

    def save(self):
        with self.lock:
            temporary = self.out / 'report.json.tmp'
            temporary.write_text(json.dumps(self.report, ensure_ascii=False, indent=2)+'\n')
            temporary.replace(self.out / 'report.json')

    def check(self, name, ok, **details):
        self.report['checks'].append({'name': name, 'passed': bool(ok), **details})
        self.save()
        print(('PASS ' if ok else 'FAIL ') + name + ': ' + json.dumps(details, ensure_ascii=False), flush=True)
        if not ok:
            raise AssertionError(name)

    def query(self, sql, origin=None):
        origin = origin or self.origin
        started = time.monotonic()
        rows, route = b.query(origin, sql, timeout=1800)
        elapsed = (time.monotonic()-started)*1000
        # Never retain large result sets in the pressure runner or its evidence files.
        with self.lock:
            self.query_log.write(json.dumps({'ts': time.time(), 'phase': self.phase, 'origin': origin,
                'sql': sql, 'rows': len(rows), 'route': route, 'ms': round(elapsed, 3)}, ensure_ascii=False)+'\n')
            self.query_log.flush()
        return rows, route, elapsed

    def health(self):
        request = urllib.request.Request(self.origin+'/health', headers={'Authorization': b.AUTH})
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)

    @staticmethod
    def tables(health):
        return {table['table']: table for table in health['tables']}

    def healthy(self, health):
        if health.get('failed'):
            raise RuntimeError(health['failed'])
        for table in health['tables']:
            if table.get('failed'):
                raise RuntimeError(table)

    def wait_fresh(self, ids=None, since=None, timeout=1200):
        ids = ids or self.ids
        started = time.monotonic()
        while time.monotonic()-started < timeout:
            h = self.health()
            self.healthy(h)
            tables = self.tables(h)
            # A pre-write empty capture may commit after a write. Require a round START
            # after `since`, not merely an incrementing checkpoint on nonblocking health.
            if all(tables[i]['ready'] and (since is None or tables[i]['last_sync_age_ms'] <= (time.monotonic()-since)*1000) for i in ids):
                return h
            time.sleep(.05)
        raise TimeoutError('freshness deadline: '+json.dumps(self.health()))

    def stage(self, name):
        h = self.health()
        self.healthy(h)
        entry = {'name': name, 'ts': time.time(), 'cache': h['cache'], 'tables': h['tables'],
                 'rss_mb': self.rss.now, 'hwm_mb': self.rss.peak}
        self.report['stages'].append(entry)
        self.save()
        print(f'STAGE {name}: allocated={h["cache"]["allocated_bytes"]/1048576:.1f}MiB RSS={self.rss.now}MiB', flush=True)
        return h

    def monitor(self):
        with (self.out / 'health.jsonl').open('w') as file:
            while not self.stop_monitor.is_set():
                try:
                    h = self.health()
                    point = {'ts': time.time(), 'phase': self.phase, 'cache': h['cache'],
                        'tables': h['tables'], 'failed': h['failed'],
                        'rss_mb': self.rss.now, 'hwm_mb': self.rss.peak}
                    file.write(json.dumps(point, ensure_ascii=False)+'\n')
                    file.flush()
                    self.samples.append(point)
                except Exception as error:
                    self.monitor_errors.append({'ts': time.time(), 'error': repr(error)})
                self.stop_monitor.wait(.25)

    def setup(self):
        for db in [self.a, self.b]:
            self.query('CREATE DATABASE '+db, b.ENDPOINT)
        self.query(f'CREATE TABLE {self.a}.items (id STRING NOT NULL,bucket BIGINT NOT NULL,amount DECIMAL(18,2) NULL,active BOOLEAN NOT NULL,payload STRING NOT NULL)', b.ENDPOINT)
        self.query(f'CREATE TABLE {self.b}.items (id BIGINT UNSIGNED NOT NULL,grp INT UNSIGNED NOT NULL,label STRING NULL,revision SMALLINT NOT NULL)', b.ENDPOINT)
        for sql in [
            f"INSERT INTO {self.a}.items SELECT concat('a-',to_string(number)),CAST(number%100000 AS BIGINT),CAST(number%100000 AS DECIMAL(18,2)),number%2=0,concat('a-',to_string(number),'-',repeat('x',96)) FROM numbers({self.args.rows})",
            f"INSERT INTO {self.b}.items SELECT CAST(number AS BIGINT UNSIGNED),CAST(number%100000 AS INT UNSIGNED),IF(number%3=0,NULL,concat('b-',to_string(number),'-',repeat('y',96))),CAST(number%100 AS SMALLINT) FROM numbers({self.args.rows})",
            f"INSERT INTO {self.a}.items VALUES ('a-dup',3,NULL,false,'duplicate'),('a-dup',3,NULL,false,'duplicate')",
            f"INSERT INTO {self.b}.items VALUES ({MAX_ID},0,'duplicate',0),({MAX_ID},0,'duplicate',0),({MAX_ID},1,'other',1)",
        ]:
            self.query(sql, b.ENDPOINT)
        counts = [int(self.query(f'SELECT COUNT(*) FROM {db}.items', b.ENDPOINT)[0][0][0]) for db in [self.a, self.b]]
        self.check('two actual heterogeneous source counts', counts == [self.args.rows+2,self.args.rows+3], counts=counts)

    def start(self):
        config = self.out / 'config.toml'
        config.write_text(f'''instance_id = "ms{uuid.uuid4().hex[:12]}"
confirm_create = true
[upstream]
url = "{b.ENDPOINT}"
user = "root"
[listen]
http = "127.0.0.1:0"
[replication]
tables = ["{self.a}.items", "{self.b}.items"]
poll_interval = "300ms"
max_staleness = "{self.args.max_staleness}s"
[cache]
partial = true
memory_limit_mb = {self.args.memory_limit_mb}
upquery_timeout_ms = 10000
''')
        self.server_log = (self.out / 'server.log').open('w')
        started = time.monotonic()
        self.child = subprocess.Popen([self.args.binary,'--config',str(config),'--output',str(self.service)],
                                      stdout=self.server_log, stderr=subprocess.STDOUT)
        self.rss = b.Rss(self.child.pid)
        self.rss.start()
        while not (self.service / 'listening.json').exists():
            if self.child.poll() is not None:
                raise RuntimeError('server exited during snapshot: '+str(self.child.returncode))
            if time.monotonic()-started > 900:
                raise TimeoutError('snapshot startup')
            time.sleep(.2)
        self.origin = json.loads((self.service / 'listening.json').read_text())['origin']
        self.report['snapshot_wall_secs'] = round(time.monotonic()-started,3)
        self.monitor_thread = threading.Thread(target=self.monitor, daemon=True)
        self.monitor_thread.start()
        self.wait_fresh()
        h = self.stage('snapshot')
        self.check('one native server, two real bases, ONE shared soft budget',
                   h['cache']['native_server_instances']==1 and h['cache']['base_tables']==2 and
                   h['cache']['memory_limit_bytes']==self.args.memory_limit_mb*1048576 and
                   h['cache']['materialization']=='partial', cache=h['cache'])
        self.check('snapshots include identical duplicate rows', [t['rows'] for t in h['tables']] == [self.args.rows+2,self.args.rows+3], rows=[t['rows'] for t in h['tables']])
        for name, db, columns, predicate in [
            ('a_id',self.a,A_COLUMNS,'id=?'), ('a_range',self.a,A_COLUMNS,'bucket>=? AND bucket<?'),
            ('a_payload',self.a,'id,bucket','payload=?'),
            ('b_id',self.b,B_COLUMNS,'id=?'), ('b_range',self.b,B_COLUMNS,'grp>=? AND grp<?'),
            ('b_label',self.b,'id,revision','label=?'),
        ]:
            self.wait_fresh()
            self.query(f'CREATE CACHE {name} FROM SELECT {columns} FROM {db}.items WHERE {predicate}')
        h = self.stage('six partial caches')
        self.check('cache creation did not fill full readers', all(r['key_slots']<20 for r in h['cache']['readers']), readers=h['cache']['readers'])

    def sql_range(self, table, lo, width):
        return f'SELECT {A_COLUMNS if table==0 else B_COLUMNS} FROM {self.a if table==0 else self.b}.items WHERE {"bucket" if table==0 else "grp"}>={lo} AND {"bucket" if table==0 else "grp"}<{lo+width}'

    def expected_range(self, table, lo, width):
        rows=[]
        for key in range(lo,lo+width):
            for i in range(key,self.args.rows,BASE):
                if table==0:
                    rows.append([f'a-{i}',str(key),f'{key}.00','1' if i%2==0 else '0',f'a-{i}-'+'x'*96])
                else:
                    rows.append([str(i),str(key),None if i%3==0 else f'b-{i}-'+'y'*96,str(i%100)])
        if lo<=3<lo+width and table==0:
            rows += [['a-dup','3',None,'0','duplicate']]*2
        if table==1:
            if lo==0: rows += [[str(MAX_ID),'0','duplicate','0']]*2
            if lo<=1<lo+width: rows += [[str(MAX_ID),'1','other','1']]
        return bag(rows)

    def compare(self, sql, label, require_local=True):
        self.wait_fresh()
        expected = self.query(sql,b.ENDPOINT)[0]
        before = self.health()['cache']
        actual, route, ms = self.query(sql)
        after = self.health()['cache']
        self.check(label,bag(actual)==bag(expected) and (route=='readyset' or not require_local),
                   rows=len(actual),route=route,ms=round(ms,3),miss_delta=after['miss_keys']-before['miss_keys'])
        return {'ms':ms,'miss_delta':after['miss_keys']-before['miss_keys'],'route':route}

    def baseline(self):
        self.phase='baseline'
        self.probes=[f"SELECT {A_COLUMNS} FROM {self.a}.items WHERE id='a-6'",
                     f'SELECT {B_COLUMNS} FROM {self.b}.items WHERE id=42',
                     self.sql_range(0,2,2),self.sql_range(1,2,2),
                     f"SELECT id,bucket FROM {self.a}.items WHERE payload='a-12345-{'x'*96}'",
                     f"SELECT id,revision FROM {self.b}.items WHERE label='b-12346-{'y'*96}'",
                     f"SELECT {A_COLUMNS} FROM {self.a}.items WHERE id='a-dup'",
                     f'SELECT {B_COLUMNS} FROM {self.b}.items WHERE id={MAX_ID}']
        self.report['cold_hot'] = []
        for index,sql in enumerate(self.probes):
            self.report['cold_hot'].append({'probe':index,'cold':self.compare(sql,f'cold-{index}'),
                                           'hot':self.compare(sql,f'hot-{index}')})
        self.stage('cold/hot baseline')

    def heat(self, count, skew=False):
        self.phase='heat_b_only' if skew else 'heat_balanced'
        started=time.monotonic()
        def lookup(index):
            table=1 if skew else index%2
            slot=index if skew else index//2
            lo=((slot*67 + (50000 if skew else 100))%(BASE-self.args.range_width))
            sql=self.sql_range(table,lo,self.args.range_width)
            expected=self.expected_range(table,lo,self.args.range_width)
            rows,route,ms=self.query(sql)
            if bag(rows)!=expected:
                raise AssertionError(f'heat bad bag table={table} slot={slot} route={route}')
            return {'table':table,'lo':lo,'route':route,'ms':ms}
        with ThreadPoolExecutor(max_workers=8) as pool:
            answers=list(pool.map(lookup,range(count)))
        routes=dict(Counter(r['route'] for r in answers))
        result={'queries':count,'secs':round(time.monotonic()-started,3),'routes':routes,
                'p50_ms':percentile([r['ms'] for r in answers],.5),'p95_ms':percentile([r['ms'] for r in answers],.95)}
        self.report[self.phase]=result
        self.check(self.phase+' all query bags correct, actual local reader traffic',routes.get('readyset',0)>=count*.95,**result)
        h=self.stage(self.phase+' complete')
        if not self.args.smoke:
            self.check(self.phase+' automatic memory-pressure eviction',sum(r['eviction_epoch'] for r in h['cache']['readers'])>0,
                       epochs={r['name']:r['eviction_epoch'] for r in h['cache']['readers']})
        return answers

    def settle(self, label):
        self.phase='settle_'+label
        time.sleep(self.args.settle)
        h=self.stage('settled '+label)
        self.check('allocated settles near ONE soft target: '+label,
                   h['cache']['allocated_bytes']<=self.args.memory_limit_mb*1048576*1.25,
                   allocated=h['cache']['allocated_bytes'],soft_target=self.args.memory_limit_mb*1048576,
                   rss_mb=self.rss.now,hwm_mb=self.rss.peak,threshold_ratio=1.25)

    def replay(self, answers):
        self.phase='post_eviction_replay'
        results={}
        for table in [0,1]:
            found=None
            for answer in [a for a in answers if a['table']==table][:32]:
                sql=self.sql_range(table,answer['lo'],self.args.range_width)
                cold=self.compare(sql,f'post-eviction table {table} lo {answer["lo"]}')
                if cold['miss_delta']>0:
                    hot=self.compare(sql,f'post-eviction hot table {table}')
                    found={'lo':answer['lo'],'replay':cold,'hot':hot}
                    break
            results[str(table)]=found
            if not self.args.smoke:
                self.check(f'table {table} actually replays an evicted previously-read key',found is not None,result=found)
        self.report['replay']=results
        self.save()

    def update(self):
        self.phase='large_update'
        a_sql=f"SELECT {A_COLUMNS} FROM {self.a}.items WHERE id='a-6'"
        b_sql=f'SELECT {B_COLUMNS} FROM {self.b}.items WHERE id=42'
        bag_sql=f'SELECT {B_COLUMNS} FROM {self.b}.items WHERE id={MAX_ID}'
        a_range_sql=self.sql_range(0,6,2)
        a_range_before=self.query(a_range_sql,b.ENDPOINT)[0]
        a_range_after=[row for row in a_range_before if int(row[1])%5!=1]
        a_before=self.query(a_sql,b.ENDPOINT)[0]
        a_after=[[r[0],str(int(r[1])+100000),f'{int(r[2].split(".")[0])+1}.00',r[3],'updated-'+r[4]] for r in a_before]
        b_before=self.query(b_sql,b.ENDPOINT)[0]
        old_bag=self.query(bag_sql,b.ENDPOINT)[0]
        new_bag=[[r[0],r[1],'b-during-a',str(int(r[3])+1)] for r in old_bag]
        allowed={'a':[bag(a_before),bag(a_after)],'a_range':[bag(a_range_before),bag(a_range_after)],
                 'b':[bag(b_before)],'b_bag':[bag(old_bag),bag(new_bag)]}
        points=[]; errors=[]; halt=threading.Event()
        def traffic(label,sql):
            while not halt.is_set():
                try:
                    h=self.health(); states=self.tables(h)
                    rows,route,ms=self.query(sql)
                    point={'ts':time.time(),'label':label,'route':route,'ms':round(ms,3),
                           'a_applying':states[self.ids[0]]['applying'],'b_applying':states[self.ids[1]]['applying'],
                           'b_ready':states[self.ids[1]]['ready'],'b_age_ms':states[self.ids[1]]['last_sync_age_ms'],
                           'correct':bag(rows) in allowed[label]}
                    points.append(point)
                    if not point['correct']: errors.append({'point':point,'rows':rows})
                except Exception:
                    errors.append(traceback.format_exc())
                    break
                halt.wait(.25)
        threads=[threading.Thread(target=traffic,args=(label,sql),daemon=True) for label,sql in
                 [('a',a_sql),('a_range',a_range_sql),('b',b_sql),('b_bag',bag_sql)]]
        for thread in threads:thread.start()
        try:
            a_start=time.monotonic()
            self.query(f"UPDATE {self.a}.items SET bucket=bucket+100000,amount=amount+1.00,payload=concat('updated-',payload) WHERE bucket%5=1",b.ENDPOINT)
            a_sql_secs=time.monotonic()-a_start
            a_write_done=time.monotonic()
            seen=False
            until=time.monotonic()+120
            while time.monotonic()<until:
                h=self.health(); self.healthy(h); state=self.tables(h)[self.ids[0]]
                if state['applying']:seen=True;break
                if (state['last_batch'] or {}).get('changes',0)>=self.args.rows//5*2:break
                time.sleep(.02)
            self.check('observed A actual native mutation gate',seen or self.args.smoke,observed=seen)
            b_start=time.monotonic()
            self.query(f"UPDATE {self.b}.items SET label='b-during-a',revision=revision+1 WHERE id={MAX_ID}",b.ENDPOINT)
            b_done=time.monotonic()
            b_fresh=self.wait_fresh(ids=[self.ids[1]],since=b_done)
            b_delay=time.monotonic()-b_done
            b_independent=self.tables(b_fresh)[self.ids[0]]['applying']
            if self.args.expect_independent_sync:
                self.check('B fresh round commits before A finishes, below own staleness limit',
                           (b_independent and b_delay<self.args.max_staleness) or self.args.smoke,
                           a_still_applying=b_independent,b_fresh_round_after_sql_secs=round(b_delay,3))
                b_local,b_route,b_ms=self.query(bag_sql)
                self.check('B updated duplicate bag readable locally while A batch remains open',
                           bag(b_local)==bag(new_bag) and b_route=='readyset',route=b_route,ms=round(b_ms,3))
            a_fresh=self.wait_fresh(ids=[self.ids[0]],since=a_write_done)
            a_delay=time.monotonic()-a_write_done
            self.report['update']={'a_source_sql_secs':round(a_sql_secs,3),'a_catchup_after_sql_secs':round(a_delay,3),
                'b_source_sql_secs':round(b_done-b_start,3),'b_fresh_round_after_sql_secs':round(b_delay,3),
                'a_still_applying_at_b_fresh':b_independent,
                'a_last_batch':self.tables(a_fresh)[self.ids[0]]['last_batch'],
                'b_last_batch':self.tables(b_fresh)[self.ids[1]]['last_batch']}
        finally:
            halt.set()
            for thread in threads:thread.join(timeout=60)
            (self.out/'update_traffic.json').write_text(json.dumps(points,indent=2))
        self.check('all concurrent reads are complete allowed before/after bags',not errors and bool(points),samples=len(points),errors=errors[:5])
        impacted=[p for p in points if p['label']=='b' and p['a_applying'] and not p['b_applying']]
        local=[p for p in impacted if p['route']=='readyset']
        stale=[p for p in impacted if p['route']=='databend' and p['b_age_ms']>=self.args.max_staleness*1000]
        self.report['update']['b_during_a']={'samples':len(impacted),'local':len(local),'stale_upstream':len(stale),
            'p50_ms':percentile([p['ms'] for p in impacted],.5),'p95_ms':percentile([p['ms'] for p in impacted],.95),
            'max_sync_age_ms':max((p['b_age_ms'] for p in impacted),default=0),'routes':dict(Counter(p['route'] for p in impacted))}
        self.check('B has local reads while A mutation gate is held',bool(local) or self.args.smoke,**self.report['update']['b_during_a'])
        if self.args.expect_independent_sync:
            self.check('B no longer expires behind A large batch',not stale,stale_samples=len(stale))
        else:
            self.check('B safely falls back after its OWN freshness limit',bool(stale) or self.args.smoke,stale_samples=len(stale))
        state=self.tables(self.health())[self.ids[0]]
        self.check('whole A UPDATE captured once with all before/after images',state['last_batch']['changes']==self.args.rows//5*2,batch=state['last_batch'])
        for sql,label in [(a_sql,'A updated point'),(b_sql,'B unchanged point'),(bag_sql,'B delayed UPDATE retains duplicate bags'),
                          (self.sql_range(0,6,2),'A old range membership'),(self.sql_range(0,100006,2),'A moved range membership')]:
            self.compare(sql,label)
        self.stage('large update complete')

    def run(self):
        try:
            print('OUTPUT='+str(self.out)+' DATABASES='+self.a+','+self.b,flush=True)
            self.setup(); self.start(); self.baseline()
            answers=self.heat(self.args.heat)
            self.settle('balanced'); self.replay(answers)
            self.update(); self.settle('after_update')
            # Old pressure queries produce 60s local HTTP tombstones. Do not mistake the
            # 2048 lifecycle budget for native eviction; let the first burst expire.
            self.phase='lifecycle_cooldown'
            time.sleep(61 if not self.args.smoke else 1)
            self.heat(self.args.skew_heat,skew=True)
            self.settle('B_skew')
            for sql,label in [(self.probes[0],'A correct after B-only pressure'),(self.probes[1],'B correct after B-only pressure'),
                              (self.probes[-2],'A identical duplicates preserved'),(self.probes[-1],'B identical duplicates preserved')]:
                self.compare(sql,label)
            h=self.stage('final')
            self.check('both sources healthy after budget pressure and updates',all(t['failed'] is None for t in h['tables']),tables=h['tables'])
            self.report['peaks']={'allocated_bytes':max((p['cache']['allocated_bytes'] for p in self.samples),default=0),
                                  'rss_mb':self.rss.peak,'samples':len(self.samples),'monitor_errors':self.monitor_errors}
            audit=[json.loads(line) for line in (self.service/'source_queries.jsonl').read_text().splitlines()]
            self.check('exactly one Stream/table and no Databend helper tables',sum(r.get('sql','').startswith('CREATE STREAM ') for r in audit)==2 and
                       not any(r.get('sql','').startswith('CREATE TABLE ') for r in audit))
            self.report['passed']=True
        except Exception:
            self.report['error']=traceback.format_exc()
            print(self.report['error'],flush=True)
        finally:
            self.stop_monitor.set()
            if hasattr(self,'monitor_thread'):self.monitor_thread.join(timeout=15)
            if self.child and self.child.poll() is None:
                self.child.send_signal(signal.SIGTERM)
                try:self.child.wait(timeout=90)
                except subprocess.TimeoutExpired:self.child.kill();self.child.wait()
            if self.rss:
                self.rss.stop.set();self.rss.join(timeout=5);self.rss.file.close()
            if hasattr(self,'server_log'):self.server_log.close()
            self.query_log.close()
            self.save()
            print('RESULT '+json.dumps({'passed':self.report['passed'],'checks':len(self.report['checks']),'report':str(self.out/'report.json')}),flush=True)
        return 0 if self.report['passed'] else 1


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint',default='http://192.168.191.96:8000')
    parser.add_argument('--binary',default='/data1/vmbox/readyset/target/release/readyset-databend')
    parser.add_argument('--output',required=True)
    parser.add_argument('--rows',type=int,default=10000000)
    parser.add_argument('--memory-limit-mb',type=int,default=3072)
    parser.add_argument('--heat',type=int,default=1400)
    parser.add_argument('--skew-heat',type=int,default=600)
    parser.add_argument('--range-width',type=int,default=60)
    parser.add_argument('--max-staleness',type=float,default=5)
    parser.add_argument('--settle',type=float,default=15)
    parser.add_argument('--smoke',action='store_true')
    parser.add_argument('--expect-independent-sync',action=argparse.BooleanOptionalAction,default=True,
                        help='require B to commit while A is applying (default for native sync WAL)')
    parser.add_argument('--confirm-create',action='store_true')
    args=parser.parse_args()
    if not args.confirm_create:parser.error('--confirm-create required')
    if args.rows%BASE or args.rows<BASE:parser.error('rows must be a positive multiple of 100000')
    if args.range_width*(args.rows//BASE)+3>10000:parser.error('range result must stay under the native HTTP result cap')
    if args.heat>1500 or args.skew_heat>1500:parser.error('keep each heat burst below the 2048 HTTP tombstone cap')
    return Scale(args).run()


if __name__=='__main__':
    raise SystemExit(main())
