#!/usr/bin/env python3
"""Bounded live HTTP multi-table regression. Only NEW readyset_gateway_* databases.
Reuses the existing verifier's authenticated HTTP/pagination client. No cleanup, SQL retry,
service replacement, or extra native server. Run full and partial independently.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import traceback
import uuid

from gateway_verify import Verification
from m0 import Client


def bag(rows):
    return sorted(rows, key=lambda r: json.dumps(r, ensure_ascii=False))


class MultiVerification(Verification):
    def __init__(self, args):
        super().__init__(args)
        self.a = self.database + '_a'
        self.b = self.database + '_b'
        self.ids = [self.a + '.items', self.b + '.items']
        self.instance = 'mt' + uuid.uuid4().hex[:12]
        self.run_count = 0

    def health(self):
        return self.http('GET', '/health')['response']

    def states(self):
        return {t['table']: t for t in self.health()['tables']}

    def wait_sources(self, after=None, ids=None):
        after = after or {}
        ids = ids or self.ids
        wait_started = time.monotonic()
        until = wait_started + 90
        while time.monotonic() < until:
            h = self.health()
            if h.get('failed'):
                raise RuntimeError(h)
            states = {t['table']: t for t in h['tables']}
            if any(states[i]['failed'] for i in ids):
                raise RuntimeError(states)
            # An in-flight empty round may have started BEFORE the write but commit AFTER
            # it. Nonblocking per-table health is not the legacy serialized-health barrier.
            # For write fences require a successful round whose start is after this wait.
            fresh = lambda i: not after or states[i].get('last_sync_age_ms', 10**12) <= (time.monotonic() - wait_started) * 1000
            if all(states[i]['ready'] and states[i]['checkpoint'] > after.get(i, 0) and fresh(i) for i in ids):
                return h
            time.sleep(.05)
        raise TimeoutError(json.dumps(self.health()))

    def start(self):
        self.run_count += 1
        service = self.output / ('service_' + str(self.run_count))
        config = self.output / 'config.toml'
        config.write_text(f'''instance_id = "{self.instance}"
confirm_create = true
[upstream]
url = "{self.args.endpoint}"
user = "{self.args.user}"
[listen]
http = "127.0.0.1:0"
[replication]
tables = ["{self.a}.items", "{self.b}.items", "{self.a}.unsupported"]
poll_interval = "300ms"
max_staleness = "60s"
[cache]
partial = {str(self.args.partial).lower()}
{('memory_limit_mb = 512' if self.args.partial else '')}
upquery_timeout_ms = 10000
''')
        self.log = (self.output / f'server_{self.run_count}.log').open('w')
        self.child = subprocess.Popen([self.args.gateway_binary, '--config', str(config), '--output', str(service),
                                       '--enable-fault-injection', '--route-journal'], stdout=self.log, stderr=subprocess.STDOUT)
        until = time.monotonic() + 120
        while time.monotonic() < until:
            if self.child.poll() is not None:
                raise RuntimeError('server exited: ' + (self.output / f'server_{self.run_count}.log').read_text()[-6000:])
            if (service / 'listening.json').exists():
                info = json.loads((service / 'listening.json').read_text())
                self.origin = info['origin']
                self.service = service
                return self.wait_sources()
            time.sleep(.1)
        raise TimeoutError('startup')

    def stop(self):
        if self.child and self.child.poll() is None:
            self.child.send_signal(signal.SIGTERM)
            try:
                self.child.wait(timeout=75)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.child.wait()
                raise RuntimeError('test server failed to shut down cleanly')
        if getattr(self, 'log', None):
            self.log.close()

    def compare(self, sql, db=None, route='readyset', pagination=None):
        if route == 'readyset':
            self.wait_sources(ids=[self.a + '.items' if (db == self.a or self.a + '.items' in sql) else self.b + '.items'])
        expected = self.source.sql(sql if db is None else sql.replace('FROM items ', f'FROM {db}.items '))
        actual, schema, first = self.query(sql, session=None if db is None else {'database': db}, route=route, pagination=pagination)
        self.check('bag: ' + sql[:100], bag(actual), bag(expected))
        # Compare against real source types, not against the same response twice.
        source_a = db == self.a or self.a + '.items' in sql
        types = {'id': 'String' if source_a else 'UInt64', 'active': 'Boolean',
                 'amount': 'Nullable(Decimal(18, 2))', 'label': 'Nullable(String)'}
        projection = [c.strip() for c in sql[len('SELECT '):sql.index(' FROM ')].split(',')]
        self.check('schema has real projected columns/types', schema, [{'name': c, 'type': types[c]} for c in projection])
        return actual, schema

    def create(self, name, sql, db='default'):
        self.wait_sources()
        return self.query(f'CREATE CACHE {name} FROM {sql}', session={'database': db}, route='readyset')

    def setup(self):
        for db in [self.a, self.b]:
            self.source.sql('CREATE DATABASE ' + db)
        self.source.sql(f'CREATE TABLE {self.a}.items (id STRING NOT NULL,active BOOLEAN NOT NULL,amount DECIMAL(18,2) NULL)')
        self.source.sql(f'CREATE TABLE {self.b}.items (id BIGINT UNSIGNED NOT NULL,label STRING NULL)')
        self.source.sql(f'CREATE TABLE {self.a}.unsupported (value DOUBLE)')
        self.source.sql(f"INSERT INTO {self.a}.items VALUES ('same-key',true,12.30),('same-key',true,12.30),('Case',false,NULL),('case',true,-0.01),('中文😀',true,999.99)")
        self.source.sql(f"INSERT INTO {self.b}.items VALUES (18446744073709551615,NULL),(18446744073709551615,NULL),(0,''),(42,'中文😀')")

    def verify(self):
        a, b = self.a, self.b
        initial = self.start()
        self.check('one native server for all tables', initial['cache']['native_server_instances'], 1)
        self.check('no dummy or per-table engines', initial['cache']['base_tables'], 2)
        states = {t['table']: t for t in initial['tables']}
        self.check('heterogeneous snapshots', [states[i]['snapshot']['rows'] for i in self.ids], [5, 4])
        self.check('unsupported table isolated at preparation', bool(states[a + '.unsupported']['failed']), True)
        self.check('readiness survives an unsupported table', (initial['ready'], initial['all_tables_ready']), (True, False))
        self.check('exact stream identity', [states[i]['stream'] for i in self.ids], [f'{a}.readyset_{self.instance}_items', f'{b}.readyset_{self.instance}_items'])
        self.check('unauthenticated health denied', self.http('GET', '/health', auth=False)['status'], 401)
        self.check('unauthenticated liveness allowed', self.http('GET', '/livez', auth=False)['status'], 200)
        self.check('liveness distinct from readiness', self.http('GET', '/readyz', auth=False)['status'], 200)
        self.query(f'SELECT id,active,amount FROM {a}.items WHERE id=\'same-key\'', route='databend')
        self.create('a_id', f'SELECT id,active,amount FROM {a}.items WHERE id=?')
        self.create('a_bool', f'SELECT id,active FROM {a}.items WHERE active=?')
        self.create('a_range', f'SELECT id,amount FROM {a}.items WHERE amount>=? AND amount<=?')
        self.create('b_id', f'SELECT id,label FROM {b}.items WHERE id=?')
        self.create('b_label', f'SELECT id,label FROM {b}.items WHERE label=?')
        self.create('b_range', f'SELECT id,label FROM {b}.items WHERE id>=? AND id<=?')
        # Projection and physical key would both be Boolean: reject rather than a false fence.
        rejected = self.http('POST', '/v1/query', {'sql': f'CREATE CACHE ambiguous FROM SELECT active FROM {a}.items WHERE active=?'})
        self.check('small-domain-only marker rejected locally', (rejected['route'], rejected['response']['state']), ('readyset', 'Failed'))
        if rejected['response'].get('final_uri'):
            self.http('GET', rejected['response']['final_uri'])
        self.compare("SELECT id,active,amount FROM items WHERE id='same-key'", a, pagination={'max_rows_per_page': 1, 'max_rows_in_buffer': 1})
        for key in ['Case', 'case', '中文😀', 'not-found']:
            self.compare(f"SELECT id,active,amount FROM {a}.items WHERE id='{key}'")
        self.compare(f'SELECT id,active FROM {a}.items WHERE active=true')
        self.compare(f'SELECT id,active FROM {a}.items WHERE active=false')
        self.compare(f'SELECT id,amount FROM {a}.items WHERE amount>=-0.01 AND amount<=1000.00')
        self.compare('SELECT id,label FROM items WHERE id=18446744073709551615', b)
        self.compare(f"SELECT id,label FROM {b}.items WHERE label='中文😀'")
        self.compare(f'SELECT id,label FROM {b}.items WHERE id>=0 AND id<=42')
        self.query(f"SELECT id,label FROM {b}.items WHERE id=42", session={'database': a}, route='readyset')
        wrong = self.http('POST', '/v1/query', {'sql': 'SELECT id,label FROM items WHERE id=42', 'session': {'database': 'default'}, 'pagination': {'wait_time_secs': 5}})
        self.check('default DB never guesses a configured source', wrong['route'], 'databend')
        if wrong['response'].get('final_uri'):
            self.http('GET', wrong['response']['final_uri'])
        self.query(f'SELECT id,label FROM {b}.items WHERE id=42', session={'database': b, 'settings': {'timezone': 'Asia/Shanghai'}}, route='databend')
        self.wait_sources()
        self.query(f'SELECT x.id FROM {a}.items x JOIN {b}.items y ON x.id=to_string(y.id) WHERE y.id=42', route='databend')
        before = {i: self.states()[i]['checkpoint'] for i in self.ids}
        def write(which, sql):
            client = Client(self.args.endpoint, self.args.user, os.environ.get('DATABEND_PASSWORD', ''), self.output / f'writer_{which}.jsonl', which)
            return client.sql(sql)
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda pair: write(*pair), [('a', f"UPDATE {a}.items SET amount=amount+1.00 WHERE id='same-key'"), ('b', f"UPDATE {b}.items SET label='changed' WHERE id=18446744073709551615")]))
        self.wait_sources(after=before)
        rows, _ = self.compare(f"SELECT id,active,amount FROM {a}.items WHERE id='same-key'")
        self.check('decimal UPDATE retains both duplicates', rows, [['same-key', '1', '13.30']] * 2)
        self.compare(f'SELECT id,label FROM {b}.items WHERE id=18446744073709551615')
        # Separate checkpoints are not globally equalized; A-only mutations do not touch B.
        before = {i: self.states()[i]['checkpoint'] for i in self.ids}
        self.source.sql(f"DELETE FROM {a}.items WHERE id='case'")
        self.source.sql(f"INSERT INTO {a}.items VALUES ('same-key',true,13.30)")
        self.wait_sources(after=before)
        rows, _ = self.compare(f"SELECT id,active,amount FROM {a}.items WHERE id='same-key'")
        self.check('identical INSERT increases bag count', len(rows), 3)
        self.compare(f"SELECT id,active,amount FROM {a}.items WHERE id='case'")
        self.compare(f'SELECT id,label FROM {b}.items WHERE id=18446744073709551615')
        settled = self.states()[a + '.items']['checkpoint']
        self.wait_sources(after={a + '.items': settled})
        self.check('no duplicate delivery after independent COMMIT', len(self.compare(f"SELECT id,active,amount FROM {a}.items WHERE id='same-key'")[0]), 3)
        # Concurrent reads use the same shared runtime. Each response is independently checked.
        def lookup(i):
            sql = f"SELECT id,active,amount FROM {a}.items WHERE id='same-key'" if i % 2 else f'SELECT id,label FROM {b}.items WHERE id=18446744073709551615'
            rows, _, response = self.query(sql)
            expected = [['same-key', '1', '13.30']] * 3 if i % 2 else [['18446744073709551615', 'changed']] * 2
            return (bag(rows) == bag(expected), response['route'])
        with ThreadPoolExecutor(max_workers=8) as pool:
            answers = list(pool.map(lookup, range(100)))
        self.check('100 concurrent heterogeneous reads stay correct/local', answers, [(True, 'readyset')] * 100)
        self.query('DROP CACHE a_id', route='readyset')
        self.compare(f'SELECT id,label FROM {b}.items WHERE id=42')
        self.create('a_id', f'SELECT id,active,amount FROM {a}.items WHERE id=?')
        # Real paged INSERT/UPDATE changes: A's apply gate must not exclude B.
        before = self.states()[a + '.items']['checkpoint']
        self.source.sql(f"INSERT INTO {a}.items SELECT concat('bulk-',to_string(number)),number%2=0,CAST(number%100 AS DECIMAL(18,2)) FROM numbers(100000)")
        self.wait_sources(after={a + '.items': before})
        self.compare(f"SELECT id,active,amount FROM {a}.items WHERE id='bulk-42'")
        before = self.states()[a + '.items']['checkpoint']
        self.source.sql(f"UPDATE {a}.items SET amount=amount+1.00 WHERE id LIKE 'bulk-%'")
        applying_seen = False
        until = time.monotonic() + 45
        while time.monotonic() < until:
            states = self.states()
            if states[a + '.items']['failed']:
                raise RuntimeError(states)
            if states[a + '.items']['applying']:
                applying_seen = True
                self.query(f"SELECT id,active,amount FROM {a}.items WHERE id='same-key'", route='databend')
                for _ in range(20):
                    self.query(f'SELECT id,label FROM {b}.items WHERE id=42', route='readyset')
                break
            time.sleep(.02)
        self.check('observed real A mutation gate while B served locally', applying_seen, True)
        self.wait_sources(after={a + '.items': before})
        self.compare(f"SELECT id,active,amount FROM {a}.items WHERE id='bulk-42'")
        self.check('shared process memory budget configured once', self.health()['cache']['memory_limit_bytes'], 512*1024*1024 if self.args.partial else None)
        # Actual schema drift in partial; explicit table-only fault in full.
        if self.args.partial:
            self.source.sql(f'ALTER TABLE {a}.items ADD COLUMN extra BIGINT DEFAULT 0')
        else:
            self.check('scoped fault accepted', self.http('POST', '/__fault/table', {'table': a + '.items'})['status'], 200)
        until = time.monotonic() + 45
        while time.monotonic() < until and not self.states()[a + '.items']['failed']:
            time.sleep(.1)
        states = self.states()
        self.check('A fails closed', bool(states[a + '.items']['failed']), True)
        self.check('B stays online', states[b + '.items']['failed'], None)
        for _ in range(5):
            self.query(f"SELECT id,active,amount FROM {a}.items WHERE id='same-key'", route='databend')
            self.compare(f'SELECT id,label FROM {b}.items WHERE id=42')
        before = self.states()[b + '.items']['checkpoint']
        self.source.sql(f"UPDATE {b}.items SET label='survives' WHERE id=42")
        self.wait_sources(after={b + '.items': before}, ids=[b + '.items'])
        self.compare(f'SELECT id,label FROM {b}.items WHERE id=42')
        audit = [json.loads(line) for line in (self.service / 'source_queries.jsonl').read_text().splitlines()]
        self.check('one Stream per supported table', sum(e.get('sql', '').startswith('CREATE STREAM ') for e in audit), 2)
        self.check('no Databend helper tables', any(e.get('sql', '').startswith('CREATE TABLE ') for e in audit), False)
        self.check('PID stable throughout DDL/sync/failure', self.health()['engine_pid'], initial['engine_pid'])
        (self.output / 'before_restart.json').write_text(json.dumps(self.health(), ensure_ascii=False, indent=2))
        if not self.args.partial:
            self.stop()
            self.source.sql(f"INSERT INTO {a}.items VALUES ('restart',true,99.99)")
            self.source.sql(f"UPDATE {b}.items SET label='restart' WHERE id=42")
            restart = self.start()
            self.check('restart uses new common generation', restart['generation'] != initial['generation'], True)
            self.check('restart keeps instance Stream identity', [t['stream'] for t in restart['tables']], [t['stream'] for t in initial['tables']])
            self.check('restart explicitly starts empty cache catalog', self.query('SHOW CACHES', route='readyset')[0], [])
            self.create('restart_a', f'SELECT id,active,amount FROM {a}.items WHERE id=?')
            self.create('restart_b', f'SELECT id,label FROM {b}.items WHERE id=?')
            self.compare(f"SELECT id,active,amount FROM {a}.items WHERE id='restart'")
            self.compare(f'SELECT id,label FROM {b}.items WHERE id=42')

    def run(self):
        failure = None
        try:
            self.setup()
            self.verify()
        except Exception:
            failure = traceback.format_exc()
            print(failure, flush=True)
        finally:
            try:
                if self.origin:
                    (self.output / 'last_health.json').write_text(json.dumps(self.health(), ensure_ascii=False, indent=2))
                self.stop()
            except Exception:
                failure = (failure or '') + traceback.format_exc()
            (self.output / 'requests.json').write_text(json.dumps(self.requests, ensure_ascii=False, indent=2))
            report = {'passed': failure is None, 'partial': self.args.partial, 'databases': [self.a, self.b], 'output': str(self.output), 'checks': self.checks, 'error': failure}
            (self.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
            print(json.dumps({'passed': report['passed'], 'checks': len(self.checks), 'report': str(self.output / 'report.json')}, ensure_ascii=False), flush=True)
        return 0 if failure is None else 1


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--user', default='root')
    p.add_argument('--gateway-binary', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--partial', action='store_true')
    p.add_argument('--confirm-create', action='store_true')
    args = p.parse_args()
    p.error('--confirm-create required') if not args.confirm_create else None
    return MultiVerification(args).run()


if __name__ == '__main__':
    raise SystemExit(main())
