#!/usr/bin/env python3
"""Real Databend restart/persistence verifier. ONLY new readyset_gateway_* DBs, retained.

One output dir per mode, reused by every life of the server, so ReadySet's standalone
authority and base RocksDB reopen. Lives:
  1 fresh -> 2 clean restart (writes while down) -> 3 COMMIT response lost in-process ->
  4 SIGKILL while A's COMMIT is withheld (never executed) -> 5 SIGKILL mid A batch ->
  6 final clean restart.
No helper tables, SQL retries, Stream cleanup, or existing service replacement.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import traceback
import urllib.error
import urllib.request
import uuid

from bootstrap_recovery_verify import FaultProxy
from m0 import Client, NoRedirect

MAX = '18446744073709551615'


class Scenario:
    def __init__(self, args, mode, root):
        self.args, self.mode = args, mode
        self.output = root / mode
        self.output.mkdir()
        self.checks, self.events, self.lives = [], [], []
        nonce = uuid.uuid4().hex[:12]
        self.dbs = ['readyset_gateway_restart_' + nonce + '_' + x for x in ('a', 'b')]
        self.a, self.b = [db + '.items' for db in self.dbs]
        self.instance = 'rr' + nonce
        self.stream_a = self.dbs[0] + '.readyset_' + self.instance + '_items'
        self.source = Client(args.endpoint, args.user, os.environ.get('DATABEND_PASSWORD', ''), self.output / 'queries.jsonl', 'admin')
        self.proc = self.log = self.origin = self.proxy = None
        self.service = self.output / 'service'
        self.version = 0

    def note(self, event, **data):
        data.update(event=event, utc=time.time())
        self.events.append(data)
        print('EVENT ' + self.mode + ' ' + json.dumps(data, ensure_ascii=False)[:400], flush=True)

    def check(self, name, actual, expected=True):
        passed = actual == expected
        self.checks.append({'name': name, 'actual': actual, 'expected': expected, 'passed': passed})
        print(('PASS ' if passed else 'FAIL ') + self.mode + ': ' + name, flush=True)
        if not passed:
            raise AssertionError((name, actual, expected))

    def http(self, method, path, body=None, timeout=30):
        headers = {'Authorization': self.source.auth, 'Content-Type': 'application/json'}
        req = urllib.request.Request(self.origin + path, data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
        try:
            response = urllib.request.build_opener(NoRedirect()).open(req, timeout=timeout)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, dict(response.headers), json.load(response)

    def health(self):
        status, _, body = self.http('GET', '/health')
        if status != 200:
            raise RuntimeError(body)
        return body

    def states(self):
        return {t['table']: t for t in self.health()['tables']}

    def query(self, sql, route):
        status, headers, first = self.http('POST', '/v1/query', {'sql': sql}, timeout=60)
        self.check('HTTP 200: ' + sql[:60], status, 200)
        self.check('route ' + route + ': ' + sql[:60], next((v for k, v in headers.items() if k.lower() == 'x-readyset-route'), None), route)
        if first.get('error'):
            raise RuntimeError(first)
        rows, response = list(first.get('data', [])), first
        while response.get('next_uri'):
            uri = response['next_uri']
            if not uri.startswith('/v1/query/' + first['id'] + '/'):
                raise RuntimeError('invalid continuation')
            status, _, response = self.http('GET', uri)
            if status != 200 or response.get('error'):
                raise RuntimeError(response)
            if uri.endswith('/final'):
                break
            rows.extend(response.get('data', []))
        return rows

    def wait(self, condition, timeout, what):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            if self.proc.poll() is not None:
                raise RuntimeError('server exited: ' + str(self.proc.returncode))
            result = condition()
            if result:
                return result
            time.sleep(.05)
        raise TimeoutError(what + ': ' + json.dumps(self.health())[:3000])

    def fresh(self, table, timeout=120):
        started = time.monotonic()
        def condition():
            state = self.states()[table]
            if state['failed']:
                raise RuntimeError(table + ' failed: ' + state['failed'])
            return state['ready'] and state['last_sync_age_ms'] <= (time.monotonic() - started) * 1000
        self.wait(condition, timeout, table + ' fresh')

    def launch(self, life):
        config = self.output / 'config.toml'
        config.write_text(f'''instance_id = "{self.instance}"
confirm_create = true
[upstream]
url = "{self.proxy.origin}"
user = "{self.args.user}"
[listen]
http = "127.0.0.1:0"
[replication]
tables = ["{self.a}", "{self.b}"]
poll_interval = "100ms"
max_staleness = "5s"
[cache]
partial = {str(self.mode == 'partial').lower()}
{('memory_limit_mb = 512' if self.mode == 'partial' else '')}
upquery_timeout_ms = 10000
''')
        listening = self.service / 'listening.json'
        if listening.exists():
            listening.unlink()
        self.log = (self.output / f'server_{life}.log').open('w')
        started = time.monotonic()
        self.proc = subprocess.Popen([self.args.gateway_binary, '--config', str(config), '--output', str(self.service), '--route-journal'],
                                     stdout=self.log, stderr=subprocess.STDOUT)
        self.wait(lambda: listening.exists(), 600, 'listening')
        self.origin = json.loads(listening.read_text())['origin']
        self.fresh(self.a, 600)
        self.fresh(self.b, 600)
        states = self.states()
        info = {'life': life, 'ready_secs': round(time.monotonic() - started, 3),
                'recovery': {t: states[t]['snapshot'].get('recovery') for t in (self.a, self.b)},
                'load_secs': {t: states[t]['snapshot'].get('total_secs') for t in (self.a, self.b)},
                'rows': {t: states[t]['rows'] for t in (self.a, self.b)}}
        self.lives.append(info)
        self.note('life', **info)
        return info['recovery']

    def stop(self, kill=False):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try:
                code = self.proc.wait(timeout=90)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
                raise RuntimeError('shutdown timed out')
            self.note('stopped', kill=kill, code=code)
            if not kill:
                self.check('clean SIGTERM exit', code, 0)
        if self.log:
            self.log.close()
            self.log = None

    def verify(self, label):
        """Exact bags/counts local vs Databend, through caches restored from persistence."""
        sql_a = "SELECT id,val FROM " + self.a + " WHERE id='bag'"
        sql_b = 'SELECT id,label FROM ' + self.b + ' WHERE id=' + MAX
        self.fresh(self.a)
        self.fresh(self.b)
        for sql in (sql_a, sql_b):
            self.check(label + ' local equals Databend: ' + sql[:50], sorted(self.query(sql, 'readyset')), sorted(self.source.sql(sql)))
        sql = "SELECT id,val FROM " + self.a + " WHERE id='id-7'"
        self.check(label + ' A ordinary row local', self.query(sql, 'readyset'), self.source.sql(sql))
        states = self.states()
        for t in (self.a, self.b):
            self.check(label + ' row count ' + t[-7:], str(states[t]['rows']), self.source.sql('SELECT count(*) FROM ' + t)[0][0])

    def write(self, a_val, b_label):
        self.version += 1
        if a_val:
            self.source.sql("UPDATE " + self.a + " SET val='" + a_val + "' WHERE id='bag'")
        if b_label:
            self.source.sql("UPDATE " + self.b + " SET label='" + b_label + "' WHERE id=" + MAX)

    def arm(self, fault):
        self.proxy.hit.clear()
        self.proxy.release.clear()
        self.proxy.fault = fault
        self.proxy.armed = True

    def run(self):
        error = None
        try:
            for db in self.dbs:
                self.source.sql('CREATE DATABASE ' + db)
            self.source.sql('CREATE TABLE ' + self.a + ' (id STRING NOT NULL, val STRING NOT NULL)')
            self.source.sql("INSERT INTO " + self.a + " SELECT concat('id-',to_string(number)), 'old' FROM numbers(" + str(self.args.rows) + ')')
            self.source.sql("INSERT INTO " + self.a + " VALUES ('bag','old'),('bag','old'),('bag','old')")
            self.source.sql('CREATE TABLE ' + self.b + ' (id BIGINT UNSIGNED NOT NULL, label STRING NULL)')
            self.source.sql('INSERT INTO ' + self.b + ' VALUES (' + MAX + ',NULL),(' + MAX + ',NULL),(' + MAX + ',NULL)')
            self.proxy = FaultProxy(self.args.endpoint, self.stream_a, 'none', self.output)
            self.proxy.armed = False

            r = self.launch(1)
            self.check('life1 both created', r, {self.a: 'created', self.b: 'created'})
            self.query('CREATE CACHE a_id FROM SELECT id,val FROM ' + self.a + ' WHERE id=?', 'readyset')
            self.query('CREATE CACHE b_id FROM SELECT id,label FROM ' + self.b + ' WHERE id=?', 'readyset')
            self.write('v1', 'b1')
            self.verify('life1')

            # 2. Clean restart; writes while down are consumed after resume, caches persist.
            self.stop()
            self.write('v2', 'b2')
            r = self.launch(2)
            self.check('clean restart resumes both tables', r, {self.a: 'resumed', self.b: 'resumed'})
            self.check('declared caches restored without CREATE', sorted(self.query('SHOW CACHES', 'readyset')[i][0] for i in range(2)), ['a_id', 'b_id'])
            self.verify('life2')

            # 3. COMMIT executed but its response lost: A fails closed in-process, B continues.
            self.arm('commit_reply_lost')
            self.write('v3', None)
            self.check('A COMMIT executed then response dropped', self.proxy.hit.wait(60))
            self.wait(lambda: self.states()[self.a]['failed'], 60, 'A fail closed')
            self.check('A served from Databend while failed', sorted(self.query("SELECT id,val FROM " + self.a + " WHERE id='bag'", 'databend')), sorted(self.source.sql("SELECT id,val FROM " + self.a + " WHERE id='bag'")))
            self.write(None, 'b3')
            self.fresh(self.b)
            self.check('B unaffected', sorted(self.query('SELECT id,label FROM ' + self.b + ' WHERE id=' + MAX, 'readyset')), [[MAX, 'b3']] * 3)
            self.stop()
            r = self.launch(3)
            self.check('lost COMMIT reply: A resumes (Stream moved), B resumes', r, {self.a: 'resumed', self.b: 'resumed'})
            self.verify('life3')

            # 4. COMMIT withheld and never executed, then SIGKILL: only A is rebuilt.
            self.arm('crash_before_commit')
            self.write('v4', None)
            self.check('A COMMIT withheld', self.proxy.hit.wait(60))
            self.stop(kill=True)
            self.proxy.release.set()
            cleanup = Client(self.args.endpoint, self.args.user, os.environ.get('DATABEND_PASSWORD', ''), self.output / 'queries.jsonl', 'owned-abandoned-rollback')
            cleanup.session, cleanup.node = self.proxy.capture_session, self.proxy.capture_node
            cleanup.sql('ROLLBACK')
            self.write(None, 'b4')
            r = self.launch(4)
            self.check('uncommitted batch: A rebuilt, B resumed', r, {self.a: 'rebuilt: interrupted batch was not committed', self.b: 'resumed'})
            self.verify('life4')

            # 5. SIGKILL in the middle of a large A batch (every row deleted+inserted).
            self.source.sql("UPDATE " + self.a + " SET val='v5'")
            self.wait(lambda: self.states()[self.a]['applying'], 120, 'A applying')
            self.stop(kill=True)
            self.write(None, 'b5')
            r = self.launch(5)
            self.check('mid-batch crash: A rebuilt or resumed, B resumed', (r[self.b], r[self.a] in ('resumed', 'rebuilt: interrupted batch was not committed')), ('resumed', True))
            self.verify('life5')

            # 6. Final clean restart.
            self.stop()
            r = self.launch(6)
            self.check('final restart resumes both', r, {self.a: 'resumed', self.b: 'resumed'})
            self.verify('life6')
            self.check('one native server', self.health()['cache']['native_server_instances'], 1)
            self.check('one Stream per table', [self.source.sql("SELECT count(*) FROM system.streams WHERE database='" + db + "'") for db in self.dbs], [[['1']], [['1']]])
            self.check('no helper tables', [self.source.sql("SELECT count(*) FROM system.tables WHERE database='" + db + "'") for db in self.dbs], [[['1']], [['1']]])
        except Exception:
            error = traceback.format_exc()
            print(error, flush=True)
        finally:
            try:
                if self.proc and self.proc.poll() is None:
                    (self.output / 'last_health.json').write_text(json.dumps(self.health(), indent=2))
                self.stop()
            except Exception:
                error = (error or '') + traceback.format_exc()
            if self.proxy:
                self.proxy.close()
        report = {'passed': error is None, 'mode': self.mode, 'databases': self.dbs, 'lives': self.lives,
                  'checks': self.checks, 'events': self.events, 'error': error}
        (self.output / 'report.json').write_text(json.dumps(report, indent=2))
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--user', default='root')
    parser.add_argument('--gateway-binary', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--confirm-create', action='store_true', required=True)
    parser.add_argument('--rows', type=int, default=50001)
    parser.add_argument('--modes', nargs='+', choices=['full', 'partial'], default=['full', 'partial'])
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    reports = [Scenario(args, mode, output).run() for mode in args.modes]
    report = {'passed': all(r['passed'] for r in reports), 'checks': sum(len(r['checks']) for r in reports), 'cases': reports}
    (output / 'report.json').write_text(json.dumps(report, indent=2))
    print('RESULT ' + json.dumps({'passed': report['passed'], 'checks': report['checks'], 'report': str(output / 'report.json')}), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
