#!/usr/bin/env python3
"""Real Databend bootstrap/Stream recovery faults. ONLY new readyset_gateway_* DBs.
No helper tables, Stream cleanup, SQL retries, or existing service replacement.
Faults are applied by a loopback proxy only to this run's A capture transaction.
"""
import argparse
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time
import traceback
from urllib.parse import urlsplit
import urllib.error
import urllib.request
import uuid

from m0 import Client, NoRedirect


class FaultProxy:
    def __init__(self, endpoint, source, fault, output):
        self.target = urlsplit(endpoint)
        if self.target.scheme != 'http' or self.target.path not in ('', '/'):
            raise ValueError('explicit native HTTP endpoint required')
        self.source, self.fault = source, fault
        self.output = output
        self.hit, self.release = threading.Event(), threading.Event()
        self.fresh_release = threading.Event()
        self.lock = threading.Lock()
        self.owners, self.events = {}, []
        self.capture_session = None
        self.capture_node = None
        self.armed = True
        self.down = False
        self.consume_count = 0
        self.commit_count = 0
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                self.dispatch()

            def do_GET(self):
                self.dispatch()

            def dispatch(self):
                if proxy.down:
                    # Simulated Databend outage: refuse every request, nothing forwarded.
                    self.send_error(503, 'simulated Databend outage')
                    return
                try:
                    raw = self.rfile.read(int(self.headers.get('Content-Length', 0)))
                    body = json.loads(raw) if raw else {}
                    sql = body.get('sql', '').replace('`', '')
                    internal = body.get('session', {}).get('internal', {})
                    internal = json.loads(internal) if isinstance(internal, str) else internal
                    previous = (internal or {}).get('last_query_ids', [])
                    with proxy.lock:
                        owner = proxy.owners.get(previous[0]) if previous else None
                        if proxy.source in sql:
                            owner = proxy.source
                        elif not sql:
                            owner = proxy.owners.get(self.path.split('/')[3]) if self.path.startswith('/v1/query/') else None
                        snapshot = owner == proxy.source and ' AT (STREAM =>' in sql and not sql.startswith('SELECT count(*)')
                        consume = owner == proxy.source and 'WITH CONSUME' in sql
                        commit = owner == proxy.source and sql == 'COMMIT'
                        if consume:
                            proxy.consume_count += 1
                        if commit:
                            proxy.commit_count += 1
                        hook = None
                        if proxy.armed:
                            if snapshot and proxy.fault in ('late_commit', 'slow_bootstrap'):
                                hook = 'snapshot_response'
                            elif commit and proxy.fault in ('commit_reply_lost', 'crash_before_commit'):
                                hook = proxy.fault
                            elif consume and proxy.fault == 'slow_bootstrap_wait' and proxy.consume_count == 2:
                                hook = 'fresh_round'
                            if hook:
                                proxy.armed = False
                        event = {'sql': sql, 'path': self.path, 'owner': owner, 'hook': hook, 'utc': time.time()}
                        if sql:
                            proxy.events.append(event)
                    if hook in ('crash_before_commit', 'fresh_round'):
                        proxy.capture_session = body.get('session')
                        proxy.capture_node = self.headers.get('X-DATABEND-STICKY-NODE')
                        proxy.hit.set()
                        release = proxy.fresh_release if hook == 'fresh_round' else proxy.release
                        if not release.wait(30):
                            raise TimeoutError('fault release not received')
                        if hook == 'crash_before_commit':
                            event['not_forwarded'] = True
                            self.send_error(503, 'test withheld COMMIT before process death')
                            return
                    status, headers, data = proxy.forward(self.command, self.path, raw, dict(self.headers))
                    response = json.loads(data) if data else {}
                    qid = response.get('id')
                    with proxy.lock:
                        if qid and owner:
                            proxy.owners[qid] = owner
                        event.update(query_id=qid, state=response.get('state'), error=response.get('error'))
                    if hook == 'snapshot_response':
                        proxy.hit.set()
                        if not proxy.release.wait(30):
                            raise TimeoutError('snapshot fault release not received')
                    if hook == 'commit_reply_lost':
                        # Observe the server's terminal success before discarding EVERY byte
                        # of the COMMIT response. Never resubmit COMMIT to resolve ambiguity.
                        current = response
                        while current.get('next_uri'):
                            uri = current['next_uri']
                            if not uri.startswith('/v1/query/' + qid + '/'):
                                raise RuntimeError('unexpected COMMIT continuation')
                            _, _, page = proxy.forward('GET', uri, b'', dict(self.headers))
                            if uri.endswith('/final'):
                                break
                            current = json.loads(page)
                        if current.get('state') != 'Succeeded' or current.get('error'):
                            raise RuntimeError('COMMIT did not succeed before lost response')
                        event['observed_commit_success'] = True
                        proxy.hit.set()
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return
                    self.send_response(status)
                    for key, value in headers:
                        if key.lower() not in ('content-length', 'transfer-encoding', 'connection'):
                            self.send_header(key, value)
                    self.send_header('Content-Length', str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                except Exception as error:
                    with proxy.lock:
                        proxy.events.append({'proxy_error': repr(error), 'utc': time.time()})
                    try:
                        self.send_error(502, 'isolated fault proxy failed')
                    except OSError:
                        pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.origin = 'http://127.0.0.1:' + str(self.server.server_port)

    def forward(self, method, path, body, headers):
        if not (path.startswith('/v1/query') or path == '/v1/session/heartbeat'):
            raise RuntimeError('invalid proxy path')
        conn = http.client.HTTPConnection(self.target.hostname, self.target.port or 80, timeout=65)
        kept = {k: v for k, v in headers.items() if k.lower() not in ('host', 'connection', 'content-length', 'transfer-encoding')}
        try:
            conn.request(method, path, body=body or None, headers=kept)
            response = conn.getresponse()
            data = response.read(16 * 1024 * 1024 + 1)
            if len(data) > 16 * 1024 * 1024:
                raise RuntimeError('oversized upstream response')
            return response.status, response.getheaders(), data
        finally:
            conn.close()

    def close(self):
        self.release.set()
        self.fresh_release.set()
        self.server.shutdown()
        self.server.server_close()
        (self.output / 'proxy.json').write_text(json.dumps(self.events, indent=2))


class Scenario:
    def __init__(self, args, mode, fault, root):
        self.args, self.mode, self.fault = args, mode, fault
        self.output = root / (mode + '_' + fault)
        self.output.mkdir()
        self.checks = []
        nonce = uuid.uuid4().hex[:12]
        self.dbs = ['readyset_gateway_boundary_' + nonce + '_' + x for x in ('a', 'b')]
        self.a, self.b = [db + '.items' for db in self.dbs]
        self.instance = 'br' + nonce
        self.streams = [db + '.readyset_' + self.instance + '_items' for db in self.dbs]
        self.source = self.client('admin')
        self.old = self.client('old-consumer')
        self.proxy, self.proc, self.log = None, None, None
        self.run_count = 0
        self.origin = None

    def client(self, label):
        return Client(self.args.endpoint, self.args.user, os.environ.get('DATABEND_PASSWORD', ''), self.output / 'queries.jsonl', label)

    def check(self, name, actual, expected=True):
        passed = actual == expected
        self.checks.append({'name': name, 'actual': actual, 'expected': expected, 'passed': passed})
        print(('PASS ' if passed else 'FAIL ') + self.mode + '/' + self.fault + ': ' + name, flush=True)
        if not passed:
            raise AssertionError((name, actual, expected))

    def http(self, method, path, body=None):
        headers = {'Authorization': self.source.auth, 'Content-Type': 'application/json'}
        req = urllib.request.Request(self.origin + path, data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
        opener = urllib.request.build_opener(NoRedirect())
        try:
            response = opener.open(req, timeout=20)
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
        status, headers, first = self.http('POST', '/v1/query', {'sql': sql})
        self.check('query HTTP success: ' + sql[:60], status, 200)
        self.check('query route: ' + sql[:60], next((v for k, v in headers.items() if k.lower() == 'x-readyset-route'), None), route)
        if first.get('error'):
            raise RuntimeError(first)
        rows = list(first.get('data', []))
        response = first
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

    def launch(self):
        self.run_count += 1
        self.service = self.output / ('service_' + str(self.run_count))
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
retry_backoff = "60s"
[cache]
partial = {str(self.mode == 'partial').lower()}
{('memory_limit_mb = 512' if self.mode == 'partial' else '')}
upquery_timeout_ms = 10000
''')
        self.log = (self.output / ('server_' + str(self.run_count) + '.log')).open('w')
        self.proc = subprocess.Popen([self.args.gateway_binary, '--config', str(config), '--output', str(self.service), '--route-journal'], stdout=self.log, stderr=subprocess.STDOUT)

    def wait(self, condition, timeout=60):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            if self.proc is not None and self.proc.poll() is not None:
                raise RuntimeError('server exited: ' + str(self.proc.returncode))
            result = condition()
            if result:
                return result
            time.sleep(.05)
        raise TimeoutError('condition timeout')

    def listen(self):
        self.wait(lambda: (self.service / 'listening.json').exists())
        self.origin = json.loads((self.service / 'listening.json').read_text())['origin']

    def ready(self, ids=None, after=None):
        ids = ids or [self.a, self.b]
        started = time.monotonic()
        def condition():
            states = self.states()
            if any(states[i]['failed'] for i in ids):
                raise RuntimeError(states)
            return all(states[i]['ready'] and (after is None or states[i]['last_sync_age_ms'] <= (time.monotonic() - started) * 1000) for i in ids)
        self.wait(condition)

    def stop(self, crash=False):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
            try:
                self.proc.wait(timeout=75)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
                raise RuntimeError('server shutdown timed out')
        if self.log:
            self.log.close()
        self.proc = None

    def cache(self, suffix, source, columns):
        self.query('CREATE CACHE ' + suffix + ' FROM SELECT ' + columns + ' FROM ' + source + ' WHERE id=?', 'readyset')

    def verify_b(self):
        self.ready([self.b])
        self.cache('b_id_' + str(self.run_count), self.b, 'id,label')
        self.source.sql("UPDATE " + self.b + " SET label='b-new' WHERE id=18446744073709551615")
        self.ready([self.b], after=True)
        sql = 'SELECT id,label FROM ' + self.b + ' WHERE id=18446744073709551615'
        self.check('B duplicate update independently local and fresh', self.query(sql, 'readyset'), [['18446744073709551615', 'b-new']] * 3)

    def recover(self):
        self.stop()
        self.launch()
        self.listen()
        self.ready()
        self.cache('a_recovered', self.a, 'id,val')
        sql = "SELECT id,val FROM " + self.a + " WHERE id='bag'"
        self.check('fresh generation reconstructs complete A duplicate bag', self.query(sql, 'readyset'), [['bag', 'new']] * 3)
        self.check('A row count exact after rebuild', self.states()[self.a]['rows'], self.args.rows + 3)
        self.verify_b()

    def run(self):
        error = None
        try:
            self.check('real Databend available', bool(self.source.sql('SELECT version()')))
            for db in self.dbs:
                self.source.sql('CREATE DATABASE ' + db)
            self.source.sql('CREATE TABLE ' + self.a + ' (id STRING NOT NULL, val STRING NOT NULL)')
            self.source.sql("INSERT INTO " + self.a + " SELECT concat('id-',to_string(number)), 'old' FROM numbers(" + str(self.args.rows) + ')')
            self.source.sql("INSERT INTO " + self.a + " VALUES ('bag','old'),('bag','old'),('bag','old')")
            self.source.sql('CREATE TABLE ' + self.b + ' (id BIGINT UNSIGNED NOT NULL, label STRING NULL)')
            self.source.sql('INSERT INTO ' + self.b + ' VALUES (18446744073709551615,NULL),(18446744073709551615,NULL),(18446744073709551615,NULL)')
            for stream, table in zip(self.streams, (self.a, self.b)):
                self.source.sql('CREATE STREAM ' + stream + ' ON TABLE ' + table + ' APPEND_ONLY=false')
            if self.fault != 'slow_bootstrap':
                self.source.sql("UPDATE " + self.a + " SET val='new'")
            if self.fault == 'late_commit':
                self.old.sql('BEGIN')
                self.check('old consumer read whole Stream before delayed COMMIT', self.old.sql('SELECT count(*) FROM ' + self.streams[0] + ' WITH CONSUME'), [[str((self.args.rows + 3) * 2)]])
            self.proxy = FaultProxy(self.args.endpoint, self.a, self.fault, self.output)
            self.launch()
            self.check('fault hook reached only owned A transaction', self.proxy.hit.wait(60))
            if self.fault == 'late_commit':
                self.old.sql('COMMIT')
                self.check('old real COMMIT advanced Stream before new snapshot returned', self.source.sql('SELECT count(*) FROM ' + self.streams[0]), [['0']])
                self.proxy.release.set()
                self.listen()
                if self.args.expect_vulnerable:
                    self.ready()
                    self.cache('a_vulnerable', self.a, 'id,val')
                    time.sleep(6.2)
                    self.check('baseline falsely stays ready beyond staleness bound', self.states()[self.a]['ready'])
                    sql = "SELECT id,val FROM " + self.a + " WHERE id='bag'"
                    self.check('baseline upstream bag has real update', self.source.sql(sql), [['bag', 'new']] * 3)
                    self.check('baseline reproduces permanent old local duplicate bag', self.query(sql, 'readyset'), [['bag', 'old']] * 3)
                    return {'passed': True, 'checks': self.checks, 'expected_vulnerability_reproduced': True}
                self.wait(lambda: self.states()[self.a]['failed'])
                state = self.states()[self.a]
                self.check('late COMMIT rejected by native Stream version CAS', '2734' in state['failed'])
                self.check('A never ready after conflicting bootstrap', state['ready'], False)
                self.query("SELECT id,val FROM " + self.a + " WHERE id='bag'", 'databend')
                self.verify_b()
                self.recover()
            elif self.fault == 'commit_reply_lost':
                self.listen()
                self.wait(lambda: self.states()[self.a]['failed'])
                self.check('lost COMMIT reply fails A closed', self.states()[self.a]['ready'], False)
                self.check('COMMIT executed once without SQL retry', self.proxy.commit_count, 1)
                self.check('real Stream advanced despite discarded reply', self.source.sql('SELECT count(*) FROM ' + self.streams[0]), [['0']])
                self.query("SELECT id,val FROM " + self.a + " WHERE id='bag'", 'databend')
                self.verify_b()
                self.recover()
            elif self.fault == 'crash_before_commit':
                self.check('Stream unconsumed before withheld COMMIT', self.source.sql('SELECT count(*) FROM ' + self.streams[0]), [[str((self.args.rows + 3) * 2)]])
                self.stop(crash=True)
                self.proxy.release.set()
                cleanup = self.client('owned-abandoned-capture-rollback')
                cleanup.session = self.proxy.capture_session
                cleanup.node = self.proxy.capture_node
                cleanup.sql('ROLLBACK')
                self.check('SIGKILL before COMMIT retained complete Stream delta', self.source.sql('SELECT count(*) FROM ' + self.streams[0]), [[str((self.args.rows + 3) * 2)]])
                self.recover()
            else:
                self.source.sql("UPDATE " + self.a + " SET val='new'")
                time.sleep(6.2)
                self.proxy.hit.clear()
                self.proxy.fault = 'slow_bootstrap_wait'
                self.proxy.armed = True
                self.proxy.release.set()
                self.check('fresh post-bootstrap consume held separately', self.proxy.hit.wait(30))
                self.listen()
                self.check('aged pinned bootstrap not reported fresh', self.states()[self.a]['ready'], False)
                self.query("SELECT id,val FROM " + self.a + " WHERE id='bag'", 'databend')
                self.proxy.fresh_release.set()
                self.ready()
                self.cache('a_after_slow_snapshot', self.a, 'id,val')
                self.check('fresh next round catches post-pin writes', self.query("SELECT id,val FROM " + self.a + " WHERE id='bag'", 'readyset'), [['bag', 'new']] * 3)
                self.verify_b()
            self.check('one native server for both tables', self.health()['cache']['native_server_instances'], 1)
            self.check('no global native failure', self.health()['cache']['failed'], False)
            self.check('exactly one retained Stream per table', [self.source.sql("SELECT count(*) FROM system.streams WHERE database='" + db + "'") for db in self.dbs], [[['1']], [['1']]])
        except Exception:
            error = traceback.format_exc()
            print(error, flush=True)
        finally:
            if self.proxy:
                self.proxy.release.set()
                self.proxy.fresh_release.set()
            try:
                if self.origin and self.proc and self.proc.poll() is None:
                    (self.output / 'last_health.json').write_text(json.dumps(self.health(), indent=2))
                self.stop()
            except Exception:
                error = (error or '') + traceback.format_exc()
            if self.old.session.get('txn_state') in ('Active', 'Fail'):
                try:
                    self.old.sql('ROLLBACK')
                except Exception:
                    error = (error or '') + traceback.format_exc()
            if self.proxy:
                self.proxy.close()
            report = {'passed': error is None, 'mode': self.mode, 'fault': self.fault, 'databases': self.dbs, 'checks': self.checks, 'error': error}
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
    parser.add_argument('--expect-vulnerable', action='store_true', help='negative control: expect the archived pre-fix binary to return stale local bags')
    parser.add_argument('--modes', nargs='+', choices=['full', 'partial'], default=['full', 'partial'])
    parser.add_argument('--faults', nargs='+', choices=['late_commit', 'commit_reply_lost', 'crash_before_commit', 'slow_bootstrap'], default=['late_commit', 'commit_reply_lost', 'crash_before_commit', 'slow_bootstrap'])
    args = parser.parse_args()
    if args.rows < 0:
        parser.error('rows must be nonnegative')
    if args.expect_vulnerable and args.faults != ['late_commit']:
        parser.error('--expect-vulnerable requires --faults late_commit')
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    reports = [Scenario(args, mode, fault, output).run() for mode in args.modes for fault in args.faults]
    report = {'passed': all(r['passed'] for r in reports), 'checks': sum(len(r['checks']) for r in reports), 'cases': reports}
    (output / 'report.json').write_text(json.dumps(report, indent=2))
    print('RESULT ' + json.dumps({'passed': report['passed'], 'checks': report['checks'], 'report': str(output / 'report.json')}), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
