#!/usr/bin/env python3
"""Real Databend + native ReadySet domain failure isolation. ONLY new readyset_gateway_* DBs.

Injects ReadySet's own `handle-packet` failpoint (failure_injection build) into the base
domain that owns table A, then requires table B to keep syncing, serving fresh local reads
and accepting a new cache. No helper tables, SQL retries, cleanup or service replacement.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import struct
import subprocess
import time
import traceback
import urllib.error
import urllib.request
import uuid

from m0 import Client, NoRedirect


def bincode_str(value):
    data = value.encode()
    return struct.pack('<Q', len(data)) + data


class Scenario:
    def __init__(self, args, mode, fault, root, attempt=1):
        self.args, self.mode, self.fault = args, mode, fault
        self.output = root / (mode + '_' + fault + ('' if attempt == 1 else '_attempt' + str(attempt)))
        self.output.mkdir()
        self.checks, self.events = [], []
        nonce = uuid.uuid4().hex[:12]
        self.dbs = ['readyset_gateway_isolation_' + nonce + '_' + x for x in ('a', 'b')]
        self.a, self.b = [db + '.items' for db in self.dbs]
        self.instance = 'di' + nonce
        self.source = Client(args.endpoint, args.user, os.environ.get('DATABEND_PASSWORD', ''), self.output / 'queries.jsonl', 'admin')
        self.proc = self.log = self.origin = None
        self.failpoint_on = False

    def note(self, event, **data):
        data.update(event=event, utc=time.time())
        self.events.append(data)
        print('EVENT ' + self.mode + ' ' + json.dumps(data, ensure_ascii=False)[:300], flush=True)

    def check(self, name, actual, expected=True):
        passed = actual == expected
        self.checks.append({'name': name, 'actual': actual, 'expected': expected, 'passed': passed})
        print(('PASS ' if passed else 'FAIL ') + self.mode + ': ' + name, flush=True)
        if not passed:
            raise AssertionError((name, actual, expected))

    def http(self, method, url, body=None, raw=None, timeout=20):
        headers = {'Authorization': self.source.auth}
        data = raw
        if body is not None:
            headers['Content-Type'] = 'application/json'
            data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        opener = urllib.request.build_opener(NoRedirect())
        try:
            response = opener.open(req, timeout=timeout)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, dict(response.headers), response.read()

    def health(self):
        status, _, body = self.http('GET', self.origin + '/health')
        if status != 200:
            raise RuntimeError(body)
        return json.loads(body)

    def states(self):
        return {t['table']: t for t in self.health()['tables']}

    def query(self, sql, route):
        status, headers, raw = self.http('POST', self.origin + '/v1/query', {'sql': sql}, timeout=60)
        first = json.loads(raw)
        self.check('query HTTP success: ' + sql[:70], status, 200)
        self.check('query route: ' + sql[:70], next((v for k, v in headers.items() if k.lower() == 'x-readyset-route'), None), route)
        if first.get('error'):
            raise RuntimeError(first)
        rows, response = list(first.get('data', [])), first
        while response.get('next_uri'):
            uri = response['next_uri']
            if not uri.startswith('/v1/query/' + first['id'] + '/'):
                raise RuntimeError('invalid continuation')
            status, _, page = self.http('GET', self.origin + uri)
            response = json.loads(page)
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

    def fresh(self, table, timeout=60):
        """Ready, not failed, and a round that STARTED after this call committed."""
        started = time.monotonic()
        def condition():
            state = self.states()[table]
            if state['failed']:
                raise RuntimeError(table + ' failed: ' + state['failed'])
            return state['ready'] and state['last_sync_age_ms'] <= (time.monotonic() - started) * 1000
        self.wait(condition, timeout, table + ' fresh')

    def failpoint(self, action):
        url = self.targets['native_http'].rstrip('/') + '/failpoint'
        status, _, raw = self.http('POST', url, raw=bincode_str('handle-packet') + bincode_str(action))
        self.check('native failpoint accepted: ' + action, (status, raw[:4].hex()), (200, '00000000'))
        self.failpoint_on = action != 'off'
        self.note('failpoint', action=action)

    def b_round(self, label):
        self.source.sql("UPDATE " + self.b + " SET label='" + label + "' WHERE id=18446744073709551615")
        self.fresh(self.b)
        sql = 'SELECT id,label FROM ' + self.b + ' WHERE id=18446744073709551615'
        expected = [['18446744073709551615', label]] * 3
        self.check('B upstream duplicate bag ' + label, self.source.sql(sql), expected)
        self.check('B local fresh duplicate bag ' + label, self.query(sql, 'readyset'), expected)

    def a_upstream(self):
        sql = "SELECT id,val FROM " + self.a + " WHERE id='bag'"
        expected = self.source.sql(sql)
        self.check('A served from Databend with upstream bag', self.query(sql, 'databend'), expected)

    def launch(self, log_name='server.log'):
        service = self.output / 'service'
        config = self.output / 'config.toml'
        config.write_text(f'''instance_id = "{self.instance}"
confirm_create = true
[upstream]
url = "{self.args.endpoint}"
user = "{self.args.user}"
[listen]
http = "127.0.0.1:0"
[replication]
tables = ["{self.a}", "{self.b}"]
poll_interval = "100ms"
max_staleness = "5s"
retry_backoff = "45s"
[cache]
partial = {str(self.mode == 'partial').lower()}
{('memory_limit_mb = 512' if self.mode == 'partial' else '')}
upquery_timeout_ms = 10000
''')
        self.log = (self.output / log_name).open('w')
        env = dict(os.environ, RUST_LOG=os.environ.get('RUST_LOG', 'warn,readyset_server::controller=info'))
        self.proc = subprocess.Popen([self.args.gateway_binary, '--config', str(config), '--output', str(service),
                                      '--enable-fault-injection', '--route-journal'], stdout=self.log, stderr=subprocess.STDOUT, env=env)
        until = time.monotonic() + 120
        while not (service / 'listening.json').exists():
            if self.proc.poll() is not None or time.monotonic() > until:
                raise RuntimeError('startup failed')
            time.sleep(.1)
        self.origin = json.loads((service / 'listening.json').read_text())['origin']

    def isolated_phase(self):
        started = time.monotonic()
        a_failed = self.wait(lambda: self.states()[self.a]['failed'], self.args.a_fail_timeout, 'A fail closed')
        self.note('a_failed', seconds=round(time.monotonic() - started, 3), reason=a_failed)
        self.check('A fails closed (not ready)', self.states()[self.a]['ready'], False)
        self.a_upstream()
        # B must keep running while the failpoint is STILL active for A's domain.
        for i in range(3):
            self.b_round('b-during-' + str(i))
        # Graph migration for B while A's domain is failing/recovering.
        self.query('CREATE CACHE b_label FROM SELECT id,label FROM ' + self.b + ' WHERE label=?', 'readyset')
        sql = "SELECT id,label FROM " + self.b + " WHERE label='b-during-2'"
        self.check('B new cache local during A fault', self.query(sql, 'readyset'), [['18446744073709551615', 'b-during-2']] * 3)
        self.check('engine not globally failed during A fault', self.health()['cache']['failed'], False)
        self.failpoint('off')
        self.b_round('b-after')
        # A recovers ITSELF (no restart) after its backoff: a table stopped between batches
        # resumes; one whose batch died before COMMIT is rebuilt alone. B is never touched.
        started = time.monotonic()
        def self_recovered():
            state = self.states()[self.a]
            return (not state['failed'] and state['ready'] and state['last_sync_age_ms'] is not None
                    and state['last_sync_age_ms'] <= (time.monotonic() - started) * 1000)
        self.wait(self_recovered, 240, 'A self-recovery')
        recovery = self.states()[self.a]['snapshot'].get('recovery')
        self.note('a_self_recovery', seconds=round(time.monotonic() - started, 3), recovery=recovery)
        expected = 'resumed' if self.fault == 'rejects_requests' else 'rebuilt: interrupted batch was not committed'
        self.check('A recovered in-process', recovery, expected)
        sql = "SELECT id,val FROM " + self.a + " WHERE id='bag'"
        self.check('A local equals Databend after self-recovery', sorted(self.query(sql, 'readyset')), sorted(self.source.sql(sql)))
        self.check('B never failed', self.states()[self.b]['failed'], None)
        self.check('one native server for both tables', self.health()['cache']['native_server_instances'], 1)
        self.check('engine not globally failed', self.health()['cache']['failed'], False)
        self.check('one retained Stream per table', [self.source.sql("SELECT count(*) FROM system.streams WHERE database='" + db + "'") for db in self.dbs], [[['1']], [['1']]])
        # Restart on the same storage: A recovers from its own restart marker, B resumes.
        self.restart_after_fault()

    def restart_after_fault(self):
        self.proc.send_signal(signal.SIGTERM)
        self.check('SIGTERM exit after A fault', self.proc.wait(timeout=90), 0)
        self.log.close()
        listening = self.output / 'service' / 'listening.json'
        listening.unlink()
        self.source.sql("UPDATE " + self.b + " SET label='b-restart' WHERE id=18446744073709551615")
        self.launch('server_restart.log')
        self.fresh(self.a, 180)
        self.fresh(self.b, 180)
        states = self.states()
        recovery = {t: states[t]['snapshot'].get('recovery') for t in (self.a, self.b)}
        self.note('restart_recovery', **recovery)
        # A already recovered in-process, so both tables simply resume.
        self.check('A recovery after restart', recovery[self.a], 'resumed')
        self.check('B resumed after restart', recovery[self.b], 'resumed')
        for sql in ("SELECT id,val FROM " + self.a + " WHERE id='bag'",
                    'SELECT id,label FROM ' + self.b + ' WHERE id=18446744073709551615'):
            self.check('after restart local equals Databend: ' + sql[:50],
                       sorted(self.query(sql, 'readyset')), sorted(self.source.sql(sql)))
        self.check('A rows after restart', str(states[self.a]['rows']), self.source.sql('SELECT count(*) FROM ' + self.a)[0][0])

    def run(self):
        error = None
        try:
            for db in self.dbs:
                self.source.sql('CREATE DATABASE ' + db)
            self.source.sql('CREATE TABLE ' + self.a + ' (id STRING NOT NULL, val STRING NOT NULL)')
            self.source.sql("INSERT INTO " + self.a + " SELECT concat('id-',to_string(number)), 'old' FROM numbers(" + str(self.args.rows) + ')')
            self.source.sql("INSERT INTO " + self.a + " VALUES ('bag','old'),('bag','old'),('bag','old')")
            self.source.sql('CREATE TABLE ' + self.b + ' (id BIGINT UNSIGNED NOT NULL, label STRING NULL)')
            self.source.sql('INSERT INTO ' + self.b + ' VALUES (18446744073709551615,NULL),(18446744073709551615,NULL),(18446744073709551615,NULL)')
            self.launch()
            self.fresh(self.a, 180)
            self.fresh(self.b, 180)
            self.query('CREATE CACHE a_bag FROM SELECT id,val FROM ' + self.a + ' WHERE id=?', 'readyset')
            self.query('CREATE CACHE b_id FROM SELECT id,label FROM ' + self.b + ' WHERE id=?', 'readyset')
            sql_a = "SELECT id,val FROM " + self.a + " WHERE id='bag'"
            self.check('A local before fault', self.query(sql_a, 'readyset'), [['bag', 'old']] * 3)
            self.b_round('b-before')
            status, _, raw = self.http('GET', self.origin + '/__fault/native')
            self.check('fault target discovery', status, 200)
            self.targets = json.loads(raw)
            self.note('targets', **self.targets)
            domains = self.targets['base_domains']
            self.check('A and B have separate native base domains', domains[self.a] != domains[self.b])
            if self.fault == 'transient_crash':
                # Large A batch (every row deleted+inserted, many 10K Table RPCs). Once A is
                # applying, a ONE-SHOT native failpoint makes A's next Input packet fail: the
                # replica exits and ReadySet's controller recovers the domain on its own.
                # fail's one-shot count is consumed by the next packet of ANY domain (also the
                # idle Spin packets of other domains) and fails it only if that packet is A's
                # (readyset-dataflow Domain::check_failpoint). So re-arm while A is still
                # applying, and disarm as soon as A failed: no pending one-shot may hit B later.
                self.source.sql("UPDATE " + self.a + " SET val='new'")
                self.wait(lambda: self.states()[self.a]['applying'], 120, 'A applying')
                arms = 0
                until = time.monotonic() + 60
                while time.monotonic() < until:
                    self.failpoint('1*return(' + str(domains[self.a]) + ')')
                    arms += 1
                    time.sleep(.02)
                    state = self.states()[self.a]
                    if state['failed'] or not state['applying']:
                        break
                self.failpoint('off')
                self.note('one_shot_arms', arms=arms)
            else:
                # A is first stopped by the adapter, so no adapter traffic reaches A's domain.
                # Then A's native domain rejects every packet/request, for as long as needed.
                status, _, _ = self.http('POST', self.origin + '/__fault/table', {'table': self.a})
                self.check('A stopped before its domain fault', status, 200)
                # No in-flight A round may still contact A's domain once the fault is active.
                quiet = 0
                until = time.monotonic() + 60
                while quiet < 5:
                    state = self.states()[self.a]
                    quiet = quiet + 1 if state['failed'] and not state['syncing'] else 0
                    if time.monotonic() > until:
                        raise TimeoutError('A worker did not stop')
                    time.sleep(.1)
                self.failpoint('return(' + str(domains[self.a]) + ')')
                self.source.sql("UPDATE " + self.a + " SET val='new' WHERE id='bag'")
            if self.args.expect_vulnerable:
                # Pre-fix control: keep B writing while A's native domain keeps failing.
                b_failure = None
                until = time.monotonic() + 90
                n = 0
                while time.monotonic() < until and b_failure is None:
                    n += 1
                    self.source.sql("UPDATE " + self.b + " SET label='b-v" + str(n) + "' WHERE id=18446744073709551615")
                    deadline = time.monotonic() + 3
                    while time.monotonic() < deadline:
                        b_failure = self.states()[self.b]['failed']
                        if b_failure:
                            break
                        time.sleep(.1)
                self.note('baseline_b', failed=b_failure, rounds=n)
                self.check('baseline: A domain failure drags B down', b_failure is not None)
                self.check('baseline: B failure is a checkpoint/offset RPC error', any(s in b_failure for s in ('replication', 'Domain', 'domain', 'offset', 'checkpoint')))
                self.failpoint('off')
                self.check('baseline: B stays failed after fault cleared', self.states()[self.b]['failed'] is not None)
            else:
                self.isolated_phase()
        except Exception:
            error = traceback.format_exc()
            print(error, flush=True)
        finally:
            try:
                if self.failpoint_on:
                    self.failpoint('off')
            except Exception:
                error = (error or '') + traceback.format_exc()
            try:
                if self.proc and self.proc.poll() is None:
                    (self.output / 'last_health.json').write_text(json.dumps(self.health(), indent=2))
                    self.proc.send_signal(signal.SIGTERM)
                    try:
                        code = self.proc.wait(timeout=75)
                        self.note('shutdown', code=code)
                    except subprocess.TimeoutExpired:
                        self.proc.kill()
                        self.proc.wait()
                        raise RuntimeError('server shutdown timed out')
            except Exception:
                error = (error or '') + traceback.format_exc()
            if self.log:
                self.log.close()
            log = (self.output / 'server.log').read_text(errors='replace') if (self.output / 'server.log').exists() else ''
            # Only the life with the injected fault; the restart log must not inflate the counts.
            # Evidence that the fault was a real native domain failure handled by ReadySet.
            self.note('native_log', domain_failed=log.count('domain failed with an error'),
                      controller_recovery=log.count('Handling failure of domain'))
            if error is None and self.fault == 'transient_crash':
                try:
                    self.check('native replica of A really exited', log.count('domain failed with an error') >= 1)
                    self.check('controller ran native domain recovery', log.count('Handling failure of domain') >= 1)
                    self.check('native recovery succeeded (no controller failure)', log.count('Controller failed'), 0)
                except AssertionError:
                    error = traceback.format_exc()
        report = {'passed': error is None, 'mode': self.mode, 'fault': self.fault, 'databases': self.dbs, 'checks': self.checks, 'events': self.events, 'error': error}
        (self.output / 'report.json').write_text(json.dumps(report, indent=2))
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--user', default='root')
    parser.add_argument('--gateway-binary', required=True, help='a failure_injection build')
    parser.add_argument('--output', required=True)
    parser.add_argument('--confirm-create', action='store_true', required=True)
    parser.add_argument('--rows', type=int, default=50001)
    parser.add_argument('--a-fail-timeout', type=float, default=180)
    parser.add_argument('--modes', nargs='+', choices=['full', 'partial'], default=['full', 'partial'])
    parser.add_argument('--faults', nargs='+', choices=['rejects_requests', 'transient_crash'], default=['rejects_requests', 'transient_crash'])
    parser.add_argument('--expect-vulnerable', action='store_true', help='negative control for the pre-fix binary')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    reports = []
    for mode in args.modes:
        for fault in args.faults:
            for attempt in range(1, 4):
                report = Scenario(args, mode, fault, output, attempt).run()
                report['attempt'] = attempt
                reports.append(report)
                # A one-shot failpoint may be consumed by another domain's packet first. That
                # attempt proves nothing either way; rerun with fresh databases (max 3).
                inconclusive = fault == 'transient_crash' and report['error'] and 'A fail closed' in report['error']
                if not inconclusive:
                    break
    final = [r for r in reports if not (r['fault'] == 'transient_crash' and r['error'] and 'A fail closed' in r['error'])]
    report = {'passed': all(r['passed'] for r in reports if r in final) and len({(r['mode'], r['fault']) for r in final}) == len(args.modes) * len(args.faults),
              'checks': sum(len(r['checks']) for r in final), 'attempts': len(reports), 'cases': reports}
    (output / 'report.json').write_text(json.dumps(report, indent=2))
    print('RESULT ' + json.dumps({'passed': report['passed'], 'checks': report['checks'], 'report': str(output / 'report.json')}), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
