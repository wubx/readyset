#!/usr/bin/env python3
"""Recovery after forwarded requests whose outcome the gateway cannot confirm.

Creates readyset_gateway_unc_* with table `a` (id BIGINT, v VARCHAR), starts readyset-databend
with [upstream] forward_timeout = 5s and a cache on `id`, then:
  rejected   a start Databend refuses before creating a query (unknown role): never uncertain,
             local reads continue;
  timeout    a slow single-table INSERT (about 10-15 s in Databend) sent with wait_time_secs=30:
             the gateway gives up after 5 s (502), the INSERT still commits later;
  abandoned  the same slow INSERT answered `Running` (wait_time_secs=1) and never polled.
For the last two, a poller reads the inserted key through the gateway every 200 ms while
another thread watches Databend directly. Checked: while unresolved every read is forwarded;
once Databend shows the row, no gateway read misses it (no stale local read); local reads
come back after the statement finished, with the row. Creates only readyset_gateway_*
objects; no cleanup.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request

HEAVY = '(SELECT sum(number % 7) FROM numbers({n})) >= 0'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--user', default='root')
    p.add_argument('--gateway-binary', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--port', type=int, default=18105)
    p.add_argument('--heavy-rows', type=int, default=60_000_000_000, help='numbers() size of the slow subquery')
    p.add_argument('--confirm-create', action='store_true', required=True)
    args = p.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    password = os.environ.get('DATABEND_PASSWORD', '')
    auth = 'Basic ' + base64.b64encode(f'{args.user}:{password}'.encode()).decode()
    db = 'readyset_gateway_unc_' + time.strftime('%Y%m%d_%H%M%S') + '_' + os.urandom(3).hex()
    a = f'{db}.a'
    gw = f'http://127.0.0.1:{args.port}'
    up = args.endpoint
    report = {'db': db, 'args': vars(args), 'scenarios': {}, 'checks': []}

    def http(origin, method, path, body=None, timeout=120):
        req = urllib.request.Request(origin + path, method=method,
                                     data=None if body is None else json.dumps(body).encode(),
                                     headers={'Authorization': auth, 'Content-Type': 'application/json'})
        try:
            r = urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            r = e
        with r:
            raw = r.read()
            try:
                body = json.loads(raw) if raw else {}
            except ValueError:
                body = {'raw': raw[:200].decode('utf-8', 'replace')}
            return r.status, r.headers.get('X-ReadySet-Route'), body

    def sql(origin, text, extra=None, timeout=120):
        status, route, resp = http(origin, 'POST', '/v1/query', {'sql': text, **(extra or {})}, timeout)
        rows = list(resp.get('data') or [])
        while resp.get('next_uri') and not resp.get('error'):
            uri = resp['next_uri']
            _, _, resp = http(origin, 'GET', uri, timeout=timeout)
            if uri.endswith('/final'):
                break
            rows += resp.get('data') or []
        return status, route, resp, rows

    def must(text):
        status, _, resp, rows = sql(up, text)
        if status != 200 or resp.get('error'):
            raise RuntimeError(f'{text[:80]}: {status} {resp.get("error")}')
        return rows

    def health():
        return http(gw, 'GET', '/health', timeout=10)[2]

    def check(name, ok, detail=''):
        report['checks'].append({'check': name, 'ok': bool(ok), 'detail': detail})
        print(('PASS ' if ok else 'FAIL ') + name + (f': {detail}' if detail else ''), flush=True)

    def point(k):
        status, route, resp, rows = sql(gw, f'SELECT id, v FROM {a} WHERE id = {k}', timeout=30)
        return status, route, rows

    def wait_local(k, secs=30):
        until = time.time() + secs
        while time.time() < until:
            status, route, rows = point(k)
            if route == 'readyset':
                return True, rows
            time.sleep(0.2)
        return False, None

    for s in (f'CREATE DATABASE {db}',
              f'CREATE TABLE {a} (id BIGINT NOT NULL, v VARCHAR NOT NULL)',
              f"INSERT INTO {a} SELECT number, concat('v', to_string(number)) FROM numbers(10000)"):
        must(s)
    (out / 'config.toml').write_text(f'''instance_id = "unc{os.urandom(3).hex()}"
confirm_create = true
[upstream]
url = "{up}"
user = "{args.user}"
forward_timeout = "5s"
[listen]
http = "127.0.0.1:{args.port}"
[replication]
tables = ["{a}"]
poll_interval = "1s"
max_staleness = "10s"
[storage]
dir = "{out / 'storage'}"
''')
    log = (out / 'server.log').open('w')
    server = subprocess.Popen([args.gateway_binary, '--config', str(out / 'config.toml'), '--route-journal'],
                              stdout=log, stderr=subprocess.STDOUT)
    try:
        while True:
            if server.poll() is not None:
                raise RuntimeError('server exited')
            try:
                if health()['tables'][0]['ready']:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        status, _, resp, _ = sql(gw, f'CREATE CACHE unc_id FROM SELECT id, v FROM {a} WHERE id = ?')
        check('create cache', status == 200 and not resp.get('error'), json.dumps(resp.get('error')))
        ok, rows = wait_local(42)
        check('baseline read is local', ok and rows == [['42', 'v42']], str(rows))

        # 1. Rejected before a query exists: certain that nothing ran.
        status, route, resp, _ = sql(gw, 'SELECT 1', {'session': {'role': 'no_such_role'}})
        h = health()
        check('rejected start passes Databend error through', status == 200 and resp.get('error') and resp.get('id') == '',
              json.dumps(resp.get('error'))[:160])
        check('rejected start is not uncertain', not h['uncertain_upstream'] and h['unresolved_upstream'] == 0,
              f"uncertain={h['uncertain_upstream']} unresolved={h['unresolved_upstream']}")
        ok, rows = wait_local(42, 10)
        check('local reads continue after a rejected start', ok and rows == [['42', 'v42']], str(rows))
        report['scenarios']['rejected'] = {'status': status, 'error': resp.get('error')}

        def slow_insert(name, key, wait_secs):
            """Send the slow INSERT through the gateway; watch gateway reads and Databend."""
            text = f"INSERT INTO {a} SELECT {key}, 'slow{key}' FROM numbers(1) WHERE {HEAVY.format(n=args.heavy_rows)}"
            stop = threading.Event()
            reads, commit = [], {}
            started = time.time()

            def watch_databend():
                while not stop.is_set():
                    rows = must(f'SELECT count(*) FROM {a} WHERE id = {key}')
                    if rows[0][0] == '1':
                        commit['at'] = time.time()
                        return
                    time.sleep(0.2)

            def poll_gateway():
                while not stop.is_set():
                    t = time.time()
                    try:
                        status, route, rows = point(key)
                        reads.append({'t': t, 'status': status, 'route': route, 'found': bool(rows)})
                    except Exception as e:
                        reads.append({'t': t, 'error': str(e)[:120]})
                    time.sleep(0.2)

            threads = [threading.Thread(target=watch_databend, daemon=True), threading.Thread(target=poll_gateway, daemon=True)]
            for t in threads:
                t.start()
            t0 = time.time()
            status, route, resp = http(gw, 'POST', '/v1/query', {'sql': text, 'pagination': {'wait_time_secs': wait_secs}}, timeout=120)
            answered = time.time()
            h_after = health()
            # Wait for the commit, then for local reads to return with the row.
            deadline = time.time() + 180
            while 'at' not in commit and time.time() < deadline:
                time.sleep(0.5)
            recovered = None
            while time.time() < deadline:
                if any(r.get('route') == 'readyset' and r.get('found') and r['t'] > commit.get('at', 1e18) for r in reads[-5:]):
                    recovered = time.time()
                    break
                time.sleep(0.5)
            time.sleep(2)
            stop.set()
            for t in threads:
                t.join(timeout=10)
            h_end = health()
            c = commit.get('at')
            stale = [r for r in reads if c and r['t'] > c + 0.05 and not r.get('found') and 'error' not in r]
            local_while_unresolved = [r for r in reads if r.get('route') == 'readyset' and r['t'] > answered and (c is None or r['t'] < c)]
            errors = [r for r in reads if 'error' in r or r.get('status') != 200]
            result = {
                'gateway_status': status, 'gateway_answer_secs': round(answered - t0, 2),
                'response_state': resp.get('state'), 'response_error': resp.get('error'),
                'uncertain_after_answer': h_after['uncertain_upstream'], 'unresolved_after_answer': h_after['unresolved_upstream'],
                'inflight_after_answer': h_after['inflight'],
                'commit_secs_after_send': round(c - t0, 2) if c else None,
                'local_again_secs_after_commit': round(recovered - c, 2) if recovered and c else None,
                'reads': len(reads), 'reads_local': sum(r.get('route') == 'readyset' for r in reads),
                'reads_after_commit_missing_row': len(stale), 'local_reads_while_unresolved': len(local_while_unresolved),
                'read_errors': len(errors),
                'uncertain_at_end': h_end['uncertain_upstream'], 'unresolved_at_end': h_end['unresolved_upstream'],
                'inflight_at_end': h_end['inflight'],
                'counters': {k: h_end['counters'].get(k) for k in ('unresolved', 'resolved')},
            }
            report['scenarios'][name] = result
            print('SCENARIO ' + name + ' ' + json.dumps(result), flush=True)
            return result

        # 2. The gateway gives up waiting; Databend commits afterwards.
        r = slow_insert('timeout', 20_000_001, 30)
        check('timeout: gateway answers 502 after forward_timeout', r['gateway_status'] == 502 and 4 <= r['gateway_answer_secs'] <= 8,
              f"{r['gateway_status']} after {r['gateway_answer_secs']}s")
        check('timeout: outcome recorded as unresolved', r['uncertain_after_answer'] and r['unresolved_after_answer'] == 1)
        check('timeout: the write commits after the gateway gave up', r['commit_secs_after_send'] and r['commit_secs_after_send'] > r['gateway_answer_secs'],
              f"commit after {r['commit_secs_after_send']}s")
        check('timeout: no local read while unresolved', r['local_reads_while_unresolved'] == 0)
        check('timeout: no read misses the committed row', r['reads_after_commit_missing_row'] == 0)
        check('timeout: local reads return after the write finished', r['local_again_secs_after_commit'] is not None,
              f"{r['local_again_secs_after_commit']}s after commit")
        check('timeout: resolved at the end', not r['uncertain_at_end'] and r['unresolved_at_end'] == 0 and r['counters']['resolved'] >= 1)
        check('timeout: no read errors', r['read_errors'] == 0)

        # 3. Answered Running, never polled by the client.
        r = slow_insert('abandoned', 20_000_002, 1)
        check('abandoned: first answer is Running', r['gateway_status'] == 200 and r['response_state'] == 'Running', str(r['response_state']))
        check('abandoned: tracked as in flight', r['inflight_after_answer'] == 1)
        check('abandoned: no local read before the write finished', r['local_reads_while_unresolved'] == 0)
        check('abandoned: no read misses the committed row', r['reads_after_commit_missing_row'] == 0)
        check('abandoned: local reads return after the write finished', r['local_again_secs_after_commit'] is not None,
              f"{r['local_again_secs_after_commit']}s after commit")
        check('abandoned: nothing left in flight or unresolved', r['inflight_at_end'] == 0 and r['unresolved_at_end'] == 0 and not r['uncertain_at_end'])
        check('abandoned: no read errors', r['read_errors'] == 0)

        # Final consistency: both slow rows are local and equal to Databend.
        for key in (20_000_001, 20_000_002, 42):
            _, route, rows = point(key)
            want = must(f'SELECT id, v FROM {a} WHERE id = {key}')
            check(f'final read of {key} is local and equal', route == 'readyset' and rows == want, f'{route} {rows}')
        h = health()
        report['health'] = {'counters': h['counters'], 'rows': h['tables'][0]['rows']}
        check('local rows equal Databend', h['tables'][0]['rows'] == int(must(f'SELECT count(*) FROM {a}')[0][0]))
    finally:
        server.send_signal(signal.SIGTERM)
        try:
            report['exit_code'] = server.wait(timeout=120)
        except subprocess.TimeoutExpired:
            server.kill()
            report['exit_code'] = 'killed'
        log.close()
        events = sorted((out / 'storage' / 'runs').glob('*/events.jsonl'))
        if events:
            report['events'] = [json.loads(l) for l in events[-1].read_text().splitlines()
                                if '"upstream_' in l]
        report['passed'] = bool(report['checks']) and all(c['ok'] for c in report['checks'])
        (out / 'report.json').write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print('RESULT ' + json.dumps({'passed': report['passed'], 'checks': len(report['checks']),
                                  'report': str(out / 'report.json')}), flush=True)


if __name__ == '__main__':
    main()
