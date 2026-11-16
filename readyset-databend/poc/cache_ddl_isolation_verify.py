#!/usr/bin/env python3
"""Cache DDL isolation: building caches on a large table A must not pause table B.

Creates a new readyset_gateway_cddl_* database with A (ROWS rows, default 10M) and a small
B, starts readyset-databend over both (one shared runtime), declares a_id and b_id, then:
  - a writer updates B every --write-interval seconds directly in Databend;
  - readers query B (b_id) and A (a_id) through readyset-databend continuously;
  - /health is sampled every 0.2s;
  - after a baseline, three CREATE CACHE on A run back to back, then one DROP CACHE.
Pass criteria: every A DDL succeeds (no "source is not ready"); during the DDL window B
stays locally readable and fresh; B's write->visible lag stays bounded; A forwards only
around its own index builds and is local again afterwards; final rows match Databend.
Run it with a binary from before the per-source migration guard as a negative control.
Creates only readyset_gateway_* objects; no cleanup.
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


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--user', default='root')
    p.add_argument('--gateway-binary', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--rows', type=int, default=10_000_000)
    p.add_argument('--port', type=int, default=18102)
    p.add_argument('--write-interval', type=float, default=0.5)
    p.add_argument('--baseline', type=float, default=20)
    p.add_argument('--b-fresh-max', type=float, default=3.0, help='max B write->visible lag (s)')
    p.add_argument('--expect-isolated', choices=['yes', 'no'], default='yes',
                   help="'no' for the negative control: B is expected to stall")
    p.add_argument('--confirm-create', action='store_true', required=True)
    args = p.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    password = os.environ.get('DATABEND_PASSWORD', '')
    auth = 'Basic ' + base64.b64encode(f'{args.user}:{password}'.encode()).decode()
    db = 'readyset_gateway_cddl_' + time.strftime('%Y%m%d_%H%M%S') + '_' + os.urandom(3).hex()
    a, b = f'{db}.a', f'{db}.b'
    gw = f'http://127.0.0.1:{args.port}'
    report = {'db': db, 'checks': [], 'ddl': [], 'binary': args.gateway_binary}
    lock = threading.Lock()
    samples, reads, writes = [], [], []

    def http(origin, method, path, body=None, timeout=600):
        req = urllib.request.Request(origin + path, method=method,
                                     data=None if body is None else json.dumps(body).encode(),
                                     headers={'Authorization': auth, 'Content-Type': 'application/json'})
        try:
            r = urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            r = e
        with r:
            return r.status, r.headers.get('X-ReadySet-Route'), json.load(r)

    def sql(origin, text, timeout=600):
        status, route, resp = http(origin, 'POST', '/v1/query', {'sql': text}, timeout)
        rows = list(resp.get('data') or [])
        while resp.get('next_uri') and not resp.get('error'):
            uri = resp['next_uri']
            _, _, resp = http(origin, 'GET', uri, timeout=timeout)
            if uri.endswith('/final'):
                break
            rows += resp.get('data') or []
        if status != 200 or resp.get('error'):
            raise RuntimeError(f'{status} {json.dumps(resp.get("error"))[:400]}')
        return route, rows

    def check(name, ok, **detail):
        report['checks'].append({'name': name, 'passed': bool(ok), **detail})
        print(('PASS ' if ok else 'FAIL ') + name + ' ' + json.dumps(detail)[:400], flush=True)

    up = args.endpoint
    for s in (f'CREATE DATABASE {db}',
              f'CREATE TABLE {a} (id BIGINT NOT NULL, ext_id STRING NOT NULL, cust_id BIGINT NOT NULL, '
              f'payload STRING NOT NULL) CLUSTER BY (id)',
              f"INSERT INTO {a} SELECT number, substr(sha2(to_string(number),256),1,24), number % 1000000, "
              f"concat('p', to_string(number)) FROM numbers({args.rows}) ORDER BY number",
              f'CREATE TABLE {b} (id BIGINT NOT NULL, label STRING NOT NULL, revision BIGINT NOT NULL)',
              f"INSERT INTO {b} SELECT number, 'init', 0 FROM numbers(1000)"):
        sql(up, s)
    (out / 'config.toml').write_text(f'''instance_id = "cddl{os.urandom(3).hex()}"
confirm_create = true
[upstream]
url = "{up}"
user = "{args.user}"
[listen]
http = "127.0.0.1:{args.port}"
[replication]
tables = ["{a}", "{b}"]
poll_interval = "1s"
max_staleness = "10s"
[storage]
dir = "{out / 'storage'}"
[cache]
partial = true
memory_limit_mb = 2048
''')
    log = (out / 'server.log').open('w')
    server = subprocess.Popen([args.gateway_binary, '--config', str(out / 'config.toml')],
                              stdout=log, stderr=subprocess.STDOUT)
    stop = threading.Event()
    try:
        started = time.monotonic()
        while True:
            if server.poll() is not None:
                raise RuntimeError('server exited')
            try:
                h = http(gw, 'GET', '/health', timeout=5)[2]
                if len(h['tables']) == 2 and all(t['ready'] for t in h['tables']):
                    break
            except Exception:
                pass
            time.sleep(0.5)
        report['load_secs'] = round(time.monotonic() - started, 1)
        for q in (f'CREATE CACHE a_id FROM SELECT id, ext_id, cust_id, payload FROM {a} WHERE id = ?',
                  f'CREATE CACHE b_id FROM SELECT id, label, revision FROM {b} WHERE id = ?'):
            sql(gw, q)
        while not all(t['ready'] for t in http(gw, 'GET', '/health', timeout=5)[2]['tables']):
            time.sleep(0.2)

        def writer():
            n = 0
            while not stop.is_set():
                n += 1
                t = time.time()
                try:
                    sql(up, f"UPDATE {b} SET label = 'w{n}', revision = {n} WHERE id = 7", timeout=60)
                    with lock:
                        writes.append((n, t, time.time()))
                except Exception as e:  # recorded, never fatal
                    with lock:
                        writes.append((n, t, None, str(e)[:200]))
                stop.wait(args.write_interval)

        def reader(table, query):
            while not stop.is_set():
                t = time.time()
                try:
                    route, rows = sql(gw, query, timeout=60)
                    with lock:
                        reads.append((table, t, time.time(), route, rows))
                except Exception as e:
                    with lock:
                        reads.append((table, t, time.time(), 'error', str(e)[:200]))
                time.sleep(0.02)

        def sampler():
            while not stop.is_set():
                try:
                    h = http(gw, 'GET', '/health', timeout=5)[2]
                    with lock:
                        samples.append((time.time(), {t['table']: {k: t.get(k) for k in (
                            'ready', 'unavailable', 'last_sync_age_ms', 'syncing', 'applying')} for t in h['tables']}))
                except Exception:
                    pass
                stop.wait(0.2)

        threads = [threading.Thread(target=writer, daemon=True), threading.Thread(target=sampler, daemon=True),
                   threading.Thread(target=reader, args=('b', f'SELECT id, label, revision FROM {b} WHERE id = 7'), daemon=True),
                   threading.Thread(target=reader, args=('a', f'SELECT id, ext_id, cust_id, payload FROM {a} WHERE id = 4242'), daemon=True)]
        for t in threads:
            t.start()
        time.sleep(args.baseline)
        ddl_start = time.time()
        for name, col in (('a_ext', 'ext_id'), ('a_cust', 'cust_id'), ('a_payload', 'payload')):
            t0 = time.time()
            try:
                route, rows = sql(gw, f'CREATE CACHE {name} FROM SELECT id, ext_id, cust_id, payload FROM {a} WHERE {col} = ?')
                ok, err = True, None
            except RuntimeError as e:
                ok, err = False, str(e)
            report['ddl'].append({'sql': f'CREATE CACHE {name}', 'secs': round(time.time() - t0, 2), 'ok': ok, 'error': err})
            print('DDL ' + json.dumps(report['ddl'][-1]), flush=True)
        t0 = time.time()
        try:
            sql(gw, 'DROP CACHE a_payload')
            ok, err = True, None
        except RuntimeError as e:
            ok, err = False, str(e)
        report['ddl'].append({'sql': 'DROP CACHE a_payload', 'secs': round(time.time() - t0, 2), 'ok': ok, 'error': err})
        ddl_end = time.time()
        time.sleep(args.baseline)
        stop.set()
        for t in threads:
            t.join(timeout=70)

        def window(rows, lo, hi):
            return [r for r in rows if lo <= r[1] <= hi]

        def route_stats(table, lo, hi):
            rs = [r for r in window(reads, lo, hi) if r[0] == table]
            n = len(rs)
            return {'reads': n, 'local': sum(r[3] == 'readyset' for r in rs),
                    'forwarded': sum(r[3] == 'databend' for r in rs), 'errors': sum(r[3] == 'error' for r in rs)}

        before = route_stats('b', ddl_start - args.baseline, ddl_start)
        during = route_stats('b', ddl_start, ddl_end)
        after = route_stats('b', ddl_end, ddl_end + args.baseline)
        report['b_reads'] = {'before': before, 'during_ddl': during, 'after': after}
        report['a_reads'] = {'before': route_stats('a', ddl_start - args.baseline, ddl_start),
                             'during_ddl': route_stats('a', ddl_start, ddl_end),
                             'after_last_5s': route_stats('a', ddl_end + args.baseline - 5, ddl_end + args.baseline)}
        bs = [(t, s[b]) for t, s in samples if b in s]
        bd = [s for t, s in bs if ddl_start <= t <= ddl_end]
        # A batch with changes disables B's local reads while it applies (by design); any
        # other reason during A's DDL (stale, capture_slow, ...) is the regression.
        bad = [s for s in bd if not s['ready'] and s['unavailable'] != 'source_applying']
        report['b_health_during_ddl'] = {
            'samples': len(bd), 'not_ready_other_than_applying': len(bad),
            'unavailable': sorted({s['unavailable'] for s in bd if s['unavailable']}),
            'max_last_sync_age_ms': max((s['last_sync_age_ms'] or 0) for s in bd) if bd else None}
        # Freshness: first local B read showing revision >= n, after write n committed.
        lags = []
        local_b = sorted((r[2], int(r[4][0][2])) for r in reads if r[0] == 'b' and r[3] == 'readyset' and r[4])
        for w in writes:
            if len(w) != 3 or w[2] is None or not (ddl_start <= w[2] <= ddl_end):
                continue
            seen = next((t for t, rev in local_b if t >= w[2] and rev >= w[0]), None)
            lags.append(None if seen is None else seen - w[2])
        ok_lags = sorted(l for l in lags if l is not None)
        report['b_fresh_during_ddl'] = {'writes': len(lags), 'never_seen_locally': sum(l is None for l in lags),
                                        'p50_secs': ok_lags[len(ok_lags) // 2] if ok_lags else None,
                                        'max_secs': ok_lags[-1] if ok_lags else None}
        report['ddl_window_secs'] = round(ddl_end - ddl_start, 2)
        print('B ' + json.dumps({k: report[k] for k in ('b_reads', 'b_health_during_ddl', 'b_fresh_during_ddl')}), flush=True)
        print('A ' + json.dumps(report['a_reads']), flush=True)

        check('B reads never failed', all(v['errors'] == 0 for v in report['b_reads'].values()), **report['b_reads'])
        ratio = lambda s: s['local'] / s['reads'] if s['reads'] else 0.0  # noqa: E731
        report['b_local_ratio'] = {'before': round(ratio(before), 4), 'during_ddl': round(ratio(during), 4)}
        isolated = (during['reads'] > 0 and during['errors'] == 0
                    and report['b_health_during_ddl']['not_ready_other_than_applying'] == 0
                    and (report['b_health_during_ddl']['max_last_sync_age_ms'] or 0) <= args.b_fresh_max * 1000
                    and report['b_fresh_during_ddl']['never_seen_locally'] == 0
                    and (report['b_fresh_during_ddl']['max_secs'] or 0) <= args.b_fresh_max
                    and ratio(during) >= min(0.9, ratio(before) - 0.05))
        if args.expect_isolated == 'yes':
            check('all A cache DDL succeeded, none rejected', all(d['ok'] for d in report['ddl']), ddl=report['ddl'])
            check('B local, ready and fresh throughout A cache DDL', isolated, ratio=report['b_local_ratio'],
                  reads=during, health=report['b_health_during_ddl'], fresh=report['b_fresh_during_ddl'])
        else:
            check('negative control: B stalls during A cache DDL', not isolated, ratio=report['b_local_ratio'],
                  reads=during, health=report['b_health_during_ddl'], fresh=report['b_fresh_during_ddl'])
        check('A local again after its DDL', report['a_reads']['after_last_5s']['forwarded'] == 0
              and report['a_reads']['after_last_5s']['local'] > 0, **report['a_reads']['after_last_5s'])
        # Final correctness: B row, and the new A caches, against Databend. Local reads may
        # lag by up to one sync round after the writer stops: wait for convergence.
        q = f'SELECT id, label, revision FROM {b} WHERE id = 7'
        want = sql(up, q)[1]
        deadline = time.time() + 15
        route, got = sql(gw, q)
        while (route != 'readyset' or got != want) and time.time() < deadline:
            time.sleep(0.5)
            route, got = sql(gw, q)
        check('B final row local and equal to Databend', route == 'readyset' and got == want, got=got, want=want)
        created = {d['sql'].split()[-1] for d in report['ddl'] if d['ok'] and d['sql'].startswith('CREATE')}
        for name, col, key in (('a_ext', 'ext_id', "'" + sql(up, 'SELECT substr(sha2(to_string(4242424),256),1,24)')[1][0][0] + "'"),
                               ('a_cust', 'cust_id', '424242')):
            if name not in created:
                continue
            q = f'SELECT id, ext_id, cust_id, payload FROM {a} WHERE {col} = {key}'
            deadline = time.time() + 30
            route, got = sql(gw, q)
            while route != 'readyset' and time.time() < deadline:
                time.sleep(0.5)
                route, got = sql(gw, q)
            check(f'{name} local and equal to Databend', route == 'readyset'
                  and sorted(map(json.dumps, got)) == sorted(map(json.dumps, sql(up, q)[1])), rows=len(got))
        h = http(gw, 'GET', '/health', timeout=10)[2]
        report['forward_reasons'] = {t['table']: t['counters'].get('forward_reasons') for t in h['tables']}
        runs = sorted((out / 'storage' / 'runs').iterdir())
        events = [json.loads(l) for l in (runs[-1] / 'events.jsonl').read_text().splitlines()] if runs and (runs[-1] / 'events.jsonl').exists() else []
        report['cache_ddl_events'] = [{k: e.get(k) for k in ('sql', 'secs', 'ok', 'rejected')} for e in events if e.get('event') == 'cache_ddl']
    finally:
        stop.set()
        server.send_signal(signal.SIGTERM)
        try:
            report['exit_code'] = server.wait(timeout=300)
        except subprocess.TimeoutExpired:
            server.kill()
            report['exit_code'] = 'killed'
        log.close()
    check('clean exit', report.get('exit_code') == 0)
    report['passed'] = all(c['passed'] for c in report['checks'])
    with lock:
        (out / 'samples.jsonl').write_text(''.join(json.dumps(s) + '\n' for s in samples))
        (out / 'writes.jsonl').write_text(''.join(json.dumps(w) + '\n' for w in writes))
        (out / 'reads.jsonl').write_text(''.join(json.dumps(r[:4]) + '\n' for r in reads))
    (out / 'report.json').write_text(json.dumps(report, indent=2))
    print('RESULT ' + json.dumps({'passed': report['passed'], 'report': str(out / 'report.json')}), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
