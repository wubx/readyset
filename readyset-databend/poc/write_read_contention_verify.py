#!/usr/bin/env python3
"""Continuous writes vs high-concurrency cold reads on one large table (partial mode).

Creates readyset_gateway_wrc_* with A (ROWS rows; id, ext_id = sha2 prefix, cust_id,
amount, payload), starts readyset-databend over A with caches on id and ext_id and a small
memory budget (reads stay mostly cold: replay from the local base), then per concurrency:
  1. reads only (databend-loadtest, uniform keys);
  2. writes only: every --tick, INSERT --insert-rows new rows and UPDATE --update-rows
     random existing rows in Databend; a probe measures write->locally-visible lag;
  3. reads and writes together.
Reports read QPS/latency/local ratio/forward reasons, write lag, batch timing and A's
`unavailable` reasons sampled from /health, so blocking in either direction is visible.
Creates only readyset_gateway_* objects; no cleanup.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import random
import resource
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request

COLS = 'id, ext_id, cust_id, amount, payload'
QUERIES = {'id': ('int', 'WHERE id = {k}'), 'ext': ('ext', "WHERE ext_id = '{k}'")}


def pct(values, p):
    if not values:
        return None
    v = sorted(values)
    return round(v[min(len(v) - 1, int(len(v) * p))], 3)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--user', default='root')
    p.add_argument('--gateway-binary', required=True)
    p.add_argument('--loadtest', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--rows', type=int, default=10_000_000)
    p.add_argument('--port', type=int, default=18103)
    p.add_argument('--memory-limit-mb', type=int, default=2048)
    p.add_argument('--query', choices=sorted(QUERIES), default='ext')
    p.add_argument('--concurrency', nargs='+', type=int, default=[64, 256])
    p.add_argument('--duration', type=float, default=40)
    p.add_argument('--tick', type=float, default=1.0)
    p.add_argument('--insert-rows', type=int, default=1000)
    p.add_argument('--update-rows', type=int, default=100)
    p.add_argument('--pids', nargs='*', default=[], help='extra name=pid to sample CPU for, e.g. databend=16747 rustfs=8317')
    p.add_argument('--confirm-create', action='store_true', required=True)
    args = p.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    password = os.environ.get('DATABEND_PASSWORD', '')
    auth = 'Basic ' + base64.b64encode(f'{args.user}:{password}'.encode()).decode()
    db = 'readyset_gateway_wrc_' + time.strftime('%Y%m%d_%H%M%S') + '_' + os.urandom(3).hex()
    a = f'{db}.a'
    gw = f'http://127.0.0.1:{args.port}'
    up = args.endpoint
    report = {'db': db, 'args': vars(args), 'phases': []}

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

    def health():
        return http(gw, 'GET', '/health', timeout=10)[2]

    hz = os.sysconf('SC_CLK_TCK')

    def ticks(pid):
        try:
            f = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
            return int(f[11]) + int(f[12])
        except (OSError, IndexError):
            return None

    def host_ticks():
        f = [int(x) for x in Path('/proc/stat').read_text().split('\n', 1)[0].split()[1:]]
        return sum(f), f[3] + f[4]

    def cpu_snapshot(pids):
        children = resource.getrusage(resource.RUSAGE_CHILDREN)
        return time.monotonic(), host_ticks(), {n: ticks(pid) for n, pid in pids.items()}, children.ru_utime + children.ru_stime

    def cpu_delta(a, b):
        secs = b[0] - a[0]
        total, idle = b[1][0] - a[1][0], b[1][1] - a[1][1]
        cores = {n: (None if a[2][n] is None or b[2][n] is None else round((b[2][n] - a[2][n]) / hz / secs, 2)) for n in a[2]}
        cores['loadtest'] = round((b[3] - a[3]) / secs, 2)
        return {'host_busy_pct': round(100 * (total - idle) / max(1, total), 1), 'cores': cores}

    gen = (lambda lo, n: f"SELECT number + {lo}, substr(sha2(to_string(number + {lo}),256),1,24), "
                         f"(number + {lo}) % 1000000, CAST(((number + {lo}) % 1000000) / 100 AS DECIMAL(18,2)), "
                         f"concat('p', to_string(number + {lo}), '-', repeat('x', 60)) FROM numbers({n})")
    for s in (f'CREATE DATABASE {db}',
              f'CREATE TABLE {a} (id BIGINT NOT NULL, ext_id STRING NOT NULL, cust_id BIGINT NOT NULL, '
              f'amount DECIMAL(18,2) NOT NULL, payload STRING NOT NULL) CLUSTER BY (id)',
              f'INSERT INTO {a} {gen(0, args.rows)} ORDER BY 1'):
        sql(up, s)
    (out / 'config.toml').write_text(f'''instance_id = "wrc{os.urandom(3).hex()}"
confirm_create = true
[upstream]
url = "{up}"
user = "{args.user}"
[listen]
http = "127.0.0.1:{args.port}"
[replication]
tables = ["{a}"]
poll_interval = "1s"
max_staleness = "10s"
[storage]
dir = "{out / 'storage'}"
[cache]
partial = true
memory_limit_mb = {args.memory_limit_mb}
upquery_timeout_ms = 5000
''')
    log = (out / 'server.log').open('w')
    server = subprocess.Popen([args.gateway_binary, '--config', str(out / 'config.toml')],
                              stdout=log, stderr=subprocess.STDOUT)
    next_id = [args.rows]
    try:
        started = time.monotonic()
        while True:
            if server.poll() is not None:
                raise RuntimeError('server exited')
            try:
                if health()['tables'][0]['ready']:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        report['load_secs'] = round(time.monotonic() - started, 1)
        pids = {'readyset': server.pid} | {n: int(v) for n, v in (x.split('=', 1) for x in args.pids)}
        for q, (_, where) in QUERIES.items():
            shape = where.replace("'{k}'", '?').replace('{k}', '?')
            sql(gw, f'CREATE CACHE wrc_{q} FROM SELECT {COLS} FROM {a} {shape}')
        while not health()['tables'][0]['ready']:
            time.sleep(0.2)

        def phase(name, conc, reads, writes):
            stop = threading.Event()
            lock = threading.Lock()
            samples, wlog, lags = [], [], []

            def sampler():
                while not stop.is_set():
                    try:
                        t = health()['tables'][0]
                        with lock:
                            samples.append({k: t.get(k) for k in ('ready', 'unavailable', 'last_sync_age_ms', 'applying')}
                                           | {'batch_ms': (t.get('last_batch') or {}).get('total_ms'),
                                              'batch_seq': (t.get('last_batch') or {}).get('sequence')})
                    except Exception:
                        pass
                    stop.wait(0.2)

            def probe(key, committed):
                q = f'SELECT {COLS} FROM {a} WHERE id = {key}'
                until = time.time() + 30
                while time.time() < until:
                    if stop.is_set():
                        with lock:
                            lags.append('cut')  # phase ended first: no verdict
                        return
                    try:
                        route, rows = sql(gw, q, timeout=30)
                        if route == 'readyset' and rows:
                            with lock:
                                lags.append(time.time() - committed)
                            return
                    except Exception:
                        pass
                    time.sleep(0.05)
                with lock:
                    lags.append(None)

            def writer():
                while not stop.is_set():
                    tick = time.time()
                    lo, n = next_id[0], args.insert_rows
                    next_id[0] += n
                    try:
                        t0 = time.time()
                        sql(up, f'INSERT INTO {a} {gen(lo, n)}', timeout=120)
                        t1 = time.time()
                        ids = ','.join(str(random.randrange(args.rows)) for _ in range(args.update_rows))
                        if args.update_rows:
                            sql(up, f'UPDATE {a} SET amount = amount + 1 WHERE id IN ({ids})', timeout=120)
                        t2 = time.time()
                        with lock:
                            wlog.append({'insert_ms': round((t1 - t0) * 1000), 'update_ms': round((t2 - t1) * 1000)})
                        threading.Thread(target=probe, args=(lo + n - 1, t1), daemon=True).start()
                    except Exception as e:
                        with lock:
                            wlog.append({'error': str(e)[:200]})
                    stop.wait(max(0.0, args.tick - (time.time() - tick)))

            before = health()['counters'].get('forward_reasons', {})
            cpu0 = cpu_snapshot(pids)
            threads = [threading.Thread(target=sampler, daemon=True)]
            if writes:
                threads.append(threading.Thread(target=writer, daemon=True))
            for t in threads:
                t.start()
            result = {'phase': name, 'concurrency': conc if reads else 0}
            kind, where = QUERIES[args.query]
            if reads:
                cmd = [args.loadtest, 'point', '--endpoint', gw, '--user', args.user,
                       '--template', f'SELECT {COLS} FROM {a} {where}', '--kind', kind,
                       '--key-space', str(args.rows), '--expect-rows', '1', '--concurrency', str(conc),
                       '--duration', str(args.duration), '--warmup', '5', '--label', name,
                       '--seed', str(random.randrange(1, 1 << 20))]
                r = json.loads(subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1])
                result['reads'] = {k: r.get(k) for k in ('qps', 'p50_ms', 'p99_ms', 'p999_ms', 'max_ms', 'ok', 'failed', 'routes', 'errors')}
            else:
                time.sleep(args.duration + 5)
            cpu1 = cpu_snapshot(pids)
            stop.set()
            for t in threads:
                t.join(timeout=60)
            time.sleep(1)
            result['cpu'] = cpu_delta(cpu0, cpu1)
            after = health()['counters'].get('forward_reasons', {})
            result['forward_reasons'] = {k: after[k] - before.get(k, 0) for k in after if after[k] != before.get(k, 0)}
            with lock:
                n = len(samples)
                reasons = {}
                for s in samples:
                    if s['unavailable']:
                        reasons[s['unavailable']] = reasons.get(s['unavailable'], 0) + 1
                batches = {}
                for s in samples:
                    if s['batch_seq'] is not None:
                        batches[s['batch_seq']] = s['batch_ms']
                result['a_samples'] = {'samples': n, 'unavailable_share': round(sum(reasons.values()) / n, 4) if n else None,
                                       'reasons': reasons,
                                       'max_last_sync_age_ms': max((s['last_sync_age_ms'] or 0) for s in samples) if samples else None}
                bms = [v for v in batches.values() if v is not None]
                result['batches'] = {'distinct': len(bms), 'p50_ms': pct(bms, .5), 'p90_ms': pct(bms, .9), 'max_ms': max(bms) if bms else None}
                if writes:
                    ok = [l for l in lags if l not in (None, 'cut')]
                    result['writes'] = {'rounds': len(wlog), 'errors': sum('error' in w for w in wlog),
                                        'insert_ms_p50': pct([w['insert_ms'] for w in wlog if 'insert_ms' in w], .5),
                                        'update_ms_p50': pct([w['update_ms'] for w in wlog if 'update_ms' in w], .5),
                                        'probes': len(lags), 'cut_by_phase_end': sum(l == 'cut' for l in lags),
                                        'never_visible': sum(l is None for l in lags),
                                        'visible_p50_secs': pct(ok, .5), 'visible_p99_secs': pct(ok, .99),
                                        'visible_max_secs': max(ok) if ok else None}
            report['phases'].append(result)
            print('PHASE ' + json.dumps(result), flush=True)

        for conc in args.concurrency:
            phase(f'reads-c{conc}', conc, True, False)
            phase(f'writes-only-c{conc}', conc, False, True)
            phase(f'reads+writes-c{conc}', conc, True, True)

        # Correctness after all writes: count, a sample of new and updated rows.
        time.sleep(5)
        h = health()['tables'][0]
        report['final'] = {'local_rows': h['rows'], 'databend_rows': int(sql(up, f'SELECT count(*) FROM {a}')[1][0][0]),
                           'ready': h['ready'], 'failed': h['failed']}
        mismatch = 0
        keys = [random.randrange(args.rows) for _ in range(20)] + [random.randrange(args.rows, next_id[0]) for _ in range(20)]
        for k in keys:
            q = f'SELECT {COLS} FROM {a} WHERE id = {k}'
            route, got = sql(gw, q)
            mismatch += int(route != 'readyset' or got != sql(up, q)[1])
        report['final']['sample_keys'] = len(keys)
        report['final']['sample_mismatch_or_forwarded'] = mismatch
        report['final']['cache'] = {k: health()['cache'].get(k) for k in ('allocated_bytes', 'hit_queries', 'miss_keys', 'replay_timeouts')}
        report['memory'] = Path(f'/proc/{server.pid}/status').read_text().split('VmHWM:')[1].split('\n')[0].strip()
        print('FINAL ' + json.dumps(report['final']), flush=True)
    finally:
        server.send_signal(signal.SIGTERM)
        try:
            report['exit_code'] = server.wait(timeout=300)
        except subprocess.TimeoutExpired:
            server.kill()
            report['exit_code'] = 'killed'
        log.close()
        (out / 'report.json').write_text(json.dumps(report, indent=2))
    print('RESULT ' + json.dumps({'report': str(out / 'report.json'), 'exit_code': report.get('exit_code')}), flush=True)


if __name__ == '__main__':
    main()
