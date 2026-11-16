#!/usr/bin/env python3
"""Point-lookup baseline: Databend (several table layouts) vs readyset-databend.

Drives `databend-loadtest point` over a matrix of tables x queries x concurrency and samples
CPU (whole host + given PIDs) and Databend's system.processes while each run is in flight.
Then starts readyset-databend over the `src` table (partial mode), declares one cache per
query shape, checks results against Databend, and runs the same matrix with uniform (mostly
cold) and hot key sets. Databend's shared configuration is never changed.

The data generator these keys assume (see databend-loadtest/README.md):
  id = number, ext_id = substr(sha2(to_string(number),256),1,24), cust_id = number % 1000000.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import random
import signal
import subprocess
import threading
import time
import urllib.request

COLS = 'id, ext_id, cust_id, status, amount, payload'
QUERIES = {
    'id': ('int', 1, 'WHERE id = {k}'),
    'ext': ('ext', 1, "WHERE ext_id = '{k}'"),
    'cust': ('mod', 10, 'WHERE cust_id = {k}'),
}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--db', required=True)
    p.add_argument('--databend', default='http://127.0.0.1:8000')
    p.add_argument('--user', default='root')
    p.add_argument('--loadtest', required=True)
    p.add_argument('--readyset-binary', required=True)
    p.add_argument('--work', required=True)
    p.add_argument('--tables', nargs='+', default=['d0', 'd1', 'd2', 'd3'])
    p.add_argument('--concurrency', nargs='+', type=int, default=[1, 16, 64, 256])
    p.add_argument('--duration', type=float, default=30)
    p.add_argument('--warmup', type=float, default=5)
    p.add_argument('--key-space', type=int, default=10_000_000)
    p.add_argument('--hot', type=int, default=100_000)
    p.add_argument('--pids', nargs='*', type=int, default=[], help='Databend query PIDs to sample')
    p.add_argument('--rs-port', type=int, default=18101)
    p.add_argument('--skip-databend', action='store_true')
    p.add_argument('--skip-readyset', action='store_true')
    args = p.parse_args()
    work = Path(args.work)
    work.mkdir(parents=True, exist_ok=True)
    password = os.environ.get('DATABEND_PASSWORD', '')
    auth = 'Basic ' + base64.b64encode(f'{args.user}:{password}'.encode()).decode()
    results = (work / 'results.jsonl').open('a')

    def query(endpoint, sql):
        req = urllib.request.Request(endpoint + '/v1/query', data=json.dumps({'sql': sql}).encode(),
                                     headers={'Authorization': auth, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=600) as r:
            route = r.headers.get('X-ReadySet-Route')
            body = json.load(r)
        rows = list(body.get('data') or [])
        while body.get('next_uri') and not body.get('error'):
            uri = body['next_uri']
            with urllib.request.urlopen(urllib.request.Request(endpoint + uri, headers={'Authorization': auth}), timeout=600) as r:
                body = json.load(r)
            if uri.endswith('/final'):
                break
            rows += body.get('data') or []
        if body.get('error'):
            raise RuntimeError(json.dumps(body['error'])[:500])
        return route, rows

    def ticks(pid):
        try:
            f = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
            return int(f[11]) + int(f[12])
        except (OSError, IndexError):
            return None

    def host_ticks():
        f = [int(x) for x in Path('/proc/stat').read_text().split('\n', 1)[0].split()[1:]]
        return sum(f), f[3] + f[4]

    def run(label, endpoint, template, kind, rows, conc, hot=None, duration=None, pids=()):
        cmd = [args.loadtest, 'point', '--endpoint', endpoint, '--user', args.user, '--template', template,
               '--kind', kind, '--key-space', str(args.key_space), '--expect-rows', str(rows),
               '--concurrency', str(conc), '--duration', str(duration or args.duration),
               '--warmup', str(args.warmup), '--label', label, '--seed', str(random.randrange(1, 1 << 20))]
        if hot:
            cmd += ['--hot', str(hot)]
        procs = {'max': 0, 'running_max': 0}
        stop = threading.Event()

        def reasons():
            if endpoint == args.databend:
                return None
            req = urllib.request.Request(endpoint + '/health', headers={'Authorization': auth})
            try:
                return json.load(urllib.request.urlopen(req, timeout=10))['counters'].get('forward_reasons', {})
            except Exception:  # diagnostics must never fail a run
                return None

        before = reasons()

        def sample_processes():
            time.sleep(args.warmup + 1)
            while not stop.wait(1):
                try:
                    _, rs = query(args.databend, "SELECT status, count() FROM system.processes "
                                                 f"WHERE command LIKE '%{args.db}%' AND command NOT LIKE '%system.processes%' GROUP BY status")
                except Exception:  # sampling must never fail a run
                    continue
                total = sum(int(r[1]) for r in rs)
                procs['max'] = max(procs['max'], total)
                procs['running_max'] = max(procs['running_max'], sum(int(r[1]) for r in rs if r[0].lower() in ('query', 'running', 'busy')))
                procs.setdefault('statuses', {}).update({r[0]: max(int(r[1]), procs.get('statuses', {}).get(r[0], 0)) for r in rs})

        sampler = threading.Thread(target=sample_processes, daemon=True)
        sampler.start()
        h0, p0 = host_ticks(), {pid: ticks(pid) for pid in pids}
        t0 = time.monotonic()
        out = subprocess.run(cmd, capture_output=True, text=True, env=os.environ, check=False)
        elapsed = time.monotonic() - t0
        h1, p1 = host_ticks(), {pid: ticks(pid) for pid in pids}
        stop.set()
        hz = os.sysconf('SC_CLK_TCK')
        try:
            r = json.loads(out.stdout.strip().splitlines()[-1])
        except (IndexError, json.JSONDecodeError):
            r = {'label': label, 'failed_to_run': out.stderr[-800:]}
        busy = (h1[0] - h0[0]) - (h1[1] - h0[1])
        after = reasons()
        if before is not None and after is not None:
            r['forward_reasons'] = {k: after[k] - before.get(k, 0) for k in after if after[k] != before.get(k, 0)}
        r.update({
            'host_cpu_pct': round(100 * busy / max(1, h1[0] - h0[0]), 1),
            'process_cores': {str(pid): (None if p0[pid] is None or p1[pid] is None else round((p1[pid] - p0[pid]) / hz / elapsed, 2)) for pid in pids},
            'databend_processes': procs, 'hot': hot, 'utc': time.time(),
        })
        r.pop('template', None)
        results.write(json.dumps(r) + '\n')
        results.flush()
        print(json.dumps({k: r.get(k) for k in ('label', 'concurrency', 'qps', 'p50_ms', 'p99_ms', 'failed', 'routes', 'forward_reasons', 'host_cpu_pct', 'process_cores')}), flush=True)
        return r

    meta = {'db': args.db, 'started': time.time(), 'nproc': os.cpu_count(),
            'databend_version': query(args.databend, 'SELECT version()')[1][0][0],
            'settings': query(args.databend, "SELECT name, value FROM system.settings WHERE value <> default")[1],
            'configs': query(args.databend, "SELECT name, value FROM system.configs WHERE name IN ('max_running_queries','data_cache_storage','max_active_sessions')")[1],
            'loadavg': Path('/proc/loadavg').read_text().strip()}
    (work / ('meta-readyset.json' if args.skip_databend else 'meta.json')).write_text(json.dumps(meta, indent=2))

    if not args.skip_databend:
        for table in args.tables:
            for q, (kind, rows, where) in QUERIES.items():
                template = f'SELECT {COLS} FROM {args.db}.{table} {where}'
                for conc in args.concurrency:
                    run(f'databend-{table}-{q}-c{conc}', args.databend, template, kind, rows, conc, pids=args.pids)

    if args.skip_readyset:
        return
    rs_dir = work / 'readyset'
    rs_dir.mkdir(exist_ok=True)
    endpoint = f'http://127.0.0.1:{args.rs_port}'
    (rs_dir / 'config.toml').write_text(f'''instance_id = "pointq1"
confirm_create = true
[upstream]
url = "{args.databend}"
user = "{args.user}"
[listen]
http = "127.0.0.1:{args.rs_port}"
[replication]
tables = ["{args.db}.src"]
poll_interval = "1s"
max_staleness = "10s"
[storage]
dir = "{rs_dir / 'storage'}"
[cache]
partial = true
memory_limit_mb = 4096
upquery_timeout_ms = 5000
''')
    log = (rs_dir / 'server.log').open('a')
    server = subprocess.Popen([args.readyset_binary, '--config', str(rs_dir / 'config.toml')], stdout=log, stderr=subprocess.STDOUT)
    try:
        started = time.monotonic()
        while True:
            if server.poll() is not None:
                raise RuntimeError('readyset-databend exited')
            try:
                req = urllib.request.Request(endpoint + '/health', headers={'Authorization': auth})
                health = json.load(urllib.request.urlopen(req, timeout=5))
                if health['tables'] and all(t['ready'] for t in health['tables']):
                    break
            except Exception:
                pass
            time.sleep(1)
        ready = {'ready_secs': round(time.monotonic() - started, 1), 'snapshot': health['tables'][0].get('snapshot')}
        for q, (_, _, where) in QUERIES.items():
            shape = where.replace("'{k}'", '?').replace('{k}', '?')
            existing = {r[0] for r in query(endpoint, 'SHOW CACHES')[1]}
            if f'pq_{q}' in existing:
                continue
            # Building a cache on a non-indexed column of a large base table holds the migration
            # lock for a while; the table goes stale meanwhile and the next CREATE CACHE is
            # rejected ("source is not ready") until a batch lands again. Retry, recording waits.
            began = time.monotonic()
            while True:
                try:
                    query(endpoint, f'CREATE CACHE pq_{q} FROM SELECT {COLS} FROM {args.db}.src {shape}')
                    break
                except RuntimeError as e:
                    if 'source is not ready' not in str(e) or time.monotonic() - began > 300:
                        raise
                    time.sleep(2)
            ready.setdefault('create_cache_secs', {})[q] = round(time.monotonic() - began, 1)
        checks = []
        rnd = random.Random(7)
        import hashlib
        for _ in range(20):
            i = rnd.randrange(args.key_space)
            for q, (kind, _, where) in QUERIES.items():
                k = {'int': str(i), 'ext': hashlib.sha256(str(i).encode()).hexdigest()[:24], 'mod': str(i % 1_000_000)}[kind]
                sql = f'SELECT {COLS} FROM {args.db}.src {where.format(k=k)}'
                route, got = query(endpoint, sql)
                _, want = query(args.databend, sql)
                checks.append({'q': q, 'key': k, 'route': route, 'equal': sorted(map(json.dumps, got)) == sorted(map(json.dumps, want)), 'rows': len(got)})
        ready['checks_total'] = len(checks)
        ready['checks_equal_local'] = sum(c['equal'] and c['route'] == 'readyset' for c in checks)
        ready['checks_failed'] = [c for c in checks if not (c['equal'] and c['route'] == 'readyset')]
        (rs_dir / 'ready.json').write_text(json.dumps(ready, indent=2))
        print('READYSET ' + json.dumps({k: ready[k] for k in ('ready_secs', 'checks_total', 'checks_equal_local')}), flush=True)
        pids = [server.pid]
        for dist, hot in (('uniform', None), ('hot', args.hot)):
            for q, (kind, rows, where) in QUERIES.items():
                template = f'SELECT {COLS} FROM {args.db}.src {where}'
                if hot:
                    run(f'readyset-warm-{q}', endpoint, template, kind, rows, 64, hot=hot, duration=60, pids=pids)
                for conc in args.concurrency:
                    run(f'readyset-{dist}-{q}-c{conc}', endpoint, template, kind, rows, conc, hot=hot, pids=pids)
        req = urllib.request.Request(endpoint + '/health', headers={'Authorization': auth})
        (rs_dir / 'final_health.json').write_text(json.dumps(json.load(urllib.request.urlopen(req, timeout=10)), indent=2))
        status = Path(f'/proc/{server.pid}/status').read_text()
        (rs_dir / 'memory.txt').write_text('\n'.join(l for l in status.splitlines() if l.startswith(('VmRSS', 'VmHWM'))))
    finally:
        server.send_signal(signal.SIGTERM)
        server.wait(timeout=300)
        log.close()
    print('DONE', flush=True)


if __name__ == '__main__':
    main()
