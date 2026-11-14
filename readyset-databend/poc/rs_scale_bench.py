#!/usr/bin/env python3
"""ReadySet-side scale benchmark for readyset-databend (production mode). NEW POC database.

1. Load a 10M-row source table in Databend.
2. Start readyset-databend: snapshot -> local ReadySet (timed, RSS sampled every second).
3. CREATE CACHE for IN / range / text-equality shapes (each a full reader; timed, RSS).
4. Large Stream batches: UPDATE 1M rows (2M changes), INSERT 1M rows, UPDATE 10M rows
   (20M changes). Per batch: Stream read+apply, persist, fence and commit times.
   While the 20M batch runs, a cached read must be forwarded to Databend.
5. After every step, locally served results are compared with Databend. score has 100,000
   distinct values (about 100 rows each) so check results stay under the 10,000-row local
   result limit (larger results are forwarded by design).
Results: <out>/results.json, rss.csv, server logs.
"""
import base64
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime

ENDPOINT = os.environ.get('DATABEND_ENDPOINT', 'http://192.168.191.96:8000')
BINARY = os.environ.get('BINARY', '/data1/vmbox/readyset/target/release/readyset-databend')
ROWS = int(os.environ.get('ROWS', '10000000'))
PORT = int(os.environ.get('PORT', '8003'))
TS = datetime.now().strftime('%Y%m%d_%H%M%S')
DB = f'readyset_gateway_rsbench_{TS}'
OUT = f'/data1/vmbox/readyset-rsbench-{TS}'
AUTH = 'Basic ' + base64.b64encode(f"root:{os.environ.get('DATABEND_PASSWORD', '')}".encode()).decode()
results = {'database': DB, 'rows': ROWS, 'binary': BINARY, 'steps': []}


def save():
    with open(f'{OUT}/results.json', 'w') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)


def log(msg):
    print(f'[{datetime.now():%H:%M:%S}] {msg}', flush=True)


def query(origin, sql, timeout=1800):
    """Run one statement over the Databend HTTP API. Returns (rows, route header)."""
    def call(path, body=None):
        req = urllib.request.Request(origin + path, data=None if body is None else json.dumps(body).encode(),
                                     headers={'Authorization': AUTH, 'Content-Type': 'application/json'})
        try:
            r = urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            r = e
        with r:
            raw = r.read()
            return (json.loads(raw) if raw else {}), r.headers.get('X-ReadySet-Route')
    r, route = call('/v1/query', {'sql': sql, 'session': {'database': DB},
                                  'pagination': {'wait_time_secs': 5, 'max_rows_per_page': 10000}})
    rows = list(r.get('data') or [])
    while True:
        if r.get('error'):
            raise RuntimeError(f'{sql[:80]}: {r["error"]}')
        uri = r.get('next_uri')
        if not uri:
            break
        if uri.endswith('/final'):
            call(uri)
            break
        r, _ = call(uri)
        rows += r.get('data') or []
    return rows, route


class Rss(threading.Thread):
    def __init__(self, pid):
        super().__init__(daemon=True)
        self.pid, self.stop, self.peak, self.now = pid, threading.Event(), 0, 0
        self.file = open(f'{OUT}/rss.csv', 'w')
        self.file.write('unix_ts,rss_mb,hwm_mb\n')

    def run(self):
        while not self.stop.wait(1):
            try:
                fields = dict(l.split(':', 1) for l in open(f'/proc/{self.pid}/status'))
                rss = int(fields['VmRSS'].split()[0]) // 1024
                hwm = int(fields['VmHWM'].split()[0]) // 1024
            except (OSError, KeyError):
                # An exiting/zombie process can still have status but no VmRSS/VmHWM.
                return
            self.now, self.peak = rss, max(self.peak, hwm)
            self.file.write(f'{time.time():.0f},{rss},{hwm}\n')
            self.file.flush()


def status(run):
    with open(f'{run}/status.json') as f:
        return json.load(f)


def wait_batch(run, after, timeout=3600):
    """Wait until a batch with changes newer than `after` is reported in status.json."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            st = status(run)
        except (OSError, ValueError):
            time.sleep(0.2)
            continue
        if st.get('failed'):
            raise RuntimeError(st['failed'])
        lb = st.get('last_batch') or {}
        if lb.get('sequence', 0) > after:
            return st
        time.sleep(0.2)
    raise TimeoutError('batch did not complete')


CHECKS = [
    'SELECT id, payload FROM orders WHERE score IN (3, 4)',
    'SELECT id, payload, score, note FROM orders WHERE score >= 2 AND score < 4',
    "SELECT id, score FROM orders WHERE payload = 'p-123456'",
    "SELECT id, score FROM orders WHERE payload = 'p-9999999'",
]


def local_query(origin, run, sql, attempts=5):
    """A cached read, served locally. Any forwarded statement (including a forwarded read)
    keeps local reads off until the next completed sync, so wait for readiness first."""
    for _ in range(attempts):
        floor = status(run)['checkpoint']
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            st = status(run)
            if st.get('failed'):
                raise RuntimeError(st['failed'])
            if st['ready'] and st['checkpoint'] > floor:
                break
            time.sleep(0.1)
        t = time.monotonic()
        rows, route = query(origin, sql)
        if route == 'readyset':
            return rows, (time.monotonic() - t) * 1000
    raise AssertionError(f'never served locally: {sql}')


def compare(origin, run, label):
    """Cached (locally served) result must equal Databend's for every check query."""
    out = []
    for sql in CHECKS:
        local, local_ms = local_query(origin, run, sql)
        t = time.monotonic()
        direct, _ = query(ENDPOINT, sql)
        direct_ms = (time.monotonic() - t) * 1000
        same = sorted(map(tuple, local)) == sorted(map(tuple, direct))
        out.append({'sql': sql, 'route': 'readyset', 'rows': len(local), 'equal': same,
                    'local_ms': round(local_ms, 1), 'databend_ms': round(direct_ms, 1)})
        log(f'  {label}: {"OK " if same else "MISMATCH"} local rows={len(local)} local={local_ms:.1f}ms databend={direct_ms:.1f}ms  {sql}')
        if not same:
            raise AssertionError(f'{label}: mismatch for {sql}')
    return out


def du(path):
    return int(subprocess.run(['du', '-sm', path], capture_output=True, text=True).stdout.split()[0])


def main():
    os.makedirs(OUT, exist_ok=False)
    log(f'database={DB} out={OUT}')
    for sql in [f'CREATE DATABASE {DB}',
                f'CREATE TABLE {DB}.orders (id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL)',
                f"INSERT INTO {DB}.orders SELECT CAST(number AS BIGINT), concat('p-', to_string(number)), CAST(number % 100000 AS BIGINT), IF(number % 2 = 0, NULL, 'n') FROM numbers({ROWS})"]:
        query(ENDPOINT, sql)
    run = f'{OUT}/service'
    t0 = time.monotonic()
    child = subprocess.Popen([BINARY, '--endpoint', ENDPOINT, '--user', 'root', '--database', DB, '--table', 'orders',
                              '--listen', f'127.0.0.1:{PORT}', '--interval', '1', '--max-staleness', '30',
                              '--confirm-create', '--output', run],
                             stdout=open(f'{OUT}/stdout.log', 'w'), stderr=open(f'{OUT}/stderr.log', 'w'),
                             env={**os.environ, 'DATABEND_PASSWORD': os.environ.get('DATABEND_PASSWORD', '')})
    rss = Rss(child.pid)
    rss.start()
    try:
        while not os.path.exists(f'{run}/listening.json'):
            if child.poll() is not None:
                raise RuntimeError('readyset-databend exited; see stderr.log')
            time.sleep(0.5)
        origin = json.load(open(f'{run}/listening.json'))['origin']
        st = status(run)
        step = {'name': 'snapshot', 'wall_secs': round(time.monotonic() - t0, 1), 'snapshot': st['snapshot'],
                'rss_mb': rss.now, 'peak_mb': rss.peak, 'storage_mb': du(f'{run}/engine')}
        results['steps'].append(step); save()
        log(f'snapshot: {step}')

        for name, columns, where in [('c_score_in', 'id, payload', 'score IN (?, ?)'),
                                     ('c_range', 'id, payload, score, note', 'score >= ? AND score < ?'),
                                     ('c_payload', 'id, score', 'payload = ?')]:
            before = rss.now
            t = time.monotonic()
            query(origin, f'CREATE CACHE {name} FROM SELECT {columns} FROM orders WHERE {where}')
            step = {'name': f'create cache {name}', 'secs': round(time.monotonic() - t, 1),
                    'rss_before_mb': before, 'rss_mb': rss.now, 'peak_mb': rss.peak}
            time.sleep(3)
            step['rss_settled_mb'] = rss.now
            results['steps'].append(step); save()
            log(f'{step}')
        results['steps'].append({'name': 'compare after caches', 'checks': compare(origin, run, 'caches')}); save()

        for name, sql, expect in [
                ('UPDATE 1M rows', f'UPDATE {DB}.orders SET score = score + 1 WHERE id % 10 = 0', 2 * ROWS // 10),
                ('INSERT 1M rows', f"INSERT INTO {DB}.orders SELECT CAST(number + {ROWS} AS BIGINT), concat('p-', to_string(number + {ROWS})), CAST(number % 100000 AS BIGINT), NULL FROM numbers({ROWS // 10})", ROWS // 10),
                ('UPDATE all rows', f'UPDATE {DB}.orders SET score = score + 1', 2 * (ROWS + ROWS // 10))]:
            seq = status(run)['checkpoint']
            t = time.monotonic()
            query(ENDPOINT, sql)
            dml = time.monotonic() - t
            probe = None
            if name == 'UPDATE all rows':
                # Mid-batch read: must be forwarded to Databend, never a half-applied result.
                time.sleep(8)
                st_mid = status(run)
                local, route = query(origin, CHECKS[0])
                direct, _ = query(ENDPOINT, CHECKS[0])
                probe = {'route': route, 'equal': sorted(map(tuple, local)) == sorted(map(tuple, direct)),
                         'checkpoint_at_probe': st_mid['checkpoint']}
                log(f'  mid-batch read: {probe}')
            st = wait_batch(run, seq)
            lb = st['last_batch']
            step = {'name': name, 'dml_secs': round(dml, 1), 'visible_secs': round(time.monotonic() - t, 1),
                    'batch': lb, 'expected_changes': expect, 'rss_mb': rss.now, 'peak_mb': rss.peak,
                    'storage_mb': du(f'{run}/engine'), 'mid_batch_probe': probe}
            results['steps'].append(step); save()
            log(f'{name}: changes={lb["changes"]} (expected {expect}) total={lb["total_ms"]/1000:.1f}s '
                f'rate={lb["changes_per_sec"]:.0f}/s finish={lb["finish"]["finish_ms"]/1000:.1f}s rss={rss.now}MB peak={rss.peak}MB')
            time.sleep(3)
            results['steps'].append({'name': f'compare after {name}', 'checks': compare(origin, run, name)}); save()
        results['final'] = {'rss_mb': rss.now, 'peak_mb': rss.peak, 'storage_mb': du(f'{run}/engine'), 'status': status(run)}
        results['passed'] = True
        save()
        log(f'DONE {results["final"]["rss_mb"]}MB rss, {results["final"]["peak_mb"]}MB peak')
    except Exception as e:
        results['passed'] = False
        results['error'] = f'{type(e).__name__}: {e}'
        save()
        log(f'ERROR {results["error"]}')
        raise
    finally:
        rss.stop.set()
        if child.poll() is None:
            child.send_signal(signal.SIGINT)
            try:
                child.wait(timeout=120)
            except subprocess.TimeoutExpired:
                child.kill()


if __name__ == '__main__':
    sys.exit(main())
