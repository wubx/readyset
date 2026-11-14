#!/usr/bin/env python3
"""Remote 10M-row native partial/replay/eviction validation. Creates ONLY a new POC DB.

Uses the existing HTTP benchmark helpers; no external Python packages. All comparisons use
bags, not sets, so duplicate rows are checked. Keeps DB and evidence; stops its own services.
MEMORY_LIMIT_MB defaults to 2048 (soft target, not an RSS cap). ROWS defaults to 10000000.
"""
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time
import urllib.request
from datetime import datetime

import rs_scale_bench as b

ROWS = int(os.environ.get('ROWS', '10000000'))
LIMIT = int(os.environ.get('MEMORY_LIMIT_MB', '2048'))
# Keep one burst below the 2048 local-query tombstone budget (60s TTL), otherwise the
# front end deliberately forwards unrelated to reader residency or memory pressure.
HEAT = int(os.environ.get('HEAT_QUERIES', '1800'))
STAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
DB = f'readyset_gateway_partial_{STAMP}'
OUT = Path(f'/data1/vmbox/readyset-partial-{STAMP}')
BINARY = os.environ.get('BINARY', b.BINARY)
report = {'database': DB, 'rows': ROWS, 'memory_limit_mb': LIMIT, 'checks': [], 'stages': []}


def save():
    (OUT / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))


def check(name, ok, **detail):
    report['checks'].append({'name': name, 'ok': bool(ok), **detail})
    save()
    b.log(f'{"PASS" if ok else "FAIL"} {name}: {detail}')
    if not ok:
        raise AssertionError(name)


def current(run):
    origin = json.loads((run / 'listening.json').read_text())['origin']
    request = urllib.request.Request(origin + '/health', headers={'Authorization': b.AUTH})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def stage(name, run, rss):
    s = current(run)
    item = {'name': name, 'cache': s['cache'], 'rss_mb': rss.now, 'hwm_mb': rss.peak,
            'checkpoint': s['checkpoint'], 'failed': s['failed']}
    report['stages'].append(item)
    save()
    b.log(f'{name}: rss={rss.now}MiB allocated={s["cache"]["allocated_bytes"] // 1048576}MiB '
          f'keys={sum(r["key_slots"] for r in s["cache"]["readers"])}')
    return s


def bag(rows):
    return sorted(map(tuple, rows))


def wait_ready(run, after=None):
    until = time.monotonic() + 180
    while time.monotonic() < until:
        # status.json is sampled by the sync loop. A forwarded read can invalidate readiness
        # after that file was written; require live HTTP state before asserting a local hit.
        s = current(run)
        if s.get('failed'):
            raise RuntimeError(s['failed'])
        if s['ready'] and (after is None or s['checkpoint'] > after):
            return s
        time.sleep(.05)
    raise TimeoutError('replica did not become ready')


def compare(origin, run, sql, label, require_local=True):
    wait_ready(run)
    expected, _ = b.query(b.ENDPOINT, sql)
    before = current(run)['cache']
    t = time.monotonic()
    actual, route = b.query(origin, sql)
    ms = (time.monotonic() - t) * 1000
    check(label, bag(actual) == bag(expected) and (route == 'readyset' or not require_local),
          rows=len(actual), route=route, ms=round(ms, 2),
          misses_before=before['miss_keys'], misses_after=current(run)['cache']['miss_keys'])
    return actual


def stop(child):
    if child.poll() is None:
        child.send_signal(signal.SIGTERM)
        try:
            child.wait(timeout=90)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()


def start(extra, label):
    run = OUT / label
    child = subprocess.Popen([BINARY, '--endpoint', b.ENDPOINT, '--user', 'root', '--database', DB,
                              '--table', 'orders', '--listen', '127.0.0.1:0', '--interval', '1',
                              '--max-staleness', '60', '--confirm-create', '--output', str(run),
                              '--partial', '--memory-limit-mb', str(LIMIT), *extra],
                             stdout=(OUT / f'{label}_stdout.log').open('w'),
                             stderr=(OUT / f'{label}_stderr.log').open('w'))
    metrics = OUT / f'{label}_metrics'
    metrics.mkdir()
    b.OUT = str(metrics)
    rss = b.Rss(child.pid)
    rss.start()
    until = time.monotonic() + 600
    while not (run / 'listening.json').exists():
        if child.poll() is not None:
            raise RuntimeError(f'{label} startup exited: {child.returncode}')
        if time.monotonic() > until:
            stop(child)
            raise TimeoutError('startup deadline')
        time.sleep(.25)
    origin = json.loads((run / 'listening.json').read_text())['origin']
    return child, run, origin, rss


def main():
    OUT.mkdir(exist_ok=False)
    b.DB, b.OUT = DB, str(OUT)
    b.log(f'DB={DB} OUT={OUT}')
    save()
    child = rss = None
    try:
        for sql in [f'CREATE DATABASE {DB}',
                    f'CREATE TABLE {DB}.orders (id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL)',
                    f"INSERT INTO {DB}.orders SELECT CAST(number AS BIGINT), concat('p-',to_string(number)), CAST(number % 100000 AS BIGINT), IF(number % 2 = 0,NULL,'n') FROM numbers({ROWS})"]:
            b.query(b.ENDPOINT, sql)
        t = time.monotonic()
        child, run, origin, rss = start([], 'service')
        s = stage('snapshot', run, rss)
        report['snapshot_wall_secs'] = round(time.monotonic() - t, 2)
        check('internal reader is partial, not a full id copy',
              s['cache']['materialization'] == 'partial' and
              all(r['key_slots'] < 10 for r in s['cache']['readers']), cache=s['cache'])
        for name, projection, where in [
                ('c_id', 'id,payload,score,note', 'id=?'),
                ('c_in', 'id,payload', 'score IN (?,?)'),
                ('c_range', 'id,payload,score,note', 'score>=? AND score<?'),
                ('c_payload', 'id,score', 'payload=?')]:
            t = time.monotonic()
            b.query(origin, f'CREATE CACHE {name} FROM SELECT {projection} FROM orders WHERE {where}')
            report.setdefault('cache_create_secs', {})[name] = round(time.monotonic() - t, 2)
        s = stage('four caches empty', run, rss)
        check('creating caches does not materialize all rows', all(r['key_slots'] < 20 for r in s['cache']['readers']))

        tests = ['SELECT id,payload,score,note FROM orders WHERE id=123456',
                 'SELECT id,payload FROM orders WHERE score IN (3,4)',
                 'SELECT id,payload,score,note FROM orders WHERE score>=2 AND score<4',
                 "SELECT id,score FROM orders WHERE payload='p-9999999'",
                 'SELECT id,payload FROM orders WHERE score IN (3,999999)',
                 'SELECT id,payload,score,note FROM orders WHERE id=-777']
        for i, sql in enumerate(tests):
            compare(origin, run, sql, f'cold-{i}')
            compare(origin, run, sql, f'hot-{i}')
        cold = stage('cold/hot checked', run, rss)
        check('native cold misses observed', cold['cache']['miss_keys'] > 0)

        # Exercise repeated distinct keys at concurrency. Each range yields <= 5,000 rows,
        # below the result cap. Native replay is unbounded by the HTTP result size.
        def heat(i):
            lo = (i * 47) % 99500
            sql = f'SELECT id,payload,score,note FROM orders WHERE score>={lo} AND score<{lo+50}'
            rows, route = b.query(origin, sql)
            # No writes yet: verify every returned row, including multiplicities and NULLs.
            expected = []
            for score in range(lo, lo+50):
                for id_ in range(score, ROWS, 100000):
                    expected.append((str(id_), f'p-{id_}', str(score), None if id_ % 2 == 0 else 'n'))
            if bag(rows) != sorted(expected):
                raise AssertionError(f'heat {i} bad result, route={route}')
            return route
        started = time.monotonic()
        routes = {'readyset': 0, 'databend': 0}
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            for i, route in enumerate(pool.map(heat, range(HEAT)), 1):
                routes[route] = routes.get(route, 0) + 1
                if i % 250 == 0:
                    stage(f'heat {i}', run, rss)
        report['heat'] = {'queries': HEAT, 'secs': round(time.monotonic() - started, 2), 'routes': routes}
        s = stage('heat finished', run, rss)
        check('all heat queries compared exactly', sum(routes.values()) == HEAT, **report['heat'])
        check('heat exercised native readers, not source fallback', routes['readyset'] == HEAT, routes=routes)
        epochs = sum(r['eviction_epoch'] for r in s['cache']['readers'])
        check('automatic memory-pressure eviction observed', epochs > 0, epoch_sum=epochs)
        time.sleep(5)
        s = stage('settled after heat', run, rss)
        check('allocated memory settles near soft target', s['cache']['allocated_bytes'] <= LIMIT * 1048576 * 1.25,
              allocated=s['cache']['allocated_bytes'], target=LIMIT * 1048576)
        misses_before_recheck = current(run)['cache']['miss_keys']
        for i, sql in enumerate(tests):
            compare(origin, run, sql, f'after-eviction-{i}')
        check('post-eviction reads exercise real local replay',
              current(run)['cache']['miss_keys'] > misses_before_recheck)

        # DML during concurrent cold/hot traffic: check each response is a complete before or
        # after image, never a partial applied batch. Two identical copies test bag semantics.
        seq = b.status(str(run))['checkpoint']
        duplicate = f"INSERT INTO {DB}.orders VALUES (-777,'duplicate',3,NULL),(-777,'duplicate',3,NULL),(-777,'other',4,'n')"
        b.query(b.ENDPOINT, duplicate)
        wait_ready(run, seq)
        compare(origin, run, tests[-1], 'duplicate key and identical rows')
        probe = 'SELECT id,payload,score,note FROM orders WHERE id=123456'
        before, _ = b.query(b.ENDPOINT, probe)
        after = [[r[0], r[1], str(int(r[2])+100000), r[3]] for r in before]
        seq = b.status(str(run))['checkpoint']
        errors, samples = [], []
        halt = threading.Event()
        def traffic():
            try:
                while not halt.is_set():
                    got, route = b.query(origin, probe)
                    if bag(got) not in [bag(before), bag(after)]:
                        errors.append({'rows': got, 'route': route})
                    samples.append(route)
                    time.sleep(.2)  # Do not exhaust HTTP query tombstones while probing sync.
            except Exception as e:
                errors.append(str(e))
        thread = threading.Thread(target=traffic, daemon=True)
        thread.start()
        try:
            b.query(b.ENDPOINT, f'UPDATE {DB}.orders SET score=score+100000 WHERE id % 10 = 6')
            b.wait_batch(str(run), seq)
        finally:
            halt.set()
            thread.join(timeout=60)
        wait_ready(run, seq)
        check('concurrent read during 1M-row update sees complete row', not errors and bool(samples),
              samples=len(samples), local=samples.count('readyset'), upstream=samples.count('databend'), errors=errors[:3])
        for i, sql in enumerate(tests):
            compare(origin, run, sql, f'after-update-{i}')
        seq = b.status(str(run))['checkpoint']
        b.query(b.ENDPOINT, f"DELETE FROM {DB}.orders WHERE id=-777 AND payload='duplicate'")
        wait_ready(run, seq)
        compare(origin, run, tests[-1], 'delete identical copies preserves other duplicate key')
        report['final'] = stage('final primary process', run, rss)
        stop(child); rss.stop.set(); child = None

        # Real HTTP zero-wait miss -> source, then native background replay -> local hit.
        child, run, origin, rss = start(['--upquery-timeout-ms', '0'], 'zero_wait')
        b.query(origin, 'CREATE CACHE c_zero FROM SELECT id,payload FROM orders WHERE payload=?')
        wait_ready(run)
        sql = "SELECT id,payload FROM orders WHERE payload='p-1234567'"
        compare(origin, run, sql, 'zero-wait first query correct', require_local=False)
        wait_ready(run)
        compare(origin, run, sql, 'zero-wait background replay serves later hit')
        s = stage('zero-wait final', run, rss)
        check('replay timeout did not poison engine', s['failed'] is None and s['cache']['replay_timeouts'] > 0)
        report['passed'] = True
    except Exception as e:
        report['passed'] = False
        report['error'] = f'{type(e).__name__}: {e}'
        raise
    finally:
        if child is not None:
            stop(child)
        if rss is not None:
            rss.stop.set()
        save()
        b.log(f'Report: {OUT}/report.json passed={report.get("passed")}')


if __name__ == '__main__':
    main()
