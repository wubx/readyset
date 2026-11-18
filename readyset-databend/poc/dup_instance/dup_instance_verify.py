#!/usr/bin/env python3
"""Two readyset-databend processes with the SAME instance_id (separate storage) over one table.

This is the deployment mistake the Stream guard is about: both consume one Stream
(`readyset_<instance_id>_<table>`), so each sees only part of the changes. Creates
readyset_gateway_dupid_* with `a` (id BIGINT, v VARCHAR, n BIGINT), starts instance A,
waits until it is ready, then starts B with the same instance_id, declares
`SELECT id, v, n FROM a WHERE id = ?` on both, and for --duration seconds inserts --rows
new rows and updates one old row every --tick directly in Databend. Afterwards, for each
instance: local row count vs Databend, every written id read through the instance (local
answers only), wrong values, and its table events. With --restart-check, both are then
stopped, only A is restarted (the duplicate is gone), more rows are written, and A must
serve every id locally with Databend's values again. Creates only readyset_gateway_* objects.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import random
import signal
import subprocess
import time
import urllib.error
import urllib.request


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--user', default='root')
    p.add_argument('--gateway-binary', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--ports', nargs=2, type=int, default=[18107, 18108])
    p.add_argument('--duration', type=float, default=30)
    p.add_argument('--tick', type=float, default=0.3)
    p.add_argument('--rows', type=int, default=5)
    p.add_argument('--restart-check', action='store_true')
    p.add_argument('--confirm-create', action='store_true', required=True)
    args = p.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    password = os.environ.get('DATABEND_PASSWORD', '')
    auth = 'Basic ' + base64.b64encode(f'{args.user}:{password}'.encode()).decode()
    db = 'readyset_gateway_dupid_' + time.strftime('%Y%m%d_%H%M%S') + '_' + os.urandom(3).hex()
    a = f'{db}.a'
    instance_id = 'dup' + os.urandom(3).hex()
    up = args.endpoint
    report = {'db': db, 'instance_id': instance_id, 'args': vars(args), 'instances': {}}

    def http(origin, method, path, body=None, node=None):
        headers = {'Authorization': auth, 'Content-Type': 'application/json'}
        if node:
            headers['X-DATABEND-STICKY-NODE'] = node
        req = urllib.request.Request(origin + path, method=method, headers=headers,
                                     data=None if body is None else json.dumps(body).encode())
        try:
            r = urllib.request.urlopen(req, timeout=120)
        except urllib.error.HTTPError as e:
            r = e
        with r:
            return r.status, r.headers.get('X-ReadySet-Route'), json.load(r)

    def sql(origin, text):
        status, route, resp = http(origin, 'POST', '/v1/query', {'sql': text})
        rows = list(resp.get('data') or [])
        node = resp.get('node_id') or None
        while resp.get('next_uri') and not resp.get('error'):
            uri = resp['next_uri']
            _, _, resp = http(origin, 'GET', uri, node=node)
            if uri.endswith('/final'):
                break
            rows += resp.get('data') or []
        if status != 200 or resp.get('error'):
            raise RuntimeError(f'{text[:80]}: {status} {resp.get("error")}')
        return route, rows

    def health(port):
        return http(f'http://127.0.0.1:{port}', 'GET', '/health')[2]

    for s in (f'CREATE DATABASE {db}',
              f'CREATE TABLE {a} (id BIGINT NOT NULL, v VARCHAR NOT NULL, n BIGINT NOT NULL)',
              f"INSERT INTO {a} SELECT number, concat('v', to_string(number)), 0 FROM numbers(1000)"):
        sql(up, s)
    servers = {}
    logs = []

    def start(name, port):
        d = out / name
        d.mkdir(exist_ok=True)
        (d / 'config.toml').write_text(f'''instance_id = "{instance_id}"
confirm_create = true
[upstream]
url = "{up}"
user = "{args.user}"
[listen]
http = "127.0.0.1:{port}"
[replication]
tables = ["{a}"]
poll_interval = "1s"
max_staleness = "10s"
retry_backoff = "1s"
[storage]
dir = "{d / 'storage'}"
''')
        log = (d / 'server.log').open('a')
        logs.append(log)
        servers[name] = (subprocess.Popen([args.gateway_binary, '--config', str(d / 'config.toml')],
                                          stdout=log, stderr=subprocess.STDOUT), port)

    def wait_ready(port, secs=120):
        until = time.time() + secs
        while time.time() < until:
            try:
                if health(port)['tables'][0]['ready']:
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False

    try:
        start('A', args.ports[0])
        report['A_ready'] = wait_ready(args.ports[0])
        start('B', args.ports[1])
        report['B_ready'] = wait_ready(args.ports[1])
        for name, (_, port) in servers.items():
            sql(f'http://127.0.0.1:{port}', f'CREATE CACHE dup_id FROM SELECT id, v, n FROM {a} WHERE id = ?')
        written, updated = set(range(1000)), set()
        next_id = 1000
        t0 = time.time()
        while time.time() - t0 < args.duration:
            tick = time.time()
            sql(up, f"INSERT INTO {a} SELECT number + {next_id}, concat('v', to_string(number + {next_id})), 0 FROM numbers({args.rows})")
            written |= set(range(next_id, next_id + args.rows))
            next_id += args.rows
            k = random.randrange(1000)
            sql(up, f'UPDATE {a} SET n = n + 1 WHERE id = {k}')
            updated.add(k)
            time.sleep(max(0.0, args.tick - (time.time() - tick)))
        time.sleep(8)
        truth = {int(r[0]): r for r in sql(up, f'SELECT id, v, n FROM {a}')[1]}
        report['databend_rows'] = len(truth)
        report['written_ids'] = len(written)
        report['updated_ids'] = len(updated)
        for name, (proc, port) in servers.items():
            gw = f'http://127.0.0.1:{port}'
            h = health(port)
            t = h['tables'][0]
            missing, wrong, forwarded = [], [], 0
            for k in sorted(written):
                route, rows = sql(gw, f'SELECT id, v, n FROM {a} WHERE id = {k}')
                if route != 'readyset':
                    forwarded += 1
                    continue
                if not rows:
                    missing.append(k)
                elif rows[0] != truth[k]:
                    wrong.append(k)
            events = []
            for f in sorted((out / name / 'storage' / 'runs').glob('*/events.jsonl')):
                for line in f.read_text().splitlines():
                    e = json.loads(line)
                    if e['event'] in ('table_sync_failed', 'table_recovery_failed', 'table_recovered',
                                      'stream_consumed_elsewhere', 'table_stopped'):
                        events.append({k: e.get(k) for k in ('event', 'attempt', 'recovery', 'action', 'reason')
                                       if e.get(k) is not None})
            result = {'alive': proc.poll() is None, 'ready': t['ready'], 'failed': t['failed'],
                      'local_rows': t['rows'], 'databend_rows': len(truth),
                      'local_reads': len(written) - forwarded, 'forwarded_reads': forwarded,
                      'missing_rows': len(missing), 'stale_values': len(wrong),
                      'missing_sample': missing[:10], 'stale_sample': wrong[:10],
                      'detections': [e.get('action') for e in events if e['event'] == 'stream_consumed_elsewhere'],
                      'stopped': any(e['event'] == 'table_stopped' for e in events),
                      'events': events[:20], 'event_count': len(events)}
            report['instances'][name] = result
            print(f'INSTANCE {name} ' + json.dumps(result)[:1500], flush=True)
        if args.restart_check:
            for proc, _ in servers.values():
                proc.send_signal(signal.SIGTERM)
            for proc, _ in servers.values():
                proc.wait(timeout=120)
            servers.clear()
            start('A', args.ports[0])
            ready = wait_ready(args.ports[0])
            for _ in range(20):
                sql(up, f"INSERT INTO {a} SELECT number + {next_id}, concat('v', to_string(number + {next_id})), 0 FROM numbers({args.rows})")
                written |= set(range(next_id, next_id + args.rows))
                next_id += args.rows
                sql(up, f'UPDATE {a} SET n = n + 1 WHERE id = {random.randrange(1000)}')
                time.sleep(args.tick)
            time.sleep(8)
            truth = {int(r[0]): r for r in sql(up, f'SELECT id, v, n FROM {a}')[1]}
            gw = f'http://127.0.0.1:{args.ports[0]}'
            local = wrong = 0
            for k in sorted(written):
                route, rows = sql(gw, f'SELECT id, v, n FROM {a} WHERE id = {k}')
                if route == 'readyset':
                    local += 1
                    wrong += (not rows) or rows[0] != truth[k]
            t = health(args.ports[0])['tables'][0]
            recovered = [json.loads(l).get('recovery') for f in sorted((out / 'A' / 'storage' / 'runs').glob('*/events.jsonl'))
                         for l in f.read_text().splitlines() if json.loads(l)['event'] == 'table_recovered']
            report['restart'] = {'ready': ready, 'failed': t['failed'], 'local_rows': t['rows'],
                                 'databend_rows': len(truth), 'ids': len(written), 'local_reads': local,
                                 'wrong_local_answers': wrong, 'recovery': recovered[-1] if recovered else None}
            print('RESTART ' + json.dumps(report['restart']), flush=True)
    finally:
        for proc, _ in servers.values():
            proc.send_signal(signal.SIGTERM)
        for proc, _ in servers.values():
            try:
                proc.wait(timeout=120)
            except subprocess.TimeoutExpired:
                proc.kill()
        for log in logs:
            log.close()
        (out / 'report.json').write_text(json.dumps(report, indent=2))
    bad = {n: r['missing_rows'] + r['stale_values'] for n, r in report['instances'].items()}
    print('RESULT ' + json.dumps({'report': str(out / 'report.json'), 'wrong_local_answers': bad,
                                  'detections': {n: r['detections'] for n, r in report['instances'].items()},
                                  'stopped': {n: r['stopped'] for n, r in report['instances'].items()}}), flush=True)


if __name__ == '__main__':
    main()
