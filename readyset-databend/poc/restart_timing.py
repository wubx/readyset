#!/usr/bin/env python3
"""Restart timing on EXISTING large tables (e.g. the 10M-row scale-bench databases).

Life 1 starts with an empty storage dir (full snapshot), declares caches and stops with
SIGTERM; life 2 reopens the same dir (resume). Reports time to ready, per-table recovery,
RSS, and checks sample keys local vs Databend after resume. Read-only on Databend except
the instance Stream it creates. No cleanup.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.error
import urllib.request
import uuid

from m0 import Client, NoRedirect


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--user', default='root')
    p.add_argument('--gateway-binary', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--tables', nargs=2, required=True, help='db.table for A and B')
    p.add_argument('--caches', nargs=2, required=True, help='CREATE CACHE sql for A and B (named a_rt, b_rt)')
    p.add_argument('--probes', nargs='+', required=True, help='SELECT statements matching the caches')
    p.add_argument('--partial', action='store_true')
    p.add_argument('--memory-limit-mb', type=int, default=3072)
    p.add_argument('--confirm-create', action='store_true', required=True)
    args = p.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    source = Client(args.endpoint, args.user, os.environ.get('DATABEND_PASSWORD', ''), out / 'queries.jsonl', 'admin')
    instance = 'rt' + uuid.uuid4().hex[:12]
    service = out / 'service'
    (out / 'config.toml').write_text(f'''instance_id = "{instance}"
confirm_create = true
[upstream]
url = "{args.endpoint}"
user = "{args.user}"
[listen]
http = "127.0.0.1:0"
[replication]
tables = ["{args.tables[0]}", "{args.tables[1]}"]
poll_interval = "300ms"
max_staleness = "30s"
[cache]
partial = {str(args.partial).lower()}
{('memory_limit_mb = ' + str(args.memory_limit_mb)) if args.partial else ''}
upquery_timeout_ms = 10000
''')
    report = {'instance': instance, 'tables': args.tables, 'lives': [], 'checks': []}
    state = {}

    def http(method, path, body=None):
        req = urllib.request.Request(state['origin'] + path, data=None if body is None else json.dumps(body).encode(),
                                     headers={'Authorization': source.auth, 'Content-Type': 'application/json'}, method=method)
        try:
            r = urllib.request.build_opener(NoRedirect()).open(req, timeout=120)
        except urllib.error.HTTPError as e:
            r = e
        with r:
            return r.status, dict(r.headers), json.load(r)

    def query(sql):
        status, headers, first = http('POST', '/v1/query', {'sql': sql})
        if status != 200 or first.get('error'):
            raise RuntimeError(first)
        rows, resp = list(first.get('data', [])), first
        while resp.get('next_uri'):
            uri = resp['next_uri']
            _, _, resp = http('GET', uri)
            if uri.endswith('/final'):
                break
            rows.extend(resp.get('data', []))
        return next((v for k, v in headers.items() if k.lower() == 'x-readyset-route'), None), rows

    def rss(pid):
        for line in Path(f'/proc/{pid}/status').read_text().splitlines():
            if line.startswith(('VmRSS', 'VmHWM')):
                yield line.split(':')[0], int(line.split()[1]) // 1024

    def life(n):
        listening = service / 'listening.json'
        if listening.exists():
            listening.unlink()
        log = (out / f'server_{n}.log').open('w')
        started = time.monotonic()
        proc = subprocess.Popen([args.gateway_binary, '--config', str(out / 'config.toml'), '--output', str(service)],
                                stdout=log, stderr=subprocess.STDOUT)
        while not listening.exists():
            if proc.poll() is not None:
                raise RuntimeError('server exited')
            time.sleep(.2)
        state['origin'] = json.loads(listening.read_text())['origin']
        listen_secs = time.monotonic() - started
        while True:
            _, _, h = http('GET', '/health')
            t = {x['table']: x for x in h['tables']}
            if any(t[x]['failed'] for x in args.tables):
                raise RuntimeError(t)
            if all(t[x]['ready'] for x in args.tables):
                break
            time.sleep(.2)
        info = {'life': n, 'listen_secs': round(listen_secs, 3), 'ready_secs': round(time.monotonic() - started, 3),
                'recovery': {x: t[x]['snapshot'].get('recovery') for x in args.tables},
                'load_secs': {x: t[x]['snapshot'].get('total_secs') for x in args.tables},
                'rows': {x: t[x]['rows'] for x in args.tables}, 'memory_mib': dict(rss(proc.pid))}
        print('LIFE ' + json.dumps(info), flush=True)
        report['lives'].append(info)
        return proc, log

    def check(name, ok):
        report['checks'].append({'name': name, 'passed': bool(ok)})
        print(('PASS ' if ok else 'FAIL ') + name, flush=True)

    proc, log = life(1)
    try:
        for i, sql in enumerate(args.caches):
            route, rows = query(sql)
            check('cache created ' + str(i), route == 'readyset')
        # Warm every probe once so full readers are populated before the stop.
        for sql in args.probes:
            route, rows = query(sql)
            check('life1 local: ' + sql[:60], route == 'readyset' and sorted(rows) == sorted(source.sql(sql)))
    finally:
        proc.send_signal(signal.SIGTERM)
        check('life1 clean exit', proc.wait(timeout=300) == 0)
        log.close()
    proc, log = life(2)
    try:
        check('life2 resumed both', all(v == 'resumed' for v in report['lives'][1]['recovery'].values()))
        for sql in args.probes:
            started = time.monotonic()
            while True:
                route, rows = query(sql)
                if route == 'readyset' or time.monotonic() - started > 600:
                    break
                time.sleep(.5)
            check('life2 local after resume: ' + sql[:60], route == 'readyset' and sorted(rows) == sorted(source.sql(sql)))
            report.setdefault('first_local_secs', {})[sql[:60]] = round(time.monotonic() - started, 3)
    finally:
        proc.send_signal(signal.SIGTERM)
        check('life2 clean exit', proc.wait(timeout=300) == 0)
        log.close()
    report['passed'] = all(c['passed'] for c in report['checks'])
    (out / 'report.json').write_text(json.dumps(report, indent=2))
    print('RESULT ' + json.dumps({'passed': report['passed'], 'report': str(out / 'report.json')}), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
