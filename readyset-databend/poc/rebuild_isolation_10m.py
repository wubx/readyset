#!/usr/bin/env python3
"""A rebuilds itself in-process while B keeps serving: 10M-row isolation check.

Reuses an existing service dir (e.g. restart_timing's) over two large tables. Triggers a
real rebuild of A by adding a column to A in Databend, then keeps updating B and measures
B's write-to-fresh latency and local reads for the whole rebuild. Modifies only A's schema
(synthetic readyset_gateway_* table); no cleanup.
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

from m0 import Client, NoRedirect


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--user', default='root')
    p.add_argument('--gateway-binary', required=True)
    p.add_argument('--service-dir', required=True, help='existing --output dir to resume')
    p.add_argument('--config', required=True)
    p.add_argument('--tables', nargs=2, required=True)
    p.add_argument('--b-probe', required=True, help='SELECT on B served by a declared cache')
    p.add_argument('--b-key', required=True, help='WHERE clause selecting the probed B rows')
    p.add_argument('--a-probe', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--confirm-alter', action='store_true', required=True)
    args = p.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    source = Client(args.endpoint, args.user, os.environ.get('DATABEND_PASSWORD', ''), out / 'queries.jsonl', 'admin')
    a, b = args.tables
    service = Path(args.service_dir)
    report = {'checks': [], 'b_rounds': []}
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

    def tables():
        return {t['table']: t for t in http('GET', '/health')[2]['tables']}

    def check(name, ok, **detail):
        report['checks'].append({'name': name, 'passed': bool(ok), **detail})
        print(('PASS ' if ok else 'FAIL ') + name + ' ' + json.dumps(detail)[:300], flush=True)

    listening = service / 'listening.json'
    if listening.exists():
        listening.unlink()
    log = (out / 'server.log').open('w')
    proc = subprocess.Popen([args.gateway_binary, '--config', args.config, '--output', str(service)], stdout=log, stderr=subprocess.STDOUT)
    try:
        while not listening.exists():
            if proc.poll() is not None:
                raise RuntimeError('server exited')
            time.sleep(.2)
        state['origin'] = json.loads(listening.read_text())['origin']
        started = time.monotonic()
        while not all(t['ready'] for t in tables().values()):
            time.sleep(.2)
        t = tables()
        check('start resumed both', all(t[x]['snapshot'].get('recovery') == 'resumed' for x in (a, b)),
              recovery={x: t[x]['snapshot'].get('recovery') for x in (a, b)}, secs=round(time.monotonic() - started, 3))
        source.sql('ALTER TABLE ' + a + ' ADD COLUMN rt_extra BIGINT DEFAULT 0')
        altered = time.monotonic()
        while not tables()[a]['failed']:
            time.sleep(.1)
        report['a_failed_after_secs'] = round(time.monotonic() - altered, 3)
        n = 0
        a_back = None
        while time.monotonic() - altered < 1800:
            n += 1
            label = 'rt-rebuild-' + str(n)
            source.sql("UPDATE " + b + " SET label='" + label + "' WHERE " + args.b_key)
            written = time.monotonic()
            while True:
                tb = tables()[b]
                if tb['failed']:
                    raise RuntimeError('B failed: ' + tb['failed'])
                if tb['ready'] and tb['last_sync_age_ms'] <= (time.monotonic() - written) * 1000:
                    break
                time.sleep(.02)
            fresh = time.monotonic() - written
            route, rows = query(args.b_probe)
            ta = tables()[a]
            report['b_rounds'].append({'n': n, 'fresh_secs': round(fresh, 3), 'route': route,
                                       'label_ok': bool(rows) and all(label in json.dumps(r) for r in rows),
                                       'a_failed': bool(ta['failed']), 'a_applying': ta['applying'], 'a_syncing': ta['syncing']})
            if not ta['failed'] and ta['ready'] and a_back is None:
                a_back = time.monotonic() - altered
                report['a_recovery'] = ta['snapshot']
                if n >= 5:
                    break
            if a_back is not None and n >= 5:
                break
            time.sleep(.5)
        during = [r for r in report['b_rounds'] if r['a_failed']]
        fresh = sorted(r['fresh_secs'] for r in during)
        report['b_during_a_rebuild'] = {'rounds': len(during), 'local': sum(r['route'] == 'readyset' for r in during),
                                        'label_ok': sum(r['label_ok'] for r in during),
                                        'fresh_p50_secs': fresh[len(fresh) // 2] if fresh else None,
                                        'fresh_max_secs': fresh[-1] if fresh else None}
        report['a_back_after_secs'] = round(a_back, 3) if a_back else None
        check('A rebuilt itself for the new schema', (report.get('a_recovery') or {}).get('recovery') == 'rebuilt: source schema changed',
              a_back_after_secs=report['a_back_after_secs'])
        check('B kept syncing and serving locally during A rebuild',
              during and all(r['route'] == 'readyset' and r['label_ok'] for r in during), **report['b_during_a_rebuild'])
        route, rows = query(args.a_probe)
        check('A local and correct after rebuild', route == 'readyset' and sorted(rows) == sorted(source.sql(args.a_probe)))
    finally:
        proc.send_signal(signal.SIGTERM)
        check('clean exit', proc.wait(timeout=300) == 0)
        log.close()
    report['passed'] = all(c['passed'] for c in report['checks'])
    (out / 'report.json').write_text(json.dumps(report, indent=2))
    print('RESULT ' + json.dumps({'passed': report['passed'], 'report': str(out / 'report.json')}), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
