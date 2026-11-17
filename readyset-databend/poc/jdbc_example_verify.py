#!/usr/bin/env python3
"""Compile and run examples/jdbc/ReadOnlyExample.java against a real readyset-databend.

Creates readyset_gateway_jdbcex_* with `orders` (id BIGINT, status VARCHAR, amount
DECIMAL(18,2)), starts readyset-databend over it, declares
  CREATE CACHE orders_by_id FROM SELECT id, status, amount FROM <db>.orders WHERE id = ?
over HTTP, compiles the example with javac and runs it:
  matching   the default query: must print the row Databend has and "route: local cache";
  missing    the default query for an absent id: no row, still local;
  mismatch   EXAMPLE_SQL with fewer columns: must print "forwarded" and bump
             counters.forward_reasons.shape_not_matched;
and checks each verdict against the service's own /health counters (parsed as JSON).
Creates only readyset_gateway_* objects; no cleanup.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = Path(__file__).resolve().parent
EXAMPLE = HERE.parent / 'examples' / 'jdbc' / 'ReadOnlyExample.java'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--user', default='root')
    p.add_argument('--gateway-binary', required=True)
    p.add_argument('--jdbc-classpath', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--port', type=int, default=18106)
    p.add_argument('--confirm-create', action='store_true', required=True)
    args = p.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    password = os.environ.get('DATABEND_PASSWORD', '')
    auth = 'Basic ' + base64.b64encode(f'{args.user}:{password}'.encode()).decode()
    db = 'readyset_gateway_jdbcex_' + time.strftime('%Y%m%d_%H%M%S') + '_' + os.urandom(3).hex()
    orders = f'{db}.orders'
    gw = f'http://127.0.0.1:{args.port}'
    report = {'db': db, 'example': str(EXAMPLE), 'runs': [], 'checks': []}

    def http(origin, method, path, body=None):
        req = urllib.request.Request(origin + path, method=method,
                                     data=None if body is None else json.dumps(body).encode(),
                                     headers={'Authorization': auth, 'Content-Type': 'application/json'})
        try:
            r = urllib.request.urlopen(req, timeout=120)
        except urllib.error.HTTPError as e:
            r = e
        with r:
            return r.status, json.load(r)

    def sql(origin, text):
        status, resp = http(origin, 'POST', '/v1/query', {'sql': text})
        rows = list(resp.get('data') or [])
        while resp.get('next_uri') and not resp.get('error'):
            uri = resp['next_uri']
            _, resp = http(origin, 'GET', uri)
            if uri.endswith('/final'):
                break
            rows += resp.get('data') or []
        if status != 200 or resp.get('error'):
            raise RuntimeError(f'{text[:80]}: {status} {resp.get("error")}')
        return rows

    def health():
        return http(gw, 'GET', '/health')[1]

    def check(name, ok, detail=''):
        report['checks'].append({'check': name, 'ok': bool(ok), 'detail': detail})
        print(('PASS ' if ok else 'FAIL ') + name + (f': {detail}' if detail else ''), flush=True)

    for s in (f'CREATE DATABASE {db}',
              f'CREATE TABLE {orders} (id BIGINT NOT NULL, status VARCHAR NOT NULL, amount DECIMAL(18,2) NOT NULL)',
              f"INSERT INTO {orders} SELECT number, if(number % 3 = 0, 'paid', 'open'), "
              f"CAST((number % 10000) / 100 AS DECIMAL(18,2)) FROM numbers(1000)"):
        sql(args.endpoint, s)
    (out / 'config.toml').write_text(f'''instance_id = "jex{os.urandom(3).hex()}"
confirm_create = true
[upstream]
url = "{args.endpoint}"
user = "{args.user}"
[listen]
http = "127.0.0.1:{args.port}"
[replication]
tables = ["{orders}"]
[storage]
dir = "{out / 'storage'}"
''')
    log = (out / 'server.log').open('w')
    server = subprocess.Popen([args.gateway_binary, '--config', str(out / 'config.toml')],
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
        sql(gw, f'CREATE CACHE orders_by_id FROM SELECT id, status, amount FROM {orders} WHERE id = ?')
        while not health()['tables'][0]['ready']:
            time.sleep(0.2)
        classes = out / 'classes'
        classes.mkdir()
        compiled = subprocess.run(['javac', '--release', '17', '-encoding', 'UTF-8', '-Xlint:all', '-Werror',
                                   '-cp', args.jdbc_classpath, '-d', str(classes), str(EXAMPLE)],
                                  capture_output=True, text=True)
        check('example compiles (javac --release 17 -Xlint:all -Werror)', compiled.returncode == 0,
              (compiled.stdout + compiled.stderr).strip()[:500])

        def run(name, env_extra):
            env = dict(os.environ, READYSET_JDBC_URL=f'jdbc:databend://127.0.0.1:{args.port}/{db}',
                       READYSET_HEALTH_URL=f'{gw}/health', READYSET_USER=args.user,
                       DATABEND_PASSWORD=password, **env_extra)
            before = health()['counters']
            proc = subprocess.run(['java', '-cp', f'{classes}:{args.jdbc_classpath}', 'ReadOnlyExample'],
                                  env=env, capture_output=True, text=True, timeout=120)
            after = health()['counters']
            lines = [l for l in proc.stdout.splitlines() if l.startswith(('row:', 'rows:', 'route:'))]
            reasons = {k: v - before['forward_reasons'].get(k, 0) for k, v in after['forward_reasons'].items()
                       if v != before['forward_reasons'].get(k, 0)}
            result = {'run': name, 'exit_code': proc.returncode, 'output': lines,
                      'local': after['readyset'] - before['readyset'], 'forwarded': after['databend'] - before['databend'],
                      'forward_reasons': reasons, 'stderr': '\n'.join(l for l in proc.stderr.splitlines() if 'INFO' not in l)[-500:]}
            report['runs'].append(result)
            print('RUN ' + json.dumps(result, ensure_ascii=False), flush=True)
            return result

        # 1. Cached shape, existing id.
        want = sql(args.endpoint, f'SELECT id, status, amount FROM {orders} WHERE id = 42')
        r = run('matching', {'EXAMPLE_ID': '42'})
        expect = f'row: id={want[0][0]} status={want[0][1]} amount={want[0][2]}'
        check('matching: exit 0', r['exit_code'] == 0, r['stderr'])
        check('matching: row equals Databend', expect in r['output'], f'{r["output"]} vs {expect}')
        check('matching: example reports local cache', 'route: local cache (readyset)' in r['output'])
        check('matching: /health agrees (1 local, 0 forwarded)', r['local'] == 1 and r['forwarded'] == 0,
              f"local={r['local']} forwarded={r['forwarded']}")

        # 2. Cached shape, absent id: an empty result is still a local answer.
        r = run('missing', {'EXAMPLE_ID': '999999'})
        check('missing: no row, reported local', 'rows: 0' in r['output'] and 'route: local cache (readyset)' in r['output'],
              str(r['output']))
        check('missing: /health agrees', r['local'] == 1 and r['forwarded'] == 0)

        # 3. Different shape (fewer columns): forwarded, and the reason says why.
        r = run('mismatch', {'EXAMPLE_SQL': 'SELECT id, status FROM orders WHERE id = ?', 'EXAMPLE_ID': '42'})
        check('mismatch: row from Databend', f'row: id=42 status={want[0][1]}' in r['output'], str(r['output']))
        check('mismatch: example reports forwarded', any(l.startswith('route: forwarded') for l in r['output']))
        check('mismatch: /health agrees (0 local, 1 forwarded, shape_not_matched)',
              r['local'] == 0 and r['forwarded'] == 1 and r['forward_reasons'] == {'shape_not_matched': 1},
              f"local={r['local']} forwarded={r['forwarded']} reasons={r['forward_reasons']}")
    finally:
        server.send_signal(signal.SIGTERM)
        try:
            report['exit_code'] = server.wait(timeout=120)
        except subprocess.TimeoutExpired:
            server.kill()
            report['exit_code'] = 'killed'
        log.close()
        report['passed'] = bool(report['checks']) and all(c['ok'] for c in report['checks'])
        (out / 'report.json').write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print('RESULT ' + json.dumps({'passed': report['passed'], 'checks': len(report['checks']),
                                  'report': str(out / 'report.json')}), flush=True)


if __name__ == '__main__':
    main()
