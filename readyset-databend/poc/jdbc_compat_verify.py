#!/usr/bin/env python3
"""databend-jdbc compatibility, used the way production uses it.

Creates readyset_gateway_jdbc_* with table `a` (BIGINT id, VARCHAR ext_id, nullable INT, TINYINT
UNSIGNED, BOOLEAN, DECIMAL(18,2), nullable STRING with quotes / CJK / emoji), starts
readyset-databend over it with caches on id and ext_id, then runs poc/jdbc/JdbcCompatProbe.java
with databend-jdbc: DriverManager connections (no pool), PreparedStatement with bound parameters,
reads and writes on the same URL. Each read is compared with the same JDBC call on Databend.
Records the gateway route journal (paths, header and session key NAMES only) and /health.
Creates only readyset_gateway_* objects; no cleanup.
"""
import argparse
import base64
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = Path(__file__).resolve().parent


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--user', default='root')
    p.add_argument('--gateway-binary', required=True)
    p.add_argument('--jdbc-classpath', required=True, help='databend-jdbc jar (shaded) and dependencies')
    p.add_argument('--output', required=True)
    p.add_argument('--rows', type=int, default=100_000)
    p.add_argument('--port', type=int, default=18104)
    p.add_argument('--partial', action='store_true')
    p.add_argument('--load-secs', type=int, default=20)
    p.add_argument('--load-threads', type=int, default=32)
    p.add_argument('--load-target', choices=['gateway', 'databend'], default='gateway')
    p.add_argument('--write-interval-ms', type=int, default=0, help='also INSERT through the same URL every N ms during the load')
    p.add_argument('--confirm-create', action='store_true', required=True)
    args = p.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    password = os.environ.get('DATABEND_PASSWORD', '')
    auth = 'Basic ' + base64.b64encode(f'{args.user}:{password}'.encode()).decode()
    db = 'readyset_gateway_jdbc_' + time.strftime('%Y%m%d_%H%M%S') + '_' + os.urandom(3).hex()
    gw = f'http://127.0.0.1:{args.port}'
    report = {'db': db, 'args': vars(args), 'steps': []}

    def http(origin, method, path, body=None, timeout=600):
        req = urllib.request.Request(origin + path, method=method,
                                     data=None if body is None else json.dumps(body).encode(),
                                     headers={'Authorization': auth, 'Content-Type': 'application/json'})
        try:
            r = urllib.request.urlopen(req, timeout=timeout)
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
            raise RuntimeError(f'{status} {json.dumps(resp.get("error"))[:400]}')
        return rows

    def health():
        return http(gw, 'GET', '/health', timeout=10)[1]

    a = f'{db}.a'
    up = args.endpoint
    for s in (f'CREATE DATABASE {db}',
              f'CREATE TABLE {a} (id BIGINT NOT NULL, ext_id VARCHAR NOT NULL, i32 INT NULL, u8 TINYINT UNSIGNED NOT NULL, '
              f'flag BOOLEAN NOT NULL, amount DECIMAL(18,2) NOT NULL, note STRING NULL)',
              f"INSERT INTO {a} SELECT number, concat('ext-', to_string(number)), "
              f"if(number % 2 = 0, NULL, CAST(number % 100000 AS INT)), CAST(number % 200 AS UINT8), number % 3 = 0, "
              f"CAST((number % 100000) / 100 AS DECIMAL(18,2)), if(number % 7 = 2, NULL, concat('n', to_string(number))) "
              f"FROM numbers({args.rows})",
              f"UPDATE {a} SET note = '它''s \"q\" \\\\ 😀 ' || ext_id WHERE id IN (3, 42)"):
        sql(up, s)
    (out / 'config.toml').write_text(f'''instance_id = "jdbc{os.urandom(3).hex()}"
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
partial = {'true' if args.partial else 'false'}
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
        cols = 'id, ext_id, i32, u8, flag, amount, note'
        for name, where in (('id', 'id = ?'), ('ext', 'ext_id = ?')):
            sql(gw, f'CREATE CACHE jdbc_{name} FROM SELECT {cols} FROM {a} WHERE {where}')
        while not health()['tables'][0]['ready']:
            time.sleep(0.2)
        classes = out / 'classes'
        classes.mkdir()
        subprocess.run(['javac', '--release', '17', '-encoding', 'UTF-8', '-cp', args.jdbc_classpath, '-d', str(classes),
                        str(HERE / 'jdbc' / 'JdbcCompatProbe.java')], check=True)
        host = urllib.parse.urlparse(up).netloc
        cmd = ['java', '-cp', f'{classes}:{args.jdbc_classpath}', 'JdbcCompatProbe',
               f'jdbc:databend://127.0.0.1:{args.port}/{db}', f'jdbc:databend://{host}/{db}', gw + '/health',
               args.user, password, 'a', str(args.rows), str(args.load_secs), str(args.load_threads), args.load_target, str(args.write_interval_ms)]
        probe = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
        (out / 'probe.stderr').write_text(probe.stderr)
        for line in probe.stdout.splitlines():
            if line.startswith('{'):
                step = json.loads(line)
                report['steps'].append(step)
                print('STEP ' + json.dumps(step, ensure_ascii=False)[:1200], flush=True)
            elif line.startswith('SUMMARY '):
                report['summary'] = json.loads(line[8:])
        report['probe_exit'] = probe.returncode
        h = health()
        report['health'] = {'counters': h['counters'], 'rows': h['tables'][0]['rows'], 'failed': h['tables'][0]['failed']}
    finally:
        server.send_signal(signal.SIGTERM)
        try:
            report['exit_code'] = server.wait(timeout=120)
        except subprocess.TimeoutExpired:
            server.kill()
            report['exit_code'] = 'killed'
        log.close()
        runs = sorted((out / 'storage' / 'runs').glob('*/routes.jsonl'))
        if runs:
            seen = {}
            for line in runs[-1].read_text().splitlines():
                r = json.loads(line)
                path = re.sub(r'^/v1/query/[^/]+', '/v1/query/<id>', r['path'].split('?')[0])
                path = re.sub(r'/page/\d+$', '/page/<n>', path)
                key = (r['method'], path,
                       r['status'], r.get('route'), tuple(sorted(r.get('databend_headers', []))),
                       tuple(sorted(r.get('session_keys', []))))
                seen[key] = seen.get(key, 0) + 1
            report['route_shapes'] = [{'method': k[0], 'path': k[1], 'status': k[2], 'route': k[3],
                                       'headers': list(k[4]), 'session_keys': list(k[5]), 'count': n}
                                      for k, n in sorted(seen.items(), key=lambda kv: -kv[1])]
        (out / 'report.json').write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print('RESULT ' + json.dumps({'report': str(out / 'report.json'), 'summary': report.get('summary'),
                                  'exit_code': report.get('exit_code')}), flush=True)


if __name__ == '__main__':
    main()
