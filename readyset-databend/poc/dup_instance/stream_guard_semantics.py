#!/usr/bin/env python3
"""Databend semantics behind a "Stream consumed by someone else" guard, measured directly.

Creates readyset_gateway_dupid_* with table t and a standard Stream st on it, then runs
two HTTP sessions S1 (the instance under test) and S2 (an intruder with the same Stream),
each exactly like readyset-databend: BEGIN; SELECT ... FROM st WITH CONSUME; COMMIT.
Before every case a few rows are inserted, so the Stream always has pending changes, and
`applied` is system.streams.table_version as S1 last committed it (the local marker).

  E1 intruder consumed before S1's BEGIN
  E2 intruder consumed after S1's BEGIN, before S1's consume read
  E3 intruder consumed after S1's consume read, before S1's COMMIT
  E4 no intruder
For each case: rows S1 read, system.streams.table_version read inside S1's transaction
right after the consume read ("in-txn version"), whether that equals `applied`, and the
outcome of S1's COMMIT. A guard "abort unless in-txn version == applied" is reliable if,
in every case where S1 missed changes, the in-txn version differs or COMMIT fails.
Creates only readyset_gateway_* objects; no cleanup.
"""
import argparse
import base64
import json
import os
import time
import urllib.error
import urllib.request


class Session:
    def __init__(self, endpoint, auth, name):
        self.endpoint, self.auth, self.name = endpoint, auth, name
        self.session = {}
        self.node = None

    def http(self, method, path, body=None):
        headers = {'Authorization': self.auth, 'Content-Type': 'application/json'}
        if self.node and (path != '/v1/query' or self.session.get('need_sticky')):
            headers['X-DATABEND-STICKY-NODE'] = self.node
        req = urllib.request.Request(self.endpoint + path, method=method, headers=headers,
                                     data=None if body is None else json.dumps(body).encode())
        try:
            r = urllib.request.urlopen(req, timeout=120)
        except urllib.error.HTTPError as e:
            r = e
        with r:
            return r.status, json.load(r)

    def sql(self, text):
        status, resp = self.http('POST', '/v1/query', {'sql': text, 'session': self.session,
                                                       'pagination': {'wait_time_secs': 10}})
        rows = list(resp.get('data') or [])
        while True:
            if resp.get('session'):
                self.session = resp['session']
            if resp.get('node_id'):
                self.node = resp['node_id']
            uri = resp.get('next_uri')
            if not uri or resp.get('error'):
                break
            _, resp = self.http('GET', uri)
            if uri.endswith('/final'):
                break
            rows += resp.get('data') or []
        return status, resp.get('error'), rows


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--user', default='root')
    p.add_argument('--output', required=True)
    p.add_argument('--confirm-create', action='store_true', required=True)
    args = p.parse_args()
    auth = 'Basic ' + base64.b64encode(f"{args.user}:{os.environ.get('DATABEND_PASSWORD', '')}".encode()).decode()
    db = 'readyset_gateway_dupid_' + time.strftime('%Y%m%d_%H%M%S') + '_' + os.urandom(3).hex()
    admin = Session(args.endpoint, auth, 'admin')
    s1, s2 = Session(args.endpoint, auth, 'S1'), Session(args.endpoint, auth, 'S2')

    def must(sess, text):
        status, error, rows = sess.sql(text)
        if status != 200 or error:
            raise RuntimeError(f'{sess.name} {text[:60]}: {status} {error}')
        return rows

    must(admin, f'CREATE DATABASE {db}')
    must(admin, f'CREATE TABLE {db}.t (id BIGINT NOT NULL, v VARCHAR NOT NULL)')
    must(admin, f'CREATE STREAM {db}.st ON TABLE {db}.t APPEND_ONLY = false')
    consume = f'SELECT id, change$action FROM {db}.st WITH CONSUME'
    next_id = [0]

    def version():
        rows = must(admin, f"SELECT table_version FROM system.streams WHERE database = '{db}' AND name = 'st'")
        return int(rows[0][0])

    def insert(n=3):
        lo = next_id[0]
        next_id[0] += n
        must(admin, f"INSERT INTO {db}.t SELECT number + {lo}, 'x' FROM numbers({n})")
        return set(range(lo, lo + n))

    def intruder():
        must(s2, 'BEGIN')
        rows = must(s2, consume)
        must(s2, 'COMMIT')
        return len(rows)

    # Establish S1's applied position: consume everything once.
    insert()
    must(s1, 'BEGIN'); must(s1, consume); must(s1, 'COMMIT')
    applied = version()
    report = {'db': db, 'cases': []}
    for case in ('E1', 'E2', 'E3', 'E4'):
        expected = insert()
        intruder_rows = None
        if case == 'E1':
            intruder_rows = intruder()
        must(s1, 'BEGIN')
        if case == 'E2':
            intruder_rows = intruder()
        rows = must(s1, consume)
        if case == 'E3':
            intruder_rows = intruder()
        in_txn = version()
        status, error, _ = s1.sql('COMMIT')
        committed = status == 200 and not error
        if not committed:
            s1.sql('ROLLBACK')
        seen = {int(r[0]) for r in rows}
        after = version()
        result = {
            'case': case, 'expected_new_ids': len(expected), 's1_rows': len(rows),
            's1_missed': len(expected - seen), 'intruder_rows': intruder_rows,
            'applied_before': applied, 'in_txn_version': in_txn, 'in_txn_equals_applied': in_txn == applied,
            's1_commit': 'ok' if committed else (error or {}).get('message', str(status))[:160],
            'version_after': after,
        }
        result['guard_would_abort'] = not result['in_txn_equals_applied']
        result['silent_loss_without_guard'] = committed and result['s1_missed'] > 0
        result['silent_loss_with_guard'] = committed and result['s1_missed'] > 0 and not result['guard_would_abort']
        report['cases'].append(result)
        print('CASE ' + json.dumps(result), flush=True)
        if committed:
            applied = after
        else:
            # A failed COMMIT leaves the Stream where the intruder put it.
            applied = after
    os.makedirs(args.output, exist_ok=False)
    with open(os.path.join(args.output, 'report.json'), 'w') as f:
        json.dump(report, f, indent=2)
    print('RESULT ' + json.dumps({'report': os.path.join(args.output, 'report.json'),
                                  'guard_reliable': not any(c['silent_loss_with_guard'] for c in report['cases'])}))


if __name__ == '__main__':
    main()
