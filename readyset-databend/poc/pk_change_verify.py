#!/usr/bin/env python3
"""Live check: primary-key (id) changes in Databend replicate correctly into ReadySet.

Creates a NEW readyset_gateway_* POC database, starts the readyset-databend single binary
against it, declares point / IN / equality / range caches, then changes ids in Databend
(move, swap, batch shift, overlapping chain, key reuse, move-and-back, id + column change)
and compares every cached query with the same query run directly on Databend.
A second, observer-only Stream records what Databend reports for each scenario.
The last scenarios create duplicate ids and identical rows (Databend does not enforce keys):
the replica must hold the same rows as Databend and capture must keep running.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from gateway_verify import Verification
from m0 import Client

ROWS = 20
RANGE_ALL = 'score>=-1000000000 AND score<1000000000'


class PkChangeVerification(Verification):
    def __init__(self, args):
        super().__init__(args)
        self.facts = {'stream': {}}
        self.observer = Client(args.endpoint, args.user, os.environ.get('DATABEND_PASSWORD', ''),
                               self.output / 'observer_queries.jsonl', 'observer')

    def checkpoint(self):
        return self.http('GET', '/health')['response']['checkpoint']

    def synced(self, before):
        # A capture already running when the DML committed may finish without it; require
        # a second round that began after `before` was read.
        return self.wait_ready(after=before + 1)

    def raw_stream(self, label):
        rows = self.observer.sql(
            f'SELECT id, payload, score, change$action, change$is_update, change$row_id '
            f'FROM {self.database}.pk_observer WITH CONSUME ORDER BY change$row_id, change$action')
        self.facts['stream'][label] = rows
        for row in rows:
            print(f'  stream {label}: {row}', flush=True)
        return rows

    def compare(self, sql):
        return self.native_compare(f'SELECT {sql}')

    def compare_all(self, label, keys):
        db = self.database
        whole = self.compare(f'id,payload,score,note FROM {db}.source_rows WHERE {RANGE_ALL}')
        health = self.http('GET', '/health')['response']
        self.check(f'{label}: replica row count', health['rows'], len(whole[0]))
        for key in sorted(set(keys)):
            self.compare(f'id,payload,score,note FROM {db}.source_rows WHERE id={key}')
        keys = sorted(set(keys))
        for a, b in zip(keys, keys[1:] + keys[:1]):
            self.compare(f'id,payload,score,note FROM {db}.source_rows WHERE id IN ({a},{b})')
        for score in sorted({int(r[2]) for r in whole[0]})[:6]:
            self.compare(f'id,payload FROM {db}.source_rows WHERE score={score}')

    def scenario(self, label, statements, keys, transaction=False):
        print(f'== {label}', flush=True)
        before = self.checkpoint()
        if transaction:
            self.source.sql('BEGIN')
        for sql in statements:
            self.source.sql(sql.format(db=self.database))
        if transaction:
            self.source.sql('COMMIT')
        self.synced(before)
        rows = self.raw_stream(label)
        self.compare_all(label, keys)
        return rows

    def run(self):
        error = None
        db = self.database
        try:
            self.source.sql(f'CREATE DATABASE {db}')
            self.source.sql(f'CREATE TABLE {db}.source_rows (id BIGINT NOT NULL,payload STRING NOT NULL,score BIGINT NOT NULL,note STRING NULL)')
            self.source.sql(f"INSERT INTO {db}.source_rows SELECT CAST(number+1 AS BIGINT),concat('p-',to_string(number+1)),CAST((number+1)*10 AS BIGINT),IF(number%2=0,NULL,'n') FROM numbers({ROWS})")
            self.source.sql(f'CREATE STREAM {db}.pk_observer ON TABLE {db}.source_rows APPEND_ONLY = false')
            command = [self.args.gateway_binary, '--endpoint', self.args.endpoint, '--user', self.args.user,
                       '--database', db, '--table', 'source_rows', '--output', str(self.output / 'service'),
                       '--port', '0', '--interval', '1', '--confirm-create']
            self.child = subprocess.Popen(command, stdout=(self.output / 'gateway_stdout.log').open('w'),
                                          stderr=(self.output / 'gateway_stderr.log').open('w'))
            deadline = time.monotonic() + 75
            while not (self.output / 'service/listening.json').exists():
                if self.child.poll() is not None:
                    raise RuntimeError('readyset-databend exited; inspect gateway_stderr.log')
                if time.monotonic() > deadline:
                    raise TimeoutError('bootstrap timeout')
                time.sleep(.1)
            self.origin = json.loads((self.output / 'service/listening.json').read_text())['origin']
            self.wait_ready()
            for name, columns, where in [
                    ('pk_by_id', 'id,payload,score,note', 'id=?'),
                    ('pk_by_ids', 'id,payload,score,note', 'id IN (?,?)'),
                    ('pk_by_score', 'id,payload', 'score=?'),
                    ('pk_range', 'id,payload,score,note', 'score>=? AND score<?')]:
                self.query(f'CREATE CACHE {name} FROM SELECT {columns} FROM {db}.source_rows WHERE {where}', route='readyset')
            self.compare_all('initial', range(1, ROWS + 1))

            rows = self.scenario('move id 1 -> 101', ['UPDATE {db}.source_rows SET id=101 WHERE id=1'], [1, 101])
            self.check('move: Stream reports DELETE old id + INSERT new id',
                       sorted((r[0], r[3]) for r in rows), [('1', 'DELETE'), ('101', 'INSERT')])
            self.check('move: both sides are one update of one row (same row_id)',
                       (len({r[5] for r in rows}), sorted({r[4] in ('1', 'true') for r in rows})), (1, [True]))
            self.scenario('swap ids 2 <-> 3 in one statement',
                          ['UPDATE {db}.source_rows SET id = CASE WHEN id=2 THEN 3 ELSE 2 END WHERE id IN (2,3)'], [2, 3])
            self.scenario('batch shift ids 4..8 -> 1004..1008',
                          ['UPDATE {db}.source_rows SET id=id+1000 WHERE id BETWEEN 4 AND 8'],
                          list(range(4, 9)) + list(range(1004, 1009)))
            # 4..8 were moved away above, so 8 is free: new ids 8..11 overlap old ids 9..12.
            self.scenario('overlapping chain 9..12 -> 8..11 (new ids collide with old ids)',
                          ['UPDATE {db}.source_rows SET id=id-1 WHERE id BETWEEN 9 AND 12'], range(8, 13))
            self.scenario('delete 14, move 15 -> 14 in one transaction',
                          ['DELETE FROM {db}.source_rows WHERE id=14',
                           'UPDATE {db}.source_rows SET id=14 WHERE id=15'], [14, 15], transaction=True)
            self.scenario('reuse a key deleted in an earlier batch (101 -> 1)',
                          ['UPDATE {db}.source_rows SET id=1 WHERE id=101'], [1, 101])
            rows = self.scenario('move 16 -> 116 and back in one transaction (net no-op)',
                                 ['UPDATE {db}.source_rows SET id=116 WHERE id=16',
                                  'UPDATE {db}.source_rows SET id=16 WHERE id=116'], [16, 116], transaction=True)
            self.check('move-and-back: Stream reports no change', rows, [])
            self.scenario('move 17 -> 117 and back with a new payload in one transaction',
                          ['UPDATE {db}.source_rows SET id=117 WHERE id=17',
                           "UPDATE {db}.source_rows SET id=17, payload='came-back' WHERE id=117"], [17, 117],
                          transaction=True)
            self.scenario('change id, payload and score together (18 -> 118)',
                          ["UPDATE {db}.source_rows SET id=118, payload='moved', score=999 WHERE id=18"], [18, 118])
            self.scenario('insert then move a new row (500 -> 501) in one transaction',
                          ["INSERT INTO {db}.source_rows VALUES (500,'new',5000,NULL)",
                           'UPDATE {db}.source_rows SET id=501 WHERE id=500'], [500, 501], transaction=True)

            # Databend does not enforce key uniqueness. The replica is a multiset like the
            # source table, so duplicate ids replicate and stay equal to Databend.
            self.scenario('duplicate id: move 19 -> 20 while 20 exists',
                          ['UPDATE {db}.source_rows SET id=20 WHERE id=19'], [19, 20])
            self.check('duplicate id: two rows for id 20 locally',
                       len(self.query(f'SELECT id,payload,score,note FROM {db}.source_rows WHERE id=20', route='readyset')[0]), 2)
            self.scenario('duplicate id: update both rows of id 20',
                          ['UPDATE {db}.source_rows SET score=score+1 WHERE id=20'], [20])
            self.scenario('duplicate id: delete one of the two rows of id 20',
                          ["DELETE FROM {db}.source_rows WHERE id=20 AND payload='p-19'"], [20])
            self.scenario('identical full rows inserted twice',
                          ["INSERT INTO {db}.source_rows VALUES (700,'twin',7,NULL),(700,'twin',7,NULL)"], [700])
            self.check('identical twins: two copies locally',
                       len(self.query(f'SELECT id,payload,score,note FROM {db}.source_rows WHERE id=700', route='readyset')[0]), 2)
            self.scenario('identical twins: update both', ['UPDATE {db}.source_rows SET score=8 WHERE id=700'], [700])
            self.scenario('identical twins: REPLACE INTO with the same values (no real change)',
                          ["REPLACE INTO {db}.source_rows ON (id) SELECT id, payload, score, note FROM {db}.source_rows WHERE id=17"], [17])
            self.scenario('identical twins: delete both', ['DELETE FROM {db}.source_rows WHERE id=700'], [700])
            health = self.http('GET', '/health')['response']
            self.check('capture keeps running with duplicate keys', (health['failed'], health['ready']), (None, True))
        except Exception as e:
            error = f'{type(e).__name__}: {e}'
            print('ERROR ' + error, flush=True)
        finally:
            if self.child and self.child.poll() is None:
                self.child.send_signal(signal.SIGINT)
                try:
                    self.child.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    self.child.kill()
                    self.child.wait(timeout=10)
            report = {'passed': error is None, 'error': error, 'checks': self.checks, 'facts': self.facts,
                      'database': self.database, 'raw_directory': str(self.output)}
            (self.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + '\n')
            (self.output / 'http_requests.jsonl').write_text(
                ''.join(json.dumps(r, ensure_ascii=False) + '\n' for r in self.requests))
            print(f'Checks: {sum(c["passed"] for c in self.checks)}/{len(self.checks)}', flush=True)
            print('Report: ' + str(self.output / 'report.json'), flush=True)
        return 1 if error else 0


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--user', default='root')
    p.add_argument('--gateway-binary', required=True, help='path to the readyset-databend binary')
    p.add_argument('--output', default='/tmp/readyset-databend-pk-change')
    p.add_argument('--confirm-create', action='store_true', required=True)
    args = p.parse_args()
    Path(args.output).mkdir(parents=True, exist_ok=True)
    return PkChangeVerification(args).run()


if __name__ == '__main__':
    sys.exit(main())
