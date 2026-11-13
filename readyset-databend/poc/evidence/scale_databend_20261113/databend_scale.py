#!/usr/bin/env python3
"""Databend-side scale probe for Stream-based sync (10M rows). NEW POC database only.

Measures: change-computation cost of large UPDATEs (non-consuming count vs full consuming
read in one transaction), whether a LIMIT/count consuming read advances the whole Stream
(cheap advance), idle transaction survival, timeout settings, and how lakehouse-style writes
(INSERT OVERWRITE, REPLACE INTO, MERGE INTO, DELETE, TRUNCATE, compact, recluster) appear in
a Stream. Results: <out>/results.json, progress on stdout.
"""
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

ENDPOINT = os.environ.get('DATABEND_ENDPOINT', 'http://192.168.191.96:8000')
USER = os.environ.get('DATABEND_USER', 'root')
ROWS = int(os.environ.get('ROWS', '10000000'))
IDLE = int(os.environ.get('IDLE_SECS', '300'))
TS = datetime.now().strftime('%Y%m%d_%H%M%S')
DB = f'readyset_gateway_scale_{TS}'
OUT = f'/data1/vmbox/readyset-scale-results-{TS}'
COLS = 'id, payload, score, note, change$action, change$row_id, change$is_update'


class Db:
    def __init__(self):
        self.auth = 'Basic ' + base64.b64encode(f"{USER}:{os.environ.get('DATABEND_PASSWORD', '')}".encode()).decode()
        self.session = {'database': 'default'}
        self.node = None

    def http(self, path, body=None):
        headers = {'Authorization': self.auth, 'Content-Type': 'application/json'}
        if self.node and self.session.get('need_sticky'):
            headers['X-DATABEND-STICKY-NODE'] = self.node
        req = urllib.request.Request(ENDPOINT + path, data=None if body is None else json.dumps(body).encode(),
                                     headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=600) as r:
                raw = r.read()
        except urllib.error.HTTPError as e:
            raw = e.read()
        return len(raw), (json.loads(raw) if raw else {})

    def run(self, sql, page_rows=100000, keep=20):
        t0 = time.monotonic()
        nbytes, r = self.http('/v1/query', {'sql': sql, 'session': self.session,
                                            'pagination': {'wait_time_secs': 5, 'max_rows_per_page': page_rows}})
        rows, sample, first, pages = 0, [], None, 0
        while True:
            if r.get('session'):
                self.session = r['session']
            if r.get('node_id'):
                self.node = r['node_id']
            if r.get('error'):
                raise RuntimeError(json.dumps(r['error'], ensure_ascii=False)[:600])
            data = r.get('data') or []
            if data and first is None:
                first = time.monotonic() - t0
            rows += len(data)
            pages += 1 if data else 0
            sample.extend(data[:max(0, keep - len(sample))])
            uri = r.get('next_uri')
            if not uri:
                break
            if uri.endswith('/final'):
                self.http(uri)
                break
            n, r = self.http(uri)
            nbytes += n
        return {'rows': rows, 'secs': round(time.monotonic() - t0, 3), 'first_row_secs': None if first is None else round(first, 3),
                'pages': pages, 'bytes': nbytes, 'txn_state': self.session.get('txn_state'), 'sample': sample}


results = {'database': DB, 'rows': ROWS, 'endpoint': ENDPOINT, 'started': TS, 'steps': []}


def save():
    with open(f'{OUT}/results.json', 'w') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)


def step(db, name, sql, **kw):
    print(f'[{datetime.now():%H:%M:%S}] {name}: {sql[:150]}', flush=True)
    rec = {'name': name, 'sql': sql}
    try:
        rec.update(db.run(sql, **kw))
        print(f'    ok rows={rec["rows"]} secs={rec["secs"]} first_row={rec["first_row_secs"]} bytes={rec["bytes"]}'
              + (f' sample={rec["sample"][:6]}' if rec['rows'] <= 12 else ''), flush=True)
    except Exception as e:
        rec['error'] = str(e)
        print(f'    ERROR {e}', flush=True)
    results['steps'].append(rec)
    save()
    return rec


def has_data(db, stream):
    return step(db, f'has_data {stream}', f"SELECT has_data, table_version FROM system.streams WHERE database='{DB}' AND name='{stream}'")


def stream_summary(db, label, stream):
    return step(db, f'{label}: summary (non-consuming)',
                f'SELECT change$action, change$is_update, count(*) FROM {DB}.{stream} GROUP BY 1, 2 ORDER BY 1, 2')


def full_consume(db, label, stream):
    """Whole Stream through one transaction: BEGIN; consuming read of every row; COMMIT."""
    t0 = time.monotonic()
    step(db, f'{label}: BEGIN', 'BEGIN')
    read = step(db, f'{label}: consuming read (all rows, client discards)', f'SELECT {COLS} FROM {DB}.{stream} WITH CONSUME', keep=0)
    commit = step(db, f'{label}: COMMIT', 'COMMIT')
    results['steps'].append({'name': f'{label}: transaction total', 'secs': round(time.monotonic() - t0, 3),
                             'read_rows': read.get('rows'), 'commit_error': commit.get('error')})
    save()
    has_data(db, stream)


def main():
    os.makedirs(OUT, exist_ok=False)
    print(f'database={DB} results={OUT}/results.json', flush=True)
    db = Db()
    step(db, 'version', 'SELECT version()')
    step(db, 'timeout/txn settings', "SELECT name, value, default FROM system.settings WHERE name LIKE '%timeout%' OR name LIKE '%txn%' OR name LIKE '%transaction%' ORDER BY name", keep=100)
    step(db, 'create database', f'CREATE DATABASE {DB}')
    step(db, f'load {ROWS} rows', f"CREATE TABLE {DB}.t (id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL)")
    step(db, f'insert {ROWS} rows', f"INSERT INTO {DB}.t SELECT CAST(number AS BIGINT), concat('p-', to_string(number)), CAST(number % 1000 AS BIGINT), IF(number % 2 = 0, NULL, 'n') FROM numbers({ROWS})")
    step(db, 'table stats', f"SELECT count(*) FROM {DB}.t")
    step(db, 'create stream on 10M table', f'CREATE STREAM {DB}.s ON TABLE {DB}.t APPEND_ONLY = false')
    has_data(db, 's')

    # 1. Change-computation cost of large updates.
    for label, where, n in [('U1M', 'WHERE id % 10 = 0', ROWS // 10), ('U10M', '', ROWS)]:
        step(db, f'{label}: UPDATE {n} rows', f'UPDATE {DB}.t SET score = score + 1 {where}')
        has_data(db, 's')
        stream_summary(db, label, 's')
        stream_summary(db, label + ' (repeat, warm)', 's')
        full_consume(db, label, 's')

    # 2. Cheap advance: does a LIMIT / count consuming read advance past everything?
    for label, sql in [('ADV-limit1', f'SELECT id FROM {DB}.s WITH CONSUME LIMIT 1'),
                       ('ADV-where-false', f'SELECT id FROM {DB}.s WITH CONSUME WHERE 1 = 0'),
                       ('ADV-count', f'SELECT count(*) FROM {DB}.s WITH CONSUME')]:
        step(db, f'{label}: UPDATE {ROWS} rows', f'UPDATE {DB}.t SET score = score + 1')
        step(db, f'{label}: autocommit consuming read', sql)
        has_data(db, 's')
        stream_summary(db, label + ' after', 's')

    # 3. Idle transaction survival.
    step(db, f'U-small for idle test', f'UPDATE {DB}.t SET score = score + 1 WHERE id < 10')
    step(db, f'IDLE: BEGIN', 'BEGIN')
    step(db, f'IDLE: consuming read', f'SELECT {COLS} FROM {DB}.s WITH CONSUME')
    print(f'    idle {IDLE}s inside transaction', flush=True)
    time.sleep(IDLE)
    step(db, f'IDLE: COMMIT after {IDLE}s idle', 'COMMIT')
    has_data(db, 's')
    db.session.pop('txn_state', None)

    # 4. Lakehouse-style writes, each on a fresh 10M copy with its own Stream.
    ops = [
        ('L-overwrite-same', 'INSERT OVERWRITE {c} SELECT * FROM {c}', 'real change: 0 rows'),
        ('L-overwrite-1pct', 'INSERT OVERWRITE {c} SELECT id, payload, IF(id % 100 = 0, score + 1, score), note FROM {c}', 'real change: 1% of rows updated'),
        ('L-replace-1M-half-same', 'REPLACE INTO {c} ON (id) SELECT id, payload, IF(id % 2 = 0, score + 1, score), note FROM {c} WHERE id < 1000000', 'real change: 500k updated, 500k identical'),
        ('L-merge-500k-upd-500k-ins', 'MERGE INTO {c} USING (SELECT CAST(number + %d AS BIGINT) AS id, \'m\' AS payload, CAST(-1 AS BIGINT) AS score, CAST(NULL AS STRING) AS note FROM numbers(1000000)) AS s ON {c}.id = s.id WHEN MATCHED THEN UPDATE SET score = s.score WHEN NOT MATCHED THEN INSERT *' % (ROWS - 500000), 'real change: 500k updated, 500k inserted'),
        ('L-delete-1M', 'DELETE FROM {c} WHERE id % 10 = 1', 'real change: 1M deleted'),
        ('L-truncate', 'TRUNCATE TABLE {c}', 'real change: all rows deleted'),
        ('L-recluster', 'ALTER TABLE {c} CLUSTER BY (score)', 'real change: 0 rows (followed by RECLUSTER FINAL)'),
    ]
    for label, op, truth in ops:
        c = f'{DB}.c_{label.lower().replace("-", "_")}'
        step(db, f'{label}: copy 10M', f'CREATE TABLE {c} AS SELECT * FROM {DB}.t')
        step(db, f'{label}: stream', f'CREATE STREAM {c}_s ON TABLE {c} APPEND_ONLY = false')
        step(db, f'{label}: op ({truth})', op.format(c=c))
        if label == 'L-recluster':
            step(db, f'{label}: RECLUSTER FINAL', f'ALTER TABLE {c} RECLUSTER FINAL')
        has_data(db, f'c_{label.lower().replace("-", "_")}_s')
        stream_summary(db, label, f'c_{label.lower().replace("-", "_")}_s')

    # 5. Compaction of a fragmented table (many small inserts).
    f = f'{DB}.frag'
    step(db, 'COMPACT: create', f'CREATE TABLE {f} (id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL)')
    for i in range(200):
        db.run(f"INSERT INTO {f} SELECT CAST(number + {i * 5000} AS BIGINT), 'f', CAST(number % 7 AS BIGINT), NULL FROM numbers(5000)")
    step(db, 'COMPACT: blocks before', f"SELECT block_count, row_count FROM fuse_snapshot('{DB}', 'frag') LIMIT 1")
    step(db, 'COMPACT: stream', f'CREATE STREAM {f}_s ON TABLE {f} APPEND_ONLY = false')
    step(db, 'COMPACT: OPTIMIZE TABLE COMPACT', f'OPTIMIZE TABLE {f} COMPACT')
    step(db, 'COMPACT: blocks after', f"SELECT block_count, row_count FROM fuse_snapshot('{DB}', 'frag') LIMIT 1")
    has_data(db, 'frag_s')
    stream_summary(db, 'COMPACT (real change: 0)', 'frag_s')
    results['finished'] = datetime.now().strftime('%Y%m%d_%H%M%S')
    save()
    print('DONE', flush=True)


if __name__ == '__main__':
    sys.exit(main())
