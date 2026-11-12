#!/usr/bin/env python3
"""Loopback-only Databend native HTTP subset + explicit full-replica query routing POC.

Single configured principal, fixed four-column source schema, one table, 0..8 templates.
Uses NEW Stream/outbox/meta names in an explicitly authorized POC database. No restart recovery.
"""
import argparse
import base64
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from m0 import Client, NoRedirect
from m1_export import parse_row
from m2 import Engine, checksum, parse_change

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z_0-9]{0,127}\Z")
LIFECYCLE = re.compile(r"/v1/query/([A-Za-z_0-9-]+)(?:/(page/[0-9]+|final|kill))?\Z")
MAX_BODY = 1024 * 1024
MAX_RESPONSE = 16 * 1024 * 1024
MAX_LOCAL_QUERY_BYTES = 8 * 1024 * 1024
MAX_LOCAL_TOTAL_BYTES = 64 * 1024 * 1024
CONTROL_RESPONSE_RESERVE = 2 * 1024 * 1024


def load_declaration(path):
    d = json.loads(Path(path).read_text())
    if not isinstance(d, dict) or set(d) not in ({'database', 'table'}, {'database', 'table', 'queries'}):
        raise ValueError('declaration requires database, table and optional queries')
    d.setdefault('queries', [])
    if not all(isinstance(d[k], str) and IDENTIFIER.fullmatch(d[k]) for k in ('database', 'table')):
        raise ValueError('invalid source identifier')
    # This POC deliberately refuses to create change-tracking objects in business databases.
    if not d['database'].startswith('readyset_gateway_'):
        raise ValueError('POC source database must be a newly created readyset_gateway_* database')
    if not isinstance(d['queries'], list) or not 0 <= len(d['queries']) <= 8:
        raise ValueError('declare 0..8 templates')
    names = set()
    for q in d['queries']:
        if not isinstance(q, dict) or set(q) != {'name', 'sql'} or not isinstance(q['name'], str) or not IDENTIFIER.fullmatch(q['name']):
            raise ValueError('invalid query declaration')
        if q['name'].lower() in names or q['name'].lower() in {'m2_by_id', 'm2_source'} or not isinstance(q['sql'], str) or len(q['sql']) > 16384:
            raise ValueError('duplicate/reserved query name or invalid SQL')
        names.add(q['name'].lower())
    return d


def cache_control_sql(sql):
    """Reserve the cache-control namespace, NOT parse its SQL grammar.

    Skip comments between leading keywords so malformed/unsupported cache DDL is
    rejected locally rather than accidentally forwarded. Rust's existing ReadySet
    parser is the only authority for accepted statements and options.
    """
    words, pos = [], 0
    while len(words) < 3:
        skip = re.match(r'(?:\s+|--[^\n]*(?:\n|$)|\#[^\n]*(?:\n|$)|/\*[\s\S]*?\*/)*', sql[pos:])
        pos += skip.end()
        word = re.match(r'[A-Za-z_][A-Za-z_0-9]*', sql[pos:])
        if not word:
            break
        words.append(word.group().upper())
        pos += word.end()
    return (words[:2] in (['CREATE', 'CACHE'], ['DROP', 'CACHE'], ['SHOW', 'CACHES'])
            or (len(words) >= 3 and words[0] == 'CREATE' and words[1] in {'DEEP', 'SHALLOW'} and words[2] == 'CACHE')
            or words == ['DROP', 'ALL', 'CACHES']
            or (len(words) >= 3 and words[0] == 'SHOW' and words[1] in {'DEEP', 'SHALLOW'} and words[2] == 'CACHES'))


def safe_headers(headers):
    return not any(k.lower().startswith('x-databend-') and k.lower() not in {
        'x-databend-route-hint', 'x-databend-query-id', 'x-databend-sticky-node'} for k in headers)


def safe_session(body, default_role=None):
    # Never execute a local read under settings, role changes, transactions or unknown state.
    if set(body) - {'sql', 'session', 'pagination'}:
        return False
    s = body.get('session', {})
    if not isinstance(s, dict) or set(s) - {'database', 'catalog', 'settings', 'txn_state', 'need_sticky', 'need_keep_alive', 'role', 'internal'}:
        return False
    if 'role' in s and (default_role is None or s['role'] != default_role):
        return False
    if 'settings' in s and not isinstance(s['settings'], dict):
        return False
    if any(k in s and type(s[k]) is not bool for k in ('need_sticky', 'need_keep_alive')):
        return False
    if 'internal' in s:
        if not isinstance(s['internal'], str):
            return False
        try:
            internal = json.loads(s['internal'])
            if not isinstance(internal, dict) or set(internal) != {'last_node_id', 'last_query_ids'}:
                return False
            if not isinstance(internal['last_node_id'], str) or not isinstance(internal['last_query_ids'], list) or not all(isinstance(x, str) for x in internal['last_query_ids']):
                return False
        except (ValueError, TypeError):
            return False
    return (s.get('catalog', 'default') == 'default' and not s.get('settings')
            and s.get('txn_state', 'AutoCommit') == 'AutoCommit'
            and not s.get('need_sticky') and not s.get('need_keep_alive')
            and isinstance(s.get('database', 'default'), str)
            and isinstance(body.get('pagination', {}), dict)
            and not (set(body.get('pagination', {})) - {'wait_time_secs', 'max_rows_per_page', 'max_rows_in_buffer'})
            and all(type(v) is int and 0 < v <= 10000 for v in body.get('pagination', {}).values()))


class Replica:
    def __init__(self, args, declaration, output):
        self.args, self.d, self.output = args, declaration, output
        # Freeze one validated declaration for capture and engine; edits to the caller's
        # original file during bootstrap must not change which source a cache represents.
        self.declaration_path = output / 'declarations.json'
        self.declaration_path.write_text(json.dumps(declaration, indent=2) + '\n')
        self.generation = 'rs_' + uuid.uuid4().hex
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.ready = False
        self.failed = None
        self.uncertain = False
        self.last_sync = 0
        self.inflight = set()
        self.sequence = 0
        self.default_role = None
        self.engine = None
        self.worker = None
        self.local_results = {}
        self.counters = {'readyset': 0, 'databend': 0, 'empty_local': 0, 'sync_batches': 0, 'cache_ddl': 0}
        self.auth = 'Basic ' + base64.b64encode((args.user + ':' + os.environ.get('DATABEND_PASSWORD', '')).encode()).decode()
        self.opener = urllib.request.build_opener(NoRedirect())
        self.tx = self.client('capture')
        self.observer = self.client('observer')
        self.source = declaration['database'] + '.' + declaration['table']
        prefix = declaration['database'] + '.' + self.generation
        self.stream, self.outbox, self.meta = [prefix + suffix for suffix in ('_stream', '_outbox', '_meta')]

    def client(self, label):
        return Client(self.args.endpoint, self.args.user, os.environ.get('DATABEND_PASSWORD', ''),
                      self.output / 'source_queries.jsonl', label)

    def bootstrap(self):
        # Validate the actual Rust AST matcher before ANY source request or tracking DDL.
        validated = subprocess.run([str(Path(self.args.binary).resolve()), '--validate-only',
            '--declarations', str(self.declaration_path), '--input', str(self.output / 'unused'),
            '--output', str(self.output / 'unused_engine'), '--generation', self.generation],
            capture_output=True, timeout=30, text=True)
        if validated.returncode:
            raise ValueError('invalid declarations: ' + validated.stderr[-2048:])
        admin = self.client('bootstrap')
        # Check exact source schema before creating any tracking object.
        schema = admin.sql('DESCRIBE ' + self.source)
        self.default_role = admin.session.get('role')
        if [(r[0], r[1], r[2]) for r in schema] != [('id', 'BIGINT', 'NO'), ('payload', 'VARCHAR', 'NO'), ('score', 'BIGINT', 'NO'), ('note', 'VARCHAR', 'YES')]:
            raise ValueError('source schema must be id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL')
        admin.sql(f'CREATE STREAM {self.stream} ON TABLE {self.source} APPEND_ONLY = false')
        admin.sql(f'CREATE TABLE {self.outbox} (batch_id BIGINT NOT NULL, id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL, action STRING NOT NULL, row_id STRING NOT NULL, is_update BOOLEAN NOT NULL)')
        admin.sql(f'CREATE TABLE {self.meta} (generation STRING NOT NULL, batch_id BIGINT NOT NULL, row_count BIGINT NOT NULL, changes_sha256 STRING NOT NULL)')
        rows, last = [], None
        while True:
            where = '' if last is None else f' WHERE id > {last}'
            raw = admin.sql(f'SELECT id, payload, score, note FROM {self.source} AT (STREAM => {self.stream}){where} ORDER BY id LIMIT 500')
            if not raw:
                break
            for r in raw:
                row = parse_row(r)
                if last is not None and row['id'] <= last:
                    raise ValueError('source key is not unique')
                last = row['id']
                rows.append(row)
            if len(rows) > 9000:
                raise ValueError('gateway POC limited to 9000 initial rows')
        fixed_count = int(admin.sql(f'SELECT count(*) FROM {self.source} AT (STREAM => {self.stream})')[0][0])
        if len(rows) != fixed_count:
            raise ValueError('fixed snapshot row count mismatch; duplicate keys or truncated scan')
        if not rows:
            raise ValueError('gateway POC requires a nonempty initial table')
        self.state = {r['id']: r for r in rows}
        self.known = set(self.state)
        initial = self.output / 'initial.jsonl'
        initial.write_text(''.join(json.dumps(r, ensure_ascii=False) + '\n' for r in rows))
        self.engine = Engine(Path(self.args.binary).resolve(), initial, self.output / 'engine', self.generation,
                             self.output / 'engine_commands.jsonl', self.declaration_path)
        self.sync_once()
        self.worker = threading.Thread(target=self.sync_loop, name='databend-stream-sync', daemon=True)
        self.worker.start()

    def changes(self, client, sequence):
        raw = client.sql(f'SELECT id, payload, score, note, action, row_id, is_update FROM {self.outbox} WHERE batch_id = {sequence} ORDER BY id, 5, 6')
        changes = [parse_change(r) for r in raw]
        if len(changes) >= 10000:
            raise ValueError('batch exceeds bounded POC capacity')
        return changes

    def sync_once(self):
        with self.lock:
            self.ready = False
            capture_started = time.monotonic()
            n = self.sequence + 1
            self.tx.sql('BEGIN')
            try:
                self.tx.sql(f'INSERT INTO {self.outbox} SELECT {n}, id, payload, score, note, change$action, change$row_id, change$is_update FROM {self.stream}')
                changes = self.changes(self.tx, n)
                count = int(self.tx.sql(f'SELECT count(*) FROM {self.outbox} WHERE batch_id = {n}')[0][0])
                if count != len(changes):
                    raise ValueError('incomplete outbox capture')
                digest = checksum(changes)
                self.tx.sql(f"INSERT INTO {self.meta} VALUES ('{self.generation}', {n}, {count}, '{digest}')")
                if self.tx.session.get('txn_state') != 'Active':
                    raise RuntimeError('capture transaction not Active')
                self.tx.sql('COMMIT')
            except Exception as exc:
                if self.tx.session.get('txn_state') in ('Active', 'Fail'):
                    try:
                        self.tx.sql('ROLLBACK')
                    except Exception as cleanup:
                        exc.add_note(str(cleanup))
                raise
            changes = self.changes(self.observer, n)
            metadata = self.observer.sql(f'SELECT generation, batch_id, row_count, changes_sha256 FROM {self.meta} WHERE batch_id = {n}')
            if metadata != [[self.generation, str(n), str(len(changes)), checksum(changes)]]:
                raise ValueError('committed batch metadata mismatch')
            batch = dict(generation=self.generation, sequence=n, row_count=len(changes), changes_sha256=checksum(changes), changes=changes)
            new = dict(self.state)
            for c in changes:
                if c['action'] == 'DELETE':
                    if new.pop(c['row']['id'], None) != c['row']:
                        raise ValueError('before-image mismatch')
            for c in changes:
                if c['action'] == 'INSERT':
                    if c['row']['id'] in new:
                        raise ValueError('duplicate source key')
                    new[c['row']['id']] = c['row']
            if len(new) > 9000:
                raise ValueError('source exceeds POC capacity')
            result = self.engine.request({'op': 'apply', 'batch': batch})
            if not result.get('ok') or result.get('checkpoint') != n:
                raise RuntimeError('engine apply not acknowledged')
            self.known.update(new)
            if len(self.known) > 12000:
                raise ValueError('historical key limit reached')
            # A fixed snapshot plus committed batches is the oracle, NOT an unfenced scan
            # of a source table that could change concurrently with the barrier.
            result = self.engine.request({'op': 'barrier', 'rows': list(new.values()), 'missing': sorted(self.known - new.keys())})
            if not result.get('ok'):
                raise RuntimeError('reader barrier failed')
            self.state, self.sequence = new, n
            self.last_sync = capture_started
            self.ready = not self.inflight and not self.uncertain and time.monotonic() - capture_started <= self.args.max_staleness
            self.counters['sync_batches'] += 1
            (self.output / 'status.json').write_text(json.dumps(self.status(), indent=2) + '\n')

    def sync_loop(self):
        while not self.stop.wait(self.args.interval):
            try:
                self.sync_once()
            except Exception as exc:
                with self.lock:
                    self.ready = False
                    self.failed = f'{type(exc).__name__}: {exc}'
                (self.output / 'sync_failure.txt').write_text(self.failed + '\n')
                # Uncertain outcomes are not blindly retried. Upstream passthrough stays available.
                break

    def status(self):
        available = (self.ready and not self.failed and not self.uncertain and not self.inflight
                     and time.monotonic() - self.last_sync <= self.args.max_staleness)
        return {'generation': self.generation, 'checkpoint': self.sequence, 'ready': bool(available),
                'failed': self.failed, 'uncertain_upstream': self.uncertain, 'inflight': len(self.inflight),
                'rows': len(getattr(self, 'state', {})), 'counters': dict(self.counters),
                'engine_pid': self.engine.process.pid if self.engine else None}

    def forward(self, method, path, raw, headers):
        outgoing = {'Authorization': self.auth, 'Content-Type': 'application/json'}
        outgoing.update({k: v for k, v in headers.items() if k.lower().startswith('x-databend-')})
        request = urllib.request.Request(self.args.endpoint.rstrip('/') + path, data=raw, method=method, headers=outgoing)
        try:
            response = self.opener.open(request, timeout=15)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            content = response.read(MAX_RESPONSE + 1)
            if len(content) > MAX_RESPONSE:
                raise ValueError('upstream response too large; SQL not retried')
            response_headers = {k: v for k, v in response.headers.items() if k.lower().startswith('x-databend-') or k.lower() == 'content-type'}
            return response.code, content, response_headers

    def local_query_id(self, headers):
        qid = next((v for k, v in headers.items() if k.lower() == 'x-databend-query-id'), None) or 'rs_local_' + uuid.uuid4().hex
        if not re.fullmatch(r'[A-Za-z_0-9-]{1,128}', qid):
            raise ValueError('invalid query ID')
        if qid in self.local_results:
            raise ValueError('query ID already used locally')
        return qid

    def local_result_bytes(self):
        # Budget retained JSON payload, not a claim about Python's exact heap usage.
        return sum(entry.get('_bytes', 0) for _, entry in self.local_results.values() if entry)

    @staticmethod
    def local_page(stored, prefix, page=0):
        data = {k: v for k, v in stored.items() if not k.startswith('_')}
        rows, size = stored['_rows'], stored['_page_size']
        start = page * size
        data['data'] = rows[start:start + size]
        data['next_uri'] = prefix + '/page/' + str(page + 1) if start + size < len(rows) else prefix + '/final'
        return data

    def local_response(self, result, body, qid, now, control=False):
        prefix = '/v1/query/' + qid
        error = None if result.get('ok') else {'code': 1005, 'message': result.get('error', 'cache control failed')}
        rows = result.get('data', [])
        data = {'id': qid, 'session_id': '', 'node_id': '', 'state': 'Failed' if error else 'Succeeded',
                'session': body.get('session', {'database': 'default'}), 'error': error, 'warnings': [], 'has_result_set': True,
                'schema': result.get('schema', []), 'data': rows, 'affect': None, 'result_timeout_secs': 60,
                'stats': {'running_time_ms': 0, 'scan_progress': {'rows': len(rows), 'bytes': 0},
                          'write_progress': {'rows': 0, 'bytes': 0}, 'result_progress': {'rows': len(rows), 'bytes': 0}},
                'stats_uri': prefix, 'final_uri': prefix + '/final', 'next_uri': prefix + '/final', 'kill_uri': prefix + '/kill'}
        size = len(json.dumps(data, ensure_ascii=False).encode())
        if size > MAX_LOCAL_QUERY_BYTES or self.local_result_bytes() + size > MAX_LOCAL_TOTAL_BYTES:
            if control:
                raise RuntimeError('cache control response exceeded reserved result budget')
            return None  # No partial local success: safely proxy this read once instead.
        pagination = body.get('pagination', {})
        page_size = min(pagination.get('max_rows_per_page', 1000), pagination.get('max_rows_in_buffer', 1000))
        data.update(_rows=rows, _page_size=page_size, _bytes=size)
        self.local_results[qid] = (now + 60, data)
        page = self.local_page(data, prefix)
        if control:
            self.counters['cache_ddl'] = self.counters.get('cache_ddl', 0) + (error is None)
        else:
            self.counters['readyset'] += 1
            self.counters['empty_local'] += not bool(rows)
        return 200, json.dumps(page, ensure_ascii=False).encode(), {
            'X-ReadySet-Route': 'readyset', 'X-ReadySet-Template': 'cache-control' if control else result['template']}

    def handle(self, method, path, raw, headers):
        if path == '/health' and method == 'GET':
            with self.lock:
                return 200, json.dumps(self.status()).encode(), {'Content-Type': 'application/json'}
        start = path == '/v1/query' and method == 'POST'
        lifecycle = LIFECYCLE.fullmatch(path)
        if not start and not (lifecycle and (method == 'GET' or (method == 'POST' and lifecycle[2] == 'kill'))):
            return 404, b'{"error":"unsupported endpoint"}', {}
        body = None
        if start:
            body = json.loads(raw)
            if not isinstance(body, dict) or not isinstance(body.get('sql'), str) or not body['sql'].strip():
                return 400, b'{"error":"sql must be a nonempty string"}', {}
        with self.lock:
            now = time.monotonic()
            self.local_results = {k: v for k, v in self.local_results.items() if v[0] > now}
            if lifecycle and (lifecycle[1] in self.local_results or lifecycle[1].startswith('rs_local_')):
                result = self.local_results.get(lifecycle[1])
                if result is None or result[1] is None:
                    return 404, b'{"error":"local query expired or finalized"}', {}
                prefix = '/v1/query/' + lifecycle[1]
                page = 0
                if lifecycle[2] and lifecycle[2].startswith('page/'):
                    number = lifecycle[2].split('/')[1]
                    if len(number) > 10:
                        return 404, b'{"error":"local page does not exist"}', {'X-ReadySet-Route': 'readyset'}
                    page = int(number)
                    if number != str(page) or page < 1 or page * result[1]['_page_size'] >= len(result[1]['_rows']):
                        return 404, b'{"error":"local page does not exist"}', {'X-ReadySet-Route': 'readyset'}
                data = self.local_page(result[1], prefix, page)
                if lifecycle[2] in ('final', 'kill'):
                    self.local_results[lifecycle[1]] = (result[0], None)  # Tombstone: never forward finalized local IDs.
                    data['data'] = []
                    data['next_uri'] = None
                return 200, json.dumps(data, ensure_ascii=False).encode(), {'X-ReadySet-Route': 'readyset'}
            if start and cache_control_sql(body['sql']):
                # DDL never falls back to Databend, even when the replica/engine is unavailable.
                if (not safe_session(body, self.default_role) or not safe_headers(headers)
                        or body.get('session', {}).get('database', 'default') not in {'default', self.d['database']}):
                    return 400, b'{"error":"cache control requires a safe autocommit session and configured source"}', {'X-ReadySet-Route': 'readyset'}
                if (not self.ready or self.failed or self.inflight or self.uncertain
                        or now - self.last_sync > self.args.max_staleness or len(self.local_results) >= 2048
                        or self.local_result_bytes() > MAX_LOCAL_TOTAL_BYTES - CONTROL_RESPONSE_RESERVE):
                    return 503, b'{"error":"cache control requires a ready replica; statement was not forwarded"}', {'X-ReadySet-Route': 'readyset'}
                qid = self.local_query_id(headers)  # Validate BEFORE any mutation.
                try:
                    result = self.engine.request({'op': 'cache_ddl', 'sql': body['sql'],
                                                  'database': body.get('session', {}).get('database', 'default')})
                    if not result.get('ok') and not result.get('rejected'):
                        raise RuntimeError('uncertain cache mutation')
                    # Audit only. This file is not a crash-recovery catalog.
                    if result.get('ok'):
                        (self.output / 'cache_catalog.json').write_text(json.dumps(result, indent=2) + '\n')
                    return self.local_response(result, body, qid, time.monotonic(), control=True)
                except Exception as exc:
                    self.ready = False
                    self.failed = f'engine cache control failure: {exc}'
                    self.stop.set()
                    return 502, b'{"error":"cache control failed; no retry or upstream forwarding"}', {'X-ReadySet-Route': 'readyset'}
            eligible = (start and self.ready and not self.failed and not self.inflight and not self.uncertain
                        and now - self.last_sync <= self.args.max_staleness and safe_session(body, self.default_role)
                        and body.get('session', {}).get('database', 'default') in {'default', self.d['database']}
                        and safe_headers(headers)
                        and len(self.local_results) < 2048)
            if eligible:
                try:
                    result = self.engine.request({'op': 'query', 'sql': body['sql'], 'database': body.get('session', {}).get('database', 'default')})
                    if result.get('matched'):
                        try:
                            qid = self.local_query_id(headers)
                        except ValueError as exc:
                            status = 409 if str(exc) == 'query ID already used locally' else 400
                            return status, json.dumps({'error': str(exc)}).encode(), {}
                        response = self.local_response(result, body, qid, now)
                        if response is not None:
                            return response
                except Exception as exc:
                    self.ready = False
                    self.failed = f'engine query failure: {exc}'
                    self.stop.set()  # Do not send further RPCs after an uncertain engine result.
            # Serialize proxy submission and capture readiness changes; never resubmit SQL.
            if start:
                self.ready = False
            try:
                status, content, response_headers = self.forward(method, path, raw, headers)
                self.counters['databend'] += start
                if status < 300:
                    r = json.loads(content) if content else {}
                    qid = r.get('id')
                    if start and (not qid or r.get('state') not in ('Running', 'Starting', 'Succeeded', 'Failed')):
                        self.uncertain = True
                    if qid and r.get('state') in ('Running', 'Starting'):
                        if len(self.inflight) >= 2048:
                            self.uncertain = True
                        else:
                            self.inflight.add(qid)
                    elif qid and r.get('state') in ('Succeeded', 'Failed'):
                        self.inflight.discard(qid)
                        self.ready = False  # Need a post-completion capture, even if SQL was a write.
                    if lifecycle and lifecycle[2] == 'final':
                        # A final response without a terminal state cannot prove the write finished.
                        if lifecycle[1] in self.inflight:
                            self.uncertain = True
                elif start:
                    self.uncertain = True  # Conservative: do not infer nonexecution from HTTP status.
                response_headers['X-ReadySet-Route'] = 'databend'
                return status, content, response_headers
            except Exception:
                if start or self.inflight:
                    self.uncertain = True
                self.ready = False
                raise

    def close(self):
        self.stop.set()
        if self.worker:
            self.worker.join(timeout=60)
        with self.lock:
            if self.engine:
                try:
                    if self.engine.process.poll() is None:
                        self.engine.finish()
                finally:
                    self.engine.close()


class Server(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, address, replica):
        self.replica = replica
        self.slots = threading.BoundedSemaphore(16)
        super().__init__(address, Handler)

    def process_request(self, request, address):
        if not self.slots.acquire(blocking=False):
            request.close()
            return
        try:
            super().process_request(request, address)
        except Exception:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def setup(self):
        super().setup()
        self.connection.settimeout(20)

    def log_message(self, fmt, *args):
        pass  # Do not log credentials or user SQL to stderr.

    def respond(self, status, data, headers):
        self.send_response(status)
        self.send_header('Content-Type', headers.get('Content-Type', 'application/json'))
        for k, v in headers.items():
            if k.lower() != 'content-type':
                self.send_header(k, v)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    def dispatch(self):
        r = self.server.replica
        auth = self.headers.get('Authorization', '')
        if not hmac.compare_digest(auth.encode(), r.auth.encode()):
            self.respond(401, b'{"error":"authentication required"}', {'WWW-Authenticate': 'Basic realm="readyset-poc"'})
            return
        try:
            if self.headers.get('Transfer-Encoding') or len(self.headers.get_all('Content-Length', [])) > 1:
                raise ValueError('chunked or ambiguous request bodies unsupported')
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 <= size <= MAX_BODY:
                self.respond(413, b'{"error":"request too large"}', {})
                return
            raw = self.rfile.read(size) if self.command == 'POST' else None
            if raw is not None and len(raw) != size:
                raise ValueError('incomplete body')
            status, content, headers = r.handle(self.command, self.path, raw, self.headers)
            # Record route and option NAMES only; never persist authorization or session values.
            with (r.output / 'routes.jsonl').open('a') as journal:
                record = {'method': self.command, 'path': self.path, 'status': status, 'route': headers.get('X-ReadySet-Route'),
                          'databend_headers': [k for k in self.headers if k.lower().startswith('x-databend-')]}
                if self.command == 'POST' and self.path == '/v1/query':
                    payload = json.loads(raw)
                    record['body_keys'] = list(payload)
                    record['session_keys'] = list(payload.get('session') or {})
                journal.write(json.dumps(record) + '\n')
            self.respond(status, content, headers)
        except (ValueError, json.JSONDecodeError) as exc:
            self.respond(400, json.dumps({'error': str(exc)}).encode(), {})
        except Exception:
            self.respond(502, b'{"error":"gateway/upstream failure; SQL was not retried"}', {})

    do_GET = dispatch
    do_POST = dispatch


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--user', default='root')
    p.add_argument('--declarations', help='legacy source binding plus optional bootstrap queries')
    p.add_argument('--database', help='authorized readyset_gateway_* source database')
    p.add_argument('--table', help='source table; declare query caches later with CREATE CACHE SQL')
    p.add_argument('--output', required=True)
    p.add_argument('--binary', default='target/debug/databend-m2')
    p.add_argument('--port', type=int, default=8001)
    p.add_argument('--interval', type=float, default=1.0)
    p.add_argument('--max-staleness', type=float, default=10.0)
    p.add_argument('--confirm-create', action='store_true')
    args = p.parse_args()
    url = urllib.parse.urlsplit(args.endpoint)
    if url.scheme not in ('http', 'https') or not url.hostname or url.username or url.password or url.path not in ('', '/') or url.query or url.fragment:
        p.error('endpoint must be a confirmed HTTP(S) origin')
    if not args.confirm_create or not 0 < args.interval <= 60 or not 0 < args.max_staleness <= 60 or not 0 <= args.port <= 65535:
        p.error('requires --confirm-create, valid port and intervals in (0,60]')
    if args.declarations:
        if args.database or args.table:
            p.error('use --declarations OR --database/--table')
        declaration = load_declaration(args.declarations)
    else:
        if (not args.database or not args.table or not args.database.startswith('readyset_gateway_')
                or not IDENTIFIER.fullmatch(args.database) or not IDENTIFIER.fullmatch(args.table)):
            p.error('requires --database readyset_gateway_* --table <identifier> or --declarations')
        declaration = {'database': args.database, 'table': args.table, 'queries': []}
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    replica = Replica(args, declaration, output)
    server = None
    try:
        replica.bootstrap()
        server = Server(('127.0.0.1', args.port), replica)
        info = {'origin': 'http://127.0.0.1:' + str(server.server_port), 'database': declaration['database'],
                'generation': replica.generation, 'stream': replica.stream, 'outbox': replica.outbox, 'meta': replica.meta}
        (output / 'listening.json').write_text(json.dumps(info, indent=2) + '\n')
        print(json.dumps(info), flush=True)
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        if server:
            server.server_close()
        replica.close()
        (output / 'final_status.json').write_text(json.dumps(replica.status(), indent=2) + '\n')


if __name__ == '__main__':
    main()
