"""HTTP control-plane routing tests; SQL semantics are checked by the Rust parser."""
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock

from gateway import cache_control_sql, load_declaration
from test_gateway import replica


class CacheControlRouterTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.r = replica()
        self.r.output = Path(self.tmp.name)
        self.r.stop = threading.Event()
        self.r.engine.request = Mock(return_value={
            'ok': True, 'schema': [{'name': 'name', 'type': 'String'}],
            'data': [['by_id']], 'caches': ['by_id']})

    def post(self, sql, **kwargs):
        return self.r.handle('POST', '/v1/query', json.dumps({'sql': sql, **kwargs}).encode(), {})

    def test_control_namespace_including_comments_and_unsupported_variants(self):
        for sql in ['CREATE CACHE c FROM SELECT 1', 'create deep cache c FROM SELECT 1',
                    'CREATE SHALLOW CACHE c FROM SELECT 1', 'DROP CACHE c', 'SHOW CACHES',
                    'SHOW DEEP CACHES', 'SHOW SHALLOW CACHES', 'DROP ALL CACHES',
                    '-- comment\n CREATE /* comment */ CACHE c FROM SELECT 1',
                    '# comment\n SHOW /* comment */ CACHES']:
            self.assertTrue(cache_control_sql(sql), sql)
        for sql in ['CREATE TABLE t (id INT)', 'SELECT \'CREATE CACHE c\'', 'SHOW TABLES',
                    'DROP TABLE t', 'SELECT * FROM caches', 'CREATE CACHED_TABLE t']:
            self.assertFalse(cache_control_sql(sql), sql)

    def test_create_show_drop_are_local_with_local_lifecycle(self):
        for sql in ['CREATE CACHE by_id FROM SELECT id FROM poc.users WHERE id=?',
                    'SHOW CACHES', 'DROP CACHE by_id']:
            status, raw, headers = self.post(sql)
            data = json.loads(raw)
            self.assertEqual(status, 200)
            self.assertEqual(headers['X-ReadySet-Route'], 'readyset')
            self.assertEqual(data['state'], 'Succeeded')
            self.assertEqual(self.r.engine.request.call_args.args[0]['op'], 'cache_ddl')
            self.assertEqual(self.r.engine.request.call_args.args[0]['sql'], sql)
            self.assertEqual(self.r.handle('GET', data['final_uri'], None, {})[0], 200)
            self.assertEqual(self.r.handle('GET', data['final_uri'], None, {})[0], 404)
        self.assertEqual(self.r.forwarded, [])
        self.assertEqual(self.r.counters['cache_ddl'], 3)
        self.assertTrue(self.r.ready)
        self.assertEqual(json.loads((self.r.output / 'cache_catalog.json').read_text())['caches'], ['by_id'])

    def test_preflight_rejection_keeps_existing_cache_available(self):
        self.r.engine.request.return_value = {'ok': False, 'rejected': True, 'error': 'duplicate cache'}
        status, raw, _ = self.post('CREATE CACHE by_id FROM SELECT id FROM poc.users WHERE id=?')
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(raw)['state'], 'Failed')
        self.assertIn('duplicate', json.loads(raw)['error']['message'])
        self.assertTrue(self.r.ready)
        self.assertFalse(self.r.stop.is_set())
        self.assertEqual(self.r.forwarded, [])
        self.assertFalse((self.r.output / 'cache_catalog.json').exists())

    def test_unsafe_sessions_and_headers_reject_without_engine_or_source(self):
        for session in [{'txn_state': 'Active'}, {'settings': {'max_threads': '1'}},
                        {'database': 'other'}, {'role': 'other'}, {'catalog': 'other'}, None]:
            self.assertEqual(self.post('SHOW CACHES', session=session)[0], 400)
        for header in ['X-Databend-Tenant', 'X-Databend-Warehouse', 'X-Databend-Unknown']:
            raw = b'{"sql":"SHOW CACHES"}'
            self.assertEqual(self.r.handle('POST', '/v1/query', raw, {header: 'x'})[0], 400)
        self.r.engine.request.assert_not_called()
        self.assertEqual(self.r.forwarded, [])

    def test_invalid_or_duplicate_query_id_is_checked_before_mutation(self):
        for qid in ['../bad', 'a' * 129, 'existing']:
            self.r.local_results['existing'] = (float('inf'), None)
            with self.assertRaises(ValueError):
                self.r.handle('POST', '/v1/query', b'{"sql":"DROP CACHE by_id"}', {'X-Databend-Query-ID': qid})
        self.r.engine.request.assert_not_called()
        self.assertEqual(self.r.forwarded, [])

    def test_unavailable_and_overloaded_control_is_never_forwarded(self):
        for mode in ['unready', 'failed', 'inflight', 'uncertain', 'stale', 'full']:
            r = replica()
            if mode == 'unready': r.ready = False
            if mode == 'failed': r.failed = 'dead'
            if mode == 'inflight': r.inflight.add('q')
            if mode == 'uncertain': r.uncertain = True
            if mode == 'stale': r.last_sync -= 20
            if mode == 'full': r.local_results = {str(i): (float('inf'), None) for i in range(2048)}
            for sql in ['CREATE CACHE c FROM SELECT 1', 'SHOW CACHES', 'DROP CACHE c']:
                self.assertEqual(r.handle('POST', '/v1/query', json.dumps({'sql': sql}).encode(), {})[0], 503, mode)
            self.assertEqual(r.engine.calls, [])
            self.assertEqual(r.forwarded, [])

    def test_uncertain_migration_fails_closed_and_is_not_retried(self):
        self.r.engine.request.side_effect = TimeoutError('unknown migration outcome')
        self.assertEqual(self.post('CREATE CACHE c FROM SELECT id FROM poc.users WHERE id=?')[0], 502)
        self.assertFalse(self.r.ready)
        self.assertTrue(self.r.failed)
        self.assertTrue(self.r.stop.is_set())
        self.assertEqual(self.post('SHOW CACHES')[0], 503)
        self.r.engine.request.assert_called_once()
        self.assertEqual(self.r.forwarded, [])

    def test_read_query_id_rejection_does_not_break_existing_replica(self):
        r = replica()
        r.local_results['existing'] = (float('inf'), None)
        for qid, expected in [('../bad', 400), ('existing', 409)]:
            status, _, _ = r.handle('POST', '/v1/query', b'{"sql":"select x"}', {'X-Databend-Query-ID': qid})
            self.assertEqual(status, expected)
            self.assertTrue(r.ready)
            self.assertIsNone(r.failed)
        self.assertEqual(r.forwarded, [])

    def test_source_binding_can_start_with_no_queries(self):
        config = self.r.output / 'source.json'
        config.write_text(json.dumps({'database': 'readyset_gateway_sql', 'table': 'users'}))
        self.assertEqual(load_declaration(config)['queries'], [])


if __name__ == '__main__':
    unittest.main()
