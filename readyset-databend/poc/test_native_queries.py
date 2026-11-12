"""Offline routing/page/budget tests. Native SQL rewrite is exercised by remote Rust tests."""
import json
import time
import unittest
from unittest.mock import Mock, patch

import gateway
from test_gateway import replica


class NativeResultTest(unittest.TestCase):
    def result(self, rows):
        r = replica()
        r.engine.request = Mock(return_value={'ok': True, 'matched': True, 'template': 'by_score',
                                             'schema': [{'name': 'payload', 'type': 'String'}], 'data': rows})
        return r

    def post(self, r, **options):
        status, raw, headers = r.handle('POST', '/v1/query', json.dumps({'sql': 'select x', **options}).encode(), {})
        return status, json.loads(raw), headers

    def get(self, r, path):
        status, raw, headers = r.handle('GET', path, None, {})
        return status, json.loads(raw), headers

    def test_local_pages_preserve_duplicate_rows_and_snapshot(self):
        rows = [['a'], ['a'], ['b'], [None], ['中文']]
        r = self.result(rows)
        status, first, headers = self.post(r, pagination={'max_rows_per_page': 3, 'max_rows_in_buffer': 2})
        self.assertEqual(status, 200)
        self.assertEqual(headers['X-ReadySet-Route'], 'readyset')
        self.assertEqual(first['data'], rows[:2])
        self.assertFalse(any(k.startswith('_') for k in first))
        r.ready = False  # Existing result pages do not re-query an engine or source.
        _, second, _ = self.get(r, first['next_uri'])
        _, repeated, _ = self.get(r, first['next_uri'])
        self.assertEqual(second, repeated)
        _, third, _ = self.get(r, second['next_uri'])
        self.assertEqual(first['data'] + second['data'] + third['data'], rows)
        self.assertEqual(self.get(r, third['next_uri'])[0], 200)
        self.assertEqual(self.get(r, first['stats_uri'])[0], 404)
        self.assertEqual(r.local_result_bytes(), 0)
        r.engine.request.assert_called_once()
        self.assertEqual(r.forwarded, [])

    def test_invalid_pages_are_local_and_do_not_finalize(self):
        r = self.result([['a'], ['b'], ['c']])
        _, first, _ = self.post(r, pagination={'max_rows_per_page': 1})
        prefix = '/v1/query/' + first['id']
        for page in ['0', '01', '3', '999999999999999999999']:
            self.assertEqual(self.get(r, prefix + '/page/' + page)[0], 404)
        self.assertEqual(self.get(r, first['next_uri'])[1]['data'], [['b']])
        self.assertEqual(r.forwarded, [])

    def test_kill_releases_retained_result_without_forwarding(self):
        r = self.result([['a'], ['b']])
        _, first, _ = self.post(r, pagination={'max_rows_per_page': 1})
        self.assertGreater(r.local_result_bytes(), 0)
        self.assertEqual(self.get(r, first['kill_uri'])[1]['data'], [])
        self.assertEqual(self.get(r, first['next_uri'])[0], 404)
        self.assertEqual(r.local_result_bytes(), 0)
        self.assertEqual(r.forwarded, [])

    def test_expiry_releases_result_budget(self):
        r = self.result([['a']])
        _, first, _ = self.post(r)
        expiry, stored = r.local_results[first['id']]
        r.local_results[first['id']] = (time.monotonic() - 1, stored)
        self.assertEqual(self.get(r, first['stats_uri'])[0], 404)
        self.assertEqual(r.local_result_bytes(), 0)
        self.assertEqual(r.forwarded, [])

    def test_oversized_read_falls_back_once_without_truncation_or_disabling_engine(self):
        for limit in ['MAX_LOCAL_QUERY_BYTES', 'MAX_LOCAL_TOTAL_BYTES']:
            r = self.result([['a' * 200]])
            with patch.object(gateway, limit, 1):
                _, first, headers = self.post(r)
            self.assertEqual(headers['X-ReadySet-Route'], 'databend')
            self.assertEqual(first['data'], [['original']])
            self.assertEqual(len(r.forwarded), 1)
            self.assertEqual(r.local_results, {})
            self.assertEqual(r.counters['readyset'], 0)
            self.assertIsNone(r.failed)

    def test_control_reserves_budget_before_mutation_and_never_forwards(self):
        r = self.result([])
        r.local_results['old'] = (float('inf'), {'_bytes': gateway.MAX_LOCAL_TOTAL_BYTES})
        status, data, headers = self.post(r, sql='SHOW CACHES')
        self.assertEqual(status, 503)
        r.engine.request.assert_not_called()
        self.assertEqual(r.forwarded, [])


if __name__ == '__main__':
    unittest.main()
