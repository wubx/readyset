"""Offline tests for the experiment's bounded HTTP/session handling; no database access."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from m0 import Client, SqlError


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.journal = Path(self.directory.name) / "queries.jsonl"
        self.client = Client("http://127.0.0.1:8000", "root", "test-secret", self.journal, "test")

    def test_pagination_and_transaction_session(self):
        self.client.http = Mock(side_effect=[
            {"id": "q1", "state": "Running", "data": [],
             "session": {"txn_state": "Active", "need_sticky": True}, "node_id": "node1",
             "next_uri": "/v1/query/q1/page/0"},
            {"id": "q1", "state": "Succeeded", "schema": [{"name": "id", "type": "Int64"}],
             "data": [["1"]], "next_uri": "/v1/query/q1/final"},
            {},
        ])
        self.assertEqual(self.client.sql("SELECT 1"), [["1"]])
        self.assertEqual(self.client.session["txn_state"], "Active")
        self.assertEqual(self.client.node, "node1")
        self.assertEqual(self.client.http.call_count, 3)
        # Session must be carried to the next statement rather than starting a fresh transaction.
        self.client.http = Mock(return_value={"id": "q2", "state": "Succeeded", "data": []})
        self.client.sql("COMMIT")
        self.assertEqual(self.client.http.call_args_list[0].args[1]["session"]["txn_state"], "Active")
        self.assertNotIn("test-secret", self.journal.read_text())
        self.assertNotIn("Authorization", self.journal.read_text())

    def test_sql_error_not_retried_and_fail_session_retained(self):
        self.client.http = Mock(side_effect=[
            {"id": "q1", "state": "Failed", "session": {"txn_state": "Fail"},
             "error": {"code": 1006, "message": "conversion failed"}},
            {},
        ])
        with self.assertRaises(SqlError):
            self.client.sql("INSERT INTO batches SELECT ...")
        self.assertEqual(self.client.session["txn_state"], "Fail")
        self.assertEqual(self.client.http.call_count, 2)
        self.assertEqual(self.client.http.call_args_list[1].args, ("/v1/query/q1/final",))
        self.assertEqual(json.loads(self.journal.read_text())["txn_state"], "Fail")

    def test_transport_error_never_resubmits_sql(self):
        self.client.http = Mock(side_effect=TimeoutError("response lost"))
        with self.assertRaises(TimeoutError):
            self.client.sql("COMMIT")
        self.assertEqual(self.client.http.call_count, 1)
        self.assertIn("TimeoutError", self.journal.read_text())

    def test_foreign_continuation_rejected(self):
        self.client.http = Mock(side_effect=[
            {"id": "q1", "state": "Running", "next_uri": "http://other/v1/query/q1/page/0"},
            {}, {},
        ])
        with self.assertRaisesRegex(RuntimeError, "Unexpected continuation"):
            self.client.sql("SELECT 1")
        self.assertEqual([c.args[0] for c in self.client.http.call_args_list],
                         ["/v1/query", "/v1/query/q1/kill", "/v1/query/q1/final"])

    def test_incomplete_response_is_not_success(self):
        self.client.http = Mock(side_effect=[{"id": "q1", "state": "Running"}, {}, {}])
        with self.assertRaisesRegex(RuntimeError, "Incomplete query"):
            self.client.sql("INSERT INTO batches SELECT ...")


if __name__ == "__main__":
    unittest.main()
