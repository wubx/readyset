from pathlib import Path
import tempfile
import unittest

from m2 import M2, checksum, parse_change


class ChangesTest(unittest.TestCase):
    def test_utf8_int64_checksum_contract(self):
        changes = [{"action": "INSERT", "row_id": "r1", "is_update": False,
                    "row": {"id": 1, "payload": "中文", "score": -9223372036854775808, "note": None}}]
        self.assertEqual(checksum(changes), "87c722b5903e3437bb527d8de03464a79227340251c5dd3d56985e3c4f439c1b")

    def test_change_parse_preserves_metadata_and_values(self):
        row = parse_change(["42", "中文", "-9223372036854775808", "", "DELETE", "r1", "true"])
        self.assertEqual(row['action'], 'DELETE')
        self.assertTrue(row['is_update'])
        self.assertEqual(row['row']['note'], '')
        self.assertEqual(row['row']['score'], -9223372036854775808)
        self.assertFalse(parse_change(["1", "", "0", None, "INSERT", "r2", "0"])['is_update'])

    def test_unknown_action_and_boolean_fail_closed(self):
        for action, boolean in [('UPSERT', 'true'), ('INSERT', 'unknown')]:
            with self.assertRaises(ValueError):
                parse_change(['1', '', '0', None, action, 'r1', boolean])


class FakeClient:
    def __init__(self):
        self.session = {'txn_state': 'Active'}
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith('SELECT count'):
            return [['0']]
        if statement == 'BEGIN':
            self.session['txn_state'] = 'Active'
            return []
        if statement.startswith('INSERT INTO') and '.outbox' in statement:
            self.session['txn_state'] = 'Fail'
            raise RuntimeError('original capture failure')
        if statement == 'ROLLBACK':
            raise RuntimeError('cleanup failure')
        raise AssertionError(statement)


class CaptureTest(unittest.TestCase):
    def test_failed_rollback_does_not_hide_first_sql_failure_or_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = M2.__new__(M2)
            runner.database = 'isolated'
            runner.output = Path(directory)
            runner.checks = []
            runner.tx = FakeClient()
            runner.observer = FakeClient()
            with self.assertRaisesRegex(RuntimeError, 'original capture failure') as captured:
                runner.capture(1)
            self.assertIn('cleanup failure', captured.exception.__notes__[0])
            self.assertNotIn('COMMIT', runner.tx.statements)

    def test_transaction_outbox_count_mismatch_prevents_commit(self):
        runner = M2.__new__(M2)
        runner.database = 'isolated'
        runner.checks = []
        runner.tx = FakeClient()
        runner.observer = FakeClient()
        def sql(statement):
            runner.tx.statements.append(statement)
            if statement.startswith('SELECT count'):
                return [['2']]
            return []
        runner.tx.sql = sql
        runner.read_changes = lambda client, sequence=None: []
        with self.assertRaisesRegex(AssertionError, 'complete outbox'):
            runner.capture(1)
        self.assertIn('ROLLBACK', runner.tx.statements)
        self.assertNotIn('COMMIT', runner.tx.statements)


if __name__ == '__main__':
    unittest.main()
