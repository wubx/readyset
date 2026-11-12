import unittest
from m1_export import parse_row, summarize


class ExportTests(unittest.TestCase):
    def test_bigint_precision_and_null(self):
        self.assertEqual(parse_row(["9223372036854775807", "中文é", "-9223372036854775808", None]),
                         {"id": 9223372036854775807, "payload": "中文é", "score": -9223372036854775808, "note": None})

    def test_empty_string_distinct_from_null(self):
        self.assertEqual(parse_row(["7", "", "0", ""])["note"], "")
        self.assertIsNone(parse_row(["7", "", "0", None])["note"])

    def test_invalid_source_values_rejected(self):
        for raw in (["1", None, "0", None], ["1", "a", "9223372036854775808", None], ["1", "a", "1.0", None], ["1"]):
            with self.assertRaises(ValueError):
                parse_row(raw)

    def test_percentiles(self):
        self.assertEqual(summarize([4, 1, 3, 2]),
                         {"queries": 4, "p50_ms": 2, "p95_ms": 4, "p99_ms": 4, "max_ms": 4})


if __name__ == "__main__":
    unittest.main()
