#!/usr/bin/env python3
"""Create isolated M1 synthetic data, export one immutable snapshot, measure a source baseline.

No existing business objects are changed. Uses m0.Client's bounded HTTP/session handling.
The exported JSONL is the exact input and correctness oracle for the ReadySet experiment.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import random
import time
import urllib.parse
import uuid

from m0 import Client


COLUMNS = ["id", "payload", "score", "note"]


def parse_row(row):
    if len(row) != 4:
        raise ValueError(f"Expected 4 columns, got {len(row)}")
    if any(row[i] is None for i in (0, 1, 2)):
        raise ValueError("NULL in non-nullable input column")
    # HTTP fields are string encoded; converting straight to int avoids float rounding.
    result = {"id": int(row[0]), "payload": row[1], "score": int(row[2]), "note": row[3]}
    for column in ("id", "score"):
        if not -(2**63) <= result[column] < 2**63:
            raise ValueError(f"{column} out of signed BIGINT range")
    if not isinstance(result["payload"], str) or not (result["note"] is None or isinstance(result["note"], str)):
        raise ValueError("Invalid text value")
    return result


def summarize(samples):
    ordered = sorted(samples)
    if not ordered:
        raise ValueError("No samples")
    def percentile(p):
        import math
        return ordered[max(0, math.ceil(len(ordered) * p) - 1)]
    return {"queries": len(samples), "p50_ms": percentile(.5), "p95_ms": percentile(.95),
            "p99_ms": percentile(.99), "max_ms": ordered[-1]}


def run(args):
    database = "readyset_m1_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]
    output = Path(args.output) / database
    output.mkdir(parents=True, exist_ok=False)
    journal = output / "queries.jsonl"
    client = Client(args.endpoint, args.user, os.environ.get("DATABEND_PASSWORD", ""), journal, "m1_export")
    source = f"{database}.source_rows"
    stream = f"{database}.initial_boundary"
    report = {"passed": False, "endpoint": args.endpoint, "database": database, "rows_requested": args.rows,
              "seed": args.seed, "dataset": "synthetic BIGINT key, UTF-8 payload, signed BIGINT score, nullable UTF-8 note"}
    try:
        report["version"] = client.sql("SELECT version()")[0][0]
        client.sql(f"CREATE DATABASE {database}")
        client.sql(f"CREATE TABLE {source} (id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL)")
        # Track before the selected snapshot, as established by M0.
        client.sql(f"CREATE STREAM {stream} ON TABLE {source} APPEND_ONLY = false")
        client.sql(f"""INSERT INTO {source}
            SELECT CAST(number * 17 + 7 AS BIGINT),
                   concat('row-', to_string(number), '-', repeat('x', 64), IF(number % 3 = 0, '-中文é', '')),
                   CASE WHEN number = 0 THEN CAST('9223372036854775807' AS BIGINT)
                        WHEN number = 1 THEN CAST('-9223372036854775808' AS BIGINT)
                        ELSE -CAST(number AS BIGINT) * 13 END,
                   CASE WHEN number % 5 = 0 THEN NULL WHEN number % 5 = 1 THEN '' ELSE 'note-中文' END
            FROM numbers({args.rows})""")
        snapshot = client.sql(f"SELECT snapshot_id FROM FUSE_SNAPSHOT('{database}', 'source_rows') ORDER BY timestamp DESC LIMIT 1")[0][0]
        report["snapshot_id"] = snapshot
        at = f"{source} AT (SNAPSHOT => '{snapshot}')"
        count = int(client.sql(f"SELECT count(*) FROM {at}")[0][0])
        if count != args.rows:
            raise AssertionError(f"Source has {count} rows, expected {args.rows}")
        data_path = output / "rows.jsonl"
        digest = hashlib.sha256()
        exported = 0
        last_id = None
        oracle = {}
        started = time.monotonic()
        with data_path.open("wb") as target:
            while True:
                predicate = "" if last_id is None else f" WHERE id > {last_id}"
                rows = client.sql(f"SELECT id, payload, score, note FROM {at}{predicate} ORDER BY id LIMIT {args.page_size}")
                if not rows:
                    break
                for raw in rows:
                    row = parse_row(raw)
                    if last_id is not None and row["id"] <= last_id:
                        raise AssertionError("Export keys not strictly increasing")
                    last_id = row["id"]
                    encoded = (json.dumps(row, ensure_ascii=False) + "\n").encode()
                    target.write(encoded)
                    digest.update(encoded)
                    exported += 1
                    oracle[last_id] = row
        if exported != count:
            raise AssertionError(f"Exported {exported}, expected {count}")
        report.update(rows_exported=exported, export_seconds=time.monotonic() - started,
                      data_sha256=digest.hexdigest(), data_bytes=data_path.stat().st_size,
                      page_size=args.page_size)
        print(f"Exported {exported} rows from snapshot {snapshot} to {data_path}", flush=True)
        # Client.sql includes page retrieval/finalization and journal writes. This reference
        # is intentionally labeled separately: it is not protocol/network equivalent to Reader RPC.
        rng = random.Random(args.seed)
        keys = list(oracle)
        baseline = []
        for _ in range(args.baseline_queries):
            key = rng.choice(keys)
            start = time.monotonic()
            actual = client.sql(f"SELECT id, payload, score, note FROM {at} WHERE id = {key}")
            baseline.append((time.monotonic() - start) * 1000)
            if len(actual) != 1 or parse_row(actual[0]) != oracle[key]:
                raise AssertionError(f"Baseline query mismatch for {key}")
        report["databend_uniform_point_lookup"] = summarize(baseline)
        report["baseline_topology"] = "Python urllib HTTP to source including query submission, result lifecycle and local journal write; serial random queries"
        report["baseline_limitations"] = "Not directly protocol/network-equivalent to ReadySet loopback ReaderHandle RPC; not a speedup claim"
        report["passed"] = True
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(f"Report: {output / 'report.json'}", flush=True)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--user", default="root")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--baseline-queries", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="/tmp/readyset-databend-m1")
    parser.add_argument("--confirm-create", action="store_true")
    args = parser.parse_args()
    url = urllib.parse.urlsplit(args.endpoint)
    if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.path not in ("", "/") or url.query or url.fragment:
        parser.error("endpoint must be an HTTP(S) origin without credentials/path/query")
    if not args.confirm_create:
        parser.error("--confirm-create is required")
    if not (0 < args.rows <= 1000000 and 0 < args.page_size <= 1000 and 0 < args.baseline_queries <= 1000):
        parser.error("rows: 1..1000000, page-size: 1..1000, baseline-queries: 1..1000")
    run(args)


if __name__ == "__main__":
    main()
