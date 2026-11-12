#!/usr/bin/env python3
"""Destructive only inside a newly created, uniquely named POC database.

Standard library only. No automatic SQL retries. Evidence is retained on failure.
This is an experiment runner, NOT a production Databend connector.
"""
import argparse
import base64
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


class SqlError(RuntimeError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RuntimeError("Refusing HTTP redirect")


class Client:
    def __init__(self, endpoint, user, password, journal, label):
        self.endpoint = endpoint.rstrip("/")
        self.auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
        self.journal = journal
        self.label = label
        self.session = {}
        self.node = None
        self.opener = urllib.request.build_opener(NoRedirect())

    def http(self, path, payload=None, timeout=15):
        if not path.startswith("/v1/query") or urllib.parse.urlsplit(path).netloc:
            raise RuntimeError(f"Invalid query URI: {path}")
        headers = {"Authorization": self.auth, "Content-Type": "application/json"}
        if self.node and self.session.get("need_sticky"):
            headers["X-DATABEND-STICKY-NODE"] = self.node
        req = urllib.request.Request(
            self.endpoint + path,
            data=None if payload is None else json.dumps(payload).encode(),
            headers=headers,
        )
        with self.opener.open(req, timeout=timeout) as response:
            raw = response.read(16 * 1024 * 1024 + 1)
            if len(raw) > 16 * 1024 * 1024:
                raise RuntimeError("Response exceeds experiment size limit")
            return json.loads(raw) if raw else {}

    def sql(self, sql):
        start = time.monotonic()
        event = {"client": self.label, "sql": sql, "utc": datetime.now(timezone.utc).isoformat()}
        query_id = None
        finished = False
        response = {}
        try:
            response = self.http("/v1/query", {
                "sql": sql, "session": self.session,
                "pagination": {"wait_time_secs": 2, "max_rows_per_page": 1000},
            })
            query_id = response.get("id")
            event["query_id"] = query_id
            rows, schema = [], []
            deadline = start + 45
            while True:
                if response.get("session"):
                    self.session = response["session"]
                if response.get("node_id"):
                    self.node = response["node_id"]
                if response.get("schema"):
                    schema = response["schema"]
                rows.extend(response.get("data", []))
                if len(rows) > 10000:
                    raise RuntimeError("Experiment result exceeds 10000 rows")
                event.update(state=response.get("state"), error=response.get("error"),
                             txn_state=self.session.get("txn_state"))
                if response.get("error"):
                    raise SqlError(json.dumps(response["error"], ensure_ascii=False))
                uri = response.get("next_uri")
                if not uri:
                    if response.get("state") != "Succeeded":
                        raise RuntimeError(f"Incomplete query: {response.get('state')}")
                    break
                if not query_id or not re.fullmatch(
                    rf"/v1/query/{re.escape(query_id)}/(?:page/\d+|final)", uri
                ):
                    raise RuntimeError(f"Unexpected continuation URI: {uri}")
                if time.monotonic() >= deadline:
                    raise TimeoutError("Query deadline exceeded; SQL not resubmitted")
                if uri.endswith("/final"):
                    if response.get("state") != "Succeeded":
                        raise RuntimeError("Final URI before successful completion")
                    self.http(uri)
                    finished = True
                    break
                time.sleep(0.01)
                response = self.http(uri, timeout=min(15, max(1, deadline - time.monotonic())))
            event.update(data=rows, schema=schema, txn_state=self.session.get("txn_state"))
            return rows
        except Exception as exc:
            event["exception"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            if query_id and not finished:
                try:
                    if event.get("state") not in ("Succeeded", "Failed"):
                        self.http(f"/v1/query/{query_id}/kill")
                    self.http(f"/v1/query/{query_id}/final")
                except Exception as exc:
                    event["cleanup_error"] = str(exc)
            event["elapsed_ms"] = round((time.monotonic() - start) * 1000, 2)
            with self.journal.open("a") as output:
                output.write(json.dumps(event, ensure_ascii=False) + "\n")


class Experiment:
    def __init__(self, args):
        self.args = args
        self.database = "readyset_m0_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]
        self.output = Path(args.output) / self.database
        self.output.mkdir(parents=True, exist_ok=False)
        self.clients = []
        self.checks = []
        self.facts = {}
        self.created = False
        self.admin = self.client("admin")
        self.writer = self.client("writer")
        self.observer = self.client("observer")
        self.tx = self.client("transaction")

    def client(self, label):
        c = Client(self.args.endpoint, self.args.user, os.environ.get("DATABEND_PASSWORD", ""),
                   self.output / "queries.jsonl", label)
        self.clients.append(c)
        return c

    def table(self, name):
        return f"{self.database}.{name}"

    def check(self, name, actual, expected):
        passed = actual == expected
        self.checks.append({"name": name, "passed": passed, "actual": actual, "expected": expected})
        print(f"{'PASS' if passed else 'FAIL'} {name}", flush=True)
        if not passed:
            raise AssertionError(f"{name}: actual={actual!r}, expected={expected!r}")

    def delta(self, name, client=None):
        return (client or self.observer).sql(
            f"SELECT id, val, change$action, change$is_update, change$row_id FROM {self.table(name)} "
            "ORDER BY id, change$action, val"
        )

    def setup_pair(self, prefix):
        src, stream = self.table(prefix + "_src"), self.table(prefix + "_stream")
        self.admin.sql(f"CREATE TABLE {src} (id BIGINT, val STRING)")
        self.admin.sql(f"CREATE STREAM {stream} ON TABLE {src} APPEND_ONLY = false")
        self.writer.sql(f"INSERT INTO {src} VALUES (1, 'one'), (2, 'two')")
        return src, stream

    def batch_insert(self, stream, batch, val_expr="val", suffix=""):
        return (f"INSERT INTO {self.table('batches')} "
                f"SELECT {batch}, id, {val_expr}, change$action, change$row_id, change$is_update "
                f"FROM {stream}{suffix}")

    def count(self, table, where=""):
        return int(self.observer.sql(f"SELECT count(*) FROM {table}{where}")[0][0])

    def expect_sql_error(self, name, client, sql):
        try:
            client.sql(sql)
        except SqlError as exc:
            self.check(name, True, True)
            self.facts[name] = str(exc)
        else:
            self.check(name, "Succeeded", "SQL error")

    def snapshot_test(self):
        src = self.table("snapshot_src")
        stream = self.table("snapshot_stream")
        self.admin.sql(f"CREATE TABLE {src} (id BIGINT, val STRING)")
        # Enable change tracking BEFORE the chosen snapshot. This is an explicit prerequisite.
        self.admin.sql(f"CREATE STREAM {self.table('tracking_anchor')} ON TABLE {src} APPEND_ONLY = false")
        self.writer.sql(f"INSERT INTO {src} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        snapshot = self.admin.sql(
            f"SELECT snapshot_id FROM FUSE_SNAPSHOT('{self.database}', 'snapshot_src') "
            "ORDER BY timestamp DESC LIMIT 1"
        )[0][0]
        self.facts["snapshot_id"] = snapshot
        # Writes in the gap between selecting S and creating the actual consumer stream.
        self.writer.sql(f"INSERT INTO {src} VALUES (4, 'd')")
        self.writer.sql(f"UPDATE {src} SET val = 'b2' WHERE id = 2")
        self.writer.sql(f"DELETE FROM {src} WHERE id = 3")
        self.admin.sql(f"CREATE STREAM {stream} ON TABLE {src} AT (SNAPSHOT => '{snapshot}') APPEND_ONLY = false")
        first = self.admin.sql(f"SELECT id, val FROM {src} AT (SNAPSHOT => '{snapshot}') WHERE id <= 2 ORDER BY id")
        # More writes interleaved with the full snapshot scan.
        self.writer.sql(f"UPDATE {src} SET val = 'a2' WHERE id = 1")
        self.writer.sql(f"INSERT INTO {src} VALUES (5, 'e')")
        rest = self.admin.sql(f"SELECT id, val FROM {src} AT (SNAPSHOT => '{snapshot}') WHERE id > 2 ORDER BY id")
        base = first + rest
        self.check("fixed snapshot stable across interleaved writes", base, [["1", "a"], ["2", "b"], ["3", "c"]])
        delta = self.delta("snapshot_stream")
        self.check("snapshot boundary emits expected net inserts/deletes",
                   [r[:3] for r in delta],
                   [["1", "a", "DELETE"], ["1", "a2", "INSERT"],
                    ["2", "b", "DELETE"], ["2", "b2", "INSERT"],
                    ["3", "c", "DELETE"], ["4", "d", "INSERT"], ["5", "e", "INSERT"]])
        self.check("plain SELECT does not consume stream", self.delta("snapshot_stream"), delta)
        state = Counter(tuple(r) for r in base)
        for row in delta:
            state[tuple(row[:2])] += 1 if row[2] == "INSERT" else -1
        self.check("retractions reference rows in the snapshot", all(n >= 0 for n in state.values()), True)
        rebuilt = sorted([list(row) for row, n in state.items() for _ in range(n)])
        current = self.observer.sql(f"SELECT id, val FROM {src} ORDER BY id")
        self.check("snapshot plus delta equals current source (including multiplicity)", rebuilt, current)

    def pretracking_snapshot_test(self):
        src = self.table("pretracking_src")
        stream = self.table("pretracking_stream")
        self.admin.sql(f"CREATE TABLE {src} (id BIGINT, val STRING)")
        self.writer.sql(f"INSERT INTO {src} VALUES (1, 'before_tracking')")
        snapshot = self.admin.sql(
            f"SELECT snapshot_id FROM FUSE_SNAPSHOT('{self.database}', 'pretracking_src') "
            "ORDER BY timestamp DESC LIMIT 1"
        )[0][0]
        self.writer.sql(f"UPDATE {src} SET val = 'also_before_tracking' WHERE id = 1")
        try:
            self.admin.sql(f"CREATE STREAM {stream} ON TABLE {src} AT (SNAPSHOT => '{snapshot}') APPEND_ONLY = false")
        except SqlError as exc:
            self.check("pretracking snapshot rejected for missing change tracking", '"code": 2733' in str(exc), True)
            self.facts["snapshot_before_change_tracking"] = {
                "supported": False, "snapshot_id": snapshot, "error": str(exc),
                "requirement": "Enable change tracking before selecting the initialization snapshot",
            }
            print("OBSERVED historical snapshot before change tracking rejected: " + str(exc), flush=True)
        else:
            self.facts["snapshot_before_change_tracking"] = {"supported": True, "snapshot_id": snapshot}
            self.check("pretracking historical snapshot produces complete update delta",
                       [r[:3] for r in self.delta("pretracking_stream")],
                       [["1", "before_tracking", "DELETE"], ["1", "also_before_tracking", "INSERT"]])

    def existing_table_bootstrap_test(self):
        src = self.table("existing_src")
        stream = self.table("existing_stream")
        self.admin.sql(f"CREATE TABLE {src} (id BIGINT, val STRING)")
        self.writer.sql(f"INSERT INTO {src} VALUES (1, 'old_one'), (2, 'old_two')")
        self.admin.sql(f"CREATE STREAM {stream} ON TABLE {src} APPEND_ONLY = false")
        # AT STREAM binds the initial scan to this consumer's current offset. Do not consume
        # or replace the stream until the entire initial scan is complete.
        first = self.admin.sql(f"SELECT id, val FROM {src} AT (STREAM => {stream}) WHERE id = 1")
        self.writer.sql(f"UPDATE {src} SET val = 'new_one' WHERE id = 1")
        self.writer.sql(f"UPDATE {src} SET val = 'new_two' WHERE id = 2")
        self.writer.sql(f"INSERT INTO {src} VALUES (3, 'new_three')")
        rest = self.admin.sql(f"SELECT id, val FROM {src} AT (STREAM => {stream}) WHERE id > 1 ORDER BY id")
        base = first + rest
        self.check("existing table bootstraps at stream offset without rewriting source",
                   base, [["1", "old_one"], ["2", "old_two"]])
        state = Counter(tuple(row) for row in base)
        for row in self.delta("existing_stream"):
            state[tuple(row[:2])] += 1 if row[2] == "INSERT" else -1
        self.check("existing table delta retracts valid initial rows", all(n >= 0 for n in state.values()), True)
        rebuilt = sorted([list(row) for row, n in state.items() for _ in range(n)])
        self.check("existing table stream-bound snapshot plus delta equals source", rebuilt,
                   self.observer.sql(f"SELECT id, val FROM {src} ORDER BY id"))
        self.facts["existing_table_bootstrap"] = "CREATE STREAM first, scan AT(STREAM => stream); do not advance stream until initial scan completes"

    def concurrent_consumers_test(self):
        _, stream = self.setup_pair("concurrent")
        other = self.table("competing_batches")
        self.admin.sql(f"CREATE TABLE {other} "
                       "(batch_id BIGINT, id BIGINT, val STRING, action STRING, row_id STRING, is_update BOOLEAN)")
        competitor = self.client("competing_transaction")
        before = self.delta("concurrent_stream")
        self.tx.sql("BEGIN")
        competitor.sql("BEGIN")
        self.tx.sql(self.batch_insert(stream, 10))
        second_insert = self.batch_insert(stream, 10).replace(self.table("batches"), other, 1)
        second_failed = False
        try:
            competitor.sql(second_insert)
        except SqlError as exc:
            second_failed = True
            self.facts["competing_consumer_error"] = str(exc)
        self.tx.sql("COMMIT")
        if not second_failed:
            try:
                competitor.sql("COMMIT")
            except SqlError as exc:
                second_failed = True
                self.facts["competing_consumer_error"] = str(exc)
        if competitor.session.get("txn_state") in ("Active", "Fail"):
            competitor.sql("ROLLBACK")
        first_count = self.count(self.table("batches"), " WHERE batch_id = 10")
        second_count = self.count(other)
        self.check("competing consumers persist only one copy of captured delta", first_count + second_count, len(before))
        self.check("competing consumer reports a stream-version conflict",
                   second_failed and '"code": 2734' in self.facts.get("competing_consumer_error", ""), True)
        self.check("winning consumer persists complete batch", first_count, len(before))
        self.check("losing consumer leaves no outbox rows", second_count, 0)
        self.check("winning consumer advances shared stream", self.delta("concurrent_stream"), [])

    def atomicity_tests(self):
        self.admin.sql(f"CREATE TABLE {self.table('batches')} "
                       "(batch_id BIGINT, id BIGINT, val STRING, action STRING, row_id STRING, is_update BOOLEAN)")
        self.admin.sql(f"CREATE TABLE {self.table('batch_meta')} (batch_id BIGINT, row_count BIGINT)")
        # A single INSERT SELECT is the minimal consumption + durable outbox operation.
        _, stream = self.setup_pair("autocommit")
        self.admin.sql(self.batch_insert(stream, 1))
        self.check("autocommit writes all batch rows", self.count(self.table("batches"), " WHERE batch_id = 1"), 2)
        self.check("autocommit advances stream", self.delta("autocommit_stream"), [])

        # Explicit rollback: rows, metadata and stream offset must all roll back.
        _, stream = self.setup_pair("rollback")
        before = self.delta("rollback_stream")
        self.tx.sql("BEGIN")
        self.tx.sql(self.batch_insert(stream, 2))
        self.tx.sql(f"INSERT INTO {self.table('batch_meta')} VALUES (2, 2)")
        self.check("transaction remains Active after consuming INSERT", self.tx.session.get("txn_state"), "Active")
        self.check("uncommitted batch invisible to independent session", self.count(self.table("batches"), " WHERE batch_id = 2"), 0)
        self.check("uncommitted metadata invisible", self.count(self.table("batch_meta"), " WHERE batch_id = 2"), 0)
        self.check("uncommitted consumption invisible", self.delta("rollback_stream"), before)
        self.tx.sql("ROLLBACK")
        self.check("rollback restores complete stream", self.delta("rollback_stream"), before)
        self.check("rollback removes batch rows", self.count(self.table("batches"), " WHERE batch_id = 2"), 0)
        self.check("rollback removes batch metadata", self.count(self.table("batch_meta"), " WHERE batch_id = 2"), 0)

        # Commit with a source write after capture: that new row must remain for the next batch.
        src, stream = self.setup_pair("commit")
        self.tx.sql("BEGIN")
        self.tx.sql(self.batch_insert(stream, 3))
        self.tx.sql(f"INSERT INTO {self.table('batch_meta')} VALUES (3, 2)")
        self.writer.sql(f"INSERT INTO {src} VALUES (3, 'after_capture')")
        self.tx.sql("COMMIT")
        self.check("commit persists batch", self.count(self.table("batches"), " WHERE batch_id = 3"), 2)
        self.check("commit persists batch metadata", self.observer.sql(f"SELECT * FROM {self.table('batch_meta')} WHERE batch_id = 3"), [["3", "2"]])
        self.check("source write after capture survives consumption", [r[:3] for r in self.delta("commit_stream")], [["3", "after_capture", "INSERT"]])
        self.admin.sql(self.batch_insert(stream, 4))
        self.check("next batch gets source write exactly once", self.count(self.table("batches"), " WHERE batch_id = 4"), 1)
        self.check("second batch drains remaining delta", self.delta("commit_stream"), [])

        # Execution failure (not syntax/bind failure) must not consume or partially append.
        _, stream = self.setup_pair("statement_failure")
        before = self.delta("statement_failure_stream")
        self.expect_sql_error("runtime conversion failure is raised", self.admin,
                              self.batch_insert(stream, 5, "CAST(CAST(val AS BIGINT) AS STRING)"))
        self.check("failed INSERT preserves stream", self.delta("statement_failure_stream"), before)
        self.check("failed INSERT leaves no batch", self.count(self.table("batches"), " WHERE batch_id = 5"), 0)

        # Failure AFTER the consuming statement, then COMMIT must not publish earlier work.
        _, stream = self.setup_pair("transaction_failure")
        before = self.delta("transaction_failure_stream")
        self.tx.sql("BEGIN")
        self.tx.sql(self.batch_insert(stream, 6))
        self.expect_sql_error("later statement fails inside transaction", self.tx,
                              f"INSERT INTO {self.table('batch_meta')} SELECT 6, CAST(val AS BIGINT) FROM {self.table('transaction_failure_src')}")
        self.check("transaction enters Fail state", self.tx.session.get("txn_state"), "Fail")
        self.tx.sql("COMMIT")
        self.check("COMMIT of failed transaction preserves stream", self.delta("transaction_failure_stream"), before)
        self.check("COMMIT of failed transaction removes preceding batch", self.count(self.table("batches"), " WHERE batch_id = 6"), 0)
        self.check("failed transaction leaves no metadata", self.count(self.table("batch_meta"), " WHERE batch_id = 6"), 0)

        # UPDATE/DELETE payload fidelity through the outbox, not just insert counts.
        src, stream = self.setup_pair("mutations")
        self.admin.sql(self.batch_insert(stream, 7))
        self.writer.sql(f"UPDATE {src} SET val = 'one_new' WHERE id = 1")
        self.writer.sql(f"DELETE FROM {src} WHERE id = 2")
        self.writer.sql(f"INSERT INTO {src} VALUES (3, 'three')")
        before = self.delta("mutations_stream")
        self.admin.sql(self.batch_insert(stream, 8))
        persisted = self.observer.sql(f"SELECT id, val, action, is_update, row_id FROM {self.table('batches')} WHERE batch_id = 8 ORDER BY id, action, val")
        self.check("outbox preserves full update/delete payload", persisted, before)
        self.check("mutation batch drains stream", self.delta("mutations_stream"), [])

        # Demonstrate filtered no-op consumption: empty outbox is not proof of no advancement.
        _, stream = self.setup_pair("filtered")
        self.admin.sql(self.batch_insert(stream, 9, suffix=" WHERE id < 0"))
        self.check("filtered consuming INSERT writes zero rows", self.count(self.table("batches"), " WHERE batch_id = 9"), 0)
        self.check("filtered consuming INSERT still advances stream", self.delta("filtered_stream"), [])

        # No-op consumption must also participate in the explicit transaction's rollback.
        _, stream = self.setup_pair("filtered_rollback")
        before = self.delta("filtered_rollback_stream")
        self.tx.sql("BEGIN")
        self.tx.sql(self.batch_insert(stream, 11, suffix=" WHERE id < 0"))
        self.tx.sql(f"INSERT INTO {self.table('batch_meta')} VALUES (11, 0)")
        self.tx.sql("ROLLBACK")
        self.check("rollback of zero-row consumption preserves stream", self.delta("filtered_rollback_stream"), before)
        self.check("rollback of zero-row consumption removes metadata", self.count(self.table("batch_meta"), " WHERE batch_id = 11"), 0)

        # A separate WITH CONSUME SELECT must be rolled back with subsequent failed work too.
        _, stream = self.setup_pair("select_consume")
        before = self.delta("select_consume_stream")
        self.tx.sql("BEGIN")
        consumed = self.tx.sql(f"SELECT id, val FROM {stream} WITH CONSUME ORDER BY id")
        self.check("WITH CONSUME SELECT returns captured rows", consumed, [["1", "one"], ["2", "two"]])
        self.check("uncommitted WITH CONSUME invisible to observer", self.delta("select_consume_stream"), before)
        self.tx.sql("ROLLBACK")
        self.check("WITH CONSUME SELECT rollback preserves stream", self.delta("select_consume_stream"), before)

    def run(self):
        error = None
        try:
            self.facts["version"] = self.admin.sql("SELECT version()")[0][0]
            self.admin.sql(f"CREATE DATABASE {self.database}")
            self.created = True
            print(f"Database: {self.database}\nEvidence: {self.output}", flush=True)
            self.snapshot_test()
            self.pretracking_snapshot_test()
            self.existing_table_bootstrap_test()
            self.atomicity_tests()
            self.concurrent_consumers_test()
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            print(error, flush=True)
        finally:
            # Only close transactions owned by this experiment. Do not drop evidence objects.
            for c in self.clients:
                if c.session.get("txn_state") in ("Active", "Fail"):
                    try:
                        c.sql("ROLLBACK")
                    except Exception as exc:
                        self.facts[f"{c.label}_rollback_error"] = str(exc)
            report = {"endpoint": self.args.endpoint, "database": self.database,
                      "database_created": self.created, "facts": self.facts,
                      "checks": self.checks, "error": error,
                      "passed": error is None and all(c["passed"] for c in self.checks)}
            (self.output / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
            print(f"Report: {self.output / 'report.json'}", flush=True)
        return 1 if error else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True, help="Confirmed self-hosted HTTP(S) origin")
    parser.add_argument("--user", default="root")
    parser.add_argument("--output", default="/tmp/readyset-databend-m0")
    parser.add_argument("--confirm-create", action="store_true", help="Authorize creation/writes in a new isolated database")
    args = parser.parse_args()
    url = urllib.parse.urlsplit(args.endpoint)
    if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.path not in ("", "/") or url.query or url.fragment:
        parser.error("--endpoint must be an HTTP(S) origin without credentials/path/query")
    if not args.confirm_create:
        parser.error("--confirm-create is required; the experiment creates tables and streams")
    return Experiment(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
