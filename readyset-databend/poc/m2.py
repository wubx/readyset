#!/usr/bin/env python3
"""Live Stream -> transactional outbox -> serial ReadySet sink correctness experiment.

Creates only a new isolated POC database. Retains all evidence. No SQL retries or outbox cleanup.
The ReadySet child is owned by this runner and is closed at the end; no service is restarted.
"""
import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import selectors
import subprocess
import time
import urllib.parse
import uuid

from m0 import Client
from m1_export import parse_row


def checksum(changes):
    return hashlib.sha256(json.dumps(changes, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def parse_change(raw):
    if len(raw) != 7 or raw[4] not in ("INSERT", "DELETE") or raw[6] not in ("0", "1", "false", "true", False, True):
        raise ValueError("Invalid source change")
    return {"action": raw[4], "row_id": raw[5], "is_update": raw[6] in ("1", "true", True), "row": parse_row(raw[:4])}


class Engine:
    def __init__(self, binary, initial, output, generation, journal, declarations=None):
        self.stderr = (output.parent / "engine_stderr.log").open("w")
        self.journal = journal
        command = [str(binary), "--input", str(initial), "--output", str(output), "--generation", generation]
        if declarations is not None:
            command += ["--declarations", str(declarations)]
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=self.stderr, text=True, bufsize=1)
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        try:
            self.ready = self.receive(60)
            if not self.ready.get("ready"):
                raise RuntimeError(f"Engine did not become ready: {self.ready}")
        except Exception:
            self.close()
            raise

    def receive(self, timeout=45):
        if not self.selector.select(timeout):
            raise TimeoutError("ReadySet child response timed out; request NOT retried")
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError(f"ReadySet child closed stdout, exit={self.process.poll()}; inspect engine_stderr.log")
        return json.loads(line)

    def request(self, message):
        start = time.monotonic()
        self.process.stdin.write(json.dumps(message, ensure_ascii=False) + "\n")
        self.process.stdin.flush()
        response = self.receive()
        with self.journal.open("a") as output:
            output.write(json.dumps({"request": message, "response": response,
                                     "roundtrip_ms": (time.monotonic()-start)*1000}, ensure_ascii=False) + "\n")
        return response

    def finish(self):
        result = self.request({"op": "shutdown"})
        if not result.get("shutdown"):
            raise AssertionError(result)
        self.process.stdin.close()
        if self.process.wait(timeout=45):
            raise RuntimeError("ReadySet child exited unsuccessfully")
        self.close()

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.selector.close()
        if self.process.stdin and not self.process.stdin.closed:
            try:
                self.process.stdin.close()
            except BrokenPipeError:
                pass  # Child may have failed before consuming the buffered request.
        self.process.stdout.close()
        self.stderr.close()


class M2:
    def __init__(self, args):
        self.args = args
        self.database = "readyset_m2_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]
        self.output = Path(args.output) / self.database
        self.output.mkdir(parents=True, exist_ok=False)
        def client(label):
            return Client(args.endpoint, args.user, os.environ.get("DATABEND_PASSWORD", ""),
                          self.output / "queries.jsonl", label)
        self.admin, self.writer, self.tx, self.observer = [client(s) for s in ("admin", "writer", "capture_transaction", "observer")]
        self.checks = []
        self.batches = []
        self.known_ids = set()
        self.engine = None
        self.facts = {}

    def table(self, name):
        return f"{self.database}.{name}"

    def check(self, label, actual, expected):
        passed = actual == expected
        self.checks.append({"name": label, "passed": passed, "actual": actual, "expected": expected})
        print(f"{'PASS' if passed else 'FAIL'} {label}", flush=True)
        if not passed:
            raise AssertionError(f"{label}: {actual!r} != {expected!r}")

    def source_rows(self, at=""):
        result = []
        last = None
        while True:
            where = "" if last is None else f" WHERE id > {last}"
            rows = self.observer.sql(f"SELECT id, payload, score, note FROM {self.table('source_rows')}{at}{where} ORDER BY id LIMIT 500")
            if not rows:
                break
            for raw in rows:
                row = parse_row(raw)
                if last is not None and row["id"] <= last:
                    raise AssertionError("Source business key is not unique")
                last = row["id"]
                result.append(row)
            if len(result) > 10000:
                raise AssertionError("M2 source exceeds 10000 rows")
        return result

    def read_changes(self, client, sequence=None):
        if sequence is None:
            from_sql = self.table("source_stream")
        else:
            from_sql = self.table("outbox") + f" WHERE batch_id = {sequence}"
        names = "change$action, change$row_id, change$is_update" if sequence is None else "action, row_id, is_update"
        raw = client.sql(f"SELECT id, payload, score, note, {names} FROM {from_sql} ORDER BY id, 5, 6")
        return [parse_change(row) for row in raw]

    def capture(self, sequence, rollback=False):
        # Single owner and monotonic sequence in this experiment. Never overwrite/reuse committed IDs.
        existing = self.observer.sql(f"SELECT count(*) FROM {self.table('batch_meta')} WHERE batch_id = {sequence}")
        self.check(f"batch {sequence} has no prior commit", int(existing[0][0]), 0)
        before = self.read_changes(self.observer) if rollback else None
        self.tx.sql("BEGIN")
        try:
            self.tx.sql(f"INSERT INTO {self.table('outbox')} SELECT {sequence}, id, payload, score, note, "
                        f"change$action, change$row_id, change$is_update FROM {self.table('source_stream')}")
            changes = self.read_changes(self.tx, sequence)
            count = int(self.tx.sql(f"SELECT count(*) FROM {self.table('outbox')} WHERE batch_id = {sequence}")[0][0])
            self.check(f"batch {sequence} transaction reads its complete outbox", len(changes), count)
            digest = checksum(changes)
            self.tx.sql(f"INSERT INTO {self.table('batch_meta')} VALUES ('{self.database}', {sequence}, {count}, '{digest}')")
            if rollback:
                self.tx.sql("ROLLBACK")
                self.check("rolled-back capture preserves Stream", self.read_changes(self.observer), before)
                self.check("rolled-back capture leaves no outbox rows", self.observer.sql(f"SELECT count(*) FROM {self.table('outbox')} WHERE batch_id = {sequence}"), [["0"]])
                self.check("rolled-back capture leaves no metadata", self.observer.sql(f"SELECT count(*) FROM {self.table('batch_meta')} WHERE batch_id = {sequence}"), [["0"]])
                return None
            self.check(f"batch {sequence} capture remains transactional", self.tx.session.get("txn_state"), "Active")
            self.tx.sql("COMMIT")
        except Exception as original:
            if self.tx.session.get("txn_state") in ("Active", "Fail"):
                try:
                    self.tx.sql("ROLLBACK")
                except Exception as cleanup:
                    original.add_note(f"Rollback also failed: {cleanup}")
            raise
        # Delivery uses a fresh read of COMMITTED source data, not the pre-commit Python buffer.
        committed = self.read_changes(self.observer, sequence)
        metadata = self.observer.sql(f"SELECT generation, batch_id, row_count, changes_sha256 FROM {self.table('batch_meta')} WHERE batch_id = {sequence}")
        self.check(f"batch {sequence} committed metadata is unique and complete", metadata,
                   [[self.database, str(sequence), str(len(committed)), checksum(committed)]])
        self.check(f"batch {sequence} committed outbox matches captured changes", committed, changes)
        self.check(f"batch {sequence} drained Stream", self.read_changes(self.observer), [])
        batch = {"generation": self.database, "sequence": sequence, "row_count": len(committed),
                 "changes_sha256": checksum(committed), "changes": committed}
        (self.output / f"batch_{sequence:03}.json").write_text(json.dumps(batch, ensure_ascii=False, indent=2) + "\n")
        self.batches.append(batch)
        return batch

    def apply(self, batch, skip=False):
        result = self.engine.request({"op": "apply", "batch": batch})
        self.check(f"delivery {batch['sequence']} acknowledged", result.get("ok"), True)
        self.check(f"delivery {batch['sequence']} duplicate classification", result.get("skipped"), skip)
        if not skip:
            self.check(f"batch {batch['sequence']} persistent checkpoint", result.get("checkpoint"), batch["sequence"])
        return result

    def verify(self, label):
        rows = self.source_rows()
        self.known_ids.update(row["id"] for row in rows)
        live = {row["id"] for row in rows}
        missing = sorted(self.known_ids - live) + [-9223372036854775808]
        response = self.engine.request({"op": "verify", "label": label, "rows": rows, "missing": missing})
        self.check(f"{label}: all source rows and deleted keys agree", response.get("ok"), True)
        self.check(f"{label}: complete live reader contents", response.get("live_rows_verified"), len(rows))
        self.check(f"{label}: all missing keys checked", response.get("missing_verified"), len(missing))
        return response

    def negative_tests(self):
        last = self.batches[-1]
        bad = []
        gap = copy.deepcopy(last); gap["sequence"] += 2; bad.append(("gap", gap))
        generation = copy.deepcopy(last); generation["generation"] = "wrong_generation"; bad.append(("generation", generation))
        incomplete = copy.deepcopy(last); incomplete["row_count"] += 1; bad.append(("incomplete", incomplete))
        tampered = copy.deepcopy(last); tampered["changes"][0]["row"]["payload"] = "tampered"; bad.append(("checksum", tampered))
        for label, batch in bad:
            response = self.engine.request({"op": "apply", "batch": batch})
            self.check(f"{label} batch rejected before mutation", response.get("rejected"), True)
            self.check(f"{label} batch keeps checkpoint", response.get("checkpoint"), last["sequence"])
        self.verify("after rejected batches")

    def experiment(self):
        n = self.args.rows
        src = self.table("source_rows")
        self.facts["version"] = self.admin.sql("SELECT version()")[0][0]
        self.admin.sql(f"CREATE DATABASE {self.database}")
        self.admin.sql(f"CREATE TABLE {src} (id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL)")
        self.writer.sql(f"INSERT INTO {src} SELECT CAST(number+1 AS BIGINT), concat('initial-', to_string(number+1)), "
                        f"CAST(number AS BIGINT), IF(number % 2 = 0, NULL, '中文') FROM numbers({n})")
        self.admin.sql(f"CREATE STREAM {self.table('source_stream')} ON TABLE {src} APPEND_ONLY = false")
        self.admin.sql(f"CREATE TABLE {self.table('outbox')} (batch_id BIGINT NOT NULL, id BIGINT NOT NULL, payload STRING NOT NULL, "
                       "score BIGINT NOT NULL, note STRING NULL, action STRING NOT NULL, row_id STRING NOT NULL, is_update BOOLEAN NOT NULL)")
        self.admin.sql(f"CREATE TABLE {self.table('batch_meta')} (generation STRING NOT NULL, batch_id BIGINT NOT NULL, row_count BIGINT NOT NULL, changes_sha256 STRING NOT NULL)")
        initial = self.source_rows(f" AT (STREAM => {self.table('source_stream')})")
        self.check("initial fixed-boundary export has all rows", len(initial), n)
        initial_file = self.output / "initial.jsonl"
        initial_file.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in initial))
        self.known_ids.update(row["id"] for row in initial)
        self.engine = Engine(Path(self.args.binary).resolve(), initial_file, self.output / "engine",
                             self.database, self.output / "deliveries.jsonl")
        self.verify("initial snapshot")

        # Batch 1: changes to old rows, pure deletions, and brand-new keys.
        self.writer.sql(f"UPDATE {src} SET payload = 'v1-中文', score = score + 10, note = NULL WHERE id <= 200")
        self.writer.sql(f"DELETE FROM {src} WHERE id BETWEEN 201 AND 250")
        self.writer.sql(f"INSERT INTO {src} SELECT CAST(number+{n}+1 AS BIGINT), 'inserted', -CAST(number AS BIGINT), '' FROM numbers(20)")
        self.capture(1, rollback=True)  # Same changes remain available after aborted capture.
        b1 = self.capture(1)
        self.check("batch 1 includes INSERT and DELETE", sorted({c['action'] for c in b1['changes']}), ["DELETE", "INSERT"])
        self.apply(b1)
        self.verify("batch 1 update delete insert")
        self.apply(b1, skip=True)
        self.apply(b1, skip=True)
        self.verify("batch 1 delivered three times")

        # Batch 2: newer versions plus deletion of records that batch 1 inserted/updated.
        self.writer.sql(f"UPDATE {src} SET payload = 'v2-newer', score = score + 100, note = '' WHERE id <= 100")
        self.writer.sql(f"DELETE FROM {src} WHERE id BETWEEN 101 AND 125 OR id BETWEEN {n+1} AND {n+5}")
        self.writer.sql(f"UPDATE {src} SET id = {n+100}, payload = 'moved-key' WHERE id = 126")
        self.writer.sql(f"INSERT INTO {src} VALUES (201, 'reinserted', -10, NULL)")
        b2 = self.capture(2)
        self.apply(b2)
        self.verify("batch 2 newer values and moved key")
        self.apply(b1, skip=True)
        self.verify("old batch cannot overwrite v2 or resurrect deleted keys")
        response = self.engine.request({"op": "reset_sink"})
        self.check("new sink/client reads engine checkpoint", response.get("checkpoint"), 2)
        self.apply(b2, skip=True)
        self.verify("duplicate after recreating sink and client")

        # Batch 3: delete after reinsertion, plus null/empty/Int64 changes.
        self.writer.sql(f"DELETE FROM {src} WHERE id IN (1, 201)")
        self.writer.sql(f"UPDATE {src} SET score = CAST('-9223372036854775808' AS BIGINT), note = '' WHERE id = 2")
        self.writer.sql(f"UPDATE {src} SET payload = 'moved-key-newer' WHERE id = {n+100}")
        b3 = self.capture(3)
        self.apply(b3)
        self.verify("delete reinserted rows and Int64 minimum")
        self.apply(b2, skip=True)
        self.apply(b1, skip=True)
        self.verify("old insertion batches cannot revive deleted records")

        # Empty capture still commits metadata and advances the sink sequence safely.
        empty = self.capture(4)
        self.check("batch 4 is empty", empty['row_count'], 0)
        self.apply(empty)
        self.apply(empty, skip=True)
        self.verify("empty batch and its duplicate")

        # Multiple source mutations in one capture interval; do not assume a WAL event log.
        self.writer.sql(f"DELETE FROM {src} WHERE id = 3")
        self.writer.sql(f"INSERT INTO {src} VALUES (3, 'delete-then-insert', 30, '新值')")
        self.writer.sql(f"UPDATE {src} SET payload = 'intermediate' WHERE id = 4")
        self.writer.sql(f"UPDATE {src} SET payload = 'final-v4' WHERE id = 4")
        self.writer.sql(f"UPDATE {src} SET id = 101, payload = 'reuse-deleted-key' WHERE id = 5")
        b5 = self.capture(5)
        self.apply(b5)
        self.verify("collapsed updates delete-reinsert and key reuse")

        # A larger batch than previous cases, still deliberately not split into multiple RPCs.
        self.writer.sql(f"UPDATE {src} SET payload = 'bulk-new', score = -score, note = '批量' WHERE id BETWEEN 500 AND 900")
        b6 = self.capture(6)
        self.apply(b6)
        self.verify("bulk updates")
        # Re-read committed outboxes after discarding local capture buffers; no recapture.
        for batch in [b5, b3, b2, b1, b6, empty]:
            reread = self.read_changes(self.observer, batch['sequence'])
            batch = {**batch, "changes": reread}
            self.check(f"reread batch {batch['sequence']} checksum unchanged", checksum(reread), batch['changes_sha256'])
            self.apply(batch, skip=True)
        self.verify("all historical batches redelivered out of order")
        self.negative_tests()
        self.engine.finish()
        self.engine = None
        self.facts['batch_rows'] = [b['row_count'] for b in self.batches]
        self.facts['final_checkpoint'] = 6
        self.facts['rows_initial'] = n
        self.facts['rows_final'] = len(self.source_rows())

    def run(self):
        print(f"Database: {self.database}\nEvidence: {self.output}", flush=True)
        error = None
        try:
            self.experiment()
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            print(error, flush=True)
        finally:
            if self.engine:
                self.engine.close()
            if self.tx.session.get('txn_state') in ('Active', 'Fail'):
                try:
                    self.tx.sql('ROLLBACK')
                except Exception as exc:
                    self.facts['rollback_cleanup_error'] = str(exc)
            report = {"passed": error is None and all(c['passed'] for c in self.checks),
                      "database": self.database, "endpoint": self.args.endpoint, "facts": self.facts,
                      "checks": self.checks, "error": error,
                      "limitations": ["No node/process crash test", "Single serial consumer", "No automatic outbox cleanup", "No SQL frontend", "No continuous scheduler"]}
            (self.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
            print(f"Report: {self.output / 'report.json'}", flush=True)
        return 1 if error else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--user', default='root')
    parser.add_argument('--rows', type=int, default=2000)
    parser.add_argument('--binary', default='target/debug/databend-m2')
    parser.add_argument('--output', default='/tmp/readyset-databend-m2')
    parser.add_argument('--confirm-create', action='store_true')
    args = parser.parse_args()
    url = urllib.parse.urlsplit(args.endpoint)
    if url.scheme not in ('http','https') or not url.hostname or url.username or url.password or url.path not in ('','/') or url.query or url.fragment:
        parser.error('endpoint must be a confirmed HTTP(S) origin without credentials/path/query')
    if not args.confirm_create:
        parser.error('--confirm-create required: creates isolated tables/Stream and runs DML')
    if not 1000 <= args.rows <= 9000:
        parser.error('--rows must be 1000..9000 for these bounded test cases')
    if not Path(args.binary).is_file():
        parser.error('build databend-m2 first')
    return M2(args).run()


if __name__ == '__main__':
    raise SystemExit(main())
