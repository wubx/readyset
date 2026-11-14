# Bootstrap snapshot / Stream boundary fix (2026-11-14)

## Bug
On restart with an existing Stream, the snapshot `AT (STREAM => s)` and the first
`WITH CONSUME` ran in separate transactions. An old consumer's pending COMMIT could
advance the Stream from v0 to v1 between them, so the delta v0→v1 was lost. The local
copy then stayed permanently old while still reporting ready/fresh.

## Fix (native Databend semantics only; no helper tables)
- `Replicator::snapshot` / `snapshot_into` open `BEGIN` on the **capture** session.
  The snapshot pages, the count, and the first consume all run in that one transaction.
  Databend pins Stream/source metadata per transaction. At COMMIT its Stream version
  check rejects a competing late commit with `2734 StreamVersionMismatched`.
- If the bootstrap session is lost or aborted, or COMMIT fails, the source fails closed
  (`capture_failed`). It never silently reopens a fresh transaction or retries SQL.
  Recovery means a new generation resnapshot.
- Freshness for the first round starts at snapshot start (`bootstrap_started`), not at
  sync entry. A long snapshot is therefore not reported as fresh.
- Shared server: a snapshot failure rolls back the capture transaction before
  `fail_source`. Other tables are unaffected.

## Evidence (real Databend v1.2.962, http://192.168.191.96:8000, new DBs only, retained)
- `native_probe_report.json`: same-transaction `AT STREAM` stays pinned after another
  consumer's COMMIT. Consume returns the pinned delta and COMMIT fails with 2734.
- `baseline_control/`: pre-fix release `5156aea1…` reproduces the bug in full and
  partial modes. A stays ready past the 5s staleness bound and serves old 3-row bags
  while upstream is new (22 checks).
- `fault_matrix/`: fixed release `68e02684…`, full + partial × {late_commit,
  commit_reply_lost, crash_before_commit, slow_bootstrap}, 50,004-row A (multi-page),
  180/180 checks. In every case A fails closed or recovers to the exact bag and row
  count. B stays local and fresh. There is one native server, no global failure, and one
  Stream per table. COMMIT is executed once and never retried.
- Rust: `cargo test --all-targets` all ok (including 4 new boundary unit tests),
  `clippy -D warnings` clean, ignored live schema test passes (`live_schema_retry2.log`).
- `source.patch`: exact delta against the frozen pre-change copies.
  `sources.sha256`: local and remote sources are identical.

## Not covered
- The A-domain failure → B checkpoint RPC coupling (P2) and sync-WAL test
  discrimination (P2) from the review are unchanged.
- Resuming in place from the old directory is still unsupported.
