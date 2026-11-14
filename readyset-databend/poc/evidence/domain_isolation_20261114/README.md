# Per-table checkpoint isolation from other tables' native domain faults (2026-11-14)

> **已回退（superseded）。** 这里的原生 `/table_replication_offset` RPC 以及对 `readyset-server`/`readyset-client`
> 的修改已经撤销。后续方案在共享路径上不再按批读回原生 offset：同表顺序由进程内序号保证，
> 重启位置由每表重启标记记录（见 [MULTITABLE.md](../../../MULTITABLE.md) 和 [USAGE.md](../../../USAGE.md)），
> 所以 A 的 domain 故障不会拖住 B 的批次。下面的数据只作为当时方案的历史记录，
> 对现在的代码需要重新跑 `poc/domain_isolation_verify.py`。修复前版本的负对照（B 被 A 拖停）仍有参考价值。

## Bug
Every shared-runtime checkpoint (`begin_batch`, `finish`, snapshot, `CREATE CACHE`) read
`Sink::checkpoint`, which called the native global `replication_offsets` RPC. That RPC asks
every base domain and fails if any one of them errors or is Pending. When table A's native
domain was failing, table B's next round therefore failed and B stopped syncing permanently.

## Fix (native mechanisms only)
- ReadySet controller: a new `/table_replication_offset` endpoint
  (`DfState::table_replication_offset`, `readyset-server/src/controller/{state,inner}.rs`).
  It sends the existing `DomainRequest::RequestReplicationOffsets` only to the domain that
  owns the table. ReadySet assigns each base table its own domain. Same recovery guard as
  `/replication_offsets`.
- Client: `ReadySetHandle::table_replication_offset(&Relation)` (`readyset-client`).
- Adapter: `Sink::checkpoint` resolves the exact relation with `tables()` (controller
  metadata only, no domain RPC), then asks only that table's domain. Every SharedEngine
  checkpoint and the legacy single-table engine use it. Durability is unchanged: native
  synchronous WAL ACK plus an exact per-table offset check.
- Fault discovery for tests only: `GET /__fault/native`, gated by `--enable-fault-injection`.
  It returns the native HTTP router and each base table's domain index. Faults use ReadySet's
  own `handle-packet` failpoint, which exists only in `--features readyset-server/failure_injection`
  builds. The production release does not include it.

## Evidence (real Databend http://192.168.191.96:8000, new readyset_gateway_isolation_* DBs, retained)
- `baseline_control2/`: pre-fix failure-injection build `64f592a2…`, full and partial.
  A's domain rejects requests, and B fails in its first round with
  `Error during RPC (replication_offsets): … domain request to 0: failpoint`.
  B stays failed after the fault is cleared (38 checks).
- `fixed_matrix2/`: fixed failure-injection build `cf852da2…`, 200/200 checks:
  - `rejects_requests` (full/partial): A is stopped, then A's native domain rejects every
    packet and request. While the fault is active, B passes 3 fresh local duplicate-bag rounds,
    accepts a new `CREATE CACHE` (graph migration) and serves it locally. The engine is not
    globally failed. After the fault is cleared B continues, and A stays fail-closed and served
    from Databend.
  - `transient_crash` (full/partial): a 50,004-row A batch is applying when a one-shot
    failpoint makes A's next Input packet fail. The native replica exits, the controller runs
    `handle_failed_domain` recovery (log counts checked), and there is no `Controller failed`.
    A fails closed with `Table::input … Client was dropped`. B keeps syncing and migrating.
    In full mode the first attempt was inconclusive: the one-shot count was consumed by
    another domain and A finished normally. A second attempt with fresh databases passed.
    Both attempts are kept.
- `regression/`: 19/19 steps on the final sources: fmt and clippy `-D warnings` for
  readyset-databend, dataflow-state, readyset-server and readyset-client; Rust tests; sync WAL
  and reopen; live schema; release `682052d7…`; Python; native `replication_offsets` test
  (extended to compare the per-table RPC); native client/dataflow libs; multi full/partial;
  gateway full/partial/oracle; PK full/partial; bootstrap fault matrix 180 checks.
- `source.patch` is the exact delta against frozen pre-change copies. `sources.sha256` shows
  local and remote sources are identical.

## Native limitation (not changed)
`fixed_matrix/` keeps an earlier run in which the failpoint stayed active through domain
recovery. ReadySet's own recovery plan then failed (`Error applying domain migration plan`),
and the native controller failed and stopped the whole process, including B. This is
ReadySet's fail-stop policy for a background recovery that cannot complete. The adapter cannot
isolate it. Changing it means changing native controller recovery semantics.
