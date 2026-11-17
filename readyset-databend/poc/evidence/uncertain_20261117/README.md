# 结果无法确认的转发：自动恢复（2026-11-17）

所有测试都在真实 Databend 上运行（`http://192.168.191.96:8000`，v1.2.962，单个 query 节点）。
只新建 `readyset_gateway_unc_*` 和 `readyset_gateway_jdbc_*` 库，全部保留。

被测程序（`binaries.sha256`）：

| 程序 | sha256 |
|---|---|
| readyset-databend release | `7aafdb122b7a…`（完整回归中重新构建，逐字节相同） |
| databend-jdbc-0.4.8.jar | `274597a199f2…` |

## 问题

修改前，结果无法确认的转发会把 `uncertain_upstream` 置为真。代码里没有清除它的地方，整个实例从此全部转发，只能重启恢复。

用上一版 release（`0ec4ddb6…`）复现：
1. 发一条 `{"sql":"SELECT 1","session":{"role":"no_such_role"}}`；
2. 之后的缓存点查一直是 `databend`，35 秒后仍是；
3. `/health` 显示 `uncertain_upstream: true`。

能触发的情况有：
- Databend 在创建查询之前就拒绝了请求，返回 HTTP 200、查询 ID 为空、状态 Failed；
- Databend 返回 5xx；
- 转发超过固定的 15 秒超时；
- 响应无法解析；
- 第一次响应是 `Running`，之后客户端不再轮询（这种情况记为 `upstream_inflight`，同样永远不清除）。

## 修改（`src/bin/server.rs`）

- **执行前被拒绝的请求不算不确定：** 条件是响应的查询 ID 为空、状态 Failed、带有错误对象。对照 Databend 源码，这只出现在 `HttpQuery::try_create` 或 `start_query` 失败的时候，都发生在查询开始执行之前。
- **每个被跟踪的转发都有查询 ID：** 客户端自带的 `X-DATABEND-QUERY-ID` 原样保留，但必须是字母、数字、`_`、`-`，最长 128 个字符，否则返回 400，不转发。没有的话，本服务生成一个 `rs_fwd_<uuid>`。
- **按 ID 记录、可以恢复：**
  - 结果无法确认的语句，按查询 ID 记入 `unresolved`；`Running` 的语句仍记在 `inflight`，同时记下开始时间。
  - 后台任务 `resolve_upstream` 每 2 秒检查一次，只检查已经等了足够久的 ID：至少 10 秒，且不少于 `forward_timeout` 的两倍。检查方法是到 `system.processes`（整个 warehouse 范围）按 `current_query_id` 查找。
  - 不在运行的 ID 就移除，再把全局 epoch 加一，等每张表再完成一轮同步，本地读才恢复。这和普通转发语句结束后的处理一样。
  - 查询 Databend 失败时，记一条 `upstream_resolve_failed` 事件，下次再查。
  - 无法确认的语句超过 2,048 条，或 ID 不能安全地放进 SQL，仍按原来的方式永久转发（`unresolved_overflow`）。
- **转发超时可配置：** 新增 `[upstream] forward_timeout`，默认 30 秒，取值范围 5–600 秒。
- **可观测性：**
  - `/health` 新增 `unresolved_upstream` 数量，以及计数 `counters.unresolved` 和 `counters.resolved`；
  - 新增事件 `upstream_resolved`。

运行中的查询能按 `current_query_id` 在 `system.processes` 里查到，结束后就查不到，即使结果还没取走、也没调用 `/final`。这在 Databend 上实测确认过（13 秒的聚合查询）。

新增单元测试：
- `unresolved_outcomes_are_tracked_by_id_and_bounded`
- `only_a_failed_start_without_a_query_is_rejected_before_start`
- `query_ids_are_restricted_to_quotable_characters`

## 恢复验证（`poc/uncertain_recovery_verify.py`，`recovery/report.json`）：24/24 通过

配置为 `forward_timeout = 5s`。慢写入是一条 `INSERT ... SELECT`，里面的子查询要算 600 亿行，在 Databend 上约 13 秒。
每个场景里：
- 一个线程每 200 ms 经本服务读一次要插入的那个键；
- 另一个线程直连 Databend，记录这一行实际提交的时间。

| 场景 | 本服务的响应 | 结果无法确认期间 | Databend 提交（从发送算起） | 本地读恢复（从提交算起） | 提交后漏读 |
|---|---|---|---|---|---|
| 执行前被拒绝（角色不存在） | 原样返回 Databend 的错误 2206 | 不进入不确定状态，本地读继续 | — | — | — |
| 本服务等待超时（`wait_time_secs=30`） | 5.0 秒后返回 502 | 92 次读里本地 0 次 | 13.41 秒 | 4.09 秒 | 0 |
| 返回 Running 后客户端不再轮询 | 1.0 秒后返回 `Running` | 85 次读里本地 0 次 | 13.47 秒 | 2.54 秒 | 0 |

- **"本地 0 次"：** 指本服务回复之后、写入提交之前这段时间。
- **"漏读"：** 指 Databend 已经提交之后，本服务的读却没返回这一行。
- **结束状态：** 两个场景结束时 `unresolved` 和 `inflight` 都是 0；两个慢写入的行都在本地返回，和 Databend 一致；本地行数和 Databend 一致。

## JDBC 读写混用（`jdbc_w0/`、`jdbc_w1000/`、`jdbc_w100/`）：三轮都是 22/22

64 个读线程，外加 1 个写线程在同一地址写入。写线程每次插入和更新后，都在同一连接立刻读回。

| 写入频率 | 读 QPS | 读 p50 | 读 p99 | 本地比例 | 读己之写检查 |
|---|---|---|---|---|---|
| 无写入 | 5,787 | 10.7 ms | 20.1 ms | 99.70% | — |
| 每秒 1 次（实际 30 次） | 5,370 | 10.4 ms | 136.1 ms | 98.53% | 60 次，全部通过 |
| 每 100 ms 1 次（实际 98 次） | 5,457 | 10.3 ms | 134.5 ms | 98.45% | 196 次，全部通过 |

结果和上一版（`../jdbc_20261117/fence_*`）一致：
- 会话隔离用例通过；
- 转发原因仍以 `source_applying` 为主，即同步批次写入本地期间；
- 三轮都没有 `uncertain_upstream` 和 `upstream_inflight`。

## 回归

`regression/report.json`：完整回归 20/20 通过，包括 fmt、clippy、Rust 测试、多表、网关三种模式、主键变更、重启恢复和 bootstrap 故障矩阵。

## 仍有的限制

- **超出确认窗口的延迟请求：** 如果一个请求在本服务判定失败之后、经过了"至少 10 秒，且不少于 `forward_timeout` 两倍"的时间，才被 Databend 接收并开始执行，那么确认时它还不在 `system.processes` 里，会被误判为已结束。
- **跨 warehouse：** 确认只查一次 `system.processes`，范围是收到这条确认查询的那个 warehouse。如果 `[upstream] url` 后面的负载均衡会把请求分到不同的 warehouse，就可能查不到原来那条语句，从而误判为已结束。
- **多节点：** 只在单个 query 节点上验证过。
