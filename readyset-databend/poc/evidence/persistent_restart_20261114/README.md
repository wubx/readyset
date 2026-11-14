# 重启持久化：真实 Databend 重启恢复验证（2026-11-14）

**结论：** 5 万行规模下，full、partial 各 6 次启动，192/192 项检查通过。正常重启两表都续接；
COMMIT 响应丢失后续接；COMMIT 未执行时 SIGKILL、以及批次中途 SIGKILL，都只重建 A 表，B 表续接。
每次启动后，本地结果与 Databend 完全一致，包括三行重复 bag、普通行和两表行数。

## 机制

只用 ReadySet 原生持久化，未修改原生代码：
- standalone authority 保存 recipe（基表和缓存）；
- 基表 RocksDB 使用同步 WAL；
- 每表的重启标记写在原生 replication offset 里。

规则见 [USAGE.md](../../../USAGE.md#5-重启与持久化) 和 [MULTITABLE.md](../../../MULTITABLE.md)。

## 环境与步骤

- Databend `http://192.168.191.96:8000`。只新建 `readyset_gateway_restart_*` 库，全部保留。
- A 表：String 键，50,004 行，含 3 行完全相同的 `bag`。B 表：UInt64 最大值键，3 行完全相同。
- 每种模式共用一个 `--output` 目录。Databend 流量经过本机回环故障代理，代理只作用于 A 的 capture 事务。
- release `readyset-databend` sha256 `5b61887b…`。构建时的源码与提交版本仅差 rustfmt 空白调整。

| 第几次启动 | 注入 | full：A / B | partial：A / B |
|---|---|---|---|
| 1 | 首次启动 | created / created（1.77s） | created / created（1.38s） |
| 2 | SIGTERM；停机时两表写入 | resumed / resumed（1.63s） | resumed / resumed（1.23s） |
| 3 | A 的 COMMIT 执行后丢弃响应；A 运行中停止、B 继续；SIGTERM | resumed / resumed | resumed / resumed |
| 4 | A 的 COMMIT 被拦截从未执行，SIGKILL | rebuilt（未提交批次）/ resumed | 同左 |
| 5 | A 写入 10 万条变更途中（applying）SIGKILL | rebuilt（未提交批次）/ resumed | 同左 |
| 6 | SIGTERM 后启动 | resumed / resumed | resumed / resumed |

括号里是就绪耗时。每次启动后还检查了：
- 缓存不需要重新 `CREATE CACHE`，`SHOW CACHES` 中 `a_id` 和 `b_id` 都在；
- 查询的 `X-ReadySet-Route` 为 `readyset`；
- 只有一个原生 server，每表一个 Stream，没有辅助表。

5 万行规模下，新建、续接、重建都很快，这组数据不能说明续接节省的时间。千万行对比尚未完成。

## 其他验证

`rust_restart_test.log`：`shared_engine_restart_tests.rs` 中每次启动都是独立子进程，full 和 partial 均通过：
- 正常重启后，行、缓存和标记都保留；
- 批次中途 `abort()`（不跑析构）后，Stream 未动时判定重建，Stream 已前进时判定续接；
- 续接后再次重启，判定仍为续接。

## 注意

- 用 1,000 行试跑时第 5 步失败：A 的 2,000 条变更在脚本观察到 applying 之前就写完了，是测试数据量不够，不是产品问题。
- 本目录只提交 README、`report.json`、控制台日志和 Rust 测试日志。原始服务端日志保留在远端
  `/data1/vmbox/readyset-persist-build-20261114-162944/restart_50k/`。

## 文件

- `report.json`：全部检查和每次启动的 recovery、耗时与行数。
- `console.log`：验证脚本输出。
- `rust_restart_test.log`：子进程重启测试输出。
