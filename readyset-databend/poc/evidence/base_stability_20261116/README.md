# 单实例基础稳定性：本地读上限、转发原因、事件日志、按表 migration guard（2026-11-16）

所有测试都在真实 Databend 上运行（`http://192.168.191.96:8000`，v1.2.962，对象存储是同机的 rustfs）。
只新建 `readyset_gateway_pointq_*`、`readyset_gateway_cddl_*`、`readyset_gateway_isolation_*` 库，全部保留，共享实例的配置没有修改。
压测器、Databend query、rustfs 和 readyset-databend 都在同一台 32 核机器上，互相争抢 CPU。

被测程序（`binaries.sha256`）：

| 程序 | sha256 | 用途 |
|---|---|---|
| 本轮 release | `aefb44384e28…` | 双表建缓存验证、完整回归 |
| 本轮故障注入版 | `281e263f2362…` | A/B 隔离矩阵 |
| 中间版本 | `9c601598b066…` | 只含第 1 项修复，还是全局 migration 锁；点查压测，以及建缓存验证的反向对照 |
| 上一轮故障注入版 | `b3cabf03b9c7…` | 隔离用例对照 |
| `databend-loadtest` | `fb0db62f3b1f…` | 点查压测器 |

## 修复内容

1. **本地结果暂存表不再限制本地读的速率**
   - 以前收到 `/final` 后，记录会换成一个占位标记，继续占用 2,048 条名额中的一条，保存 60 秒。持续压力下，每 60 秒最多约 2,048 次本地读，超出的全部转发。
   - 现在收到 `/final` 或 `/kill` 后立即释放。客户端自带的查询 ID 记在另一张有上限的表里（65,536 个，60 秒过期）。
2. **转发原因计数**：`/health` 里新增 `counters.forward_reasons`（全局和每张表各一份），每张表新增 `unavailable` 字段，同步后没有就绪的原因也会记录。
3. **事件日志**：表失败、重试、恢复、缓存 DDL、全局 fail-closed、启动和退出，都写到 stderr，同时追加到 `events.jsonl`。
4. **migration guard 按表划分**
   - 以前每一轮同步都要拿一把全局锁的共享端，`CREATE CACHE` 拿排他端，建索引期间整个实例的所有表都停止同步。
   - 现在批次只拿本表的共享锁，缓存 DDL 只拿目标表的排他锁。加锁顺序统一为：本表 migration 锁，然后本表 Table 锁。
5. **`CREATE CACHE` 的前置条件**：从"此刻数据新鲜"放宽为"首次加载已完成、而且没有失败"，连续建多个缓存不再被拒绝。

## 1. 点查对比：Databend 原生和 ReadySet（`pointq/`）

数据是 1,000 万行，同一套生成规则：
- `id`：序号；
- `ext_id`：`sha2(id)` 的前 24 位，唯一，非 cluster key；
- `cust_id`：`id % 1,000,000`，每个值 10 行。

Databend 测了 4 种表配置（`prep_explain.log` 里有建表语句和剪枝结果）：

| 编号 | 配置 |
|---|---|
| d0 | 行顺序打乱，全部默认配置 |
| d1 | `CLUSTER BY (id)` |
| d2 | d1，加上 `row_per_block=100000`、`binary_fuse32`、bloom 只建在三个键列上 |
| d3 | d2，再加上 `PARTITION BY BUCKET(64, ext_id)`。测试前执行过 `RECLUSTER FINAL` |

全部采用闭环压测，每轮预热 5 秒，正式测 30 秒。下表是各配置在 1–256 并发里的最高 QPS，以及当时的 p99：

| 查询 | Databend 最优（d2） | ReadySet，键均匀分布 | ReadySet，热点 10 万键 |
|---|---:|---:|---:|
| `id = ?` | 385 QPS（p99 195 ms） | 16,753（p99 6.3 ms） | 18,077（p99 1.4 ms） |
| `ext_id = ?` | 331（p99 210 ms） | 15,663（p99 22 ms） | 17,889（p99 1.4 ms） |
| `cust_id = ?`（10 行） | 52（p99 408 ms） | 11,023（p99 51 ms） | 15,420（p99 1.7 ms） |

**Databend 一侧**
- 现有配置下，吞吐在并发 16 时就到顶，大致等于 8 ÷ 单次耗时，和 `max_running_queries=8` 吻合。`system.processes` 的采样逻辑没有生效，所以没有直接观察到排队。
- `cust_id` 的 10 行分散在 10 个块里，是最慢的一种查询。
- BUCKET 分区（d3）没有带来收益。

**ReadySet 一侧**
- 修复后共 27 轮，本地返回 12,633,478 次，转发 10 次。这 10 次都发生在开头的正确性抽查阶段，原因是 `capture_slow` 9 次、`stale` 1 次，也就是第 4 项问题。
- 60 次正确性抽查逐行比对值，全部和 Databend 一致。
- 缓存内存（allocated）4,091 MiB，软上限 4,096 MiB；进程 RSS 5.04 GiB；`engine/` 3.4 GB；回填超时 0 次。
- **修复前**的同类测试（`databend_and_prefix_readyset.jsonl` 中标签以 `readyset-` 开头的行），本地返回呈"约 2,048 次，然后几乎为 0"的周期，其余全部转发。这正是第 1 项问题。

## 2. 双表建缓存验证（`cache_ddl/`，脚本 `poc/cache_ddl_isolation_verify.py`）

A 表 1,000 万行，B 表 1,000 行，同一个实例。每 0.5 秒在 Databend 上更新一次 B，同时持续通过 ReadySet 读 A 和 B。先在 A 上连续执行 3 次 `CREATE CACHE`，再执行 1 次 `DROP CACHE`。

| 指标 | 本轮（两次运行） | 中间版本（反向对照） |
|---|---|---|
| A 上的 3 次 `CREATE CACHE` | 全部成功（17.0 / 11.0 / 13.1 秒和 17.0 / 11.0 / 15.1 秒） | 第 1 次成功，后 2 次被拒绝（`source is not ready`） |
| 建缓存期间 B 的本地命中率 | 98.1% / 98.1%，和建缓存前一样 | 67.0% |
| 建缓存期间 B 不可读的原因 | 只有 `source_applying`（B 自己的批次正在写入） | `stale`，出现 38 次 |
| B 的最大同步间隔 | 1.31 / 1.29 秒 | 17.6 秒 |
| B 写入后多久在本地可见 | 中位数 0.63 / 0.62 秒，最大 1.17 / 1.20 秒 | 中位数 9.7 秒，最大 18.1 秒 |
| A 自己 | 建索引期间有部分读转发（预期内），结束后 100% 回到本地 | 相同 |
| 最终正确性 | B 的行、`a_ext`、`a_cust` 都在本地返回，和 Databend 一致 | 只用来证明会卡住 |

目标表在原生建索引期间仍然会暂停同步：同一个 base domain 的写入只能排队。实测约每百万行 1.1–1.7 秒，已写进 USAGE.md。

## 3. 完整回归（`regression/`）：20/20

覆盖 fmt、clippy、Rust 测试（42 + 4 + 5，其中有新增的断言："A 的缓存 DDL 在排队等待时，B 的批次能在 5 秒内完成"）、
原生库测试、多表、网关、PK、重启恢复，以及 bootstrap 故障矩阵。

## 4. A/B 原生 domain 故障隔离（`isolation/`）：247/247

`isolation3.json` 是最终结果，4 个用例都在第一次尝试就通过。

**排查过程**
- 前两次完整运行（`isolation.json`、`isolation2.json`）各有一种 `transient_crash` 用例连续 3 次尝试都没有结论。原因是测试工具：A 的 domain 一次都没有失败，A 的批次也完整提交了。
- `fail` 库的一次性 failpoint 会被**任意 domain** 的下一个包消耗掉，包括其他 domain 空闲时发的 Spin 包，只有当这个包恰好属于 A 时才会让 A 失败（见 `Domain::check_failpoint`）。
- 新、旧两个故障注入版单独复测这个用例（`partial_transient_new.json`、`partial_transient_old.json`），都在第一次尝试就通过。
- 脚本改成：A 还在写入时反复重新设置一次性 failpoint，A 一失败就立即关闭，避免残留的 failpoint 之后误伤 B。改完以后 4 个用例全部第一次通过。

## 仍未覆盖

- 后台持续写入的大表上做高并发冷读（第 3 项：冷读回填期间持有本表 gate 的读锁，可能和批次写入互相阻塞）。
- JDBC（databend-jdbc）兼容性。
- 吞吐到顶的原因：服务进程在约 7–10 核时吞吐就不再增长，压测器也跑在同一台机器上，这一轮没有区分开。
- 真实断电、长时间稳定性测试、亿级单表。
