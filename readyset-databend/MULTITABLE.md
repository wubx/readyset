# 多表与真实 schema：共享 HTTP runtime

## 原则与当前范围

一个进程、一个原生 ReadySet server/controller、同一套 shared readers / ReadRequestHandler 和进程 allocated 软预算。
不按表启动 Engine，不增加 Router、Databend 元数据/helper 表，不重写参数化、存储、索引、replay 或淘汰算法。
TOML `replication.tables` 已接入共享运行时，单项列表也使用真实 schema。
旧 CLI `--database/--table`、`--declarations` 和 oracle 路径保留，不与 TOML 列表混用。

支持静态配置来源和动态公共缓存，重启复用本地持久化数据（见 [USAGE.md](USAGE.md#5-重启与持久化)）；
不支持在线 ADD/DROP TABLE、自动 DDL 迁移、本地 JOIN 或 JDBC。
仍是实验组件，不以功能接通代替生产 SLA、最小权限或 cgroup/RSS 硬限测试。

## schema 与来源绑定

`src/source_schema.rs`：

- 从 DESCRIBE 的有序列名、类型、nullability 生成原生 DDL / DfType / DfValue。
- 不依赖 id/payload/score/note，也不推断业务键/唯一约束；完整 before image DELETE 与重复行保持 bag semantics。
- (database,table) 精确绑定到稳定 opaque 本地 relation；相同表名的不同库不冲突。
- 未限定 FROM 仅按请求 session.database 查找，不猜 default，不忽略显式 catalog。
- 精确 allowlist：Boolean、Int8/16/32/64、UInt8/16/32/64、String、Decimal(p,s)（1<=p<=28），及 nullable。
- 暂不接受 Float/Double、Date/Timestamp、Decimal 精度>28、Variant/Array/Map/Tuple/Binary；不能偷偷转 String/f64。
- 校验列数、非空、整数范围和 decimal precision/scale；字符串使用二进制 collation。
- HTTP Boolean 的 string fields 实际是 "1"/"0"；整数/decimal 不经过浮点，输出带真实列名/类型。

`src/native_query.rs` 的 compile_for_source 复用原生 parse/rewrite/slot matching/read_cache；
schema/context 只在发现时解析。SQL 仍限单底表普通投影、AND 等值/IN/一个数值范围；未知语法和 JOIN 回源。

## 共享 runtime 与每表 adapter

```text
server.rs：现有 HTTP auth/session/分页/lifecycle/透明代理
  ├─ server/shared.rs：每表 Replicator task + 同步状态/错误处理
  └─ SharedEngine：一次 start_native（与旧 Engine 共用 builder 启动代码）
       ├─ 一个 Handle / shared readers / handler 克隆池 / cache budget
       ├─ 原生同步 WAL；各表 Table 顺序写入；仅快照/图迁移排他
       ├─ 来源 A：schema + 原生 Table + 重启标记 + read gate + failure flag
       └─ 来源 B：schema + 原生 Table + 重启标记 + read gate + failure flag
```

`src/shared_engine.rs`：

- 原生 server 使用 ReadySet 自带的 `StandaloneAuthority` 与固定存储目录：recipe（基表、缓存）和基表 RocksDB 在重启后原样恢复。
  启动时只为缺失的来源创建 native DDL。
- 上游位置只由每张表的 Databend Stream 记录。本地用一个**重启标记**说明本地行与 Stream 位置的关系，
  写在该基表自身的原生 replication offset 里（`src/restart_marker.rs`，不修改原生类型）：
  `Applied(v)` 本地等于 Stream 版本 v；`Applying(p)` 在 p 之上有未完成批次；`Rebuilding` 快照加载中。
  版本取 `system.streams.table_version`（源表 meta seq），并记录 `stream_id`：Stream 被删除重建后不信任旧位置，整表重建。
  标记和行走同一个有序 Table 通道，同步 WAL 落盘。
- 每批：首行写入前记 `Applying`，全部 Table RPC 确认、reader fence 完成后 COMMIT，再读 Stream 版本记 `Applied`。
  空批次 ROLLBACK（不动 Stream 和标记），最多每 60s 提交一次空批次，让 Stream 位置跟上源表。
  同表批次顺序由 Table mutex 和进程内序号保证，不再读回原生 offset，因此 A 的 domain 故障不会拖住 B 的批次。
- SharedEngine 选择 ReadySet 原生 `wal_flush_interval_seconds=0`：`PersistentState::write_to_db` 使用同步 RocksDB WAL，
  `Replica::handle_one_packet` 在成功处理基表后才 ACK。崩溃或进程内 domain 恢复后，已确认的行和标记都在。
- 各表有自己的 Table mutex，持有完整批次以保证同表顺序；序号在获得 Table ownership **之后**校验，拒绝并发同序号重复批次。
  不持有跨表全批次写锁，不自建 flush/replay/storage。
- 缓存声明（名称、库、原 SQL）写入 `caches.json`（临时文件 + fsync + rename）。原生 recipe 保存缓存本身；
  启动时删除没有声明的原生缓存，按声明重建读路由。CREATE 先建原生缓存再记声明，DROP 先删声明再删原生缓存，
  任一步崩溃都只会留下下次启动会清理的原生缓存。
- 批次完整生命周期持共享 migration guard；快照和图变更持排他 guard，不允许迁移跨越未完成批次。迁移排队仍可能延迟新的同步轮。
- 缓存读不拿 migration guard；A 的 batch gate 不排除 B 的读。空批次不抢该表读 gate。
- 旧单表/声明/oracle 启动仍选择原来的 1s 后台 WAL 和 global Persisted barrier，不改其语义。
- 实际数据变更期间禁用该表本地读，直到持久化和该表全部 reader 的内容 fence 完成。
- 未完成/取消的批次 fail closed 该来源，随后该表的 worker 按 `retry_backoff` 退避（翻倍，最长 5 分钟），用新的 Databend 会话
  重走"发现 schema → attach → 读 Stream 身份/版本 → 续接或只重建该表"。单表恢复不取 migration 排他锁：
  其图变更（删建本表、重建本表缓存）只由原生 handle 锁与本表 Table 锁串行，其他表的批次照常进行。
  图变更结果不确定（含请求 future 取消）仍全局 fail closed，此时进程主动退出（非零退出码），由进程守护按持久化状态重启。
- 并行复用原生 Table 写入，不代表 CPU/磁盘/内存公平隔离；图迁移或资源争用仍可能推迟 B。超过自身 max_staleness 仍回源。
  同步 WAL 每页都落盘，有吞吐成本；不是分片或硬实时 SLA。

## Stream 事务与 schema 漂移

复用现有 Replicator / DatabendClient / Pages / 心跳 / session 事务实现：

1. 每来源 prepare_schema；类型通过后创建/复用 `<db>.readyset_<instance_id>_<table>`。
2. AT Stream 固定快照分页写调用者的 Table，核对总行数，持久化后复核 schema。
3. 消费前 DESCRIBE；BEGIN，完整消费 WHOLE Stream，不过滤、不 LIMIT。
4. 逐页 parse_typed_changes + push；只按完整相同行取消等量 insert/delete，绝不去重。
5. Pages.close 成功，复核 schema；本批所有 Table RPC 确认、`Applying` 标记已落盘、该表 reader fence 完成后才 COMMIT，
   COMMIT 后读 Stream 版本写 `Applied` 标记。
6. COMMIT 后再复核 schema，才发布本轮可读状态。错误 abort/ROLLBACK；不确定提交不盲目重试。

DESCRIBE 复核不是原子 DDL 订阅；外部 DDL 仍可在检查之间发生，不宣称强 DDL 协调。
观测到漂移即该表 Error/停止消费和本地读，不迁移旧 graph，其余来源继续。

## typed physical marker / eviction epoch fence

`src/engine_fence.rs` 为旧单表与新 runtime 共用算法；物理键/行映射复用 reader schema，不按固定四列猜索引。
只检查本表依赖的 readers，不借 B 的 marker 证明 A。

- 插 marker 前预热键并记录 canonical handle 的 eviction epoch，确认物理 marker 不存在。
- 插入后只用不触发 replay 的本地物理探测；hole/epoch 改变先清理 marker，再换新 marker 重试。
- 插入和删除两个阶段都被观察到，且 epoch 未改变，才开放本地读。
- 不能因为源行带 UUID 就假定投影后仍唯一。物理投影（含参数键）必须含 String、Int64/UInt64 或 p>=20 Decimal；
  纯 Boolean/小整数等有限域投影在迁移前明确拒绝。源表仍可同步和回源；不以模糊计数绕过屏障。
- Boolean/小域键、宽范围仍可能预热很多行；fence 不保证 O(1) 行内存，也不保证在任意小预算下能发布缓存。
- 不支持聚合/JOIN，因此不把此屏障冒充通用多输入 dataflow barrier。

## HTTP 配置、路由与状态

例子见 README 与 docker/readyset-databend.toml；配置最多 32 来源、全实例最多 8 个公共缓存。
重复来源（包括大小写折叠相同的配置）启动前拒绝，避免两个任务消费同一物理 Stream。
缓存名全局唯一，reserved `rs_source_` 前缀不能与底表冲突。cache_control::plan_with 共用原有 DDL 检查，编译绑定真实来源。

- `/health.tables[]`：table、stream、checkpoint、rows、ready、syncing、applying、last_sync_age_ms、snapshot、last_batch、failed。
- 多表顶层 checkpoint 为 null（单项列表保留该表值），不虚构跨表事务位点。
- `ready` / `/readyz` 只要求至少一张表符合可读条件；`all_tables_ready` 用于判断是否全部就绪。
- schema 不支持、捕获/应用/提交失败按表隔离；`/__fault/table` 仅 --enable-fault-injection 时可用。
- 证明为普通、类型兼容单表 SELECT 的未命中/失败表读不使 B 失效；分页生命周期单独跟踪且有容量/TTL 限制。
  未经证明的请求、写操作和 session 生命周期保留原有 conservative global forward-epoch/uncertainty fence。
- auth/session/header 检查与旧前端相同，不绕过角色、事务和 settings 检查；仍是一个配置账号，不是多用户权限隔离。
- 启动只打开原生存储（不需要 Databend）就开始监听 HTTP，各表在后台并行恢复（续接 / 新建 / 只重建该表）；
  未就绪的表读请求转发 Databend，`/livez` 始终可用。
  逐表判定规则与崩溃窗口见 [USAGE.md](USAGE.md#5-重启与持久化)，`/health.tables[].snapshot.recovery` 报告每表结果。

## 验证与剩余边界

普通测试不自动建库；live 测试需显式授权，只新建并保留 readyset_gateway_* 数据库。
所有编译/测试仅在 `wu@192.168.191.96:/data1/vmbox/readyset-single-binary`，使用规定 PATH/LIBCLANG_PATH/CARGO_TARGET_DIR、--locked -j 8。

- `src/engine_multitable_tests.rs`：基础层同 controller 异构表 full/partial（旧 loader 脚手架含旧空表，非新 runtime）。
- `src/shared_engine_tests.rs`：真正 SharedEngine，无额外 base；独立/不同 checkpoint、每表 gate、空批次读、原生 eviction 后 replay、零等待冷读只回源并在热后恢复、未完成 A 批次不破坏 B、取消图变更 fail closed。
- `src/shared_engine_concurrency_tests.rs`：full/partial 中保持 A 批次未结束，B 非空/空批次独立完成；同表并发重复 sequence 拒绝、等待中的迁移取消无副作用、放弃 A 后 B 继续推进。
- `src/bin/server/config_tests.rs`：单/多项 TOML 走共享路径，重复/歧义/越界配置启动前拒绝，legacy CLI 保持旧入口。
- `src/live_schema_tests.rs`：显式 opt-in 真实 schema/快照/Decimal UPDATE bags/rollback/reconsume/独立提交/漂移。
- `poc/multitable_verify.py`：HTTP 两库同名异构表、Boolean/Decimal/String/UInt64 最大值/NULL/重复行、冷/热/范围/分页、并发读写、
  100K 插入与 200K UPDATE 变更期间 B 本地读、错误表隔离、Stream 数量、重启语义。full/partial 分别运行。
- `poc/multitable_scale_bench.py`：两张异构表各千万行、共享 3 GiB allocated 软预算、两表真实淘汰后 replay、B 偏斜加热挤出 A，
  A 200 万行 UPDATE（400 万变更）期间 B 本地读/独立过期回源及 B 小更新同步延迟；按 bags 核对，保留源端数据库/Stream。
- 旧 full/partial/oracle HTTP 和重复键回归保持独立执行；不拿新多表测试替代旧兼容测试。

历史基础层证据见 `poc/evidence/multi_schema_20261114/`；本轮 HTTP full 151/151、partial 138/138 及原生回归证据见
[poc/evidence/multi_http_20261114/README.md](poc/evidence/multi_http_20261114/README.md)，不改写历史结果。
[多表各千万行串行基线](poc/evidence/multi_scale_20261114/README.md)：49/49 项通过，保留首版约 61.5s 的 B 同步延迟证据，不改写历史。
[原生同步 WAL/并行表优化](poc/evidence/multi_parallel_20261114/README.md)：同样两表各千万行、3 GiB 预算、5s staleness，两轮各 51/51 项通过；
B 写后新鲜轮 0.522s / 0.467s 且 A 仍在 applying，两轮 B 探针均 282/282 本地、最大同步年龄均不足 1s。
A 400 万变更批次从 62.8s 增至 73.6–73.7s；新版多了范围并发探针，差异不全归因于 WAL。
1,400+600 次压力、两表实际淘汰/replay 和重复 bags 均通过，RSS 高水位最高 5.51 GiB；最终 17 步兼容/native 回归全部通过。
尚无 cgroup/RSS 硬限额测试、真实断电测试、完整最小权限账户矩阵、任意 schema 迁移或本地 JOIN。
`src/shared_engine_restart_tests.rs`（子进程真实重启，含 `abort()`）和 `poc/restart_recovery_verify.py`（真实 Databend，
六次启动：正常重启、COMMIT 响应丢失、COMMIT 未执行时 SIGKILL、批次中途 SIGKILL）覆盖重启恢复。
[持久化版本验证](poc/evidence/persistent_validation_20261114/README.md)：完整回归 20/20；A/B 原生 domain 故障隔离
240/240（含故障后同目录重启）；千万行性能与上一版持平（B 写后 0.504s，A 写入期间 B 280/280 本地）；
两表各千万行重启 64.1s → 6.8s。
[单表自恢复与先监听启动](poc/evidence/self_recovery_20261114/README.md)：13 次启动重启/自恢复 394/394（含 Databend 不可用启动、
运行中 Databend 中断、Stream 重建、在线加列）；A/B 隔离 244/244（A 进程内自恢复，B 全程不受影响）；完整回归 20/20；
千万行 A 在线重建 50s 期间 B 15/15 轮本地且新鲜。
