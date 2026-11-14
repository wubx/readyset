# 原生同步 WAL 与跨表独立同步：千万行对照（2026-11-14）

## 结论

**两轮规模测试各 51/51 项通过，最终 17 步回归全部通过。** 同样两张异构表各千万行、单原生 ReadySet server、3 GiB shared allocated 软预算、
5s max_staleness，B 小更新的写后新鲜同步等待从串行基线 **61.539s 降至 0.522s**，且 B 完成时 A 仍在应用。
A applying 窗口内 B 不变点探针 **282/282 本地返回**，没有因过期回源，最大同步年龄 **982ms**。
不是只保留了旧缓存：B 更新后的三行重复 bag 在 A 尚未完成时已能本地读到，B checkpoint 也持续推进。

最终 release 再跑相同规模，B 等待 **0.467s**、A applying 中 B 仍 **282/282 本地**、最大同步年龄 **892ms**，独立同步再次复现。

代价：A 的 400 万条变更批次 **62.829s → 73.626s / 73.734s**，墙钟约增加 17%；快照启动 **57.441s → 63.645s / 64.647s**。
**这是延迟隔离优化，不是所有吞吐都提升。** 新轮增加了 A 范围并发探针；两轮仍不足以给出性能置信区间，不能把差值全归因于 WAL。
历史基线完整保留在 [multi_scale_20261114](../multi_scale_20261114/README.md)，不改写旧结果。

本轮仍无 Router、每表引擎、替代 storage/replay、额外 SQL 参数化或 Databend helper 表。

## 原生机制与安全边界

核心改动只有启动策略和适配层锁粒度：

1. `src/engine.rs::start_native_with_wal_interval` 复用原有 builder；SharedEngine 选择原生 `wal_flush_interval_seconds=0`。
   原生 `dataflow-state/src/persistent_state/mod.rs::write_to_db` 在没有后台 WAL flusher 时设置 `write_options.set_sync(true)`。
   `readyset-server/src/worker/replica.rs::handle_one_packet` 成功处理基表输入后才发送 Table ACK。
   因此每个 row RPC 和最终本表 offset RPC 的成功 ACK，是本路径的原生同步 WAL 证据，不把后台模式 ACK 冒充刷盘。
2. 删除跨来源的**全批次写锁**。各来源持自己的 Table mutex 保序；其他来源的非空/空批次、offset 可并行推进。
   查询 checkpoint 克隆原生 controller client，不持有 adapter handle mutex 跨 RPC 等待；没有新 server。
3. checkpoint 在获取本表 Table ownership **之后**校验，避免同表两个 begin 都先通过旧 offset 检查。
   最终核对本表精确 offset；不使用另一来源 sequence 的 global min 来推断本表 durability。
4. 整个批次保留共享 migration guard，图迁移/快照持排他 guard；不允许迁移跨越未完成批次。
   同表 read gate 仍从首次变更持有到完整批次的 durability/物理 reader fence 完成，A 半批次不暴露。
5. 未完成批次只 poison 对应来源；已进入原生操作的图变更取消仍全局 fail closed。迁移排队前取消无原生副作用。
6. **legacy CLI/declarations/oracle 仍走原来的 1s 后台 WAL 与 global Persisted barrier**，未用新证明替换其行为。

没有修改生产原生存储/WAL/replay 算法。`dataflow-state/src/persistent_state/mod.rs` 此次仅增加 `#[cfg(test)] mod sync_wal_tests`。
实际优化及测试相对上一轮已验证源码的精确补丁见 `optimization-vs-validated-baseline.patch`；三个被编辑的既有适配层文件，
其修改前内容均逆向重建并与旧 `source.sha256` 的摘要精确匹配，而不是拿 dirty HEAD 差异冒充本次 diff。

## 同规模试验

远端：`wu@192.168.191.96:/data1/vmbox/readyset-single-binary`，Databend `http://192.168.191.96:8000`。
仅远端编译/测试；Rust/Cargo 1.99.0，`--locked -j 8`，规定 PATH/LIBCLANG_PATH/CARGO_TARGET_DIR。

新建并保留：

- `readyset_gateway_multiscale_20261114_131810_32f153_a.items`，String/Int64/Nullable Decimal(18,2)/Boolean/String，**10,000,002 行**。
- `readyset_gateway_multiscale_20261114_131810_32f153_b.items`，UInt64/UInt32/Nullable String/Int16，**10,000,003 行**。
- 每表一个 `readyset_ms9177f95d3631_items` Stream，无辅助表；两个同名异构表分别绑定。

与基线相同：六个 partial caches、8 并发交替压力 1,400 次、B 偏斜压力 600 次、每次 6,000 行、upquery timeout 10s、poll 300ms，
突发间避开 60s HTTP tombstone 生命周期。A 更新 200 万行，捕获 400 万条 before/after 变更；B 在观察到 A applying 后更新三行。
新增 A 范围探针，核对完整旧/新 range bags；仍保留 A 点、B 不变点及 B 三行重复 bag 探针。

| 项目 | 原串行基线 | 原生同步 WAL/并行 |
|---|---:|---:|
| B 写后启动的新鲜同步轮等待 | 61.539s | **0.522s** |
| B 新数据在 A 完成前可本地读取 | 否 | **是，三行 bag 精确一致** |
| A applying 中 B 不变点探针 | 19 local / 193 stale upstream | **282 local / 0 stale upstream** |
| 该 B 探针最大同步年龄 | 61,353ms | **982ms** |
| A 400 万变更批次 total | 62.829s | 73.626s |
| A SQL 完成到写后新鲜轮完成 | 63.434s | 73.987s |
| B 小批次自身 total | 102.123ms | 174.928ms |
| A/B 交替压力 | 1,400/1,400 本地 | 1,400/1,400 本地 |
| 仅 B 压力 | 600/600 本地 | 600/600 本地 |
| 两表淘汰后 miss → replay → hot | 均通过 | 均通过 |
| RSS 高水位 | 5,301 MiB | 5,255 MiB |

新轮所有并发探针 **1,048/1,048** 为完整允许的旧/新 bags。A 点和范围各 227 次（13 local / 214 upstream）；
B 点和三行 bag 各 297 次，均本地。筛选 A applying/B 非 applying 的不变点样本为上表 282 次，本地 p50/p95 **1.695/4.044ms**。
这些计数不是两个不同试验的等时长吞吐比较。

250ms health 遥测还记录到：A applying 的 **285 个样本**中，B checkpoint **120 → 261**，最大同步年龄 930ms。
health 采样与具体请求时刻不同，因此与探针的 982ms 不要求相等；都远低于未调整的 5s 上限。

### 最终 release 完整复跑

在新增仅 cfg(test) 的原生存储退出测试并完成 17 步矩阵后，用最终 release **重新完整执行**两表各千万行，避免不同二进制摘要混用：

- release SHA256：`5156aea1f4d372110d8ca75d23588290a81678d439a9837980c63574305a7ba0`。
- 原件 `/data1/vmbox/readyset-multi-parallel-final-scale-20261114-134625/`，本地 `final-scale/`，**51/51** 通过。
- 新建并保留 `readyset_gateway_multiscale_20261114_134625_4034f5_{a,b}`，分别 10,000,002 / 10,000,003 行；实例 `ms713e94c6de30`。
- B 写后新鲜轮 **0.467s**，完成时 A 仍 applying；B 更新后的重复 bag 已能本地读取。
- A applying 中 B 不变点 **282/282 本地**，p50/p95 **1.504/5.048ms**，最大同步年龄 **892ms**，stale upstream=0。
- A 400 万变更批次 **73.734s**；全部并发探针 **1,052/1,052** bags 正确，1,400+600 压力全部本地，淘汰/replay/重复行仍通过。
- 最终 allocated 约 **3,023.6 MiB**，RSS 高水位 **5,638 MiB（5.51 GiB）**；不把首次较低 RSS 当成上限保证。
- `source-final.sha256` / `source-final-verification.txt` 将最终编译/测试源码与本地 **51/51 个文件**精确绑定，版本差异不按 HEAD 猜测。

### 共享预算仍然有效（首次优化规模轮）

- 1,400 次交替压力 **25.486s**，端到端 p50/p95 **125.078/173.960ms**；600 次 B 压力 **16.802s**。
- 两表此前已读 `[100,160)` 范围均实际 miss +1，本地 replay **39.677/46.982ms**；再读 hot **17.524/17.490ms**、miss +0，与 Databend bags 一致。
- B 偏斜前 A/B range key slots **16,170/15,800**，偏斜后 **11,046/38,431**；A 仍可被 B 的共享预算压力挤出。
- 最终 allocated **3,051.2 MiB**，250ms 采样峰值 **3,082.9 MiB**；RSS 高水位 **5,255 MiB（5.13 GiB）**。
- 内存目标没有改大，也没有改成每表预算。epoch 是通知编号，不是淘汰行数；allocated 软目标不是 RSS/cgroup 硬限额。

## 回归与持久化前提

- `src/shared_engine_concurrency_tests.rs` full/partial：保持 A 未完成，B 非空和空批次在 5s 超时内完成；
  校验本表不同 checkpoint、A 不可读/B 可读、同源重复 sequence 拒绝、迁移等待期间取消无副作用、放弃 A 后 B 继续推进。
- `dataflow-state/src/persistent_state/sync_wal_tests.rs`：子进程两份独立原生 PersistentState，A 分页写后保持旧 offset，
  B 分页修改重复 bag 并写 offset=7，然后 `process::exit(0)` **不运行状态/RocksDB 析构**；父进程重开核对行数/重复数和不同 offset。
  此测试已通过；它验证不依赖清理析构，不是 OS/硬盘断电模拟，更不是产品崩溃续接支持。
- 既有 Databend native offset/rows close-reopen 回归通过。
- 规模前 Rust library **33 passed / 1 opt-in ignored**；严格 Clippy/fmt 已通过。
- **最终 17/17 步矩阵全部通过**，见 `regression/report.json`：shared HTTP full **151**、partial **138**；legacy full/partial/oracle **各 805**；
  duplicate/key-change full/partial **各 518**；Rust library **33**、M1 **4**、config **2**、opt-in live schema **1**；Python **45**；
  native client **150**、native dataflow **242**；native 同步 WAL 无析构退出 **1**、close/reopen **1**。严格 Clippy/fmt/release 均通过。
- 最终构建配当前脚本默认参数的 100K/表 smoke **67/67**，见 `default-smoke/`；只验证 CLI/真实链路，不拿它充当千万行压力证明。

首轮新单测使用 `vec![TypedChange;2]`，因该类型没有 Clone 而编译失败；已改为独立构造两个 change，未修改生产类型。
保留 `build/initial-test-construction-error.log`，后续全部 library/build 均成功，不抹掉失败记录。

## 证据与复现

- `summary.json`：新旧对照、checkpoint 推进、内存与探针汇总。
- `main/report.json`、`main/health.jsonl`（1,406 个样本，无监控错误）、`main/rss.csv`、`main/update_traffic.json`：首次优化规模结果。
- `final-scale/`：最终 release 的第二轮完整规模结果（1,404 个 health 样本，无监控错误），含冻结的实际执行脚本。
- `main/queries.jsonl`、`main/source_queries.jsonl`、`main/final_status.json`、日志/配置：路由、源端事务/Stream 审计。
- `main/benchmark.py`：规模轮实际运行脚本；当前脚本之后仅把独立同步检查改为默认开启，并支持 `--no-expect-independent-sync` 测历史模式。
- `build/`：构建及单测日志；`scale-build-source.txt`：规模实际源码/二进制摘要；后续新增 cfg(test) 模块不混称为原规模源码。
- `optimization-vs-validated-baseline.patch`：当前运行时与新增 native 测试相对已验证基线的精确 diff。

远端原件：`/data1/vmbox/readyset-multi-parallel-scale-20261114-131810/` 及同名 `.log`。
该目录下 `readyset-databend.validated`（`873296c1...`）冻结首次规模执行的二进制；最终 release 完整复跑另见 `final-scale/`。
原串行 release 也保留为 `/data1/vmbox/readyset-multi-scale-baseline-f3bbed15908c`。

```bash
cd /data1/vmbox/readyset-single-binary
python3 -B readyset-databend/poc/multitable_scale_bench.py \
  --output /data1/vmbox/readyset-multi-parallel-scale-<new-timestamp> \
  --rows 10000000 --memory-limit-mb 3072 \
  --heat 1400 --skew-heat 600 --range-width 60 \
  --max-staleness 5 --settle 15 --expect-independent-sync --confirm-create
```

所有测试数据库/Stream 保留，未清理；未提交/推送，未替换/重启 8001。
仍无资源公平性/硬实时保证：图迁移排队、共享磁盘/CPU/内存争用仍可能使其他表过期；每表 staleness 回源策略不变。
未做 cgroup 硬限额、断电/主机崩溃、性能置信区间、两表同时大批量更新或十张以上表压力；A 自身读 gate 仍需等待完整批次和 fence。
