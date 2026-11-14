# 同步 WAL / 每表 checkpoint / 恢复正确性 Review

## 范围与结论

以 `../multi_parallel_20261114/optimization-vs-validated-baseline.patch` 为本轮范围，追踪原生 ACK、存储、controller 和 Stream 事务调用者。当前源码与最终验证的 `source-final.sha256` **51/51** 匹配；不将整个 dirty worktree 当成本轮 diff。

未发现正常成功路径中提前 ACK、跨表 sequence 混作 durability，或同表重复 begin 通过旧 checkpoint 的缺陷。
发现一个条件性的异常重启一致性缺口、一个剩余故障隔离问题及一个 WAL 测试缺口；不据此声称真实 Databend 已发生数据丢失。两个代码问题都涉及沿用的调用链，不混称为本轮新引入的回归。未修改生产源码、旧证据或服务。

## F0 [P1，条件性恢复漏洞] 重建 snapshot 与首轮 consume 没有绑定不可变的 Stream 边界

位置：`readyset-databend/src/replicator.rs:231-248,336-386,391-406`。

同 instance_id 重用保留 Stream，snapshot 和 count 分别通过 `AT (STREAM => name)` 解析当时的 offset，之后再独立 BEGIN/CONSUME；既没有旧 consumer 的 fencing，也没有校验 Stream 边界在这段时间不变。

如果旧进程退出时 COMMIT 已发出但仍在上游执行，可能出现：

1. 业务表已经从 v0 更新到 v1；旧消费事务尚未完成 Stream offset 的 v0→v1 提交；
2. 新进程的 snapshot AT STREAM 解析到 v0，装入旧内容；
3. 旧 COMMIT 完成，Stream offset 前进到 v1；
4. count 在 v1 仍得到相同行数，通过当前校验；新进程首轮 consume 读取 v1 之后的变化，遗漏 v0→v1；
5. 本地内容仍旧，但 fresh/ready/checkpoint 均继续正常推进。

这种缺口不能靠同步 WAL 或本地 sequence 修补，因为缺失的是上游 bootstrap/consume 之间的区间。仅正常停止后重启不会覆盖它。它沿用自原重建路径；并非取消全局批次锁新引入。

**受控 HTTP 上游已复现条件成立时的产品行为，真实 Databend 的未决 COMMIT 交错尚未注入验证**：最终 release、一个 shared native server、新目录；在 snapshot 解析 v0 后模拟旧 COMMIT 将 Stream 推进到 v1（业务表早已为 v1，行数不变）。初次 `restart-boundary-report.json` 中上游当前 bag 为 `[["k","new"]]`，产品以 `route=readyset` 返回 `[["k","old"]]`，同时 health.ready/all_tables_ready=true、checkpoint=1、failed=null。

随后补齐 full/partial，以及不移动边界的正向对照，三种情况均在建 cache 后等待 **6.2 秒（超过 max_staleness=5s）** 才检查：
- `restart-boundary-guarded-partial-report.json`、`restart-boundary-guarded-full-report.json`：持续空同步后仍然 ready、仍本地返回 old；不是启动短暂落后。
- `restart-boundary-guarded-control-report.json`：snapshot 后边界稳定，consume 正常带回 before/after images，成功本地返回 new。

没有联系真实 Databend 或创建真实数据库。

复现程序 `restart_boundary.py`，参数为新输出目录，默认 partial，`--full` 测 full，`--control` 关闭边界推进注入。远端 `/data1/vmbox/readyset-review-sync-wal-20261114-140527/restart-boundary-guarded-{partial,full,control}/`（初次 `/restart-boundary-2/`）。首轮脚本实例名带下划线被配置检查拒绝，已改成合法名称；启动错误日志一并保留，不混作运行时故障。

建议：在重用 Stream 前确立旧 consumer/未决 COMMIT 的终态或 fencing，且将 snapshot 与首轮 consume 绑定到经证明一致的上游边界；不能仅比较 count。若无法证明边界，来源应保持 fail closed。先用真实 Databend 做延迟 COMMIT/响应丢失/进程退出后的重建测试，或明确限制为已确认无未决事务的正常重启。

## F1 [P2] 每表 checkpoint 仍依赖全体 base domains；其他表故障可能永久停掉健康表

位置：
- `readyset-databend/src/shared_engine.rs:191-213,234-248,572-581`
- `readyset-databend/src/lib.rs:172-187`
- `readyset-server/src/controller/state.rs:934-971`
- `readyset-databend/src/bin/server/shared.rs:122-125`

`checkpoint(source)` 最终调用 `controller.replication_offsets()`，后者查询所有 base domains，任一 domain 请求失败或任一 base 为 Pending，整个请求失败，然后才有机会在 Sink 中挑选本表。克隆 client 解除了 adapter handle mutex，但没有解除这个全表依赖。controller 恢复期间还会拒绝整个 `/replication_offsets` 请求（`controller/inner.rs:700-708`）。

执行序列：A 的 native base/domain 不可用或处于恢复 → B 在 begin/finish 查询自己的 checkpoint → 全表 RPC 因 A 失败 → B 的 sync_source 返回 Err → B 被 fail_source 且 worker break。即使 A 后来恢复，B 也不会自动恢复。若发生于 B 非空批次 finish，SharedBatch::drop 也会 poison B。这里是健康表永久回源/停止同步问题，不是提前消费 Stream 的数据丢失。

这是既有全表 RPC 依赖在本轮并行化后的剩余限制，不是宣称新引入的存储回归。现有 schema drift / source flag 故障测试没有使 native domain 不可用，不能覆盖它。本结论来自完整调用链静态核对，未在服务上注入 native domain 故障。

建议：沿原生 controller/domain 机制提供定向表 offset 查询，避免聚合不相关表；区分只读元数据查询的可重试恢复错误与本表写入结果不确定。不得通过重放非幂等 row chunks 来重试。补 A native domain 不可用/恢复、B 继续推进或按明确策略恢复的测试；如保留共享故障域，需明确收窄“故障隔离”的承诺。

## F2 [P2，验证缺口，已做负向对照] 退出测试不能识别 fsync 被完全跳过

位置：`dataflow-state/src/persistent_state/sync_wal_tests.rs:33-59,95-97`。

`process::exit(0)` 避免 Rust 状态析构，但不清空 OS 页缓存。WAL write 到 page cache 后，即使没有同步持久化，同一主机上的父进程 reopen 仍可能看到全部记录。因此它适合验证无状态析构的进程退出恢复，不足以保护 SharedEngine 依赖的同步落盘前提。

远端对照：
- 远端目录：`/data1/vmbox/readyset-review-sync-wal-20261114-140527/`。
- 使用已构建测试程序 `/data1/vmbox/readyset/target/debug/deps/dataflow_state-d7065ac174bdac7a`，没有重编译或修改仓库。
- 原测试：**1 passed**，见 `baseline.log`。
- 通过仅施加于此测试及子进程的 `LD_PRELOAD=no_sync.so`，让 fsync/fdatasync 返回成功而不调用内核同步：仍 **1 passed**，见 `no-sync.log`。
- `suppressed.log` 确认子进程对 `source_a.db/000004.log`、`source_b.db/000004.log` 的 **6 次 WAL fdatasync 被屏蔽**。
- `no_sync.c` 是完整复现代码。C 编译仅在远端进行；没有改变磁盘配置或其他进程。8001 仍为 PID 19775。

此对照不模拟断电，也不是当前生产同步 WAL 路径失败的证明。建议保留该测试并补原生写路径的同步调用/选项观察，以及 WAL sync 错误和 ACK 丢失的失败注入，验证错误不能产生成功 Table ACK、不能提交 Stream、本表 fail closed。

复现（只运行隔离测试进程）：

```bash
cd /data1/vmbox/readyset-review-sync-wal-20261114-140527
cc -shared -fPIC -O2 no_sync.c -o no_sync.so
T=/data1/vmbox/readyset/target/debug/deps/dataflow_state-d7065ac174bdac7a
"$T" --exact persistent_state::sync_wal_tests::separate_chunks_and_offset_survive_exit_without_state_drop --nocapture
READYSET_REVIEW_SYNC_LOG="$PWD/suppressed.log" LD_PRELOAD="$PWD/no_sync.so" \
  "$T" --exact persistent_state::sync_wal_tests::separate_chunks_and_offset_survive_exit_without_state_drop --nocapture
```

## 产品恢复补测清单（F0/F2 的相关验证缺口）

位置：
- `readyset-databend/src/bin/server/shared.rs:195-218`
- `readyset-databend/src/shared_engine.rs:572-584`
- `readyset-databend/poc/multitable_verify.py:258-269`

现有产品 restart 测试只在 full 模式正常 stop 后重启；新增原生退出测试直接操作 PersistentState，不经过 Table ACK、reader fence 或 Databend COMMIT。它们都没有覆盖：

1. rows 部分页已 durable、本表 offset 仍是旧值；
2. 最终 offset 已 durable、reader fence 尚未完成；
3. fence 已完成、Stream COMMIT 尚未发出；
4. COMMIT 已执行、响应丢失或进程在 sequence 更新前退出；
5. 旧 COMMIT 在新 snapshot 完成后才推进 Stream（F0）。

这些窗口不能统一当成已证明安全；恢复意图依赖“新目录 + 新 generation + snapshot AT 实际 Stream offset + consume 剩余变更”，而不是使用旧 offset 原位续跑；F0 表明还需保证这个上游边界不被未决旧事务改变。需要 full/partial 都做产品级故障注入，校验 bags、实际 Stream offset、未完成批次回源、其他表状态及重建结果；COMMIT 只尝试一次、不凭本地 offset 跳过或重放原批次。

## 已核对成立的关键性质

- SharedEngine 选择 Permanent 基表持久化、wal interval=0；没有从 adapter 打开 snapshot-mode。`PersistentState::write_to_db:2852-2907` 普通基表路径开启 sync；FullMaterialization 无 WAL 分支不是基表 durability 证明的一部分。
- 原生 `Node::process → materialize → State::process_records → write_to_db` 的错误向上传播；`Replica::handle_one_packet:464-473` 只在处理成功后 ACK。
- 所有 row chunks 顺序 await，之后才发送本表 offset RPC；不要求不同来源 sequence 可比较。Sink 还核对 generation，SharedEngine 核对精确 sequence。
- 同表 Table mutex 在 checkpoint 校验之前取得；前一批更新完成后下一批才能校验，同序号并发不会都通过。
- SharedBatch 未完成 Drop 在释放 gate 前设置本表 failed；query 获取 gate 后再次检查 failed，避免读到残缺批次。migration 全生命周期读锁保留，图变更排他。
- source_round 先完整读完并 close，finish 的 durability/fence 成功后才调用 COMMIT。COMMIT 错误停止来源，不盲目重试 row chunks。
- 当前 CLI 拒绝复用已存在 run_dir（`bin/server.rs:316-334`）；启动创建新 generation，按保留 Stream 的实际 offset snapshot（`replicator.rs:231-248,336-386`）。因此旧目录中的部分 durable rows 或残留 fence marker 不会被产品正常启动路径续用；这一点本身不能证明 F0 的上游边界稳定。

## 恢复承诺边界

本地 durable offset 不是跨 ReadySet 与 Databend Stream 的两阶段提交记录：offset 可以领先 Stream。页级记录没有每页去重 token，不能在半批次基础上直接重放整批。未来若增加旧目录原位恢复，需重新设计/验证该边界，而不能沿用本轮成功测试作为证明。

尚未证明：断电持久化、原位续接、两进程共享同一 instance_id/Stream、native domain 故障的完整隔离。单进程的共享 worker/disk 本来也不能提供硬故障隔离。

## Review 操作

- 本轮新增的唯一仓库内容是本 review 及负向对照证据；生产源码未改动。
- 没有新建/修改 Databend 表或 Stream，没有重跑规模压力，无 commit/push，8001 未替换。
- `git diff --check` 通过；当前 51 个源码摘要与最终验证基线匹配。
