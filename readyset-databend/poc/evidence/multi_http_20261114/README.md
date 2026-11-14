# HTTP 多表共享 runtime：2026-11-14 最终回归

## 实测对象与边界

- 本地编辑：`/Users/wubx/02-vm/05-Github/readyset`，分支 `poc/databend-stream-m0`；全部功能改动未提交。
- 编译/测试仅在 `wu@192.168.191.96:/data1/vmbox/readyset-single-binary`。
- 实际 checkout 工具链：Rust **1.99.0**、Cargo **1.99.0**；不是远端 home 目录的默认 Rust 版本。
- 规定环境：`PATH="$HOME/.cargo/bin:$PATH"`、`LIBCLANG_PATH=/tmp/readyset-m1-libclang`、
  `CARGO_TARGET_DIR=/data1/vmbox/readyset/target`；Cargo `--locked -j 8`。
- Databend `http://192.168.191.96:8000`，只创建/修改本轮新的 `readyset_gateway_*` 合成库；没有清理库/Stream。
- 最终完整矩阵：`/data1/vmbox/readyset-multi-final-20261114-114236/`。
- 8001 服务未替换/重启。结束时只见既有 `readyset-databe` PID **19775** 监听 8001；`GET /livez` 返回 `{"ok":true}`。
  HTTP 回归服务均由脚本以 `127.0.0.1:0` 独立启动并干净退出；没有残留后台回归任务。

**Git 元数据不假装一致：**本地已有 HEAD 为 `53036498fb2c8eb17f935636e6e13ef66d16a68d`，远端 checkout HEAD 为
`63cbe2ff679155d92cc0372c7040113372740750`。远端通过同步工作树验证，不做未经授权的 commit/push/checkout。
两个既有提交间的全部 Rust/Cargo 编译相关变化，以及本轮代码和关键原生修改，均在 `final/source.sha256` 覆盖中。
本地逐文件核对 **44/44 SHA256 相同**（含 workspace/toolchain/config），归档 70 个原始文件哈希也逐一一致；结果见 `source_verification.json`；二进制哈希见 `final/binary.sha256`。
因此测试对应的是记录的源码快照，不把远端 HEAD 标签当作本地 HEAD。

## 最终结果

`final/report.json` 的 15 个矩阵步骤均 exit 0 / `passed: true`；另补跑两个原生库全部单测。
原始命令、耗时、检查项、每次请求和每表状态均保留。

| 验证 | 结果 |
| --- | --- |
| Rust fmt、严格 Clippy all-targets `-D warnings`、release build | 通过 |
| Databend lib | **32 passed，1 ignored** |
| M1 binary / HTTP 配置单测 | **4 / 2 passed** |
| 显式授权的真实 schema / Stream 回滚重读 / 独立提交测试 | **1 passed**（单独启用 ignored 测试） |
| Python 单测 | **45 passed** |
| 双 parser signed-boundary 精确 AST 对齐 | **1 passed** |
| replication-offset | **118 lib + 3 doc passed** |
| 多表 HTTP full | **151/151** |
| 多表 HTTP partial，进程 allocated 软目标 512 MiB | **138/138** |
| 旧单表 HTTP full / partial / oracle | **805/805 × 3** |
| 完全重复行/重复键/键变更 full / partial | **518/518 × 2** |
| 原生 client / Reader 库 | **150 passed** |
| 原生 dataflow 库 | **242 passed** |

普通 Rust 命令仍保留一个需实库授权的 ignored 标记；它已在独立命令显式运行，不重复计入 lib 32 项。
这不是整个 workspace 所有外部服务依赖测试；没有运行 PostgreSQL rewrite-equivalence 环境或 cgroup 测试。

## 多表实证

每次 HTTP run 都创建两个不同数据库的同名 `items`：

- A：String id、Boolean、nullable Decimal(18,2)，含大小写不同键、Unicode、完全相同的重复行。
- B：UInt64 id（含最大值 18446744073709551615）、nullable String，含重复键和完全相同的行。
- 另配一张 Float/Double 不支持的表，准备阶段显式 Error，但 A/B 仍可本地读。
- health 与 native 控制 API证明一个 native server、恰好两个 native bases；没有旧空底表、每表引擎或 Databend helper 表。
- 每个支持的来源恰好一个 `readyset_<instance_id>_items` Stream，独立 sequence/rows/error；源端审计 SQL 可核对。
- 未限定 SQL 严格按 session.database，显式跨库来源不串表；default 不猜库。
- 普通投影、Boolean、精确 Decimal range、UInt64 上界、字符串、NULL、bag 和本地分页逐一与 Databend 对照。
- A/B 同时 UPDATE、独立 COMMIT 后不重复投递；100 个并发读全部结果正确且本地服务。
- 实际向 A INSERT **100,000** 行，再 UPDATE 这 100,000 行，消费 **200,000** before/after 变更。
  观察到 A `applying=true` 时 A 的读回源，B 的 20 次读仍全部本地。
  最后 batch total_ms：full 约 **1,214 ms**，partial 约 **2,972 ms**（捕获/应用批次耗时，不是业务 UPDATE 总耗时）。
- full 注入 A 表级故障；partial 实际 ALTER A 增列触发 drift。A fail closed、读回源；B 继续本地查询、UPDATE 和同步。
- full 重启新进程/新共同 generation，复用实例 Stream、重新快照并消费停机期间变更；公共缓存需重新声明。
- partial 的结束 allocated 约 **135 MiB**，配置软目标 512 MiB；不等于峰值/RSS 或 cgroup 上限证明。

原生 SharedEngine 单测另外覆盖：不同表 sequence 不同、全局最小水位不能替代本表 Persisted；
A 未完成写入取消不污染 B；空批次不抢读 gate；真实 `evict_single` 后重放结果保持 bag；
零等待冷读只回源且热后可本地读；取消图变更 future 会全局 fail closed 并影响 readiness。

## 实现保持原生设计

- `start_native` 只抽取原有 Builder 初始化；旧 Engine 和新 SharedEngine 共用。
- SourceCatalog/类型 codec 负责适配；SQL 仍走原生 parser/rewrite/slots/read_cache。
- 共享 native readers/handler 和内存软预算，replay/eviction 仍由 ReadySet 处理。
- 全局协调图变更/写入/刷盘，各来源持有原生 Table、Sink、Replicator 和自己的 read gate。
- 只以 `PersistencePoint::Persisted` 作刷盘证明，marker 使用真实物理行和 canonical eviction epoch。
- 物理投影只有小域 Boolean/小整数时明确拒绝建缓存，不用可能碰撞的 marker 计数冒充屏障。

## 真实失败与修正记录

保留在 `attempts/`，不作为最终通过项：

1. 初版新增测试有 Vec Display / Debug 类型错误与 Clippy 提示，修正测试表达和代码样式后严格检查通过。
2. 新测试把 Boolean 期望写成 JSON bool，实际协议是 String `"1"/"0"`；修正断言，未改协议迎合测试。
3. live 脚本误用 `--fault-injection`，改为既有 `--enable-fault-injection`。
4. 旧 `/health` 串行屏障假设不能套用于非阻塞多表 health：一个写入前已开始的空轮次会在写入后增加 checkpoint。
   当时 A rows 仍为 5，100K 尚未消费，见 `attempts/04_last_health.json`，不是 native 数据漏写证据。
   测试改为等待**写入后新启动**且完成的同步轮次，再比较结果；保留失败日志，并用后续完整矩阵重验。
5. parser 集成测试包禁用自动 test target，最初传入不存在的 target；改用 `--test tests` 加实际函数过滤，实跑 1 项通过。

## 尚未承诺

- 全局写入/刷盘仍串行；A 长批次会推迟 B 下一轮同步，B 超过自己 max_staleness 后必须回源。
- 小域键、宽范围 marker 预热仍可能很大；软预算不保证任意缓存都能成功发布。
- DESCRIBE 在多个时点复核，但不是原子 DDL 协调；暂不自动迁移 schema。
- 重启是重做快照，不是崩溃后原位续接，也不是端到端 exactly-once。
- 多表千万行规模、cgroup/RSS 硬限、完整最小权限账户矩阵、JDBC、本地 JOIN 尚未验证/实现。

已有千万行 partial 证据保持原样，见相邻 `partial_20261114/`，不能把本轮 100K 多表测试冒充千万行重测。
