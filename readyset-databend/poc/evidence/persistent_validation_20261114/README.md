# 持久化多表运行时：完整回归、A/B 故障隔离与千万行性能（2026-11-14）

被测代码为提交 `ff153170b`。远端源码与该提交逐文件哈希一致，只有文档不同。
- release `readyset-databend` sha256 `ebc3448f…`，用于回归、性能和重启耗时测试。
- 故障注入版 `625e8f10…`，用相同源码以 `--features readyset-server/failure_injection` 构建，只用于隔离测试。

Databend `http://192.168.191.96:8000`。只新建 `readyset_gateway_*` 库，全部保留。

## 1. 完整回归：20/20 步通过（`regression/`）

覆盖以下步骤：
- fmt、clippy `-D warnings`；
- readyset-databend 全部 Rust 测试，含子进程重启测试；
- 同步 WAL 和 reopen 测试；
- live schema；
- release 构建；
- Python 单测；
- 原生 server offsets 测试，原生 client 和 dataflow 库测试；
- 多表 full/partial；
- 网关 full/partial/oracle；
- PK full/partial；
- 重启恢复 192/192 检查（6 次启动 × 2 种模式）；
- bootstrap 故障矩阵 180/180 检查。

`runner.py` 为本轮运行的脚本。

## 2. A/B 原生 domain 故障隔离：240/240 检查通过（`isolation/`）

使用 ReadySet 原生 `handle-packet` failpoint，只作用于 A 表基表所在的 domain。

| 故障 | full | partial |
|---|---|---|
| A 先停止，再让 A 的 domain 持续拒绝所有包和请求 | 通过 | 通过 |
| A 在 5 万行批次写入途中，domain 单次失败；原生 replica 退出，由 controller 恢复 | 通过 | 通过（第 3 次尝试） |

故障期间，B 连续 3 轮同步都返回最新的本地数据，并能新建 cache 且本地可读；引擎没有整体失败。
撤掉故障后 B 继续同步，A 保持停止，查询转发到 Databend。

**新增：故障后在同一目录重启。**
- 持续拒绝请求：A 在两轮之间停止，重启后续接（`resumed`），并消费停止期间的更新。
- 批次中途崩溃：Stream 没有前进，重启后只重建 A（`rebuilt: interrupted batch was not committed`）。
- 两种情况下 B 都是 `resumed`；重启后 A、B 的本地结果和 A 的行数都与 Databend 一致。

partial 模式的单次崩溃前两次尝试没有结论：一次性 failpoint 先被别的 domain 消耗，A 正常写完了。脚本按设计换新库重试，第 3 次通过。三次尝试都保留在 `report.json` 里。

## 3. 千万行性能回归（`scale/`）：51/51 检查通过

两张异构表各约 1,000 万行，3 GiB 共享软预算，5s staleness。参数与上一版基准相同。

| 指标 | 本轮（持久化版本） | 上一版两轮基线 |
|---|---:|---:|
| 首次快照墙钟 | 64.6s | 63.6s / 64.6s |
| A 400 万变更批次 total | 73.5s | 73.6s / 73.7s（批次） |
| A 追平耗时（SQL 之后） | 74.1s | 74.0s / 74.2s |
| B 写后新鲜轮等待 | 0.504s | 0.522s / 0.467s |
| B 完成时 A 仍在写入 | 是 | 是 |
| A 写入期间 B 探针本地返回 | 280/280 | 282/282 |
| B 探针最大同步年龄 | 501ms | 982ms / 892ms |
| RSS 高水位 | 5,158 MiB | 5,255 / 5,638 MiB |

每批增加的重启标记写入和 Stream 版本读取，没有带来可测出的退化。只测了一轮，差异处于两轮基线的波动范围内，不能据此给出置信区间。

## 4. 千万行重启耗时（`restart_timing/`）

使用同一批 1,000 万行表，partial 模式，两个 cache。

| 启动 | 恢复方式 | 就绪耗时 | 进程 RSS |
|---|---|---:|---:|
| 第 1 次：空存储目录 | 两表 `created`（A 40.3s，B 22.9s） | 64.1s | 1,597 MiB |
| 第 2 次：SIGTERM 后同目录启动 | 两表 `resumed` | **6.8s** | 242 MiB |

- 重启后不需要重新声明，4 个探针都直接本地命中：String 键、重复 bag、UInt64 最大值键，结果与 Databend 一致。
- 第一次本地命中耗时约 0.04s，partial 模式从本地基表回填，不访问 Databend。
- 6.8s 主要花在原生 server 重新打开存储和恢复 recipe；两表的逐表处理合计不到 0.2s。
- 千万行规模下"批次中途崩溃后只重建 A"没有单独计时。按首次加载数据推算，重建 A 约需 40s，B 不受影响。
