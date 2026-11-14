# 单表自恢复、先监听启动与 Stream 重建保护：验证（2026-11-14）

评审后修复的验证。所有测试都在真实 Databend `http://192.168.191.96:8000` 上运行，只新建 `readyset_gateway_*` 库，全部保留。
被测程序：
- release sha256 `484134785ac0…`；
- 故障注入版 `b3cabf03b9c7…`，用相同源码以 `--features readyset-server/failure_injection` 构建，只用于隔离测试。

## 修复内容

- **重启标记记录 Stream 身份：** 标记里加入 `system.streams.stream_id`。Stream 被删除重建后，旧位置不再可信，判定为整表重建（`rebuilt: stream recreated`）。
  修复前，"批次中途崩溃 + Stream 重建"会被误判为"已提交"，带着半批数据续接。
- **单表进程内自恢复：** 每张表一个 worker，失败后按 `retry_backoff` 退避（默认 5s，每次翻倍，最长 5 分钟；连续健康 5 分钟后重置）。
  每次尝试都用新的 Databend 会话，依次：发现 schema → 挂载到原生运行时 → 读取 Stream 身份和版本 → 续接或只重建该表。
  - 恢复过程不取全局 migration 排他锁。删建本表、重建本表 cache 等图变更，只由原生 handle 锁和本表 Table 锁串行，其他表的批次照常进行。
  - 来源和 catalog 支持运行时挂载，也支持 schema 变化后重新挂载。所以启动时 Databend 不可用的表，之后也能接入。
  - schema 变化后重建前，先在本 Stream 上执行一次原生消费提交，让 Stream 越过 DDL；否则按旧位置做快照，读不到新列。
- **先监听 HTTP：** 打开原生存储后立即监听，不需要 Databend；各表在后台并行恢复，未就绪的表转发 Databend。
- **全局 fail-closed 时退出进程：** 原生图变更结果不确定导致全局 fail-closed 时，进程以非零码退出，由守护进程按持久化状态重启。
- **清理未配置表的 cache：** 启动时删除绑定到已不在配置中的表的原生 cache，声明保留，表重新加入配置后自动恢复。
- **持久化 instance_id：** 只用 `--output` 时，自动生成的 instance_id 保存在输出目录。
- **部署与文档：**
  - compose 增加 `stop_grace_period: 120s`；
  - USAGE.md 补充进程守护、k8s 探针、停止宽限期、停机时长与快照保留期、加密、升级、单实例约束；
  - 修正了此前"人工删除 Stream 后重启即可"这条说明：在 stream_id 保护之前，这样操作有风险。

## 1. 重启与自恢复：13 次启动，394/394（`restart/`）

full 和 partial 两种模式结果一致。

| 场景 | A | B |
|---|---|---|
| 1–6（原有场景）：新建、正常重启、COMMIT 响应丢失、COMMIT 未执行时 SIGKILL、批次中途 SIGKILL、正常重启 | 同上一轮 | 同上一轮 |
| 7. 从配置移除 B 后启动 | resumed；`SHOW CACHES` 只剩 `a_id`（B 的原生 cache 已移除） | 未配置 |
| 8. 重新加入 B（期间 B 有写入） | resumed | resumed，`b_id` 按声明重建 |
| 9. 启动时 Databend 不可用 | `/livez` 200，`/readyz` 503；Databend 恢复后自行 resumed（第 2 次尝试） | 同 A |
| 10. 运行中 Databend 中断 | 自行 resumed（第 3 次尝试） | 同 A |
| 11. 批次中途 SIGKILL，再删除 Stream | **rebuilt: stream recreated** | resumed |
| 12. 运行中 A 加列 | 停止同步后自行 **rebuilt: source schema changed**（约 5.5s），`a_id` 按新结构重编译保留 | 不受影响 |
| 13. 再正常重启 | resumed | resumed |

每次启动或自恢复后，三行重复 bag、普通行和两表行数都与 Databend 完全一致。每次启动都会检查 `/livez`，监听后立即可用。

第一次运行时第 12 步超时，由此发现一个真实问题：重建快照按旧 Stream 位置读取，读不到新加的列。修复是上面提到的 schema 变化后先让 Stream 越过 DDL。

## 2. A/B 原生 domain 故障隔离：244/244（`isolation/`）

为覆盖故障窗口，`retry_backoff` 设为 45s。

| 故障 | A 进程内自恢复 | B |
|---|---|---|
| A 停止后，A 的 domain 持续拒绝所有包和请求 | 约 43s 后 resumed | 全程未失败，3 轮都返回最新本地数据，新建的 cache 可用 |
| A 批次写入中途 domain 单次失败，原生 replica 退出并被恢复 | 约 44s 后 rebuilt（未提交批次） | 同上 |

两种故障后在同一目录重启，两表都是 resumed，本地结果与 Databend 一致。full 和 partial 都通过，单次失败的场景都是第一次尝试就成功。

## 3. 完整回归：20/20（`regression/`）

步骤与上一轮相同：fmt、clippy、Rust（含子进程重启测试）、原生库测试、多表、网关、PK、重启恢复、bootstrap 故障矩阵。
bootstrap 验证将 `retry_backoff` 设为 60s，以保持它原本检查的"bootstrap 冲突后该表停止"窗口。

## 4. 千万行（两表各约 1,000 万行，partial，3 GiB 软预算）

**重启耗时（`timing_10m/`）**

| 启动 | 开始监听 | 全部就绪 | RSS |
|---|---:|---:|---:|
| 空目录（两表并行加载：A 42.5s，B 26.9s） | **0.2s** | 43.2s（上一版串行加载 64.1s） | 1,494 MiB |
| 同目录重启（两表 resumed） | 6.4s | 6.4s | 236 MiB |

同目录重启时，前 6.4s 花在原生 server 重新打开存储，这期间 HTTP 尚未监听，`/livez` 不可用。

**A 在进程内重建，B 继续运行（`rebuild_10m/`）**
- 在 Databend 给 A 加列：A 0.13s 后停止同步，50.2s 后完成重建（其中加载 44.5s）。
- 重建期间 B 进行了 15 轮更新，每轮都在本地返回最新值，B 从未失败。
- 重建期间 B 的写后同步等待：p50 1.59s，最大 1.74s，低于 5s 的 staleness 上限。上一轮基准中 B 的这项值是 0.5s。这里升高可能是 A 重建抢占了磁盘和 CPU，但这一轮没有测 A 不重建时的同场景基线，不能下结论。
- A 重建后 cache 按新结构重编译，本地查询结果与 Databend 一致。

## 仍未覆盖

- 真实断电（主机硬重启）；cgroup/RSS 硬限额。
- 某张表本地原生存储持续损坏时，原生恢复会失败并导致进程退出，重启后仍会失败，需要人工删除 `engine` 目录（已写入 USAGE.md）。
- 多天长时间运行，以及用新版本程序打开旧版本存储目录。
- 这一轮没有重跑千万行全量性能基准。批次热路径的唯一改动是提交后多读一个 `stream_id` 字段。
