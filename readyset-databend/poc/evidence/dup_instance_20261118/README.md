# 同一个 instance_id 被两个进程共用：检测与处理（2026-11-18）

Databend：8000（`databend3307`，单节点）。脚本：`poc/dup_instance/`。二进制哈希见 `binaries.sha256`。
只创建了 `readyset_gateway_dupid_*`、`readyset_gateway_jdbc_*` 和回归自带的 `readyset_gateway_*` 库，均保留。

## 1. Databend 语义（`stream_guard_semantics.py`，dup_sem1–3）

两个会话按 readyset-databend 的方式消费同一个 Stream（`BEGIN` → `SELECT … WITH CONSUME` → `COMMIT`）。
S1 是被测实例，S2 是抢着消费的另一个进程。检查方法：S1 在消费读之后，读 `system.streams.table_version`，
与本地已应用的版本比较。三轮结果相同：

| 时序 | S1 漏掉的变更 | S1 的 COMMIT | 版本检查 |
|---|---|---|---|
| E1：S2 在 S1 的 BEGIN 之前提交 | 3/3 | 成功（不检查就静默丢失） | 不一致 |
| E2：S2 在 S1 的 BEGIN 之后、读之前提交 | 3/3 | 成功（不检查就静默丢失） | 不一致 |
| E3：S2 在 S1 读完之后、提交之前提交 | 0 | `StreamVersionMismatched`（2734） | 不一致 |
| E4：没有 S2 | 0 | 成功 | 一致 |

## 2. 双进程端到端（`dup_instance_verify.py`）

两个进程用同一个 `instance_id`，各用自己的存储，同步同一张 1,000 行的表。持续 30 秒：
每 0.3 秒插入 5 行、更新 1 行。最后逐个读写过的 id，只统计本地返回的结果。
`--restart-check`：停掉两个进程，只重启 A，再写 20 轮，检查 A。

| 轮次 | 版本 | A：本地读 / 错误 | B：本地读 / 错误 | 发现记录（A；B） | 只重启 A 后 |
|---|---|---|---|---|---|
| dup_e2e1 | 修复前 | 1,495 / **343**（缺 285，旧值 58） | 1,495 / **246**（缺 210，旧值 36） | 无；无 | — |
| dup_e2e2 | v1 | 0 / 0（已停） | 1,500 / 0 | rebuild、stopped；rebuild | 1,600 本地，0 错，重建 |
| dup_e2e3 | v1 | 1,500 / 0 | 0 / 0（已停） | 无；rebuild、stopped | 1,600 本地，0 错，续接 |
| dup_e2e4 | v1 | 0 / 0（已停） | 1,500 / 0 | rebuild、stopped；rebuild | 1,600 本地，0 错，重建 |
| dup_e2e5 | v2（最终） | 0 / 0（已停） | 1,480 / 0 | rebuild、stopped；rebuild | 1,580 本地，0 错，重建 |
| dup_e2e6 | v2 | 0 / 0（已停） | 1,415 / 0 | rebuild、stopped；rebuild | 1,515 本地，0 错，重建 |
| dup_e2e7 | v2 | 0 / 0（已停） | 1,470 / 0 | rebuild、stopped；rebuild | 1,570 本地，0 错，重建 |

修复后每一轮的结果：其中一个进程第二次发现后停住，读全部转发；另一个进程成为唯一消费者，全部本地返回且正确。
重建原因均为 `rebuilt: stream moved by another consumer`，即新的续接规则（B）生效。

## 3. 性能（`jdbc_compat_verify.py`，单节点，64 个读线程，每 100 ms 写一次）

新旧版本交替运行，每轮 JDBC 兼容性都是 22/22，读己之写全部通过，最终行数一致。

| 版本 | 轮次 | 读 QPS（各轮） | 平均 | 本地比例 |
|---|---|---|---|---|
| 修复前 07e93442 | old, old2, old3, old4 | 5,412 / 5,472 / 5,397 / 5,479 | 5,440 | 98.3–98.4% |
| v1 f17acff7 | new, new2, new3, new4 | 5,220 / 5,205 / 5,158 / 5,058 | 5,160（−5.1%） | 97.7–98.1% |
| 修复前 07e93442 | oa, ob, oc | 5,405 / 5,436 / 5,473 | 5,438 | 98.4–98.5% |
| v2 d9488324（最终） | v2a, v2b, v2c | 5,383 / 5,447 / 5,395 | 5,408（−0.5%） | 98.4% |

v1 把位置检查放在 reader fence 之前，每轮多约 45 ms（`system.streams` 查询的 p50），写入会话的读转发时间随之变长。
v2 把检查和 reader fence 并行执行，`Committing` 标记在检查通过后写入，差异落在噪声范围内。

## 4. 回归（8000 单节点）

- `regression/v1_first`：v1 第一次跑，19/20。`bootstrap_recovery` 的 late_commit 用例原先断言失败原因含 2734；
  修复后，位置检查在 COMMIT 之前就发现了冲突，失败原因变成 `stream consumed elsewhere`。
  断言已改为两者都接受，单独重跑结果为 180/180（`bootstrap_rerun1`）。
- `regression/v1`：v1，20/20。
- `regression/final`：v2，20/20。回归中重新构建的 release 与被测的 v2 哈希相同（d9488324…）。
