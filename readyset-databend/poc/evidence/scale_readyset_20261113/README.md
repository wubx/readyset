# ReadySet 侧千万行基准测试：全量物化（2026-11-13）

- 二进制：`readyset-databend` release 版，生产模式（无 oracle，每批只用一行标记行做一致性检查），全量物化（`disable_partial`）。
- 远端：`192.168.191.96`，32 核，125 GB 内存。Databend `v1.2.962`。
- 测试库：`readyset_gateway_rsbench_20261113_160020`（保留）。表 `orders(id BIGINT, payload STRING, score BIGINT, note STRING NULL)`，1,000 万行。
  `score = id % 100000`，`note` 偶数行为 NULL。
- 脚本：`../../rs_scale_bench.py`。结果：`results_full_materialization.json`。每秒的内存采样：`rss_full_materialization.csv`。
  服务日志：`server_stderr.log`。过程日志：`run.log`。
- 每一步之后，4 条检查查询都**确认由本地返回**（`X-ReadySet-Route: readyset`），并与 Databend 直查结果逐行比对，全部一致。

## 快照与建缓存

| 步骤 | 耗时 | 进程常驻内存（RSS） | 峰值 |
| --- | --- | --- | --- |
| 快照：读取并写入 1,000 万行 | 16.2 秒（61.6 万行/秒） | — | — |
| 快照完成：持久化、内部 id 缓存就绪 | 合计 30.4 秒 | 8.2 GB | 9.3 GB |
| `CREATE CACHE ... WHERE score IN (?, ?)`，投影 `id, payload` | 10.7 秒 | 11.2 GB（+3.0） | 12.4 GB |
| `CREATE CACHE ... WHERE score >= ? AND score < ?`，投影全部 4 列 | 18.6 秒 | 13.7 GB（+2.5） | 15.7 GB |
| `CREATE CACHE ... WHERE payload = ?`，投影 `id, score` | 14.3 秒 | 20.1 GB（+6.4） | 21.3 GB |

本地底表（RocksDB）占用磁盘 895 MB。内部 id 缓存在没有任何用户缓存时就存在，占了快照后 8.2 GB 中的大部分（未单独拆分）。

## 增量批次（4 个全量缓存同时在线）

| 源端操作 | Stream 变更行 | 读取并写入 ReadySet | 速度 | 持久化 + 标记行检查 | RSS / 峰值 | 磁盘 |
| --- | --- | --- | --- | --- | --- | --- |
| UPDATE 100 万行 | 200 万 | 15.4 秒 | 13.0 万行/秒 | 0.1 秒 | 18.3 / 21.3 GB | 1.0 GB |
| INSERT 100 万行 | 100 万 | 3.1 秒 | 31.8 万行/秒 | 0.7 秒 | 19.8 / 21.3 GB | 1.2 GB |
| UPDATE 全部 1,100 万行 | 2,200 万 | 163.6 秒 | 13.4 万行/秒 | 0.1 秒 | 23.0 / 23.2 GB | 2.3 GB |

- UPDATE 每行是一条 DELETE 加一条 INSERT。DELETE 要在没有主键的底表里按整行查找，比 INSERT 慢，所以按变更行计算，UPDATE 的速度约为纯 INSERT 的 40%。
- 2,200 万变更的批次进行中（开始约 8 秒后）发出的缓存查询被转发给 Databend，结果正确，没有读到写了一半的批次。

## 本地查询延迟（同一条 SQL，单次）

| 查询 | 本地 | Databend 直查 |
| --- | --- | --- |
| `score IN (3, 4)`，200 行 | 2–8 ms | 64–82 ms |
| `payload = 'p-123456'`，1 行 | 1–2 ms | 65–73 ms |
| `score >= 2 AND score < 4`，200–320 行 | **1.6–1.9 秒** | 62–71 ms |

范围查询在本地慢了约 25 倍，说明千万行时本地的范围读取路径有问题（小规模测试时没有暴露），需要单独排查。
