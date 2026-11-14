# 多表各千万行：共享软预算、淘汰回填与大批次表间影响（2026-11-14）

> 历史基线：本轮使用全批次全局写锁及后台 WAL；后续同步 WAL/并行 Table 优化不改写这里的结果。
> 本轮原样脚本已冻结为 `main/benchmark.py`（SHA256 不变），避免随当前脚本演进失去可复现版本。

## 结论

**两张异构表各 10,000,000 行，另含 5 行边界/重复样本；一个进程、一个原生 ReadySet server、两个 base，
共用 3 GiB allocated 软预算，partial 模式。主轮 49/49 项检查通过；先行 100K/表 smoke 为 65/65。**
没有修改运行时、参数化、存储、replay 或淘汰算法，没有引入 Router/每表引擎/Databend helper 表。
此次只新增 `poc/multitable_scale_bench.py`、证据和文档；使用上一轮已验证 release 二进制，未重新编译。

核心结论：

1. 两表可共同填充原生 partial readers；实际触发内存压力淘汰，两个来源的已读键都验证到 miss → 原生 replay → 本地正确结果 → 再读 hot。
2. 共享预算不是每表各 3 GiB：只加热 B 时，A 的 range reader 键槽从 **14,888 降至 7,751**，B 从 **18,362 增至 37,333**。
   这是共享预算下的竞争，不是表间内存隔离或公平性保证。
3. A 更新 **200 万行、400 万条 before/after 变更**时，B 仍有本地读取；但全局写入/WAL 协调会阻塞 B 的同步新鲜度推进。
   本轮 B 的三行小更新等待约 **61.5 秒**才完成写后启动的新鲜同步轮，超过自己的 5 秒 staleness 后按预期回源。
4. 稳态 allocated 约 **2.81–2.82 GiB**，采样峰值 **3,078.7 MiB**；RSS 高水位 **5,301 MiB（5.18 GiB）**。
   **软目标不等于 RSS/cgroup 上限，不能据此给进程只分配 3 GiB。**

未提交/推送，未替换/重启 8001，未删除测试数据库或 Stream。所有测试子进程已退出；原服务仍为 PID **19775**。

## 环境、数据与范围

- 执行主机：`wu@192.168.191.96:/data1/vmbox/readyset-single-binary`。
- Databend：`http://192.168.191.96:8000`。
- 仅远端运行测试；本地只编辑和整理证据。
- release SHA256：`f3bbed15908c7ac2a0153f16e0b65c3cfdb22f350d5d8cc66e4ac8132b476408`。
- 测试脚本 SHA256：`d0f4a907cf9973efa4862cef84ee278053c3f69333873940e606142b558ea600`。
- 原生源码仍通过上一轮 `source.sha256` 的 44/44 项核验；不按不同 HEAD 推断源码一致。
- 测试使用临时分配的 `127.0.0.1:0` listener，不占用固定业务端口。
- 配置：`partial=true`、`memory_limit_mb=3072`、`upquery_timeout_ms=10000`、poll 300ms、`max_staleness=5s`。

主轮仅创建并保留以下合成数据库：

- `readyset_gateway_multiscale_20261114_124251_3ac7d6_a`，表 `items`：
  `id String / bucket Int64 / amount Nullable(Decimal(18,2)) / active Boolean / payload String`，**10,000,002 行**。
- `readyset_gateway_multiscale_20261114_124251_3ac7d6_b`，同名表 `items`：
  `id UInt64 / grp UInt32 / label Nullable(String) / revision Int16`，**10,000,003 行**。
- 每表一个 `<db>.readyset_msfe49886a927a_items` Stream；源端审计只出现两个 `CREATE STREAM`，没有 `CREATE TABLE` helper。
- A 含两份完全相同行，B 含 `UInt64::MAX` 下两份完全相同行和一份不同内容；结果按 Counter/bag 比较，不去重。
- range 分组各 100,000 个值，每值 100 行；每次压力查询 60 个分组、**6,000 行**，低于本地 HTTP 结果上限。

先行 smoke 也只创建新的 `readyset_gateway_multiscale_20261114_124125_26a73a_{a,b}`；`smoke=true` 明确跳过规模特有前提，
不能拿 smoke 当作实际发生淘汰/长时间过期的证明。

## 快照与 partial 创建

两表快照/启动墙钟 **57.441 秒**；A 快照 **37.510 秒**，B **19.459 秒**。
快照后 allocated **161.3 MiB**、RSS **1,425 MiB**，无公共 readers。
六个缓存建立后 reader 合计只有 **12 个 key slots**（含 fence 标记键），不是千万行全量 reader。

| 公共缓存 | 创建耗时 |
|---|---:|
| A id 等值 | 0.031 s |
| A bucket 上下界范围 | 49.070 s |
| A payload 等值 | 17.030 s |
| B id 等值 | 0.011 s |
| B grp 上下界范围 | 45.068 s |
| B label 等值 | 15.023 s |

partial 不意味着建索引瞬时完成：原生 RocksDB 辅助索引仍会扫描底表。
停止进程后的原生 storage 占用 **7,350 MiB**（`du -sm`），不是整个测试目录/Databend 的占用。

## 共享预算与真实淘汰

8 个并发客户端交替访问 A/B，**1,400/1,400 全部 `readyset` 返回**，无回源；耗时 **26.860 s**。
所有查询逐行及重复数对照确定性数据生成规则，共 **840 万返回行**；冷/热、淘汰后和更新后的探针对照 Databend 本身。
HTTP 端到端 p50 **129.729 ms**、p95 **196.140 ms**，包含 6,000 行结果的传输和客户端解码，不是原生 lookup 独立计时。

| 阶段 | allocated MiB | RSS MiB | A range key slots / epoch | B range key slots / epoch |
|---|---:|---:|---:|---:|
| 六缓存建立后 | 252.6 | 1,556 | 2 / 0 | 2 / 0 |
| 交替压力结束 | 2,888.1 | 4,791 | 17,812 / 18 | 21,350 / 18 |
| 再等待 15s | 2,887.8 | 4,494 | 17,812 / 18 | 21,350 / 18 |
| 大更新后再等待 15s | 2,548.6 | 4,765 | 14,888 / 26 | 18,362 / 26 |
| 只加热 B，600 次查询后 | 2,881.7 | 5,242 | 7,751 / 38 | 37,333 / 38 |
| 再等待 15s | 2,881.7 | 5,126 | 7,751 / 38 | 37,333 / 38 |

epoch 是原生 reader 淘汰通知编号，**不是淘汰行数**；key slots 也不是行数或精确内存字节数。
B 偏斜阶段 600/600 仍全部本地、bags 正确，共 360 万返回行，耗时 **16.814 s**，p50/p95 **219.643/278.360 ms**。
两个压力突发间等待 61s 避免 60s HTTP tombstone 容量混入原生内存淘汰判断；每个突发低于 2,048 记录预算。

### 淘汰后确实发生 replay

对压力开始时已访问的两表 `[100,160)` range 再读：

| 表 | 重读行数 | miss_keys 增量 | replay 本地返回 | 随后 hot | hot miss 增量 |
|---|---:|---:|---:|---:|---:|
| A | 6,000 | +1 | 38.239 ms | 19.730 ms | 0 |
| B | 6,000 | +1 | 39.807 ms | 14.252 ms | 0 |

四次均与 Databend bags 一致；不是“配置了软预算”或“仍热的键再读”代替淘汰/回填验证。
冷指 reader 未驻留，不代表清空 RocksDB block cache/OS page cache 的磁盘冷读。

## 200 万行 UPDATE 的表间影响

A 在 Databend 执行：

```sql
UPDATE <A>.items
SET bucket = bucket + 100000,
    amount = amount + 1.00,
    payload = concat('updated-', payload)
WHERE bucket % 5 = 1;
```

- 源端 SQL **1.872 s**；实际捕获 **4,000,000 changes = 2,000,000 deletes + 2,000,000 inserts**，无 cancelled。
- A 批次 total **62.829 s**；源端 SQL 完成到写后新鲜轮完成约 **63.434 s**。
- 观察到 A 的真实 `applying=true` 后，B 修改 `UInt64::MAX` 下三行，源端 SQL **0.175 s**。
- B 写后新鲜轮等待 **61.539 s**；其六条变更的批次自身只耗时 **102.123 ms**。
- 新鲜轮必须在源端写完成后**开始**，不是只等 checkpoint 增加或完成时间晚于 SQL，避免误认预写前捕获的空轮。

期间三个并发探针持续查询 A 变更点、B 不变点和 B 三行重复 bag：**676/676 个样本全部为完整允许的旧/新结果**。
这包括本地和回源结果；没有把回源当成本地命中，也没有全表逐行读回 2,000 万行的断言。
A 更新后另对旧/新 range 成员关系与源端核对，本地各返回 100 行；B 的重复两份仍保留。

在健康采样观察到 **A applying、B 非 applying** 的 B 不变点探针中：

| 路由 | 样本数 | p50 / p95 |
|---|---:|---:|
| 本地 `readyset` | 19 | 2.287 / 3.890 ms |
| 因自身过期回源 `databend` | 193 | 39.933 / 47.336 ms |

B 最大同步年龄 **61,353 ms**，配置上限为 5,000 ms。
由此可确认：**A 的读门禁不直接关闭 B 的读门禁，但共享写锁仍能让 B 过期；不能声称同步完全独立或永不回源。**
状态采样与具体请求间有短时间差，临界样本不解释成严格原子时刻证明。

## 原始证据与复现

- [summary.json](summary.json)：方便审阅的阶段/路由/峰值摘要。
- [main/report.json](main/report.json)：主轮原始 49 项检查、阶段状态和计时，`passed=true`。
- `main/health.jsonl`：250ms 周期，**1,362 个样本**；全程无监控错误。
- `main/rss.csv`：每秒 VmRSS/VmHWM，含 HTTP 监听前的快照过程。
- `main/queries.jsonl`：全部 SQL 的路由、返回行数、耗时摘要，不持有压力结果集。
- `main/update_traffic.json`：676 个并发样本的路由/应用状态/新鲜度/正确性。
- `main/source_queries.jsonl`、`main/final_status.json`、`main/server.log`、`main/console.log`、`main/config.toml`：原始审计/状态/日志/配置。
- `smoke/report.json`、`smoke/console.log`：脚本真实类型、配置、并发和监控链路小规模先验。
- `metadata.txt`、`source.sha256`、`artifacts.sha256`：二进制/源码校验、旧服务保留状态及证据校验。

远端原件：`/data1/vmbox/readyset-multi-scale-20261114-124251/` 和同名 `.log`；
smoke 原件 `/data1/vmbox/readyset-multi-scale-smoke-20261114-124125/`；所有数据库/Stream 原样保留。

```bash
# 仅在授权远端执行；每次建两个新的 readyset_gateway_* 数据库并保留。
cd /data1/vmbox/readyset-single-binary
PYTHONPATH="$PWD/readyset-databend/poc" \
python3 -B readyset-databend/poc/evidence/multi_scale_20261114/main/benchmark.py \
  --binary /data1/vmbox/readyset-multi-scale-baseline-f3bbed15908c \
  --output /data1/vmbox/readyset-multi-scale-<new-timestamp> \
  --rows 10000000 --memory-limit-mb 3072 \
  --heat 1400 --skew-heat 600 --range-width 60 \
  --max-staleness 5 --settle 15 --confirm-create
```

仍未覆盖：cgroup/RSS 硬限额、多轮重复统计的性能置信区间、同等数据下不同预算的 A/B、十张以上表、分片/多节点、
任意 DDL/本地 JOIN、断电崩溃续接。此次没有借压测引入并行写入或改变既有正确性边界。
