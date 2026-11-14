# readyset-databend 单二进制设计（路线 B）

目标：用户只部署一个进程（先用 Docker）和一份配置，应用把 Databend 地址改成它的地址即可。
设计同时为以后的集群部署留出路径。

## 1. 进程结构

```text
应用 / JDBC / bendsql / Python driver
        │ Databend HTTP Query API（只改 host:port）
        ▼
readyset-databend（单进程）
  ├─ HTTP 前端：认证、session 检查、路由、分页与查询生命周期、回源转发、缓存 DDL
  ├─ 原生查询链路：parser → rewrite → slot 匹配 → read_cache（进程内 ReadRequestHandler）
  ├─ ReadySet server：底表（本地磁盘 RocksDB）+ dataflow + reader（内存，支持全量/部分物化）
  ├─ Databend 同步：每张表一个 Stream，定期在事务里消费增量 → 底表
  └─ 本地存储（volume）：底表、reader 派生状态、要同步的表、缓存声明、instance_id
        │
        ▼
Databend：业务库；readyset-databend 只在表所在的库里为每张表建一个 Stream
```

`gateway.py`、stdin JSON 通信和 `databend-m2` 子进程都由这一个进程替换。
现有 Python 验证脚本（`gateway_verify.py`、`in_range_verify.py`、客户端兼容探测）走的是 HTTP 黑盒，
加上 `--gateway-binary` 参数就能直接用作新二进制的回归测试。

## 2. Databend 侧只有 Stream

- 每个实例、每张表一个 Stream：`<库>.readyset_<instance_id>_<表>`，建在表所在的库里。
  不建库，不建 outbox/meta 等表。
- 消费方式：

  ```sql
  BEGIN;
  SELECT ..., change$action, change$row_id, change$is_update FROM <stream> WITH CONSUME;
  -- 写入 ReadySet 并完成校验
  COMMIT;   -- 这一步才推进 Stream
  ```

  已在远端验证（Databend v1.2.962）：事务回滚时 Stream 不推进，提交后才推进；同一事务里消费后再读 Stream 为空；
  消费不需要对业务表有写权限。消费读取不能加 WHERE/LIMIT，否则未读到的变更也会被推进掉。
- 写入 ReadySet 失败时回滚，变更留在 Stream 里。写入成功但 COMMIT 结果不确定时停止同步（fail closed），不重试。
- **只有消费读取以 `Succeeded` 读完所有页，才允许 COMMIT。** 实测：查询因结果超时被服务端关闭后，事务仍是 Active，
  此时 COMMIT 会成功，并且把没读到的变更一起推进掉。所以读取不完整时一律 ROLLBACK。
- **心跳。** 读取期间每 20 秒调用一次 `POST /v1/session/heartbeat`（`{"node_to_queries": {node: [query_id]}}`），
  因此边读边写 ReadySet 时不会撞上 `http_handler_result_timeout_secs`（默认 60 秒）。事务本身的空闲上限是
  `idle_transaction_timeout_secs`（默认 4 小时）。
- **重复主键。** Databend 不强制主键唯一。本地底表不声明主键，按多重集合处理：INSERT 增加一行，DELETE 按完整旧值删除一份。
  所以重复 `id` 和完全相同的行都与 Databend 保持一致。同一批里内容完全相同的 DELETE 和 INSERT（例如 `REPLACE INTO` 写回原值）
  在写入前先抵消。

### 2.1 同步路径（生产模式，默认）

- 快照：一条 `SELECT ... AT (STREAM => s)` 按 5 万行一页流式读取并写入底表，再用 `count(*)` 在同一位置核对行数。
- 每轮：`BEGIN` → 消费读取按页流式写入（每页一次 Table 写入）→ 确认读取完整 → 写入 checkpoint 并等待持久化 →
  标记行检查 → `COMMIT`。进程内不保存整表，每轮的开销只和变更量有关。
- 标记行检查（全量/部分模式共用）：缓存都是“单表投影 + 参数条件”。先预热随机标记键并记录 reader 的淘汰 epoch，
  再插入标记行；此后只通过共享 reader 句柄做不触发 replay 的物理行探测，等待出现和删除。
  若有 hole 或 epoch 变化，清理标记并重新预热、重试。这样不会把基于更新后底表的 replay 误当成其他热键已收到增量的证据。
  句柄的物理行包含隐藏索引列，不能和 HTTP/RPC 的最终投影混用。检查期间持有排他读 gate，超时或不确定位点一律 fail closed。
  开销与 reader 数量及重试次数有关；仅适用于当前允许的单表无聚合/无 JOIN 形状，不是通用 dataflow 屏障。
- 多页的大批次：写入期间本地读请求不等待，直接转发给 Databend，因此不会读到写了一半的批次。
- 新建缓存：先在不影响其他缓存读取的情况下，等新 reader 完成初始回放（回放期间查询会超时，属于正常现象，重试即可），
  再做一次短暂的标记行检查。
- `--oracle`（只用于 POC 回归测试，`databend-m2` 也使用它）：保留整表，每批重新校验所有 reader 的全部内容。
  限 9,000 行且 id 唯一。

### 2.2 实测规模（`poc/evidence/scale_*_20261113/`）

- Databend 侧：1,000 万行全表 UPDATE 产生 2,000 万变更行，只统计行数约 1.7 秒，完整消费约 16.6 秒（1.76 GB JSON）。
  recluster 和 compact 不产生变更；`INSERT OVERWRITE` 不管实际改了多少，都报告全表 DELETE 加全表 INSERT。
- ReadySet 侧（全量物化，release 版本，32 核机器）：1,000 万行快照 30 秒。建 3 个缓存后进程内存约 20 GB。
  增量写入：UPDATE 约 13 万变更行/秒，INSERT 约 32 万行/秒；2,200 万变更行用了 164 秒。
  点查、IN、字符串等值查询本地 1–8 ms，Databend 直查 50–80 ms。
- **范围问题已定位并修复（2026-11-14）：** 原生 mixed-comparison 对同一列的上下界生成重复索引维度，
  但边界为 `[(lo,NULL),(MAX,hi)]`，导致过量扫描；部分物化还会过量 replay。现在在相同列的所有维度上取边界交集，
  保留物理索引及排他条件过滤。千万行部分模式实测 200 行范围冷读 3.53 ms、热读 1.20 ms；
  旧的全量范围 1.6–1.9 秒属于修复前历史数据，未重测新代码全量模式的千万行耗时。
- 账号权限：业务库 `SELECT`，加上建 Stream 需要的 `CREATE`，都通过角色授予
  （Databend 不允许把建表类权限直接授给用户）。账号对业务表没有写权限。

### 2.3 部分物化实测（`poc/evidence/partial_20261114/`）

- 同一远端、Databend v1.2.962、release 单进程、1,000 万行、2 GiB allocated 软目标，保留全量模式作对照。
  快照引擎耗时 16.16 秒（启动墙钟 17.01 秒）；快照后 RSS 960 MiB，四个缓存声明完成后 RSS 1,226 MiB，
  reader 合计仅 15 个 key slots（含标记键），并非整表内存副本。新增索引仍需扫描底表：四个声明总计约 71 秒。
- 冷/热读与 Databend 按多重集合核对，含点查、IN、范围、字符串、空结果。200 行范围冷/热读 3.53/1.20 ms。
  8 并发、1,800 次每次 5,000 行的范围查询全部本地返回，逐行比对正确，27.68 秒完成；观察到淘汰 epoch 增长 32。
  淘汰后范围 miss 计数增加并在 3.94 ms 本地回填返回，不仅验证了仍然驻留的热键。
- 压力后 allocated 2,015 MiB（软目标 2,048 MiB），RSS 3,201 MiB；含百万行 UPDATE 的整轮 RSS 高水位 3,425 MiB。
  **预算不是 RSS 上限，容器不能按 2 GiB 配硬限制。** 该峰值是本轮工作负载实测，不是所有查询/增量的保证。
- 百万行 UPDATE 期间 117 次并发读取均是完整旧/新结果（17 本地、100 回源），同步后六类本地查询全部对齐。
  重复键/相同行删除及零等待超时后后台回填通过，主进程无 replay timeout/引擎错误；零等待独立进程记录一次超时，仍可本地命中。
- 一轮失败证据也保留：2,500 次短突发触发 HTTP 层 2,048 个查询生命周期记录预算，后续正确回源，不能据此判断 reader 回填失败。
  最终以预算内 1,800 次且强制全部本地返回的压力复测通过。退出时 RSS 采样器遇到 zombie 状态的无关异常已修复并单独验证。

## 3. 配置

```toml
instance_id = ""                 # 留空时首次启动生成，并持久化在 storage.dir
confirm_create = true            # 明确同意在表所在库建 Stream

[upstream]
url  = "http://databend:8000"
user = "readyset"                # 密码只从环境变量 DATABEND_PASSWORD 读取

[listen]
http = "0.0.0.0:8001"

[replication]
tables        = ["sales.orders", "sales.users"]   # 只写明确表名，不支持通配符
poll_interval = "1s"
max_staleness = "10s"

[storage]
dir = "/var/lib/readyset-databend"
```

## 4. 表的管理（静态 + SQL 动态）

静态 TOML 表列表、真实 schema、共享原生 server、每表 Stream/sequence/可读 gate 已接入 HTTP；
使用一个共同 runtime generation，各表 sequence 独立；SharedEngine 复用原生同步 WAL 的基表 ACK + 本表 offset 校验，
不同表不再等待适配层全批次写锁/全局刷盘。图迁移仍排他，旧单表后台 WAL 模式不变。详细边界见 [MULTITABLE.md](MULTITABLE.md)。
以下静态 ∪ 动态表管理 SQL 仍为目标设计，尚未实现；当前只支持静态来源列表，动态的是公共缓存目录。

- 实际同步的表 = 静态配置 ∪ 动态添加，记录在 ReadySet 本地存储里，不做自动发现。
- 管理 SQL 通过同一入口执行，只有管理员可用：
  `ALTER READYSET ADD TABLES`、`ALTER READYSET RESNAPSHOT TABLE`、`ALTER READYSET DROP TABLE`（原生没有，需要扩展，
  只能删动态表，且有缓存依赖时拒绝；会删除对应的 Stream）、`SHOW READYSET TABLES`（显示来源和状态）。
- 每张表有自己的 Stream 和状态：`Pending → Snapshotting → Online`，出错进入 `Error`；一张表加入或重做快照不影响其他表。
- 从配置去掉静态表时只告警、继续同步。
- `CREATE CACHE` 引用了还不是 `Online` 的表时拒绝，不自动加表。

## 5. 内存、磁盘与集群

- **内存：** 默认保留全量模式作兼容/对照。`--partial` 或 `[cache].partial=true` 启用原生部分物化，内部 id reader 也按需回填，
  不再保留一份额外的全量内存副本。`--memory-limit-mb` / `[cache].memory_limit_mb` 调用 `Builder::set_memory_limit`，
  每 100 ms 检查 jemalloc allocated，按原生策略淘汰部分状态。只能配合 partial 使用；oracle 仍要求全量。
  这是进程分配内存的软目标，不是 RSS 上限，也不能淘汰 RocksDB block cache、网络缓冲和所有固定开销。容器必须留余量。
- **冷读：** 原生 `upquery` 从本地 replay 路径回填。`upquery_timeout_ms` 默认 5000；超时只让该次查询回源，
  不 poison 引擎；0 表示触发后台回填后立即回源。其他不确定引擎错误仍 fail closed。
  `/health` 增加 cache 配置、allocated、reader key slots、eviction epoch、命中/缺失和超时计数。
  residency 计数是只读观测快照（淘汰可并发），不参与一致性判定，不宣称精确命中率。
- **磁盘：** 底表就是本地磁盘上的完整副本（RocksDB，读缓存 `block_cache_bytes` 默认 1 GB）。reader 只能放在内存里。
  部分物化时，内存里没有的键从本地底表补回，不访问 Databend。这样就形成"内存热点 + 本地磁盘全量"两层。
  `materialization_persistence` 是实验功能，只把中间节点放到磁盘上，不包括 reader，所以不采用。
- **集群：** 上游 ReadySet 已经删除了分布式能力。2025-12-02 删除 Consul authority（`05337e192`）；2026-05 删除 dataflow 分片、
  reader 副本和分布式模式的参数（`a259d9136`、`e28ed3e98`、`c8cfa615f`，提交说明称分片"从来没有达到生产级"）；
  高可用路线图 issue #433 在 2026-08-13 以 not planned 关闭。所以分布式要在我们这一层实现，每个节点是一个单机 ReadySet：
  - 多副本、无共享：实现简单，能扩展 QPS 和可用性，但总容量不超过一个节点。
  - 按分区：容量可以扩展。但 Stream 的消费读取不能按分区过滤，要么每个节点都读完整的变更（Databend 侧的成本乘以 N），
    要么由一个统一的捕获服务分发给各节点（需要可靠的中间日志）。查询方面，按分区键的点查和 IN 只发给对应节点，
    其他条件发给所有节点再合并结果。

| 设计点 | 单机版现在的做法 | 集群时的作用 |
| --- | --- | --- |
| `instance_id` | 持久化在 storage.dir；Stream 名字带上它 | 每个实例消费自己的 Stream |
| 缓存声明与动态表 | 存在 ReadySet 本地 | 集群时由管理端下发到每个实例，不放 Databend |
| 分页路由 | 第 1 阶段仍用 `rs_local_<uuid>`；集群前改为编码实例（`rs_<instance>_<uuid>`） | 翻页落到其他实例时转给所属实例，负载均衡器不需要会话粘性 |
| 健康检查 | `/livez`、`/readyz`（无需认证，只返回状态）；`/health`（需认证，含详细指标） | k8s 探针和摘除节点 |

## 6. 分阶段交付

每一阶段结束都是可以部署的单二进制和镜像。

1. **单进程替换 Python（已完成）。** Rust HTTP 前端移植 gateway 的全部现有行为；读取改走进程内 reader；
   同步在进程内以 Stream 消费方式运行，快照和批次都流式处理，带心跳，没有行数上限，重复主键与源表一致；
   提供 Dockerfile 和 compose 示例。数据范围暂时保持单表、固定结构；重启时复用自己的 Stream，
   读 Stream 位点处的快照再消费积累的变更，但本地副本和缓存声明需要重建。
2. **生产化同步。** 部分物化、进程内存软预算、静态多表/真实 schema 已接入。
   重启续接已完成（见 [USAGE.md](USAGE.md#5-重启与持久化)）：复用 ReadySet 原生 standalone authority 与基表 RocksDB；
   每表重启标记记录 Stream 身份与版本，处理"已写入 ReadySet、但 COMMIT 前进程退出"和 Stream 重建等窗口，
   只重建无法证明一致的那张表。运行中单表故障按退避在进程内自动恢复，HTTP 先于表恢复开始服务。
   后续为静态加动态表管理、扩展类型与原子 DDL 协调、扩展已修复范围键边界的性能覆盖、`has_data` 为 0 时跳过本轮。
3. **客户端与运维。** login/session 透传（JDBC）、metrics、文档，最后做端到端 HTTP 压测。
4. **集群。** 先做多副本无共享，加上缓存声明下发和分页的跨实例转发；容量需要扩展时，再做按分区的方案（见第 5 节）。

## 7. Docker

- `docker/Dockerfile`：多阶段构建，builder 阶段用仓库指定的 Rust 工具链和 libclang（RocksDB 绑定需要），
  运行阶段 ubuntu:24.04 加 ca-certificates 和 libssl。
- `docker/Dockerfile.prebuilt`：把宿主上已构建并 strip 过的二进制打成同样的运行镜像。
- workspace 的 release profile 带调试信息，二进制约 1.3 GB；镜像里用 `objcopy --strip-debug` 去掉。
- 挂载 `/var/lib/readyset-databend` 卷，暴露 8001，`HEALTHCHECK` 调用 `/livez`，以非 root 用户运行。

## 8. 不在本设计范围

完整 Databend 协议兼容、通配符和自动发现、Databend 侧 DDL 和表结构演进、端到端 exactly-once。
