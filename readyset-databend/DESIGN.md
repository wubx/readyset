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
  ├─ ReadySet server：底表（本地磁盘 RocksDB）+ dataflow + reader（内存，目前全量物化）
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
- 标记行检查：每个 reader 都完整物化了底表的每一行（缓存都是"单表投影 + 参数条件"），并且按顺序应用底表的更新。
  所以在批次之后写入一行随机标记行，等它在每个 reader 里出现、删除后再消失，就证明这个 reader 已经反映了整批变更。
  开销只和 reader 数量有关。检查期间暂停本地读取，因为标记行会短暂出现在底表里。
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
- **已知问题：** 千万行时本地范围查询（`score >= ? AND score < ?`）要 1.6–1.9 秒，比 Databend 直查慢约 25 倍。
  原因还没有排查。
- 账号权限：业务库 `SELECT`，加上建 Stream 需要的 `CREATE`，都通过角色授予
  （Databend 不允许把建表类权限直接授给用户）。账号对业务表没有写权限。

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

- 实际同步的表 = 静态配置 ∪ 动态添加，记录在 ReadySet 本地存储里，不做自动发现。
- 管理 SQL 通过同一入口执行，只有管理员可用：
  `ALTER READYSET ADD TABLES`、`ALTER READYSET RESNAPSHOT TABLE`、`ALTER READYSET DROP TABLE`（原生没有，需要扩展，
  只能删动态表，且有缓存依赖时拒绝；会删除对应的 Stream）、`SHOW READYSET TABLES`（显示来源和状态）。
- 每张表有自己的 Stream 和状态：`Pending → Snapshotting → Online`，出错进入 `Error`；一张表加入或重做快照不影响其他表。
- 从配置去掉静态表时只告警、继续同步。
- `CREATE CACHE` 引用了还不是 `Online` 的表时拒绝，不自动加表。

## 5. 内存、磁盘与集群

- **内存：** 目前是全量物化，每个缓存在内存里存一份完整结果，不能淘汰。另外，即使没有用户缓存，也会有一份内部的全量 id 缓存，
  它只用于标记行检查（1,000 万行约 8 GB，其中包含 RocksDB 和其他开销，没有单独拆分）。
  要让内存可控，需要改用部分物化，并配合 `Builder::set_memory_limit`：按 jemalloc 实际分配的内存触发 LRU 淘汰，
  而且只能淘汰部分物化的状态。
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
2. **生产化同步。** 重启续接（不重做快照）、静态加动态表管理、多表和类型映射、部分物化加内存上限、
   排查范围查询慢的问题、`has_data` 为 0 时跳过本轮。
   续接时要处理“已写入 ReadySet、但 COMMIT 前进程退出”的情况：本地记录 Stream 当时的版本号，避免同一批重复写入。
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
