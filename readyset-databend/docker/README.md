# readyset-databend 单二进制部署（第 1 阶段）

一个进程同时提供 Databend HTTP 入口、进程内 ReadySet 缓存和 Stream 增量同步。
应用只需把 Databend 地址换成本服务地址；声明过的查询在本地返回，其余请求原样转发给 Databend。

## 1. Databend 侧准备（DBA 执行一次）

readyset-databend 在 Databend 里只做一件事：在表所在的库里为每张同步表建一个 Stream
（`readyset_<instance_id>_<表名>`）。账号需要该库的 `SELECT` 和 `CREATE` 权限，通过角色授予：

```sql
CREATE ROLE readyset_role;
GRANT SELECT ON sales.* TO ROLE readyset_role;
GRANT CREATE ON sales.* TO ROLE readyset_role;   -- 仅用于建 Stream
CREATE USER readyset IDENTIFIED BY '<password>' WITH DEFAULT_ROLE = 'readyset_role';
GRANT ROLE readyset_role TO readyset;
```

这个账号对业务表没有写权限。经本服务转发的写语句也用它执行，因此会被 Databend 拒绝；
需要写入的应用应使用自己的账号直连 Databend，或在后续阶段使用会话透传。

## 2. 配置

见 `readyset-databend.toml`。第 1 阶段只支持一张固定结构的表：
`id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL`。行数没有上限，但要按第 5 节估算内存。

### 部分物化与内存软限制

默认仍为全量物化。按需缓存键、控制 reader 内存时，添加：

```toml
[cache]
partial = true
memory_limit_mb = 2048
upquery_timeout_ms = 5000
```

等价 CLI：`--partial --memory-limit-mb 2048 --upquery-timeout-ms 5000`。
底表仍全量保存到本地 RocksDB；用户 reader 和内部 id reader 都按需 replay，缺失不是空结果。
回填超过等待时间只让本次查询回源，不停止同步；0 表示后台预热并立即回源。

**2048 MiB 是 jemalloc allocated 的软目标，不是 RSS/cgroup 硬上限。** 每 100 ms 检查并通过原生机制淘汰部分状态，
无法淘汰底表、所有固定开销及 RocksDB block cache（默认 1 GiB）。并发回填、写批次、HTTP 结果及 allocator 保留页可能超出目标。
容器内存上限要高于软目标，并根据峰值留余量。全量模式设置内存预算、或 partial 与 `--oracle` 混用会被拒绝。

`/health.cache` 提供 materialization、memory_limit_bytes、allocated_bytes、各 reader 的 key_slots/eviction_epoch，
以及 hit_queries/miss_keys/replay_timeouts；前两种读计数基于查询前 residency 快照，只用于观测。

千万行 release 验证（[原始证据](../poc/evidence/partial_20261114/README.md)）：快照约 16 秒，
声明四个缓存后 RSS 约 1.20 GiB（新索引构建仍耗时约 71 秒）；1,800 次范围压力查询全部本地返回，
自动淘汰后 allocated 约 1.97 GiB、RSS 约 3.13 GiB。含百万行 UPDATE 的整轮 RSS 高水位约 3.34 GiB。
这不是硬峰值承诺，容器必须另留回填、增量、结果缓冲等余量，不能把 2 GiB 软目标照搬成容器上限。

## 3. 运行

```bash
# 构建镜像（任选其一）
docker build -f readyset-databend/docker/Dockerfile -t readyset-databend:dev .
#   或：已有 release 构建时
objcopy --strip-debug target/release/readyset-databend dist/readyset-databend
docker build -f readyset-databend/docker/Dockerfile.prebuilt -t readyset-databend:dev dist/

# 启动
DATABEND_PASSWORD=... docker compose -f readyset-databend/docker/compose.yaml up -d
```

镜像以非 root 用户运行，数据在卷 `/var/lib/readyset-databend`，端口 8001。
内网无法访问 Docker Hub 时，用 `--build-arg RUNTIME_IMAGE=<本地 ubuntu 镜像>` 指定基础镜像。

## 4. 使用

用同一个账号通过 bendsql、Python driver 或 HTTP 连接本服务，声明缓存：

```sql
CREATE CACHE by_id FROM SELECT id, payload, score, note FROM orders WHERE id = ?;
CREATE CACHE by_score FROM SELECT id, payload FROM orders WHERE score IN (?, ?);
SHOW CACHES;
DROP CACHE by_score;
```

| 接口 | 认证 | 用途 |
| --- | --- | --- |
| `GET /livez` | 不需要 | 进程存活 |
| `GET /readyz` | 不需要 | 至少一张表可本地读返回 200，全部不可用为 503；不代表每表就绪 |
| `GET /health` | 需要 | 位点、行数、命中/转发计数、失败原因 |

响应头 `X-ReadySet-Route` 标明本次请求由 `readyset`（本地）还是 `databend`（转发）处理。

## 5. 当前边界

- TOML `replication.tables` 可配置多张真实异构 schema 的表，共享一个原生 server；单张也走真实 schema。
  `--database/--table` 与 `--declarations` 保留旧四列单表/oracle 兼容，不能与 TOML 表列表混用。
  String、Boolean、整数、Decimal(p<=28)、nullable 的精确支持及 fence 限制见 [MULTITABLE.md](../MULTITABLE.md)。
- `/health.tables[]` 包含每张表的 Stream、sequence、rows、ready、syncing/applying、同步年龄和失败原因。
  未支持的表或表级同步失败不阻止健康表继续缓存；全局图变更结果不确定仍全局 fail closed。
- **默认全量模式按全表估算内存；部分模式可按上节启用软预算。** 全量模式每个缓存在内存里存一份完整结果，不能淘汰。此前全量模式千万行实测：快照后约 8 GB（包含一份内部的全量 id 缓存）；
  每增加一个缓存，增加 2.5 到 6.4 GB（字符串作为键时最大）；3 个缓存约 20 GB，大批量写入时峰值约 23 GB。
  本地磁盘约 1 到 2 GB。内存不够时进程会被系统杀掉，所以容器的内存上限要留出余量。
- **已修复同列上下界的范围过量扫描/回填。** 原生 mixed-comparison 重复索引维度未取边界交集；
  已收紧边界并保留排他条件过滤。千万行部分模式 200 行范围冷读约 3.53 ms、热读 1.20 ms。
  修复前全量模式的 1.6–1.9 秒仅为历史数据，新代码全量模式的千万行范围耗时尚未重测。
  这里只测了 reader 冷/热，OS/RocksDB 可能已预热；大范围仍受结果上限、回填与软预算限制。
- 单个本地结果最多 10,000 行或 4 MB，超过时这条查询转发给 Databend。
- **重启复用本地数据**（TOML 多表入口）：数据目录 `/var/lib/readyset-databend` 必须是持久卷。正常重启不重做快照，
  缓存声明自动恢复，停机期间的写入从 Stream 继续消费；崩溃后只重建当时有未完成批次的表。详见 [使用说明](../USAGE.md#5-重启与持久化)。
- 同步出错（包括写入 ReadySet 后 COMMIT 结果不确定）时停止该表同步/本地读，重启后按位置自动续接或只重建该表；其余表继续。
  确认是纯读取的未命中 SELECT 不使其他表失效；未经证明的透传写入/session 操作仍保守使全部表等待下一轮安全同步。
- JDBC 还不能连接（需要 login/session 透传，计划在第 3 阶段完成）。bendsql 和 Python driver 可用。
- 只有一个配置账号，没有多用户权限隔离。
- Databend 不强制主键唯一。重复的 `id` 和完全相同的行都会照常同步，本地结果和 Databend 一致。修改主键也可以正常同步。
- 大批量变更应用期间，该表本地读转发，其他表的缓存读不拿它的 gate。
  共享模式复用原生同步 WAL，各表 Table/offset 可独立推进；没有全批次跨表写锁。图迁移仍排他，
  共享磁盘/CPU/内存没有公平性 SLA，超过各表 max_staleness 时仍各自回源。旧单表后台 WAL 方式不变。
- 当前启动仍先顺序发现 schema/完成快照再监听 HTTP，不承诺在线动态加表或启动快照期间已对外服务。

## 6. 自测

以下命令都在仓库根目录执行，每次都会新建 `readyset_gateway_*` 库，不会改动已有的库。

```bash
B=target/debug/readyset-databend          # cargo build -p readyset-databend --bins
E=http://<databend-host>:8000

# 缓存语义：点查、多列等值、IN、数值范围、分页、缓存 DDL、故障转发
python3 -B readyset-databend/poc/gateway_verify.py --endpoint $E --user root \
  --gateway-binary $B --confirm-create --sql-declarations --native-queries --in-range-queries

# 修改主键：移动、互换、批量平移、重叠平移、复用旧主键、事务内改走再改回、重复主键、完全相同的行
python3 -B readyset-databend/poc/pk_change_verify.py --endpoint $E --user root \
  --gateway-binary $B --confirm-create

# 千万行规模（release 版，需要约 25 GB 内存）：快照、建缓存、100 万到 2,200 万变更行的写入速度和内存
BINARY=target/release/readyset-databend DATABEND_ENDPOINT=$E python3 -B readyset-databend/poc/rs_scale_bench.py

# 原生部分模式回归：在上面的 gateway_verify.py / pk_change_verify.py 命令追加 --partial

# 千万行部分物化：冷/热查询、2 GiB 软预算、自动淘汰与回填、重复行和并发增量
BINARY=target/release/readyset-databend DATABEND_ENDPOINT=$E \
  python3 -B readyset-databend/poc/partial_scale_bench.py

# Docker + 最小权限账号：同步、写入被拒、SIGTERM、停机期间写入/删除/改主键后同卷重启
DATABEND_HOST=<databend-host> IMAGE=readyset-databend:dev bash readyset-databend/docker/e2e.sh
```

`gateway_verify.py` 加 `--oracle` 时使用 POC 校验模式：每批都重新校验全部内容，仅限小表。
Python 脚本需要一个能建库的账号（密码放在 `DATABEND_PASSWORD` 环境变量里）。每个脚本最后都会打印 `Report:` 路径，
指向本次的检查明细和请求记录。`e2e.sh` 自己创建临时用户和角色，结束时删除；业务库会保留，方便事后检查。
