# readyset-databend 使用说明

readyset-databend 是一个单进程小组件：对外提供 Databend HTTP 接口，进程内嵌一个 ReadySet 缓存，
通过 Databend Stream 把多张表的变更持续同步到本地。应用只需把 Databend 地址换成本服务地址：
已声明的查询在本地返回，其余请求原样转发给 Databend。

设计细节见 [MULTITABLE.md](MULTITABLE.md)，Docker 部署见 [docker/README.md](docker/README.md)。

## 1. Databend 侧准备（DBA 执行一次）

本服务在 Databend 中只创建一种对象：每张同步表一个 Stream，名为
`<库>.readyset_<instance_id>_<表名>`。不建库，不建辅助表。Databend 需要启用 Stream 功能
（属于企业版特性，需要有效许可证）。

```sql
CREATE ROLE readyset_role;
GRANT SELECT ON sales.* TO ROLE readyset_role;
GRANT CREATE ON sales.* TO ROLE readyset_role;   -- 仅用于创建 Stream
CREATE USER readyset IDENTIFIED BY '<password>' WITH DEFAULT_ROLE = 'readyset_role';
GRANT ROLE readyset_role TO readyset;
```

该账号没有业务表的写权限。经本服务转发的写语句也用这个账号执行，会被 Databend 拒绝；
需要写入的应用请用自己的账号直连 Databend。

**同一个 Stream 只能由一个本服务实例消费。** 不要让其他程序消费 `readyset_<instance_id>_*` Stream，
也不要用相同的 `instance_id` 启动第二个实例。

## 2. 配置

```toml
# 实例标识，1..32 位小写字母或数字。决定 Stream 名称和本地存储。
# 留空时在 [storage].dir 下生成一次并保存，之后保持不变。
instance_id = "orders01"

# 明确同意创建本实例的 Stream。
confirm_create = true

[upstream]
url  = "http://databend:8000"
user = "readyset"            # 密码只从环境变量 DATABEND_PASSWORD 读取

[listen]
http = "0.0.0.0:8001"

[replication]
tables        = ["sales.orders", "crm.customers"]   # 最多 32 张，必须已存在
poll_interval = "1s"         # 每张表拉取 Stream 的间隔
max_staleness = "10s"        # 超过这个时间没完成同步，该表的读转发给 Databend

[storage]
dir = "/var/lib/readyset-databend"   # 本地数据目录，必须放在持久卷上

[cache]
partial = false              # true：按需物化，只缓存被查询过的键
# memory_limit_mb = 2048     # 仅 partial=true 时可设，是软目标，不是 RSS 硬上限
upquery_timeout_ms = 5000    # 冷数据回填的等待时间，超时这次查询转发给 Databend
```

支持的列类型：Boolean、各宽度有/无符号整数、String、Decimal(p<=28)，以及它们的 nullable 形式。
表里有不支持的类型时，只有这张表不同步，查询转发；其他表正常。
不需要主键，完全相同的重复行会原样保留。

## 3. 启动与停止

```bash
export DATABEND_PASSWORD='<password>'
readyset-databend --config /etc/readyset-databend/config.toml
```

- **启动：** 依次恢复或加载每张表，完成后才开始监听 HTTP。首次启动要做全量快照，大表需要时间。
- **停止：** 发送 SIGTERM，进程会正常关闭原生存储。
- **单实例：** 一个存储目录同一时间只能被一个进程打开，第二个进程会因 RocksDB 文件锁启动失败。
- **构建：** `cargo build --release -p readyset-databend --bin readyset-databend`。Docker 镜像构建见 docker/README.md。

## 4. 声明和使用缓存

用同一个账号，通过 bendsql、Python driver 或 HTTP 连接本服务：

```sql
CREATE CACHE orders_by_id FROM
  SELECT id, status, amount FROM sales.orders WHERE id = ?;
CREATE CACHE customers_by_region FROM
  SELECT id, name FROM crm.customers WHERE region IN (?, ?);
SHOW CACHES;
DROP CACHE customers_by_region;
```

- 查询形状和声明一致时在本地执行，例如 `SELECT id, status, amount FROM sales.orders WHERE id = 42`。
- 支持单表的普通投影、AND 连接的等值或 IN 条件，以及一个数值字段的范围条件（`>`、`>=`、`<`、`<=`、`BETWEEN`）。
- JOIN、聚合、排序、LIMIT 和其他语法都会转发给 Databend。
- 全实例最多 8 个缓存，名称全局唯一。
- 未限定库名的表只按请求会话的当前库查找，不会去猜 `default`。
- 单个本地结果最多 10,000 行或 4 MB，超过时这次查询转发。
- 响应头 `X-ReadySet-Route` 表示这次请求由 `readyset`（本地）还是 `databend`（转发）处理。

## 5. 重启与持久化

本地数据都在 `[storage].dir`：

- `engine/`：ReadySet 原生持久化，包括基表的 RocksDB 文件和保存 recipe 的 standalone authority；
- `engine/caches.json`：缓存声明；
- `instance_id`：自动生成的实例标识（配置里没写 `instance_id` 时才有）；
- `runs/<时间>/`：每次启动的状态和日志。

**正常重启（SIGTERM 后再启动）：**
- 基表数据、缓存声明和每张表的同步位置都保留，不重新做快照；
- 实测两张各约 1,000 万行的表（partial 模式）：首次启动 64s，同目录重启 6.8s 就绪，重启后查询直接本地命中；
- 停机期间 Databend 上的写入，会在启动后从 Stream 继续消费；
- 缓存定义自动恢复，不需要重新 `CREATE CACHE`；
- full 模式的缓存从本地基表重新物化，partial 模式按需回填，都不访问 Databend。

**异常退出（崩溃、kill -9、断电）后启动：** 逐表比较本地记录的位置和 Stream 当前位置。

| 退出时这张表的状态 | 启动后的处理 |
|---|---|
| 没有未完成的批次 | 直接续接 |
| 批次写到一半，或 COMMIT 没有执行 | 只删除并重新加载这一张表 |
| COMMIT 已执行但没收到响应 | 直接续接，不重复应用 |
| 首次快照没做完 | 重新加载这一张表 |
| 表结构变了 | 重新加载这一张表，按新结构重建它的缓存，不再适用的缓存声明自动删除 |

其他表不受影响。

**位置判断依据：** 每张表的同步位置记录在原生基表自身的 replication offset 里，与数据一起写入 RocksDB，
使用同步 WAL 落盘。COMMIT 只在本批所有写入都已确认之后才发出，因此 Stream 前进了就说明本批已完整写入本地。

**不会自动修复的情况：**
- 某张表运行中同步失败（例如 COMMIT 结果不确定、schema 漂移）后会停止同步，查询转发给 Databend。
  重启后按上表自动恢复。
- Stream 失效（`system.streams.invalid_reason` 非空）时，这张表启动失败，查询转发给 Databend。
  人工删除该 Stream 后重启即可：服务会重建 Stream，并按位置比较结果续接或重新加载这张表。

## 6. 监控

| 接口 | 认证 | 用途 |
| --- | --- | --- |
| `GET /livez` | 不需要 | 进程存活 |
| `GET /readyz` | 不需要 | 至少一张表可本地读返回 200，全部不可用返回 503 |
| `GET /health` | 需要 | 每张表的状态、内存与命中统计 |

`/health.tables[]` 的主要字段：
- `ready`、`failed`：这张表能否本地读，以及失败原因；
- `last_sync_age_ms`：距上次同步完成的时间；
- `rows`：本地行数；
- `syncing`、`applying`：是否正在拉取、是否正在写入本地；
- `snapshot.recovery`：本次启动对这张表的处理，取值为 `created`、`resumed` 或 `rebuilt: <原因>`。

`all_tables_ready` 表示所有表是否都已就绪。

建议告警：
- 任一表 `failed` 非空；
- `last_sync_age_ms` 持续超过 `max_staleness`；
- 进程 RSS 接近容器上限。

## 7. 资源与限制

- **内存：** full 模式每个缓存在内存里保存一份完整结果，不能淘汰，要按全表估算内存。
  partial 模式可设软预算，实测千万行场景 RSS 仍可能比预算高出约 2–3 GiB。容器内存上限要留余量。
- **磁盘：** 本地基表保存全部行。
- **同步开销：** 同步 WAL 让每次写入都落盘。多张表共享磁盘和 CPU，没有公平性保证；某张表的大批次可能拖慢其他表，
  超过 `max_staleness` 后对应的查询转发给 Databend。
- **部署：** 单进程、单节点，不提供高可用。只有一个配置账号，没有多用户权限隔离。
- **DDL：** schema 变化在每轮同步前后检查，不是原子的 DDL 协调；检测到变化后这张表停止同步，重启时重新加载。
- **客户端：** JDBC 暂不能连接；bendsql 和 Python driver 可用。
