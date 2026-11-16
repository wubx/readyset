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
retry_backoff = "5s"         # 某张表失败后首次自动恢复的等待时间，之后翻倍，最长 5 分钟

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

### 3.1 从编译到启动：单进程、多表、持久化、部分物化

下面的例子用一个进程同步 `sales.orders` 和 `crm.customers` 两张表，本地数据持久化到 `/var/lib/readyset-databend`，
并开启部分物化。两张表共享同一个原生 ReadySet 运行时和同一份内存预算。

**第 1 步：编译**

在仓库根目录执行。需要 Rust 1.99.0（仓库的 `rust-toolchain` 已指定，rustup 会自动切换）、C/C++ 编译器和 libclang
（RocksDB 绑定需要；找不到时用 `LIBCLANG_PATH` 指向 libclang 所在目录）。

```bash
cargo build --release --locked -p readyset-databend --bin readyset-databend
sudo install -m 755 target/release/readyset-databend /usr/local/bin/readyset-databend
```

Docker 镜像的构建方式见 [docker/README.md](docker/README.md)。

**第 2 步：Databend 侧准备**

按第 1 节创建账号，并对两个库都授予 `SELECT` 和 `CREATE`：

```sql
GRANT SELECT ON sales.* TO ROLE readyset_role;
GRANT CREATE ON sales.* TO ROLE readyset_role;
GRANT SELECT ON crm.*   TO ROLE readyset_role;
GRANT CREATE ON crm.*   TO ROLE readyset_role;
```

**第 3 步：准备目录和配置文件**

```bash
sudo useradd --system --home /var/lib/readyset-databend --shell /usr/sbin/nologin readyset
sudo install -d -m 700 -o readyset -g readyset /var/lib/readyset-databend   # 存放业务数据明文副本，限制权限
sudo install -d -m 755 /etc/readyset-databend
```

`/etc/readyset-databend/config.toml`：

```toml
# 固定实例标识：决定 Stream 名称（readyset_shop01_<表名>）和本地存储。上线后不要修改。
instance_id    = "shop01"
confirm_create = true          # 同意在每张表所在的库里创建本实例的 Stream

[upstream]
url  = "http://databend:8000"  # 改成你的 Databend 地址
user = "readyset"              # 密码只从环境变量 DATABEND_PASSWORD 读取

[listen]
http = "0.0.0.0:8001"          # 应用改连这个地址

[replication]
tables        = ["sales.orders", "crm.customers"]   # 多张表共享一个进程，最多 32 张
poll_interval = "1s"
max_staleness = "10s"
retry_backoff = "5s"           # 单表失败后自动恢复的首次等待时间

[storage]
dir = "/var/lib/readyset-databend"   # 开启持久化；必须是持久盘，重启后从这里续接

[cache]
partial            = true      # 部分物化：只缓存被查询过的键，基表仍全量保存在本地磁盘
memory_limit_mb    = 2048      # 内存软目标，不是 RSS 硬上限
upquery_timeout_ms = 5000      # 冷键回填等待时间，超时这次查询转发 Databend
```

密码放在单独的文件里，不要写进配置：

```bash
sudo install -m 600 -o root -g root /dev/null /etc/readyset-databend/env
echo "DATABEND_PASSWORD='<password>'" | sudo tee /etc/readyset-databend/env > /dev/null
```

**第 4 步：用 systemd 启动**

生产环境必须有进程守护（原因见 3.2 节）。`/etc/systemd/system/readyset-databend.service`：

```ini
[Unit]
Description=readyset-databend
After=network-online.target
Wants=network-online.target

[Service]
User=readyset
Group=readyset
EnvironmentFile=/etc/readyset-databend/env
Environment=RUST_LOG=warn
ExecStart=/usr/local/bin/readyset-databend --config /etc/readyset-databend/config.toml
Restart=always
RestartSec=5
# SIGTERM 后最多等 60 秒让进行中的批次提交，宽限期要大于这个时间
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now readyset-databend
journalctl -u readyset-databend -f
```

临时在前台试运行，可以不用 systemd：

```bash
export DATABEND_PASSWORD='<password>'
readyset-databend --config /etc/readyset-databend/config.toml
```

**第 5 步：确认启动成功**

`/health` 和查询需要认证，用配置里的账号：

```bash
export DATABEND_PASSWORD='<password>'
curl -s http://127.0.0.1:8001/livez     # {"ok":true}：进程已在监听
curl -s http://127.0.0.1:8001/readyz    # {"ready":true}：至少一张表可本地读
curl -s -u "readyset:$DATABEND_PASSWORD" http://127.0.0.1:8001/health \
  | jq '.tables[] | {table, ready, failed, recovery: .snapshot.recovery}'
```

首次启动每张表要做一次全量快照，`recovery` 为 `created`。快照完成后 `ready` 变为 `true`。

**第 6 步：声明缓存并验证命中**

列名按你的表结构修改。这里用 Databend HTTP 接口，bendsql 和 Python driver 连 8001 端口也可以：

```bash
q() { curl -s -i -u "readyset:$DATABEND_PASSWORD" -H 'Content-Type: application/json' \
        -d "{\"sql\": \"$1\"}" http://127.0.0.1:8001/v1/query; }

q "CREATE CACHE orders_by_id FROM SELECT id, status, amount FROM sales.orders WHERE id = ?"
q "CREATE CACHE customers_by_region FROM SELECT id, name FROM crm.customers WHERE region = ?"
q "SELECT id, status, amount FROM sales.orders WHERE id = 42" | grep -i x-readyset-route
# X-ReadySet-Route: readyset   表示本地命中；databend 表示转发
```

partial 模式下，某个键第一次被查询时要从本地磁盘回填，之后才在内存里命中。

**第 7 步：验证持久化**

```bash
sudo systemctl restart readyset-databend
curl -s -u "readyset:$DATABEND_PASSWORD" http://127.0.0.1:8001/health \
  | jq '.tables[] | {table, ready, recovery: .snapshot.recovery}'
```

两张表的 `recovery` 都应该是 `resumed`：不重新做快照，缓存声明自动恢复，不需要重新 `CREATE CACHE`。
停机期间 Databend 上的写入，会在启动后从 Stream 继续消费。

**容量估算：** 本地磁盘要能放下所有同步表的全部行。内存的 2048 MiB 是软目标，
实测千万行场景 RSS 可能比它高出 2–3 GiB，容器或 systemd 的内存上限要按这个留余量（见第 7 节）。

### 3.2 运行行为

- **启动：** 先打开本地存储并监听 HTTP，再在后台并行恢复各表。打开本地存储不需要 Databend，
  但要等原生存储重新打开后才开始监听（实测两张千万行表约 6 秒），这段时间 `/livez` 不可用。
  还没就绪的表，查询转发给 Databend；启动时连不上 Databend 也不影响进程启动，表会在 Databend 恢复后自行接入。
  首次启动要做全量快照，大表需要时间，期间 `/livez` 正常返回。
- **停止：** 发送 SIGTERM。服务等进行中的批次完成并提交（最多 60 秒），再关闭原生存储。
  容器或 k8s 的停止宽限期要大于这个时间（compose 示例为 120s）；宽限期太短，进行中的批次会被强杀，
  这张表下次启动时整表重新加载。
- **进程守护：** 必须由 systemd、Docker（`restart: unless-stopped`）或 k8s 负责拉起。原生运行时遇到无法在进程内恢复的错误时，
  服务会主动退出，重启后从本地数据续接，一般只需几秒。
- **k8s 探针：** liveness 用 `/livez`；readiness 用 `/readyz`。大表重建期间 `/readyz` 可能是 503，
  不要把 readiness 失败当作重启条件。
- **单实例：** 同一个 `instance_id` 任何时候只能运行一个进程。同一存储目录的第二个进程会因 RocksDB 文件锁启动失败；
  但用不同存储卷同时运行两个相同 `instance_id` 的实例无法被检测，会互相消费 Stream、导致数据错误。
  发布时用单副本和"先停后起"（Recreate）策略。

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

**建缓存对同步的影响：**
- `CREATE CACHE` 和 `DROP CACHE` 只暂停目标表的同步，同一实例的其他表照常同步、照常本地读。
- 目标表要等原生索引建完才继续同步：实测约每百万行 1.1–1.7 秒，1,000 万行约 11–17 秒，1 亿行估计 2–3 分钟。
  这段时间目标表的数据超过 `max_staleness` 后，它的读请求转发给 Databend，结果正确，只是变慢；建完后自动恢复本地读。
- 大表建缓存建议放在低峰期。每次执行都会在 `events.jsonl` 里记录一条 `cache_ddl` 事件，包含耗时和结果。
- 只要表已完成首次加载且没有失败，就可以建缓存，不要求此刻数据新鲜，所以可以连续执行多条 `CREATE CACHE`。

## 5. 重启与持久化

本地数据都在 `[storage].dir`：

- `engine/`：ReadySet 原生持久化，包括基表的 RocksDB 文件和保存 recipe 的 standalone authority；
- `engine/caches.json`：缓存声明；
- `instance_id`：自动生成的实例标识（配置里没写 `instance_id` 时才有；只用 `--output` 时保存在输出目录）；
- `runs/<时间>/`：每次启动的状态和日志。

**正常重启（SIGTERM 后再启动）：**
- 基表数据、缓存声明和每张表的同步位置都保留，不重新做快照；
- 实测两张各约 1,000 万行的表（partial 模式）：首次启动 43s（两表并行加载），同目录重启 6.4s 就绪，重启后查询直接本地命中；
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
| Stream 被删除或重建过 | 重新加载这一张表（无法证明之前的提交，绝不续接） |

其他表不受影响。

**运行中的故障自动恢复（不需要重启）：** 某张表同步失败（Databend 不可用或重启、网络中断、COMMIT 结果不确定、
表结构变化、该表的原生存储短暂故障等）后，只有这张表停止本地服务，查询转发给 Databend。
等待 `retry_backoff`（之后翻倍，最长 5 分钟）后，它按上表的规则自动续接或重新加载自己；其他表一直正常同步和服务。
连续健康 5 分钟后，退避时间重置。

**位置判断依据：** 每张表的同步位置记录在原生基表自身的 replication offset 里，与数据一起写入 RocksDB，
使用同步 WAL 落盘。COMMIT 只在本批所有写入都已确认之后才发出，因此 Stream 前进了就说明本批已完整写入本地。

**需要人工处理的情况：**
- **Stream 失效**（`system.streams.invalid_reason` 非空，例如停机时间超过 Databend 的快照保留期，偏移量所在的快照被清理）：
  这张表会一直恢复失败。由 DBA 删除该 Stream（`DROP STREAM <库>.readyset_<instance_id>_<表>`）即可，
  服务下次重试时会重建 Stream 并重新加载这张表。因此停机时间要小于 Databend 的数据保留期。
- **某张表的本地原生存储持续损坏**：原生运行时恢复失败时进程会退出，重启后仍会失败。
  停止服务，删除 `[storage].dir/engine` 后重启，所有表重新加载。
- **升级版本后启动失败**（本地存储格式不兼容）：同样删除 `engine` 目录后重启。升级前先确认停机窗口能容纳全量加载。
- **从配置中移除的表**：它的缓存会在启动时从内存中移除，声明和本地基表保留，重新加入配置后自动恢复；
  移除期间 Databend 上的 Stream 会持续积累变更。确定不再使用时，由 DBA 删除对应 Stream，并在停机时删除整个 `engine` 目录回收空间。

## 6. 监控

| 接口 | 认证 | 用途 |
| --- | --- | --- |
| `GET /livez` | 不需要 | 进程存活 |
| `GET /readyz` | 不需要 | 至少一张表可本地读返回 200，全部不可用返回 503 |
| `GET /health` | 需要 | 每张表的状态、内存与命中统计 |

`/health.tables[]` 的主要字段：
- `ready`、`failed`：这张表能否本地读，以及失败原因；
- `unavailable`：这张表当前不能本地读的原因，能本地读时为空，取值见下面"转发原因"；
- `last_sync_age_ms`：距上次同步完成的时间；
- `rows`：本地行数；
- `syncing`、`applying`：是否正在拉取、是否正在写入本地；
- `snapshot.recovery`：最近一次恢复对这张表的处理，取值为 `created`、`resumed` 或 `rebuilt: <原因>`；
  `snapshot.attempt` 是第几次尝试，`snapshot.previous_failure` 是恢复前的失败原因；
- `counters.forward_reasons`：这张表的查询没有在本地返回的次数，按原因统计。

`all_tables_ready` 表示所有表是否都已就绪。顶层的 `counters` 是全实例汇总：`readyset`（本地返回次数）、
`databend`（转发次数）和 `forward_reasons`。

**转发原因**（`forward_reasons` 的键和 `unavailable` 的取值）：

| 原因 | 含义 | 是否需要处理 |
|---|---|---|
| `not_cached_table` | 查询的表不在本实例配置里 | 正常 |
| `shape_not_matched` | 表已缓存，但查询形状和任何声明都不一致 | 看是否要加声明 |
| `source_applying`、`source_gate_busy` | 这张表正在写入一批变更 | 正常，持续很高说明写入很频繁 |
| `stale`、`capture_slow` | 同步落后超过 `max_staleness`，或一轮同步耗时超过它 | 偶发正常（如建缓存期间）；持续出现要排查 |
| `forward_epoch`、`forward_during_capture`、`active_forward`、`upstream_inflight` | 经本服务转发了可能是写入的语句，等下一轮同步追上 | 正常；持续出现说明应用经本服务执行了大量非缓存语句 |
| `uncertain_upstream`、`table_failed`、`engine_failed` | 上游结果不确定、这张表失败或整个运行时 fail-closed | 需要处理 |
| `unsafe_session` | 请求带有事务、非默认角色、未知的会话字段或请求头 | 看应用的会话用法（JDBC 兼容时要重点检查） |
| `upquery_timeout` | partial 模式冷键回填超过 `upquery_timeout_ms` | 持续出现要调大内存或超时 |
| `limit_miss`、`local_result_budget`、`local_results_full` | 结果超过单次行数或字节上限，或同时打开的本地结果太多 | 看查询返回的行数 |

**事件日志：** 表失败（`table_sync_failed`、`table_recovery_failed`，附带 `retry_in_secs`）、恢复成功（`table_recovered`，附带恢复方式）、
缓存 DDL（`cache_ddl`）、全局 fail-closed（`engine_failed_closed`）、启动监听（`listening`）和退出（`stopped`），
每条一行 JSON，同时写到 stderr（journald 或 `docker logs` 能看到）和 `runs/<启动时间>/events.jsonl`。

建议告警：
- 任一表 `failed` 持续非空（短暂非空是正在自动恢复）；
- 进程频繁重启，或出现 `engine_failed_closed` 事件；
- `last_sync_age_ms` 持续超过 `max_staleness`；
- `forward_reasons` 里需要处理的原因持续增长；
- 进程 RSS 接近容器上限。

## 7. 资源与限制

- **内存：** full 模式每个缓存在内存里保存一份完整结果，不能淘汰，要按全表估算内存。
  partial 模式可设软预算，实测千万行场景 RSS 仍可能比预算高出约 2–3 GiB。容器内存上限要留余量。
- **磁盘：** 本地基表保存全部行。
- **同步开销：** 同步 WAL 让每次写入都落盘。多张表共享磁盘和 CPU，没有公平性保证；某张表的大批次可能拖慢其他表，
  超过 `max_staleness` 后对应的查询转发给 Databend。
- **部署：** 单进程、单节点，不提供高可用。只有一个配置账号，没有多用户权限隔离。
- **数据安全：** 本地存储目录保存业务表的明文完整副本，要限制目录权限，必要时使用加密卷。
- **DDL：** schema 变化在每轮同步前后检查，不是原子的 DDL 协调；检测到变化后这张表停止同步，随后按新结构自动重新加载。
- **客户端：** JDBC 暂不能连接；bendsql 和 Python driver 可用。
