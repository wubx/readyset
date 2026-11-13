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
| `GET /readyz` | 不需要 | 本地查询可用时返回 200，否则 503 |
| `GET /health` | 需要 | 位点、行数、命中/转发计数、失败原因 |

响应头 `X-ReadySet-Route` 标明本次请求由 `readyset`（本地）还是 `databend`（转发）处理。

## 5. 第 1 阶段的限制

- 单表、固定结构。
- **内存按全量物化估算。** 每个缓存都在内存里存一份完整结果，不能淘汰。千万行实测：快照后约 8 GB（包含一份内部的全量 id 缓存）；
  每增加一个缓存，增加 2.5 到 6.4 GB（字符串作为键时最大）；3 个缓存约 20 GB，大批量写入时峰值约 23 GB。
  本地磁盘约 1 到 2 GB。内存不够时进程会被系统杀掉，所以容器的内存上限要留出余量。
- **本地范围查询在大表上很慢。** 千万行时 `score >= ? AND score < ?` 本地要 1.6 到 1.9 秒，而 Databend 直查只要约 70 ms。
  原因正在排查，在此之前不建议给大表的范围查询建缓存。点查、IN 和字符串等值查询本地 1 到 8 ms。
- 单个本地结果最多 10,000 行或 4 MB，超过时这条查询转发给 Databend。
- **重启会重读全量**：重启后复用自己的 Stream，先读 Stream 位点处的快照，再消费 Stream 中积累的变更，停机期间的写入不会丢；
  但本地副本是重建的（大表启动慢），`CREATE CACHE` 声明也不持久化，重启后需要重发。
- 同步出错（包括写入 ReadySet 后 COMMIT 结果不确定）时停止同步，所有读取转发给 Databend，需要重启恢复。
- JDBC 还不能连接（需要 login/session 透传，计划在第 3 阶段完成）。bendsql 和 Python driver 可用。
- 只有一个配置账号，没有多用户权限隔离。
- Databend 不强制主键唯一。重复的 `id` 和完全相同的行都会照常同步，本地结果和 Databend 一致。修改主键也可以正常同步。
- 大批量变更写入 ReadySet 期间（例如 2,200 万变更行用了约 164 秒），本地读请求转发给 Databend，结果正确，但这段时间没有缓存加速。

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

# Docker + 最小权限账号：同步、写入被拒、SIGTERM、停机期间写入/删除/改主键后同卷重启
DATABEND_HOST=<databend-host> IMAGE=readyset-databend:dev bash readyset-databend/docker/e2e.sh
```

`gateway_verify.py` 加 `--oracle` 时使用 POC 校验模式：每批都重新校验全部内容，仅限小表。
Python 脚本需要一个能建库的账号（密码放在 `DATABEND_PASSWORD` 环境变量里）。每个脚本最后都会打印 `Report:` 路径，
指向本次的检查明细和请求记录。`e2e.sh` 自己创建临时用户和角色，结束时删除；业务库会保留，方便事后检查。
