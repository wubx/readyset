# 声明式统一入口：Databend HTTP → ReadySet / Databend

## 与 ReadySet 原有工作方式的关系

**“显式声明查询缓存，匹配则本地执行，否则 upstream fallback”确实是 ReadySet 原生支持的模式。**
当前代码 `readyset/src/lib.rs` 中 `--query-caching` 默认就是 `explicit`；
`readyset-adapter/src/backend.rs` 的 `Explicit migrations` 文档说明，仅显式 `CREATE CACHE`
等操作触发查询迁移，其他查询走 upstream。写操作和事务也由 upstream 处理。

但必须区分三件事：

1. ReadySet 原生的查询声明和 fallback 主要由现有 SQL adapter 完成；这里没有现成的 Databend upstream。
2. 我们要求的**整表覆盖、不依赖热点访问填充**，依靠之前的全量初始化 + Stream outbox 链路，
   并显式关闭 partial materialization；不能把它误解为任意原生缓存的默认保证。
3. 本阶段增加的是独立的 **Databend 原生 HTTP Query API 子集适配器**，不是声称现有 MySQL/PostgreSQL
   adapter 已经完整支持 Databend，也没有提供 MySQL wire listener。

## 使用模型

```text
Databend 原生 HTTP 客户端
     │ POST /v1/query，SQL 保持原样
     ▼
127.0.0.1 上的 gateway.py
     ├─ 声明模板匹配 + 安全 session + 副本就绪 → ReadySet ReaderHandle RPC
     └─ 其他情况                             → Databend 原生 HTTP Query API

后台任务：Standard Stream → 原子 outbox/meta → ReadySet 行变更 + checkpoint → reader barrier
```

全量同步由入口管理，不需要应用逐 key 填充。匹配模板中的任意 BIGINT key，包括从未查询过的 key，
都可本地查询；**查不到返回本地空结果，不会把空结果当作 miss 回源**。

### SQL 声明入口（第一阶段）

启动只绑定一个已授权的独立 POC 源表，缓存可以从零开始，无需 JSON 查询配置：

```bash
python3 -B readyset-databend/poc/gateway.py \
  --endpoint http://192.168.1.100:8000 --user root \
  --database readyset_gateway_example --table source_rows \
  --output /tmp/readyset-gateway-sql-unique --port 8001 --confirm-create
```

源表必须事先在独立 POC 数据库内创建；此处仍执行一次全量初始化与 Stream/outbox/meta 建立。
`CREATE CACHE` 不授权访问任意业务库，也不创建第二份底表同步链路。

通过同一 Databend HTTP Query API（`POST /v1/query` 的 `sql` 字段）提交：

```sql
CREATE CACHE users_by_id FROM
SELECT id, payload, score, note FROM readyset_gateway_example.source_rows WHERE id = ?;

CREATE DEEP CACHE payload_by_id FROM
SELECT payload, note FROM readyset_gateway_example.source_rows WHERE id = ?;

SHOW CACHES;
DROP CACHE payload_by_id;
```

- 复用 `readyset-sql-parsing::parse_query_with_config` / `CreateCacheStatement`，不自建 CREATE CACHE 语法。
- 先检查原始 AST 的选项和源表、再复用 `ChangeList::from_strings` → `extend_recipe` → `ReaderHandle`。
  **不能只将原始 SQL 交给 ChangeList**：它会丢掉一些 adapter 选项，可能把不支持的语义静默忽略。
- 仍只支持固定四列的普通投影、`WHERE id = ?`。只允许绑定的源表；无前缀表名要求 session database 正确。
- 每次请求一条语句，显式简单 cache name；最多 8 个公共缓存，名称冲突（含大小写冲突）或同形重复模板报错，
  不自动覆盖。`DROP CACHE` 后可以重建；`m2_source` / `m2_by_id` 为保留内部名称。
- 普通 `CREATE CACHE` 与 `CREATE DEEP CACHE` 均走 deep/full 路径。SHALLOW、CONCURRENTLY、ALWAYS、
  AUTOPARAM、TTL/policy 等非默认选项、query ID 声明、注释/hint、多语句及 `DROP ALL CACHES` 明确拒绝。
- 新 reader 使用最近一次成功复制 barrier 的固定 oracle（初始 snapshot + 已提交批次）逐 key 验证，
  包含已删除 key；全部通过才发布路由。迁移或 barrier 结果不确定时关闭本地服务，不自动重试。
- 全列模板直接复用既有内部 full-row reader，而非新建其别名：ReadySet 的 DROP 会删除同一查询的所有别名，
  不能让公共 DROP 误删内部复制 barrier。其他投影正常创建独立查询视图，底表和源端捕获任务始终共享。
- `SHOW CACHES` / `SHOW DEEP CACHES` 返回公共目录三列 `name / query / state`，不是原生 adapter SHOW 输出的全字段兼容；
  内部验证 reader 不显示。CREATE/DROP 成功也返回当前目录。删除最后一个公共缓存仍保留底表及增量任务。
- 控制请求需要 safe autocommit session、允许的 headers、就绪且未过期的副本。事务/权限/未知状态拒绝，
  不可用时返回 503；**缓存控制语句绝不 fallback 到 Databend**，普通查询依然安全回源。
- 原生 `databend_driver` 可通过相同 HTTP SQL 入口使用此子集；没有增加 MySQL/PostgreSQL wire frontend。
- `cache_catalog.json` 仅为成功控制操作的审计快照，**不是可恢复目录**。进程重启后 SQL 动态声明需要重发；
  不声称自动恢复。旧 JSON 的初始模板会在重启时重新加载，运行期 DROP 同样不持久化。
  整个控制过程仍与捕获和普通查询共用串行锁。

### 兼容旧声明文件

仍可用 `--declarations declarations.json` 指定源表及 0..8 个启动模板；与 `--database/--table` 二选一。
`queries` 可省略或为空。示例见 `declarations.example.json`：

```json
{
  "database": "readyset_gateway_example",
  "table": "source_rows",
  "queries": [
    {
      "name": "users_by_id",
      "sql": "SELECT id, payload, score, note FROM readyset_gateway_example.source_rows WHERE id = ?"
    },
    {
      "name": "payload_by_id",
      "sql": "SELECT payload, note FROM readyset_gateway_example.source_rows WHERE id = ?"
    }
  ]
}
```

表名、数据库名可声明，但当前实验数据模型固定为：

```sql
(id BIGINT NOT NULL, payload STRING NOT NULL, score BIGINT NOT NULL, note STRING NULL)
```

源端需要唯一的 `id`。初始化扫描和每次增量都校验该前提。模板投影支持上述列的普通列列表，
过滤固定为 `WHERE id = ?`。这不是通用 schema inference、任意索引字段或任意 SQL 支持。

为防止误操作业务对象，入口只允许明确授权的 `readyset_gateway_*` POC 数据库，
需要 `--confirm-create`，为每次启动生成全新 Stream/outbox/meta 名称。不会替换既有对象，
不会替调用方创建/修改源表，失败后保留证据。启动前先调用 Rust 的 `--validate-only` 校验实际 AST 模板，
通过后才访问源端、检查列结构和创建跟踪对象。

启动文件冻结为源表绑定和初始模板，不热更新；运行期 `CREATE/SHOW/DROP CACHE` 操作的是 Rust 进程中的公共查询目录，
不会改写冻结源表配置或重新初始化复制。

### 路由规则

| 输入/状态 | 路径 |
| --- | --- |
| 相同 SELECT 投影、同一源表、`id = <Int64 literal>` | 就绪时本地 |
| SQL 关键字大小写/空白/结尾分号变化 | AST 相同则本地 |
| 不带数据库的表名 | session database 正确且模板匹配时本地 |
| 参数值换成全新 key 或不存在 key | 本地，不按访问热度判断 |
| `SELECT *`、别名、不同投影、额外谓词、LIMIT、JOIN、聚合、注释/hint、时间旅行 | 除非与受限声明完全匹配，否则透传 |
| INSERT / UPDATE / DELETE、BEGIN / COMMIT / ROLLBACK | 透传 |
| 活跃事务、非默认 role/catalog、非空 settings、未知 session/请求字段、tenant/warehouse headers | 透传，不冒用本地结果 |
| 原生普通 Autocommit session 的已知路由 metadata | 校验默认 role 和字段结构后可本地 |
| 副本未就绪、超时未同步、同步失败、engine 不可用 | 透传 |
| ReadySet 命中但结果为空 | 本地返回空，不回源 |

匹配器在 Rust `src/declaration.rs`，使用工作区 `sqlparser` 完整 AST 比较，
仅将右侧精确 Int64 字面值归一化成 `?`，并在正确 database 下解析无前缀表名。
**不是正则删 SQL 子句后匹配**，不能把 `LIMIT 0`、`OR true`、另一张表或另一数据库误路由成本地查询。
不识别的 Databend 方言直接透传原始 body，而不是先用 MySQL AST 重写后发给源端。

## HTTP 兼容范围

- `POST /v1/query`：透传请求保留原始 JSON body、源端 status/body，转发 `X-Databend-*` headers。
- `GET /v1/query/<id>`、`GET .../page/<n>`、`GET .../final`、`POST .../kill`：源端查询继续代理生命周期，
  不重新提交 SQL。只接受本 origin 下的受限相对路径；不跟随 HTTP redirect。
- 本地查询返回 Databend 风格 `id/schema/data/state/error/stats/*_uri`，数字使用十进制字符串，NULL 为 JSON null。
  单 key 最多一行，不需要继续分页；提供 stats/final/kill 路径。完成查询的 kill 是已完成状态的 no-op。
- 本地结果保留 60 秒，registry 最多 2,048 项；final 后保留临时 tombstone，避免把本地 query ID 转发到源端。
- 原生 `X-Databend-Query-ID` 本地保留；route hint 和无事务的 sticky node 不改变查询语义。
- `X-ReadySet-Route: readyset|databend` 用于观察实际执行路径，本地还有 `X-ReadySet-Template`。
- `GET /health` 返回 checkpoint、行数、ready、failed、inflight、uncertain_upstream 和路由计数。

**不是完整 Databend HTTP 服务实现**：未实现 session login/refresh/token、文件上传、stage、stream load 等端点；
未支持 Bearer token 或多用户认证。原生 `databend_driver 0.34.0` 会在 login endpoint 404 后使用 Basic/query API，
本轮明确验证了这个模式。其他驱动/版本需要单独验证，不能声称完全 drop-in。
本地 query ID 不存在于 Databend 原生 query history，不能拿它在源端做 `RESULT_SCAN` 等操作。

### 安全和失败处理

入口只监听 `127.0.0.1`，所有 query/health/lifecycle 请求均校验与配置相符的 Basic 认证。
只有一个配置 principal，本地副本不提供多租户/RLS/每用户权限隔离。密码读取私有 `DATABEND_PASSWORD`，
不放进参数、配置、日志或证据。没有对外部署或 TLS/auth 边界的生产承诺。

所有源端 SQL/HTTP 均不自动重发。透传提交结果不确定、响应不完整，或未观察到终态就 final 时，
关闭本地加速，避免误以为写入已结束；仍允许源端透传。同步失败也停止自动重试和本地服务，
保留已提交 outbox，不尝试盲目重放。

入口保守地在任何透传 query 提交/终态后使本地暂不可服务，等下一次捕获与 barrier 才重新启用。
跟踪未完成的源端查询，inflight 不为空时不使用本地。这样避免异步写仍在运行时过早重新开启缓存。
这是 POC 的保守策略，可能降低短时间内的命中率。

## 自动同步和一致性

- 启动：新 Standard Stream 固定边界，全表导出到 ReadySet，初始 checkpoint 0。
- 默认每秒执行一次原子 capture。每批从另一个客户端重读已提交 outbox/meta，再按 M2 规则应用。
- 使用“固定初始 snapshot + 已提交批次”重建预期状态，**不把仍有并发写入的当前源表扫描当作 barrier oracle**。
- 等待底表明确 `Persisted`，然后核对内部完整 reader 及每个已声明模板 reader 的实际行内容和已删除 key，才设 ready。
- `max-staleness` 默认 10 秒，按本轮 capture **开始**时间保守判断；不是源端精确 lag 指标或 SLA。
- 查询不会逐次向源端确认版本。外部写入与下一次 Stream 捕获之间仍是 eventual consistency，
  不承诺线性一致或所有跨会话 read-your-writes。要求源端最新结果时可用特殊 session 强制回源。
- HTTP query 与批次应用通过同一串行锁保护；同步、WAL 等待和全表 barrier 期间请求可能等待。
  这是正确性优先的 POC，不是完成了低延迟并发服务优化。

当前有界限制：最多 9,000 个现存初始/增量行、12,000 个历史 key、单批少于 10,000 行变更、
最多 16 个 HTTP handler、1 MiB 请求、16 MiB 单个上游响应；Rust 实验进程有 24 小时运行上限。
每轮完整 barrier、空批次 metadata 和日志/outbox 持续增长，未实现 GC，不适合无限期生产运行。
超限或失败进入源端透传，不悄悄丢批次。

## 启动和复现

开发分支必须为 `poc/databend-stream-m0`；本地开发和提交，Rust 编译/测试及 live 联调放在远端独立验证目录。

```bash
export LIBCLANG_PATH=/tmp/readyset-m1-libclang
cargo build --locked -p readyset-databend --bin databend-m2 -j 8
cargo test --locked -p readyset-databend -j 8
cargo clippy --locked -p readyset-databend --all-targets --no-deps -- -D warnings
python3 -B -m unittest discover -s readyset-databend/poc -p 'test_*.py' -v
```

自动创建独立合成源表、启动入口、执行原生 HTTP/driver 验证并关闭自己启动的服务：

```bash
python3 -B readyset-databend/poc/gateway_verify.py \
  --endpoint http://192.168.1.100:8000 --user root --confirm-create
```

新增 SQL 声明路径的同表多缓存全链路验证（零配置模板启动、动态增删、重复/容量/会话拒绝、原生 driver、晚建缓存及共享增量）：

```bash
python3 -B readyset-databend/poc/gateway_verify.py \
  --endpoint http://192.168.1.100:8000 --user root \
  --binary /data1/vmbox/readyset/target/debug/databend-m2 \
  --sql-declarations --confirm-create
```

若构建设置了外部 `CARGO_TARGET_DIR`，所有 live 命令都要显式指定该目录下的新 binary，不能误用 checkout 内旧产物。
不加 `--sql-declarations` 时仍走旧 JSON 启动模板路径，用于兼容回归。

该 live 测试需要验证机已经安装的 `databend_driver==0.34.0`；入口本身只依赖 Python 标准库及 Rust binary。
测试最后故意终止**本轮拥有的** engine 子进程，验证本地失效后可继续源端透传，不重启任何外部服务。
这只验证 fail-closed routing，**不证明崩溃恢复**。

使用自己的新 POC 源表和声明文件启动常驻入口：

```bash
python3 -B readyset-databend/poc/gateway.py \
  --endpoint http://192.168.1.100:8000 --user root \
  --declarations /path/to/declarations.json \
  --output /tmp/readyset-gateway-new-run \
  --port 8001 --interval 1 --max-staleness 10 --confirm-create
```

`output` 必须不存在。必须先创建声明中的 POC 数据库/源表并使用固定 schema；不能照抄示例去碰业务表。
入口打印实际 origin，也写入 `listening.json`。所有调用指向该 origin，SQL 不需要加 ReadySet hint：

```bash
curl -u root: -H 'Content-Type: application/json' \
  http://127.0.0.1:8001/v1/query \
  -d '{"sql":"SELECT payload, note FROM readyset_gateway_example.source_rows WHERE id = 7"}'
```

每次重启使用新 generation 重新全量初始化；旧的 outbox 和本地状态保留，但**不恢复旧 controller/authority 状态**。
若上次结果不确定，不要把重新启动当作已经验证的恢复协议，更不能自动清除旧 outbox。

## 第一阶段验证：SQL 动态声明

精选报告见 [`evidence/sql_cache_20261112/`](evidence/sql_cache_20261112/)，源码/binary 哈希、
实际工具链、原始报告路径和每项命令退出码在该目录 `report.json`。
构建与测试均在远端 `/data1/vmbox/readyset-validation`，未在开发机编译。

| 检查 | 本轮结果 |
| --- | --- |
| `databend-m1` / `databend-m2` locked build | 通过 |
| `readyset-databend` | 16 项（12 library + 4 M1）通过 |
| `replication-offset` | 118 单测 + 3 doctest 通过 |
| checkpoint 重开 / metadata 兼容 | 1 + 3 项通过 |
| Python 离线回归 | 39 项通过 |
| 严格 Clippy / Rust 格式 / 源码 diff | 通过 |
| SQL 动态声明 live | 两个新数据库，各 350/350 |
| 旧 JSON 声明 live | 新数据库，259/259 |
| M2 增量/重放回归 | 新数据库，142/142 |

SQL live 包含：零模板启动、原生 driver 在正确 session database 下使用无前缀源表进行 CREATE，
多个投影、并发同名 CREATE 仅一次成功、重复/越权/不支持选项/多语句拒绝、8 缓存容量、
新增/删除数据后的晚建缓存、DROP 后其余视图继续收到更新、删除全部再重建，以及不重启 engine、
不更换 generation、所有缓存只建立一个源端 Stream。

每轮 gateway 最后都仅终止本轮自己的 engine，验证 SELECT 可回源、控制 SQL 不透传。
最终 `ready=false / Broken pipe` 为预期故障注入，报告 `passed=true`；不代表实现了崩溃恢复。

## 历史验证与证据（SQL 动态声明前）

以下为交接时的 JSON 声明版本结果；不能将它们当成新增 SQL 控制路径的验证证据。

交接版本两次独立新数据库验证，各 **259/259 条断言通过**：

| 数据库 | 初始行数 | HTTP query：ReadySet / Databend | 自动同步 checkpoint | 源端分页请求 |
| --- | ---: | ---: | ---: | ---: |
| `readyset_gateway_20261112_091519_712fab87` | 100 | 121 / 14 | 12 | 8 |
| `readyset_gateway_20261112_091536_1d85f948` | 100 | 121 / 14 | 12 | 8 |

每轮 100 个 sparse key 的首次查询全部在本地；源端删除一行和新增一行后最终仍为 100 行。
最后都故意终止本轮 engine，最终 `ready=false / failed=Broken pipe` 是预期结果，源端透传仍正确，入口正常退出。
原生驱动既验证了首次本地命中，也验证同一连接“本地 → 未声明查询源端 → 再次本地”的实际路由计数。

离线检查：Rust **13/13**（9 项 library、4 项 M1），Python **30/30**；严格 Clippy、rustfmt、
`git diff --check` 通过。既有 M2 全链路回归在全新 1,000 行数据库再次 **142/142** 通过。

live 测试覆盖：100 个 sparse key 首次触达、多个投影模板、空结果不回源、普通/特殊 session、
未声明表/查询回源、真实多页结果、原生 driver 的本地命中及同一连接本地/源端来回切换、
网关 UPDATE、源端 DELETE/新 key、并发请求、事务自读/回滚，以及 engine 不可用时继续透传。

最终报告、路由日志、原生 HTTP 原始请求/响应、源端 SQL、代码与 binary 哈希归档在
`poc/evidence/gateway_20261112/`。大结果/完整 engine 指令留在各轮 `/tmp/readyset-databend-gateway/`。
测试中预期的故障状态会保留在 final_status，这不应误读为实验失败；以报告的断言和 passed 为准。

## 还没有完成

- 多表/通用 schema、多索引字段、任意 SQL 模板、声明持久化恢复与启动文件热更新。
- 完整 Databend client/login/auth/session/function/HTTP endpoint 兼容性。
- 自动恢复、消费者 fencing、安全 outbox GC、版本升级/降级协议。
- source 表 DDL/schema 演进；源表结构必须固定，不能运行中改 schema。
- 持续写压下的吞吐、查询 P99、真实同步 lag 和跨机端到端 benchmark。
- 跨系统 exactly-once 或生产 SLA。
