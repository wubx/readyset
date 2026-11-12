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

### 声明文件

示例见 `declarations.example.json`。初始范围仍为一张源表，可声明 1..8 个查询模板：

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

不支持通过 HTTP `CREATE CACHE` 动态修改声明；该语句不会被本入口截获，仍会透传给 Databend。

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

开发分支必须为 `poc/databend-stream-m0`；代码保持未提交。

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

该 live 测试需要本机已经安装的 `databend_driver`；入口本身只依赖 Python 标准库及 Rust binary。
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

## 验证与证据

最终版本两次独立新数据库验证，各 **259/259 条断言通过**：

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

- 多表/通用 schema、多索引字段、任意 SQL 模板、动态声明与热更新。
- 完整 Databend client/login/auth/session/function/HTTP endpoint 兼容性。
- 自动恢复、消费者 fencing、安全 outbox GC、版本升级/降级协议。
- source 表 DDL/schema 演进；源表结构必须固定，不能运行中改 schema。
- 持续写压下的吞吐、查询 P99、真实同步 lag 和跨机端到端 benchmark。
- 跨系统 exactly-once 或生产 SLA。
