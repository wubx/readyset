# ReadySet / Databend POC

该目录目前是独立实验组件，不是完整的 Databend 协议代理或生产复制器。
开发分支：`poc/databend-stream-m0`。**部署和日常使用请先看 [使用说明](USAGE.md)。**

- [M0：快照与 Stream 衔接、消费与批次落地原子性](poc/README.md)
- [M1：静态全表加载与随机 key 点查](poc/M1.md)
- [M2-A：Stream 增量同步、UPDATE / DELETE 与重复投递](poc/M2.md)
- [声明式统一入口：Databend HTTP 本地查询与透传](poc/GATEWAY.md)

**单二进制 `readyset-databend`（第 1 阶段）**：一个 Rust 进程同时提供 Databend HTTP 入口、进程内 ReadySet 缓存
和基于 Stream 的增量同步，替代 `gateway.py` + `databend-m2` 子进程，可用 Docker 部署。
Databend 侧只为每张同步表建一个 Stream，不建库、不建其他表。
见 [部署说明](docker/README.md)、[设计](DESIGN.md) 和 [验证证据](poc/evidence/phase1_20261113/README.md)。
千万行规模测试：[Databend 侧](poc/evidence/scale_databend_20261113/README.md)、[ReadySet 侧](poc/evidence/scale_readyset_20261113/README.md)、
[两表各千万行共享预算/淘汰回填/大批次影响基线](poc/evidence/multi_scale_20261114/README.md)、
[原生同步 WAL 与跨表并行优化](poc/evidence/multi_parallel_20261114/README.md)。

当前通过 HTTP SQL 动态声明同表或多表缓存；复用 ReadySet 原生解析、rewrite/参数化、recipe 和 read_cache 查询链路，
第三阶段扩展 IN 和数值范围：

```sql
CREATE CACHE users_full FROM
SELECT id, payload, score, note FROM readyset_gateway_example.source_rows WHERE id = ?;
CREATE CACHE users_payload FROM
SELECT payload, note FROM readyset_gateway_example.source_rows WHERE id = ?;
CREATE CACHE users_by_score_note FROM
SELECT id, payload FROM readyset_gateway_example.source_rows WHERE score = ? AND note = ?;
CREATE CACHE users_by_scores FROM
SELECT payload, note FROM readyset_gateway_example.source_rows WHERE score IN (?, ?, ?);
CREATE CACHE users_in_range FROM
SELECT id, payload FROM readyset_gateway_example.source_rows WHERE note = ? AND score BETWEEN ? AND ?;
SHOW CACHES;
DROP CACHE users_payload;
```

入口可用 `--database readyset_gateway_example --table source_rows` 启动，查询模板不再必须写 JSON。
此 CLI 兼容入口仍要求固定四列 schema；同一底表副本和一条增量链路服务公共查询缓存。真实 schema 和多表使用下述 TOML 入口。
保留旧 `--declarations` 启动模板兼容。新增视图通过内容 barrier 才发布路由；删除公共缓存不删除共享复制状态。
允许普通投影、四列上的 AND 等值/IN，以及一个 `id/score` 字段的 `>`、`>=`、`<`、`<=`、`BETWEEN`，
数值范围可与其他字段的等值连接；结果可为多行并支持本地 HTTP 分页。
`id/score` 为精确 Int64，`payload/note` 为字符串等值/IN（NULL 条件不开放）；条件顺序不同不保证命中。
IN 按原生 slots 检查长度/分组，一个物理查询形状仅允许一个公共声明布局；其他布局的声明拒绝，读取安全回源。
暂不允许 IN 加范围、多个范围字段、NOT IN/NOT BETWEEN、字符串范围、隐式转换、聚合/排序/LIMIT。
动态目录不支持进程重启恢复（仅指此旧 CLI 入口；TOML 多表入口的重启持久化见 [使用说明](USAGE.md)），
不等于任意 SQL、通用 schema 或完整原生 adapter 管理能力。
详见 [IN/范围原生链路与验证命令](poc/GATEWAY.md#原生-in-与数值范围第三阶段)。
[最终证据](poc/evidence/in_range_20261112/README.md)：两轮 live 各 804 项通过，engine 预算 28 项通过，
原生 dataflow 242、client/Reader 149、Python 45 项通过；两项依赖 PostgreSQL 的 rewrite-equivalence 明确排除。

## HTTP 多表与真实 schema

```toml
confirm_create = true
[upstream]
url = "http://<databend-host>:8000"
user = "readyset"
[replication]
tables = ["sales.orders", "crm.customers"]
poll_interval = "1s"
max_staleness = "30s"
[storage]
dir = "/var/lib/readyset-databend"
[cache]
partial = true
memory_limit_mb = 2048
upquery_timeout_ms = 5000
```

`readyset-databend --config config.toml` 启动**一个**原生 ReadySet server、共享 readers/内存软预算，
不是每表一个 Engine。TOML 仅配一张表也走真实 schema；不要同时传 `--database/--table/--declarations`。
每张表独立发现 schema、持有原生 Table、Stream、sequence、可读 gate 和错误状态。
支持 Boolean、各宽度有/无符号整数、String、Decimal(p<=28) 及 nullable，不猜测唯一键，保留完全相同的行。
未支持的类型/表级同步错误只禁用该表；错误表的读回源，其他表仍可使用缓存。
最多配置 32 张表、全实例合计 8 个公共缓存，名称全局唯一。

真实列名用于 `CREATE CACHE`；限定表名按配置精确绑定，未限定表名必须匹配请求 session.database，
`default` 不猜库。跨表 JOIN 仍透传。`/health.tables[]` 分别报告 Stream、checkpoint、行数、
同步/应用中标志、最后同步年龄和失败原因；多表顶层 checkpoint 为 null，不虚构共同位点。
`/readyz` 在至少一张表可用时返回 200，全部不可用返回 503；完整状态另看 `all_tables_ready`。

生产 fence 使用真实物理行与原生 eviction epoch，不在 marker 插入后重放来冒充已消费旧 delta。
仅有 Boolean/小整数等有限域的物理投影无法可靠区分 marker，声明会明确拒绝；物理投影（含键）
须含 String、Int64/UInt64 或精度至少 20 的 Decimal。原表只含小域类型仍可同步、回源，不冒险本地发布。
小域键/宽范围仍可能引发大量预热，需要给内存和 replay 超时留余量。

共享路径使用 ReadySet 原生同步 WAL，各表 Table 写入/offset 独立推进，不再用跨表全批次写锁等待全局刷盘；
图迁移/快照仍排他。A 表应用不占用 B 表读 gate；磁盘/CPU/内存争用或图迁移仍可能造成延迟，
超过 B 自身 max_staleness 仍按规则回源。legacy 后台 WAL 路径不变。
未经证明的透传写入/session 生命周期仍保守使所有表失去可读资格，
直至各表完成新轮同步。重启复用本地持久化的基表、缓存声明和同步位置，只重建崩溃时有未完成批次的表
（见 [使用说明](USAGE.md#5-重启与持久化)）；不宣称 exactly-once。
DDL 在消费前、消费后及提交后复核；这不是原子 DDL 协调，暂不自动迁移 schema。
实现与验证边界详见 [MULTITABLE.md](MULTITABLE.md)。

当前编译/live 验证主机为 `wu@192.168.191.96`，授权 Databend 端点为 `http://192.168.191.96:8000`。
本轮统一 checkout 为 `/data1/vmbox/readyset-single-binary`，编译产物在 `/data1/vmbox/readyset/target`。
历史第三阶段独立 checkout 为 `/data1/vmbox/readyset-in-range-validation`；原仓库和前两阶段验证目录不覆盖，
旧证据中的 IP/路径不改写。

M1 的 `databend-m1` Rust binary 使用当前工程的 ReadySet server/client 实现，而不是旧构建产物：

```text
Databend 固定快照
    → m1_export.py 导出 JSONL
    → databend-m1 批量写 ReadySet 底表
    → 全量物化参数化点查
    → 随机遍历全表校验、重复随机/空结果/并发查询
```

全量数据由同步工具加载，不由业务应用按热点 key 填充。第一版刻意将源端导出和本地查询分开，
确保点查阶段没有 Databend 连接或查询回源。

M2-A 已接入真实 Standard Stream、源端原子 outbox、Databend generation/sequence 位点，
串行维护完整索引，并验证 UPDATE、DELETE、删除后重插和历史批次重复投递。
两轮独立实验各 142 条断言通过；旧批次不覆盖新值、不复活已删除记录。
M2-A 本身是实验编排器；后续 `poc/gateway.py` 已加入声明式 Databend HTTP 入口和后台自动捕获，
匹配模板且副本就绪时走 ReadySet，其余请求透传 Databend，分页和 query lifecycle 也可代理。
原生 `databend_driver` 的本地/源端混合连接已验证；这是 HTTP Query API 子集，尚非完整协议兼容。
仍无进程/节点崩溃恢复或 MySQL/PostgreSQL wire frontend，不宣称端到端 exactly-once 或生产 SLA。

本阶段只使用合成数据和独立 POC 数据库；没有改写用户已有业务表。
