# databend-jdbc 兼容性（2026-11-17）

所有测试都在真实 Databend 上运行（`http://192.168.191.96:8000`，v1.2.962，单个 query 节点）。
只新建 `readyset_gateway_jdbc_*` 库，全部保留。
客户端是 databend-jdbc 0.4.8（Maven 上的最新版本，jar 已内置依赖），运行在 OpenJDK 17 上，和 readyset-databend、Databend 在同一台机器。

按生产的用法测：
- 用 `DriverManager` 建连接，不用连接池；
- 用 `PreparedStatement` 绑定参数；
- 读和写都发到同一个地址。

被测程序（`binaries.sha256`）：

| 程序 | sha256 |
|---|---|
| readyset-databend release（JDBC 兼容） | `e3cbfe3fdeab…` |
| readyset-databend release（按会话的写入栅栏，最终版本） | `0ec4ddb6efcd…` |
| databend-jdbc-0.4.8.jar | `274597a199f2…` |

## 修改前的问题

用上一轮的 release（`72e98f09…`）连接时，所有 JDBC 调用都失败：

1. **连接时登录失败：** 驱动建连接时调用 `/v1/session/login`，本服务返回 404。驱动拿不到服务端版本，之后每条语句都报 `NullPointerException`（`Capability.heartBeat()`）。
2. **会话被判为不安全：** 登录问题修好以后，读全部转发，转发原因是 `unsafe_session`。原因有三个：
   - 驱动的会话里多一个字段 `autoCommit`；
   - 第一条语句带 `"txn_state": null`；
   - 分页提示 `max_rows_in_buffer` 是 5,000,000，超过了原来 10,000 的上限。
3. **本地读被名额卡住：** 会话问题修好以后，本地读很快被 `local_results_full` 卡住。驱动读完结果后从不调用 `/final`，每个本地结果都占着一个名额，直到 60 秒 TTL。名额只有 2,048 个，所以 32 线程压测只有 2,025 次本地读，另外 10,893 次被转发。

## 修改内容（`src/bin/server.rs`）

- **会话接口：**
  - `/v1/session/login`、`/v1/session/heartbeat`、`/v1/session/logout` 转发给 Databend。这些请求不执行 SQL，不算作转发的语句。
  - 登录时固定带 `disable_session_token=true`，并从响应里去掉 `tokens`、`server_max_arrow_result_version` 和 `server_arrow_features`。这样本服务不签发 Databend token，驱动也始终用 JSON 格式。
  - `/v1/session/refresh` 仍返回 404，因为它需要 Databend token。
- **Cookie：** 请求的 `Cookie` 和响应的 `Set-Cookie` 原样透传，多个 `Set-Cookie` 都保留。Databend 的 Cookie 会话会保存临时表等服务端状态。
- **会话判定：**
  - 接受 `autoCommit`，但值必须是布尔值。Databend 忽略这个字段，事务只能由显式 `BEGIN` 开启，并通过 `txn_state` 反映出来。
  - `txn_state: null` 视为自动提交。
  - 分页提示里的 `max_rows_per_page` 和 `max_rows_in_buffer` 接受任意正整数。本地分页每页最多 10,000 行。
- **已读完的本地结果单独存放（`Drained`）：**
  - 客户端拿到最后一页以后，结果移出 2,048 个"同时打开"的名额，单独存放。
  - 单独存放的结果有上限：最多 65,536 个、64 MiB，或 60 秒 TTL。超出时淘汰最早的。
  - 被淘汰的客户端自带查询 ID 记入已结束列表。之后对这些 ID 的生命周期请求不会转给 Databend，ID 也不会被重新使用。
  - 不淘汰时，`/final`、`/kill`、状态查询和重取最后一页都照常在本地处理。

新增单元测试：
- `jdbc_session_with_auto_commit_flag_is_safe`
- `reply_keeps_every_set_cookie`
- `last_page_matches_next_uri`
- `drained_results_are_bounded_by_count_bytes_and_ttl`

## 兼容性用例（`poc/jdbc/JdbcCompatProbe.java`，由 `poc/jdbc_compat_verify.py` 驱动）

测试表有 10 万行，列为 `BIGINT`、`VARCHAR`、可空 `INT`、`TINYINT UNSIGNED`、`BOOLEAN`、`DECIMAL(18,2)` 和可空 `STRING`。
`STRING` 列里有单引号、双引号、反斜杠、中文和 emoji。缓存建在 `id = ?` 和 `ext_id = ?` 上。

每个读用例都用同样的 JDBC 调用直连 Databend 再执行一次，逐行比较。比较的内容包括：
- `getObject` 返回的值和 Java 类型；
- `ResultSetMetaData` 的列名、JDBC 类型、类型名和是否可空。

"本地"和"转发"两列是本服务 `/health` 计数器在这一步前后的差值。

三轮都是 21/21 通过：
- `gateway/`：full 模式；
- `partial/`：partial 模式；
- `databend/`：full 模式，压测改为直连 Databend，作为对照。

下表是 `gateway/` 这一轮：

| 用例 | 本地 / 转发 | 结果 |
|---|---|---|
| 建连接（login + `getMetaData`） | 0 / 1 | 驱动 0.4.8，拿到服务端版本 |
| 复用一个 PreparedStatement 查 8 个主键（含 NULL 列、特殊字符、不存在的键） | 8 / 0 | 和 Databend 一致 |
| `setString` 查 `ext_id`（含带引号和反斜杠、不存在的键） | 5 / 0 | 一致 |
| `getLong`、`getInt` + `wasNull`、`getShort`、`getBoolean`、`getBigDecimal`、`getString` | 2 / 0 | 一致 |
| Statement 直接写字面量 | 1 / 0 | 一致 |
| 未缓存的聚合 | 0 / 1 | 一致 |
| `DatabaseMetaData.getColumns` | 0 / 1 | 7 列，一致 |
| 每次新建连接再查一次，共 50 次 | 35 / 15 | 平均每次 13.8 ms |
| `query_result_format=arrow` | 1 / 0 | 驱动退回 JSON，一致，本地返回 |
| PreparedStatement 插入、更新、删除，之后在同一连接立刻读 | 0 / 6 | 立刻能读到自己的写入 |
| 写入后等同步追上 | 1 / 6 | 0.76 秒后本地返回，和 Databend 一致 |
| `setAutoCommit(false)` 后插入 | 0 / 3 | 语句照常自动提交，和直连 Databend 一样 |
| 显式 `BEGIN` + `commit()` / `rollback()` | 0 / 9 | 事务内能看到自己的行，其他会话看不到；提交后可见，回滚后消失 |
| 事务内的点查 | 0 / 3 | 转发，结果一致 |
| 和缓存表同名的临时表 | 0 / 4 | 本会话读到临时表，其他会话仍读到缓存表，和 Databend 一致 |
| `addBatch` / `executeBatch` 插入 3 行 | 0 / 3 | 通过 presign 上传，Databend 能看到 3 行 |
| 关闭连接（logout） | — | 正常 |

**说明**
- **新建连接的那 15 次转发：** 本服务每转发一条语句（包括驱动 `getMetaData` 发出的 `SELECT version()` 和未缓存的查询），所有表的本地读都会暂停，直到下一轮同步（`forward_epoch`，默认约 1 秒）。这 15 次转发就是这个原因，不是 JDBC 的问题。
- **心跳没有单独测：** 直连 Databend 抓包看到，驱动在把 `ResultSet` 交给应用之前，就已经取完了所有分页（25,000 行共 3 页，都在 0.1 秒内取完）。之后应用暂停 75 秒也不需要心跳。心跳请求经本服务转发，返回 200（见各轮 `route_shapes`）。

## 只读压测：64 线程、30 秒，每个线程一个连接，复用一个 PreparedStatement 随机查主键

| 目标 | QPS | p50 | p99 | 本地比例 | 失败 |
|---|---|---|---|---|---|
| 本服务，full | 5,919 | 10.5 ms | 19.1 ms | 99.87%（237 次转发，是压测开始时前面写入造成的 `forward_epoch`） | 0 |
| 本服务，partial | 5,935 | 10.7 ms | 18.4 ms | 99.93% | 0 |
| 直连 Databend | 556 | 112.6 ms | 149.1 ms | — | 0 |

修改前同样的压测（32 线程）：646 QPS，p50 56 ms，本地只有 2,025 次，其余被 `local_results_full` 转发。
修改后 32 线程是 6,593 QPS，p50 4.8 ms，100% 本地。

## 读写混用：64 个读线程，外加 1 个写线程在同一地址写入

### 修改前（release `e3cbfe3fdeab…`，`mixed_w1000/`、`mixed_w100/`）

| 写入频率 | 读 QPS | 读 p50 | 本地比例 | 转发原因 |
|---|---|---|---|---|
| 无写入 | 5,919 | 10.5 ms | 99.87% | — |
| 每秒 1 次 | 275 | 232 ms | **0%** | `forward_epoch` 4,974，`active_forward` 2,894，`source_applying` 417 |
| 每 100 ms 1 次 | 245 | 261 ms | **0%** | `active_forward` 6,831，`forward_epoch` 134，`source_applying` 432 |

原来"读到自己的写入"的范围是整个实例：
- 写入经本服务转发期间（`active_forward`），**所有会话、所有表**的读都转发；
- 转发结束后，每张表都要等下一轮同步覆盖这次写入（`forward_epoch`），这段时间读也都转发。

同步约每秒一轮，写入每秒一次，就永远追不上。

### 修改：按会话、按表的写入栅栏（`src/forward_scope.rs`、`src/bin/server.rs` 的 `WriteFence`）

- **分类：** 经本服务转发的语句开始前，在安全的自动提交会话里先分类（`forward_scope::classify`，用 sqlparser 的 MySQL 方言，规则保守）：
  - **普通查询：** 不设任何栅栏。
  - **单表写入：** 单表的 `INSERT`、`UPDATE`、`DELETE`，没有 JOIN、`USING`、`FROM` 子句、`ON DUPLICATE KEY`，表名最多两段。只给写入的那张表、发出写入的那个会话设栅栏。会话按 Databend `session_id` Cookie 区分；没有 Cookie 时，这张表的所有会话都设栅栏。
  - **其他一律按原来的全局方式处理：** 包括解析不了的语句、含注释或 `$` 的语句、多条语句，以及出现 `MERGE`、`CONSUME`、`FOR UPDATE`、`INTO` 等关键字的查询。
- **栅栏何时解除：** 每次写入取一个递增序号。每轮同步开始时记下"最早仍在途的写入之前"的序号，这一轮成功后，这些写入就算已被覆盖：它们在这一轮开始消费 Stream 之前已经完成。写入会话只有在自己的写入被覆盖之前，读这张表才转发（转发原因 `own_write_pending`）。
- **单表写入的特殊情况：** 第一个响应不是终态（`Running`）、HTTP 出错或响应无法解析时，仍按原来的全局方式处理（`upstream_inflight`、`uncertain`）。
- **已结束的语句：** 开始时第一个响应就已是终态的语句记入一个有上限的已结束列表。之后对它们的 `/final` 等生命周期请求，不再重新设全局栅栏。

新增单元测试：
- 分类器 2 组，共约 40 条语句：JDBC 实际发出的语句；"无法确定的一律为 Unknown"，包括多表 UPDATE/DELETE、`MERGE`、`REPLACE INTO`、`INSERT OVERWRITE`、注释、`WITH CONSUME`、`FOR UPDATE`；
- `WriteFence` 3 个：只拦本会话；必须等写入完成之后才开始的那一轮同步；"最早在途写入"规则；没有会话或会话数超出上限时拦所有会话；
- Cookie 会话 ID 解析 1 个。

### 修改后（release `0ec4ddb6efcd…`，`fence_w0/`、`fence_w1000/`、`fence_w100/`）

三轮兼容性用例都是 22/22 通过。比之前多一个用例 `session_isolation_own_write`：
- 连接 A 插入一行后，立刻在 A 上读这一行，经 Databend 读到了；
- 同一时刻连接 B 读同一行，在本地返回，这时还看不到这一行，符合预期。

| 写入频率 | 读 QPS | 读 p50 | 读 p99 | 本地比例 | 读己之写检查 |
|---|---|---|---|---|---|
| 无写入 | 5,824 | 10.5 ms | 19.7 ms | 99.73% | — |
| 每秒 1 次（实际 30 次） | 5,339 | 10.5 ms | 130.4 ms | 98.51% | 60 次，全部通过 |
| 每 100 ms 1 次（实际 94 次） | 5,462 | 10.2 ms | 119.8 ms | 98.50% | 188 次，全部通过 |

- **写线程的循环：** 每次插入后，立刻在同一连接读回这一行；再更新这一行的 `amount`，再读回。两次都要读到新值才算通过。一轮"插入 + 读 + 更新 + 读"平均约 320 ms，所以"每 100 ms"实际约每秒 3 次。
- **读线程：** 读线程的连接是别的会话，几乎全部本地返回。转发主要是 `source_applying`（同步批次写入本地期间，约 1,600–1,800 次），其次是 `forward_epoch`（约 500 次，来自压测前的用例：建临时表、事务等全局语句）。`own_write_pending` 只有 64 / 191 次，都来自写线程自己的读回。
- **读的 p99：** 从 19.7 ms 升到约 120–130 ms，原因是同步批次写入期间的转发。

## 回归

`regression/report.json`：最终版本的完整回归 20/20 通过，包括 fmt、clippy、Rust 测试、多表、网关三种模式、主键变更、重启恢复和 bootstrap 故障矩阵。
- 网关三种模式的 Python 客户端用例都会调用 `/final`，"已读完结果单独存放"的改动没有影响它们。
- 这些用例不带 Cookie，经本服务转发的写入仍按"这张表的所有会话"处理，原有的一致性检查全部通过。
- 回归中重新构建的 release 和被测 release 逐字节相同（`0ec4ddb6efcd…`）。JDBC 兼容版本 `e3cbfe3fdeab…` 也单独跑过一次 20/20 回归。

## 未覆盖

- 经负载均衡访问多个 Databend 接入节点（sticky node 和 Cookie 会话跨节点）。
- 用 access token（Bearer）连接，以及 `/v1/session/refresh`。
- `presign=off` 时的批量插入。这时驱动走 `/v1/upload_to_stage` 或 `/v1/streaming_load`，本服务对这两个接口返回 404。
