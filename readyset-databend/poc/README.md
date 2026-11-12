# M0：Databend 快照 / Stream 衔接及源端批次原子性

后续阶段：[M1 全表加载与点查](M1.md) · [M2-A 增量与重复投递正确性](M2.md)。

## 结论

2026-11-12，在用户指定的自托管 `http://192.168.1.100:8000` 上完成验证，
版本为 `Databend Query v1.2.962-652b12b49c`。最终一次完整运行 **45/45 项断言通过**，
包含 136 条 SQL，预期的错误也记录在查询日志中。离线客户端测试 **5/5 通过**。

验证支持继续实施“全量初始化 + Stream → 持久化批次 → ReadySet”方案，
但只证明本次小规模实验中的源端事务行为，不证明跨 Databend / ReadySet exactly-once、
服务端崩溃持久性或生产性能。

当前开发分支：`poc/databend-stream-m0`。M0 本身没有修改现有 Rust 执行引擎；M2-A 的位点扩展另见其文档。

## 文件

- `m0.py`：Python 标准库实验脚本，不依赖 Rust 编译或第三方 Python 包。
- `test_m0.py`：HTTP 分页、session、错误、边界和不自动重试的离线测试。
- `evidence/20261112_075558/report.json`：最终断言结果、版本、快照和观察事实。
- `evidence/20261112_075558/queries.jsonl`：完整 SQL、query ID、事务状态、结果和预期错误；不含认证头。

## 安全边界与运行

脚本每次创建新的 `readyset_m0_<UTC timestamp>_<random suffix>` 数据库；
不读取或改写已有业务表，不重启服务、不更改全局配置，不取消其他人的查询。
默认保留测试对象，便于核对。没有自动删除逻辑。

```bash
# 只做离线测试，不连接数据库
python3 -B -m unittest discover -s readyset-databend/poc -p 'test_m0.py' -v

# 会创建新数据库、表和 Stream，并在其中执行 DML、消费和事务测试。
# 只对明确授权的实例使用 --confirm-create。
python3 -B readyset-databend/poc/m0.py \
  --endpoint http://192.168.1.100:8000 \
  --user root \
  --confirm-create
```

密码从 `DATABEND_PASSWORD` 环境变量读取，未设置时为空；不要把真实密码写入脚本或命令参数。
当前指定实例的 `root` 空密码认证成功。结果默认写入 `/tmp/readyset-databend-m0/<database>/`。
脚本不会重试 SQL。请求结果不确定时退出并保留证据，而不是重新消费 Stream。

独立 HTTP 客户端分别承担管理、源表写入、观察和消费事务；事务请求携带返回的 session，
并在需要时携带 sticky node。每条查询完成后释放其结果资源，不是每条 SQL 都新建逻辑事务。
客户端有分页、行数、响应大小和时间边界；这是实验工具，不是生产连接器。

## 1. 固定 snapshot ID 与增量边界

实验流程：

1. 在测试表上先建立 Standard Stream，开启 change tracking。
2. 写入初始数据，记录快照 S。
3. 在获取 S 后、创建真正消费 Stream 前，执行 INSERT、UPDATE、DELETE。
4. 创建 `ON TABLE ... AT (SNAPSHOT => 'S') APPEND_ONLY = false` 的 Stream。
5. 按 S 分两段读取全量，两段之间继续写入源表。
6. 在本地用 multiset 应用 Stream INSERT / DELETE，检查与当前源表完全相等，包含记录重数。

结果：两段全量始终读取同一个 S；获取 S 和创建 Stream 之间的变化、全量扫描期间的变化都被捕获；
普通 SELECT 不推进 Stream。全量 + 增量重建成功。

### 重要限制：change tracking 必须覆盖起点

另一个没有预先开启 change tracking 的表，写入数据并产生历史快照后，再尝试从该历史点创建 Stream，
得到 **2733**：`Change tracking has been missing for the time range requested`。

所以不能假设任意既有历史快照都能用来创建 Standard Stream。

证据 query ID：`01a1cec1a6217c6094d8f399dfa295d5`。

## 2. 已有业务数据的表如何启动

额外验证了更直接的初始化路径，不要求重写源表数据：

```sql
-- 示意名称，实际脚本使用独立测试数据库中的 fully-qualified 对象。
CREATE STREAM source_changes ON TABLE source_table APPEND_ONLY = false;

SELECT id, val
FROM source_table AT (STREAM => source_changes)
WHERE id <= <boundary>;

-- 源表继续发生 UPDATE / INSERT。

SELECT id, val
FROM source_table AT (STREAM => source_changes)
WHERE id > <boundary>;
```

Stream 创建前已经存在数据。分段扫描期间更新旧行、插入新行，两段仍读到起始版本，
再叠加 Stream 变化后与当前源表相等。

**POC 初始化优先采用这条路径：先创建 Stream，再按它的起始位点读全量。**
初始化全量扫描完成前，禁止消费、替换或由其他消费者推进这个 Stream；
否则后续的 `AT (STREAM => ...)` 可能绑定不同起点。
若未来要边全量扫描边将变化落入 outbox，需要先固定一个不可变的快照引用并另行验证。
长时间全量扫描涉及的历史保留、清理及积压容量尚未验证。

两段扫描的 query ID：

- `01a1cec1a6b47a60b94a11cd6bf15e5a`
- `01a1cec1a7ff79f18d1efb8b2d3fe135`

## 3. Stream 消费与批次落地的原子性

实际验证的基本形态：

```sql
INSERT INTO batches
SELECT <batch_id>, id, val,
       change$action, change$row_id, change$is_update
FROM source_stream;
```

消费 DML 自身会推进 Stream，不需要再执行单独的 `WITH CONSUME` SELECT。
目标表预先创建，不能在消费事务中执行 DDL，因为 DDL 有隐式提交行为。

| 场景 | 结果 |
| --- | --- |
| autocommit 消费 INSERT | 完整写入 2 行批次，Stream 清空 |
| 显式事务写批次及 batch_meta，提交前由另一 session 观察 | 批次和 metadata 不可见，Stream 仍是原进度 |
| 显式 ROLLBACK | 批次和 metadata 均不存在，原 Stream delta 完整保留 |
| 显式 COMMIT | 批次、metadata 和 Stream 进度一起对外可见 |
| 消费语句后、提交前源表新增一行 | 新行保留给下一批，不被前一批误消费 |
| 消费 INSERT 执行期 CAST 失败 | 没有批次行，Stream 保留原 delta |
| 消费成功，后续另一条 DML 执行失败，然后 COMMIT | 原先批次被回滚，Stream 保留原 delta |
| UPDATE / DELETE / INSERT 批次 | outbox 完整保存 payload 和 change 元数据 |
| `WITH CONSUME` SELECT 在显式事务中回滚 | 返回过的变化仍保留给下次消费 |
| 零行消费在显式事务中回滚 | Stream 进度和 metadata 均回滚 |

显式事务提交证据：`01a1cec1ac8f7fe287966a3690daeca6`。
消费语句执行失败证据：`01a1cec1ae1c7af28d3659427c9ea65f`。
后续语句失败证据：`01a1cec1af5576d2b606aeb28aa49aa7`。

### 特别注意：失败事务的 COMMIT 可以返回成功

后续 DML 失败时 session 进入 `Fail`。再提交 `COMMIT`，本次版本返回 `Succeeded`，
但前面的批次没有提交，Stream 也没有推进。

证据：`01a1cec1af697fc0b93d3428fd81ab9b`。

因此生产同步器必须保留事务内所有语句的错误，遇错回滚并停止本批；
不能仅凭最后一条 COMMIT 的成功响应就发布“批次已提交”。

### 特别注意：过滤到零行也会消费

```sql
INSERT INTO batches
SELECT <batch_id>, id, val,
       change$action, change$row_id, change$is_update
FROM source_stream
WHERE id < 0;
```

目标零行，但 Stream 的 2 行 delta 被消费。不能用目标行数、目标快照未变化来推断“没有消费”。
复制原始表时不要在消费查询中随意过滤行；若业务确实过滤，需要证明投影状态维护逻辑正确。
空批次、过滤批次的提交记录需要明确设计，不能只依赖 outbox 中是否存在行。

## 4. 两个消费者竞争同一个 Stream

两个独立事务都从同一个 Stream 读取变化，各写入不同目标表，避免目标表冲突干扰结论。
第一个事务提交成功；第二个 COMMIT 返回 **2734 StreamVersionMismatched**。
失败事务没有留下目标行，两个目标合计只有一份完整 delta。

- 成功提交：`01a1cec1b7117a4280b6414100e9a06b`
- 冲突提交：`01a1cec1b7337a329c267f407e0f4900`

这验证了该竞争路径的冲突检测，不意味着自动获得选主、批次编号唯一性或通用 exactly-once。
POC 同步器仍应单消费者、串行捕获，并在冲突后重新判断源端进度。

## 5. 对下一步实现的约束

建议下一阶段采用：

```text
创建 Standard Stream
  → 按 Stream 起点读取全量（期间不消费）
  → 全量持久化并确认完整
  → 在源端事务中消费到 outbox + 保存 batch_meta
  → ReadySet 读取 outbox 并应用数据与位点
  → 确认可恢复进度
```

事务中的批次号、schema generation、实际行数与是否成功提交必须关联。
批次号不能当作数据库天然唯一约束，也不能在响应丢失时盲目重试消费。
本轮实验只用固定 batch_id 和行数验证原子性；还没有实现安全的编号分配、
提交结果不确定时的查询恢复或 outbox 回收。

### 尚未验证

- query / meta 节点 kill、进程崩溃、断电及恢复；未影响用户服务。
- 实际网络断开导致 COMMIT 响应丢失；只在离线客户端测试中验证传输异常不会重发 SQL。
- 长期历史保留 / VACUUM 与 Stream 积压。
- MAX_BATCH_SIZE 边界、更新对拆分、巨大批次的内存与吞吐。
- DDL / TRUNCATE / 表替换 / 空表初始化的完整协议。
- 多表一致性、多节点 HTTP sticky 路由、租户和权限隔离。
- ReadySet 持久化确认、批次重放、全量物化与随机点查性能：尚未连接 ReadySet。

因此 M0 的源端前提已通过本轮实验，接下来可以做 M1 静态全量装载；
不能把上述结果扩展为生产级端到端持久性保证。

## 测试对象

最终证据对应数据库：`readyset_m0_20261112_075558_07f27ffb`。
探索运行还留下以下隔离数据库，均只包含本次合成数据：

- `readyset_m0_20261112_075333_9961fad4`
- `readyset_m0_20261112_075410_1a144403`
- `readyset_m0_20261112_075451_328cfc4c`

本轮没有 DROP。后续如需清理，应核对上述精确名称并单独确认，不能用通配方式删除。

## 参考

- https://docs.databend.com/sql/sql-commands/ddl/stream/create-stream.md
- https://docs.databend.com/sql/sql-commands/query-syntax/query-at.md
- https://docs.databend.com/sql/sql-commands/ddl/transaction/begin.md
- https://docs.databend.com/sql/sql-commands/query-syntax/with-consume.md
- https://docs.databend.com/developer/apis/http.md
