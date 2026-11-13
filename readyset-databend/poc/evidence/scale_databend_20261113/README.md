# Databend 侧规模测试（1,000 万行，2026-11-13）

- Databend：`v1.2.962-652b12b49c`，`http://192.168.191.96:8000`
- 测试库：`readyset_gateway_scale_20261113_144428`（保留，未删除）
- 表结构：`id BIGINT, payload STRING, score BIGINT, note STRING NULL`，1,000 万行
- 脚本：`databend_scale.py`（主测试，结果在 `results_10m.json`，过程日志在 `run_10m.log`）；`page_gap.py`（读取中途停顿的补测）
- 客户端：Python 直接调用 HTTP Query API，每页 10 万行，读到的数据随即丢弃。读取耗时包含 JSON 传输和解析。

## 大批量 UPDATE 的变更计算与消费

| 操作 | Stream 变更行 | 只计数（不消费） | 事务内完整消费读取 | 传输量 | COMMIT |
| --- | --- | --- | --- | --- | --- |
| UPDATE 100 万行 | DELETE 100 万 + INSERT 100 万，`is_update=1` | 1.6 秒 | 2.9 秒（首行 1.2 秒） | 177 MB | 0.017 秒 |
| UPDATE 1,000 万行 | DELETE 1,000 万 + INSERT 1,000 万，`is_update=1` | 1.6–1.7 秒 | 16.6 秒（首行 1.3 秒） | 1.76 GB | 0.017 秒 |

UPDATE 本身 1.3 秒左右。每行变更的 JSON 约 88 字节。

## 推进 Stream 而不读出变更

1,000 万行全表 UPDATE 后（2,000 万变更行），以下三种自动提交的消费读取都会**推进整个 Stream**，之后 Stream 为空：

| 写法 | 耗时 |
| --- | --- |
| `SELECT id FROM s WITH CONSUME WHERE 1 = 0` | 0.036 秒 |
| `SELECT id FROM s WITH CONSUME LIMIT 1` | 1.45 秒 |
| `SELECT count(*) FROM s WITH CONSUME` | 1.73 秒 |

因此：

- 重建时可以用 `WHERE 1 = 0` 几乎零成本地推进 Stream。
- 正常同步时，消费读取绝不能加过滤或 LIMIT，否则没读到的变更也会被推进掉。

## 事务与超时

- `idle_transaction_timeout_secs = 14400`（4 小时）。事务内消费读取后空闲 300 秒再 COMMIT，成功。
- `http_handler_result_timeout_secs = 60`。消费读取中途停顿超过 60 秒才取下一页时，查询被关闭（`Query ... is closed for timed out`），**但事务仍是 Active**：
  - 停顿 30 秒：200 万行全部读完，COMMIT 成功。
  - 停顿 90 秒：只读到 31 万行，查询被关闭。**此时 COMMIT 能成功，但会把 200 万行全部推进掉**，没读到的 169 万行变更丢失。
  - 停顿 90 秒后改为 ROLLBACK：Stream 保留全部 200 万行变更。

以上停顿测试**没有发送心跳**。Databend 的 `POST /v1/session/heartbeat`（请求体 `{"node_to_queries": {"<node_id>": ["<query_id>"]}}`）
会刷新查询的最后访问时间，bendsql 和各驱动在结果超时的一半时间内会自动发送。补测（`page_gap_hb.py`）在读到约 31 万行后停顿，
期间每 20 秒发一次心跳：

- 停顿 90 秒（发 4 次心跳）、停顿 300 秒（发 14 次心跳）：查询都没有被关闭，200 万行全部读完，状态 `Succeeded`，COMMIT 后 Stream 为空。
- 心跳响应都是 `{"queries_to_remove": []}`。查询已经读完或已关闭时，服务端会在这个列表里返回它。

结论：

- 只有在消费读取以 `Succeeded` 状态完整读完所有页之后才能 COMMIT，其他任何情况都必须 ROLLBACK。当前 Rust 客户端（`upstream.rs`）
  遇到未完成的查询会报错，同步随即 ROLLBACK，所以现有代码是安全的。
- 边读边分块写入 ReadySet 是可行的，前提是读取期间按心跳协议保活（间隔小于 `http_handler_result_timeout_secs` 的一半）。
  当前 Rust 客户端还没有实现心跳，大批量同步前要补上。

## 湖仓常见写法在 Stream 中的表现（每项都在新的 1,000 万行副本上测试）

| 写法 | 实际变化 | Stream 报告 | 只计数耗时 |
| --- | --- | --- | --- |
| `INSERT OVERWRITE` 原样覆盖 | 0 | DELETE 1,000 万 + INSERT 1,000 万，`is_update=0` | 1.3 秒 |
| `INSERT OVERWRITE` 改 1% | 10 万行 | DELETE 1,000 万 + INSERT 1,000 万，`is_update=0` | 1.4 秒 |
| `REPLACE INTO ... ON (id)` 100 万行，一半值不变 | 50 万行 | DELETE 100 万 + INSERT 100 万，`is_update=0` | 0.7 秒 |
| `MERGE INTO` 50 万更新 + 50 万插入 | 100 万行 | 更新：DELETE/INSERT 各 50 万（`is_update=1`）；插入：INSERT 50 万（`is_update=0`） | 0.9 秒 |
| `DELETE` 100 万行 | 100 万行 | DELETE 100 万 | 1.5 秒 |
| `TRUNCATE TABLE` | 全部删除 | DELETE 1,000 万；Stream 仍然可用 | 1.4 秒 |
| `ALTER TABLE ... CLUSTER BY` + `RECLUSTER FINAL`（15 秒） | 0 | 无变更 | 0.09 秒 |
| 200 次小批量插入后 `OPTIMIZE TABLE COMPACT`（7 个 block 合并成 1 个） | 0 | 无变更 | 0.07 秒 |

另外，`system.streams.has_data` 在 recluster 和 compact 之后也是 1，但 Stream 里并没有变更。它只能用来在值为 0 时跳过本轮，不能当作有变更的证据。
