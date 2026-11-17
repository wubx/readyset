# JDBC 只读示例的编译运行验证（2026-11-17）

验证的是 `examples/jdbc/ReadOnlyExample.java`，脚本是 `poc/jdbc_example_verify.py`，结果在 `report.json`，10/10 通过。
环境：OpenJDK 17，databend-jdbc 0.4.8，readyset-databend release `7aafdb122b7a…`，真实 Databend v1.2.962（单个 query 节点）。
新建了库 `readyset_gateway_jdbcex_20261117_160516_62fbd7`（保留），表 `orders` 有 1,000 行。
缓存是 `CREATE CACHE orders_by_id FROM SELECT id, status, amount FROM <db>.orders WHERE id = ?`。

示例用 `javac --release 17 -Xlint:all -Werror` 编译，没有警告。之后按三种情况各运行一次，每次都把示例自己的判断和脚本读到的 `/health` 计数对比：

| 运行 | 查询 | 示例输出 | `/health` 计数 |
|---|---|---|---|
| matching | `SELECT id, status, amount FROM orders WHERE id = ?`，id=42 | `row: id=42 status=paid amount=0.42`（和 Databend 一致），`route: local cache (readyset)` | 本地 +1，转发 +0 |
| missing | 同上，id=999999 | `rows: 0`，`route: local cache (readyset)` | 本地 +1，转发 +0 |
| mismatch | `SELECT id, status FROM orders WHERE id = ?`，id=42 | `row: id=42 status=paid`，`route: forwarded to Databend` | 本地 +0，转发 +1，`shape_not_matched` +1 |

`binaries.sha256` 里记录了被测程序、驱动 jar 和示例源码的哈希。远端运行的示例源码和仓库里的文件一致。
