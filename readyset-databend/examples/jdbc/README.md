# databend-jdbc 只读示例

`ReadOnlyExample.java` 是一个只依赖 databend-jdbc 的单文件示例：连接 readyset-databend，用 PreparedStatement 做一次点查，然后用本服务 `/health` 的计数判断这次查询是在本地缓存返回，还是转发给了 Databend。

已用 JDK 17 和 databend-jdbc 0.4.8 编译运行验证，见 `poc/jdbc_example_verify.py`，证据在 `poc/evidence/jdbc_example_20261117/`。

## 1. 先建缓存（只做一次）

```bash
curl -s -u readyset:$DATABEND_PASSWORD -H 'Content-Type: application/json' \
  -d '{"sql":"CREATE CACHE orders_by_id FROM SELECT id, status, amount FROM shop.orders WHERE id = ?"}' \
  http://127.0.0.1:8001/v1/query
```

## 2. 编译运行

```bash
# databend-jdbc 0.4.8 的 jar 已内置依赖，Maven 坐标 com.databend:databend-jdbc:0.4.8
javac --release 17 -cp databend-jdbc-0.4.8.jar -d classes ReadOnlyExample.java

READYSET_JDBC_URL=jdbc:databend://127.0.0.1:8001/shop \
READYSET_HEALTH_URL=http://127.0.0.1:8001/health \
READYSET_USER=readyset DATABEND_PASSWORD=... EXAMPLE_ID=42 \
java -cp classes:databend-jdbc-0.4.8.jar ReadOnlyExample
```

输出示例：

```
row: id=42 status=paid amount=0.42
rows: 1
route: local cache (readyset)
```

可以用环境变量覆盖的参数：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `READYSET_JDBC_URL` | `jdbc:databend://127.0.0.1:8001/shop` | 本服务的地址（不是 Databend 的地址），路径是会话的当前库 |
| `READYSET_HEALTH_URL` | `http://127.0.0.1:8001/health` | 本服务的 `/health` |
| `READYSET_USER`、`DATABEND_PASSWORD` | `readyset`、空 | 本服务配置的账号 |
| `EXAMPLE_SQL` | `SELECT id, status, amount FROM orders WHERE id = ?` | 带一个 long 参数的查询 |
| `EXAMPLE_ID` | `42` | 参数值 |

## 3. 怎么判断命中了本地缓存

JDBC 驱动看不到响应头 `X-ReadySet-Route`。示例在查询前后各读一次 `/health`：
- `counters.readyset` 增加 1、`counters.databend` 不变，说明是本地返回；
- 其他情况说明转发了。

这两个计数是整个实例的，有其他流量时差值会混进去，所以这个判断只适合在测试环境或低峰期单独验证。

转发时看 `counters.forward_reasons` 里哪个原因在增长：

```bash
curl -s -u readyset:$DATABEND_PASSWORD http://127.0.0.1:8001/health \
  | jq '{readyset: .counters.readyset, databend: .counters.databend, reasons: .counters.forward_reasons}'
```

常见原因：
- `shape_not_matched`：查询和 `CREATE CACHE` 的形状不一致，例如列不同或条件写法不同；
- `not_cached_table`：查询的表不在本实例配置里；
- `own_write_pending`：这个连接刚写过这张表，同步追上之前会转发；
- `stale`、`not_ready`：表还在加载，或同步落后。

## 4. 注意事项

- **连接复用：** 每新建一个连接，本服务都要向 Databend 登录一次。生产代码里，每个线程长期持有一个连接、复用 PreparedStatement 最快。
- **事务：** 显式 `BEGIN` 之后，事务里的读全部转发。`setAutoCommit(false)` 不会开启事务。
- **空结果：** 键不存在时返回 0 行，也算本地返回。
