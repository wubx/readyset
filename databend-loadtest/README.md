# databend-loadtest

压测工具，压测对象是任何实现了 Databend HTTP 查询接口（`POST /v1/query`，加 `next_uri` 翻页）的服务。
同一套命令既能直接压 Databend，也能压前面的 readyset-databend，结果可以直接对比。

## 编译

在仓库根目录执行：

```bash
cargo build --release --locked -p databend-loadtest
# 产物：target/release/databend-loadtest
```

## `point`：闭环点查

每个并发 worker 发一条查询，跟随 `next_uri` 直到查询结束（和驱动的行为一致），检查返回行数，再发下一条。
统计窗口内的成功请求数折算成 QPS，并给出 p50/p90/p99/p99.9 延迟。预热阶段的请求不计入。

```bash
export DATABEND_PASSWORD='<password>'
databend-loadtest point \
  --endpoint http://127.0.0.1:8000 \
  --template "SELECT id, ext_id, amount FROM db.t WHERE id = {k}" \
  --kind int --key-space 10000000 --expect-rows 1 \
  --concurrency 64 --duration 30 --warmup 5 --label databend-id
```

- `--template`：SQL 模板，`{k}` 会被替换成键。
- `--kind`：键的生成方式，要和被测表的数据生成方式一致。
  - `int`：序号本身；
  - `ext`：`substr(sha2(to_string(i), 256), 1, 24)`，用来模拟随机字符串业务键；
  - `mod`：`i % --modulus`，非唯一键。
- `--key-space`：键的范围；`--hot N` 只从前 N 个键里取，用来模拟热点。
- 键序列由 splitmix64 决定，`--seed` 相同时可复现。
- `--expect-rows`：每次查询预期的返回行数，不符合的计为错误。

输出一行 JSON：`qps`、各分位延迟（毫秒）、`errors`，以及按 `X-ReadySet-Route` 响应头统计的 `routes`
（readyset-databend 返回 `readyset` 或 `databend`，直连 Databend 时为 `none`）。

压测器和被测服务在同一台机器上时会争抢 CPU，报告结果时要注明。
