# 引擎读路径压测（绕过 Python 网关）

`databend-bench`（`src/bin/bench.rs`）在一个 Rust 进程内完成：
启动 ReadySet server → 灌入合成数据 → 用与 `databend-m2` 相同的原生链路声明缓存
（`Declaration` → `native_query::compile` → `create_cache`）→ 多个 tokio 任务并发读取。

不经过 `gateway.py`、stdin JSON 通信和 Databend；压测客户端与引擎同进程，抢同一批 CPU。
所以结果是**引擎读路径的上限**，不是端到端 HTTP 指标。

## 维度

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| `--mode` | `local` | server 共享 `Readers`，用进程内 `ReadRequestHandler` 读；与 ReadySet standalone adapter 相同 |
| | `rpc` | 通过 View 的 TCP RPC 读；即 `databend-m2` 目前的方式 |
| `--paths` | `sql` | 每次请求都做 SQL 解析、原生改写、slot 匹配、读取和 JSON 转换，相当于文本 SQL 前端的全部工作 |
| | `prepared` | 复用预编译参数（随机池），只测 reader 本身 |
| `--workloads` | `point` / `in` / `range` | `id = ?`（1 行）、`id IN (10 个)`（10 行）、`score BETWEEN`（默认 50 行） |
| `--write-rps` | 例如 `20000` | 测量期间后台写入新 id，并探测写入确认后到 reader 可见的延迟 |

每个请求都核对精确行数；点查在测量前另做 1,000 次逐值校验。任一错误或行数不符都会计入报告。

## 运行（远端）

```bash
export PATH="$HOME/.cargo/bin:$PATH" LIBCLANG_PATH=/tmp/readyset-m1-libclang \
       CARGO_TARGET_DIR=/data1/vmbox/readyset/target
cargo build --locked --release -p readyset-databend --bin databend-bench -j 8

B=/data1/vmbox/readyset/target/release/databend-bench
$B --mode local --rows 1000000 --concurrency 1,8,32,128 --output /data1/vmbox/readyset-bench-runs/<new-dir>
$B --mode local --rows 1000000 --concurrency 8,32 --write-rps 20000 --output /data1/vmbox/readyset-bench-runs/<new-dir>
```

必须用 release 构建；debug 构建慢约 10 倍以上，数字没有参考价值。`--output` 必须是新目录；
引擎存储为 `DeleteOnExit`，进程退出后由 ReadySet 自行清理。

## 2026-11-13 结果（AMD 5950X 16C/32T，与 Databend 同机）

100 万行，每档 10 s（另有 2 s 预热）。完整报告见 `evidence/bench_20261113/`。

| 场景 | 路径 | c=1 QPS / p50 | c=32 QPS / p50 / p99 | c=128 QPS |
| --- | --- | --- | --- | --- |
| 点查 | local + sql | 19.7k / 49 µs | 320k / 89 µs / 178 µs | 319k |
| 点查 | local + prepared | 454k / 2.0 µs | 7.54M / 3.7 µs / 5.4 µs | 7.72M |
| IN(10) | local + sql | 12.3k / 79 µs | 212k / 136 µs / 263 µs | 211k |
| 范围(50 行) | local + sql | 14.2k / 69 µs | 251k / 117 µs / 217 µs | 244k |
| 点查 | rpc + sql | 8.7k / 113 µs | 125k / 230 µs / 521 µs | 175k |
| 点查 | rpc + prepared | 22.6k / 43 µs | 340k / 90 µs / 181 µs | 513k |

后台以 2 万行/秒写入时，local 读吞吐基本不变（点查 sql c=32 为 311k）；
写入确认后到 reader 可见的延迟 p50 约 5.3–6.0 ms，p99 约 10–13 ms（受 20 ms 探测间隔和约 1 ms 定时器精度影响）。

### 解读

- 本地读取（local + prepared）只要几微秒；性能瓶颈主要在 SQL 文本处理。
- `rpc` 比 `local` 慢：SQL 路径慢约 2.3–2.6 倍，prepared 路径慢 15–22 倍。正式架构应采用 standalone adapter 的进程内读，不要通过 TCP RPC。
- SQL 路径在 c≥32 时 p999 约 3 ms，prepared 路径只有约 25 µs。可能原因是 32 个读任务与引擎争用 32 个硬件线程，
  且测试期间同机负载约为 10（load average）。压测客户端放到另一台机器后应会改善，尚未验证。

### 本轮发现并修复的读路径开销（原生链路内）

1. `SourceContext` 原来每次编译读查询都会用双解析器重新解析固定 DDL；现改为 `OnceLock` 只解析一次。
   点查 sql c=32 从 65k 提升到 91k QPS。
2. 读路径改用 `ParsingPreset::OnlySqlparser`。`for_prod`（BothPreferSqlparser）在 sqlparser 成功时本来就返回它的 AST，
   nom-sql 只用于差异告警；perf 显示它占 SQL 路径 CPU 的 60% 以上。声明仍用 `for_prod`；
   新增单测确认读路径与声明路径的 AST 一致；sqlparser 解析失败的读取会保守回源。
   点查 sql c=32 从 91k 提升到 320k QPS。

改动后已复测：Rust 单测 24、Python 45、IN/范围 Databend live 804/804 全部通过。

## 不包括

HTTP 协议处理与连接、Databend 回源、真实 Stream 同步、跨机网络、多表或大字段，也不包括持久化写入的吞吐上限。
