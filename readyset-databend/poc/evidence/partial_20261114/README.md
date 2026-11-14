# 原生部分物化、软内存预算与千万行验证（2026-11-14）

## 结论与范围

本轮在单二进制中接入 ReadySet 原生 partial materialization、原生 allocated 软预算/淘汰、冷读超时回源，保留默认全量模式。
支持范围仍是单底表、固定四列、无 JOIN/聚合。没有引入 Router、第二个进程或 Databend 元数据表。
未提交、未推送；旧 8001 手工服务未重启/替换，测试库保留。

验证环境：`wu@192.168.191.96:/data1/vmbox/readyset-single-binary`，Databend `http://192.168.191.96:8000` v1.2.962。
所有编译/测试均在远端，使用：

```bash
export PATH="$HOME/.cargo/bin:$PATH"
export LIBCLANG_PATH=/tmp/readyset-m1-libclang
export CARGO_TARGET_DIR=/data1/vmbox/readyset/target
cargo clippy --locked -p readyset-client -p readyset-databend --all-targets --no-deps -j 8 -- -D warnings
cargo test --locked -p readyset-client -j 8 --lib view::tests
cargo test --locked -p readyset-databend -j 8
cargo build --locked -p readyset-databend --bins -j 8
cargo build --locked --release -p readyset-databend --bin readyset-databend -j 8
BINARY=$CARGO_TARGET_DIR/release/readyset-databend \
  python3 -B readyset-databend/poc/partial_scale_bench.py
```

`cargo fmt --check`、严格 Clippy、reader/view 28 个测试、Databend library 23 个和 m1 4 个测试、Python 45 个测试均通过。
三种模式 HTTP 回归各 805 项；全量/部分模式重复键与主键变更回归各 518 项均通过。
原始文件：`gateway_{full,partial,oracle}.json`、`pk_{full,partial}.json`、`regression-and-first-scale.log`。

`source-sha256.txt` 核对实际远端编译源码与本地源码一致（含全部 readyset-databend/src、锁文件及原生 view 修复）。
另核对远端已有的 readyset-server builder/lib/startup 修改与本地基线一致，未覆盖远端无关修改。
release 二进制 SHA256：`ddb4ba2271543224d49507cc7bb99ed3512e79ed1e6418100cdf69b9460e5a34`，见 `remote-build.txt`。

## 千万行成功轮次

- 数据库：`readyset_gateway_partial_20261114_095720`；远端输出 `/data1/vmbox/readyset-partial-20261114_095720`。
- 参数：`--partial --memory-limit-mb 2048 --upquery-timeout-ms 5000`，release 单进程。
- 数据：`id,payload,score,note`；千万行，score 有 100,000 个不同值，每值 100 行。
- 声明：id、score IN、score 上下界范围、payload 等值；id 声明复用内部 reader。
- 37 项阶段检查通过，见 `scale-report.json`；`scale.log` 为未经改写的完整输出。

| 阶段 | 结果 |
|---|---|
| 快照 | 引擎 16.16 s，启动墙钟 17.01 s |
| 快照后 | RSS 960 MiB；内部 reader 仅 1 key slot |
| 四个声明后 | RSS 1,226 MiB；全部 reader 仅 15 key slots（含标记键） |
| 声明耗时 | id 0.01 s、IN 13.03 s、range 45.07 s、payload 13.02 s |
| 8 并发范围压力 | 1,800 次 × 5,000 行，27.68 s，全本地、零回源；逐行及重复数核对 |
| 原生淘汰 | 压力结束 epoch 合计 32，key slots 随压力回落；不是只设置了预算 |
| 压力后等待 5 s | allocated 2,015 MiB，RSS 3,201 MiB |
| 淘汰后范围重读 | miss_keys 1,807 → 1,808，3.94 ms 本地返回，与 Databend 一致 |
| 百万行 UPDATE | 117 次并发读取：17 本地、100 回源，均为完整旧/新行，无半批次结果 |
| UPDATE 后 | 点查/IN/范围/字符串/空结果/重复行均本地返回，按多重集合对齐 |
| 重复行删除 | 两份相同内容删除后，保留相同 id 的不同内容，结果正确 |
| 主进程最终状态 | rows 10,000,001；ready=true；failed=null；replay_timeouts=0 |
| 含 UPDATE 的 RSS 高水位 | 3,425 MiB（约 3.34 GiB），不是普适峰值保证 |

allocated 的 2 GiB 软目标**不是 RSS 或 cgroup 硬上限**。RocksDB 默认 1 GiB block cache、固定开销、allocator 保留页、回填/写入及 HTTP 结果都要留余量。
新增缓存虽然不再全量物化 reader，但原生 RocksDB 新索引仍会扫描底表；创建缓存不能宣称瞬时完成。
key_slots 是原生键槽观测值，不是行数/内存字节精确计量。epoch 是淘汰通知计数，不解释成淘汰行数。

### reader 冷/热查询

| 查询 | 行数 | 冷读 ms | 热读 ms |
|---|---:|---:|---:|
| id 点查 | 1 | 1.47 | 0.89 |
| score IN (3,4) | 200 | 9.31 | 1.10 |
| score >= 2 AND score < 4 | 200 | 3.53 | 1.20 |
| payload 等值 | 1 | 1.29 | 0.87 |
| IN 含不存在的值 | 100 | 2.03 | 1.10 |
| 不存在的 id | 0 | 1.07 | 1.12 |

这里的冷读是 **reader 未驻留**，不是清空 OS page cache/RocksDB 缓存后的物理磁盘冷读；单次延迟不是分位数基准。
所有 cold/hot、淘汰后和 UPDATE 后结果都与 Databend 按多重集合比对，不去重。

## 零等待超时与后台回填

另起一个测试进程，使用同一新建测试库、不同实例 Stream，`--upquery-timeout-ms 0`：

1. 第一次 payload 冷读回源，64.41 ms，结果正确。
2. 后台原生 replay 完成，下一次查询本地返回，1.32 ms。
3. `replay_timeouts=1`、`failed=null`，同步和之后本地命中均正常，见 `zero-wait-final.json`。

## 修复和失败记录

1. **原生范围过量回填。** 同列上下界被映射到重复维度 [score,score]，旧边界 `[(lo,NULL),(MAX,hi)]` 扫描大半张表。
   在原生 `KeyComparisonBuilder` 中对相同物理列取所有维度的边界交集，保留索引布局和残余过滤。
   反向、空区间及谓词顺序有测试。范围修复前千万行冷读曾超过 5 秒；未靠延长超时绕过。
2. **HTTP 查询生命周期记录预算。** 首轮修复后 2,500 次查询结果全部正确，但 462 次回源，淘汰后首条点查也回源，整轮判失败。
   是 60 秒内的 2,048 记录预算，不是引擎 poison 或 replay timeout。失败日志完整保留在 `regression-and-first-scale.log`。
   最终压力突发控制在 1,800 次，同时新增“全部必须本地返回”的强断言；不把回源当作部分回填验证成功。
   就绪检查改用实时 `/health`，不再依赖采样 `status.json`。
3. **退出采样噪声。** 成功轮次最后 RSS 采样线程遇到 exiting/zombie 进程无 VmRSS 字段，报 KeyError；业务检查均已通过。
   已对 OSError/KeyError 安全退出，远端用模拟 zombie `/proc/status` 单独验证，不改写原始日志。
4. 后续将“淘汰后确实增加 miss、而非只命中仍热的键”的核验加入脚本断言。对本轮原始 report 独立运行等价断言已通过，
   未为了新加这一断言重跑或修改已有规模报告。最终脚本还有这一额外检查（38 项），原始本轮为 37 项。

## 一致性及剩余限制

标记键先预热，记录共享 canonical reader 的淘汰 epoch；插入后只作无 replay 的物理句柄探测，出现/删除均成功且 epoch 不变才发布就绪。
hole/淘汰则清理重试，读 gate 始终排他。这只覆盖当前单底表投影/参数条件，不是任意 JOIN/聚合的通用批次屏障。
并发原生 eviction 与 fence 的回归已覆盖，冷读超时仅本请求回源；其他不确定引擎错误仍 fail closed。

仍未解决：多表/真实 schema、重启不重做快照、缓存声明持久化、JDBC login/session、HTTP 短突发记录容量优化、广范围回填限流。
旧全量模式三缓存约 20 GB 和范围 1.6–1.9 s 为历史轮次，不是同代码 A/B；本轮只重跑了全量小表回归。
未做 Docker/cgroup 限制下的本轮千万行压力复测，软预算不能替代该验证。
