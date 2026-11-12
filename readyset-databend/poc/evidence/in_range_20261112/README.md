# 原生 IN / 数值范围：第三阶段验证证据

## 代码与环境

- 基线：`878ceff2a9ea4af10980e6bbc5f5de9a3880d152`；分支 `poc/databend-stream-m0`，验证对象为其未提交工作树。
- 本地只编辑、审查、核对文件哈希；编译、Rust 格式化、单测和 live 均在 `wu@192.168.191.96`。
- 独立 checkout：`/data1/vmbox/readyset-in-range-validation`。
- 授权 Databend：`http://192.168.191.96:8000`；仅本轮新建合成 POC 数据库。
- 原始结果：`/data1/vmbox/readyset-in-range-results-20261112-221629/`。
- `report.json` 记录 31 个源码/配置文件 SHA256、Rust/Python/driver 版本、两个 binary SHA256、每条命令及其日志 SHA256。
  归档后逐一核对：本地 31 文件与远端最终测试快照一致，16 份日志哈希一致。
- 构建环境：`PATH="$HOME/.cargo/bin:$PATH"`，`LIBCLANG_PATH=/tmp/readyset-m1-libclang`，
  `CARGO_TARGET_DIR=/data1/vmbox/readyset/target`；Cargo 使用 `--locked`、通常 `-j 8`。

## 最终结果

以 `report.json` 的 `passed: true`、各步骤 exit code 和各 run 原始 checks 为准：

| 验证 | 结果 |
| --- | --- |
| 动态声明 + 既有 AND 等值 + 新 IN/范围全链路，两次独立 live | **804/804 × 2** |
| 无 Databend 的真实 engine 结果预算/布局/区间回归 | **28/28** |
| 旧 JSON 声明入口 live | **259/259** |
| M2 Stream/更新/删除/重复投递 live（1,000 初始行） | **142/142** |
| Databend、SQL、双 parser、rewrite 单元/文档测试（native_regression） | **2,823 passed，24 ignored，2 filtered** |
| 上项内 Databend Rust 单测 | lib **19** + binary **4** |
| 原生 dataflow 库单测 | **242 passed** |
| 原生 client/Reader 库单测 | **149 passed** |
| replication-offset | lib **118** + doc **3** |
| Databend checkpoint reopen / metadata | **1 / 3 passed** |
| Python 单测 | **45 passed** |
| Clippy | databend + dataflow、all-targets、`-D warnings` 通过 |
| Rust fmt / git diff --check | 通过 |

两个 PostgreSQL 13/15 服务依赖的 rewrite-equivalence 测试**明确排除，不计为通过**。
原因延续前阶段环境缺少相关服务；命令在 report 中显式 `--skip` 对应前缀。已有 24 个 ignored 也未运行。

## 覆盖与限制

- 继续复用原生 parser、RewriteBetween、adapter rewrite、LiteralSlots::match_read、for_read、make_keys、View/read_cache；无手写应用 SQL 执行器。
- IN：单列、多不同列 AND、字符串/Unicode、重复键、空结果、部分 key 不存在、源行重复保留、长度和分组不匹配回源。
- 范围：`>`、`>=`、`<`、`<=`、`BETWEEN`，单值/反向/空区间，Int64 两端，id/score，多个等值加一个范围字段，条件顺序与 native 形状限制。
- live 与 Databend 按无序多重集对照；覆盖索引字段 UPDATE、DELETE、NULL 变化、原生 driver、分页、DROP/recreate/内部 reader 隔离、单 Stream 与不重启。
- 修复原生 full Reader 多键读取中“一个 absent point 导致丢掉其余 hits”的错误：同一个 read guard 下保留命中，full 的 absent point 为已知空；partial 的 hole 仍为 miss。
- 预算实验使用合成 engine 数据，刻意超出 gateway 的 9,000 现存行 POC 限制，以验证 engine 的 10,000/10,001 行边界。
  转义 JSON >4 MiB 时保守 miss；超限不会返回截断成功，也不禁用该 engine。
  这**不是扫描量或总堆内存硬上限**；原生 IN 的 10,000 lookup-key 展开上限另由 Rust 测试覆盖。
- live 最后故意结束**自身创建的 engine**，验证 fail-closed 和回源；这是故障降级测试，**不是崩溃恢复证明**。
- 仍为固定 schema、单源表、最多 8 公共缓存；不开放 IN 加范围、多个范围字段、NOT IN/NOT BETWEEN、字符串范围、NULL 参数或聚合/排序/LIMIT。
- 同形但不同 IN 布局的声明在迁移前拒绝，普通查询布局未匹配则原样回源；不增加危险物理别名和 DROP 引用计数问题。

## 保留目录核对

与开始本阶段时的 SHA256 快照逐文件比较，以下工作树的 tracked 和非忽略 untracked 源文件不变：

- `/data1/vmbox/readyset`：5,928 文件；
- `/data1/vmbox/readyset-validation`：5,936 文件；
- `/data1/vmbox/readyset-native-validation`：5,947 文件。

共享 target 是明确授权的编译目录，不属于上述源码“不变”保证。未覆盖 `.git`、凭据、虚拟环境或 RocksDB 运行状态；没有破坏性清理、外部服务重启或业务表写入。
