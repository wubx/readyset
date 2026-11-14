# 多表基础层 / 真实 schema 原生接入验证（2026-11-14）

**这里只完成了基础层，不是 HTTP 多表上线验收。**
范围、剩余工作和原生接口约束见 [`MULTITABLE.md`](../../../MULTITABLE.md)。
本轮多表工作未修改 ReadySet controller/dataflow/storage/replay/eviction 核心；工作区已有原生 view 范围修复属于上一轮部分物化工作。

## 已验证

全部在 `wu@192.168.191.96:/data1/vmbox/readyset-single-binary` 编译/测试：

```bash
export PATH="$HOME/.cargo/bin:$PATH"
export LIBCLANG_PATH=/tmp/readyset-m1-libclang
export CARGO_TARGET_DIR=/data1/vmbox/readyset/target
cargo clippy --locked -p readyset-databend --all-targets --no-deps -j 8 -- -D warnings
cargo test --locked -p readyset-databend -j 8
DATABEND_TEST_ENDPOINT=http://192.168.191.96:8000 \
  cargo test --locked -p readyset-databend -j 8 --lib engine::live_schema_tests -- --ignored --nocapture
cargo build --locked -p readyset-databend --bins -j 8
```

- 严格 Clippy 通过，fmt 通过。
- library：30 passed；实库测试默认 ignored（普通单测不会建库），显式启用后 1 passed；m1 4 passed。
- 原生单 controller 异构表测试覆盖 full/partial 两种模式，含两个库的同名表、String id、UInt64 最大值、Boolean、Decimal、NULL、相同重复行单份删除。
- Python 单测 45 项。
- 最终 debug 构建重跑原有 full/partial/oracle HTTP 回归，各 805 项；报告分别为 `gateway_{full,partial,oracle}.json`。
- 部分模式重复键/主键变更回归 518 项，见 `pk_partial.json`。

原始终端输出见 `final-validation.log`；源码远端格式化后取回，本地与实际测试远端逐文件 SHA256 一致，见 `source-sha256.txt`。
本轮未重跑千万行压力或 Docker/cgroup 测试；上一轮部分物化规模报告不是本轮多表规模证据。

## 真实 Databend 端到端原语测试

Databend HTTP v1.2.962：`http://192.168.191.96:8000`。
最终轮次保留新库：

- `readyset_gateway_multi_a50e1ae3de69_a`
- `readyset_gateway_multi_a50e1ae3de69_b`

两库都使用 `items` 表，但 schema 不同：

```sql
-- A
(id STRING NOT NULL, active BOOLEAN NOT NULL, amount DECIMAL(18,2) NULL)
-- B
(id BIGINT UNSIGNED NOT NULL, value STRING NULL)
```

步骤：

1. 每表真实 DESCRIBE → 类型映射 → 原生 DDL，在同一个 controller 中注册不同的底表。
2. 每表恰好一个 Stream，命名符合 `<db>.readyset_<instance_id>_<table>`；没有 helper/meta 表。
3. `AT (STREAM => ...)` 分页快照直接写入原生 Table；初始 A 两份相同行，B 一行 UInt64 最大值 + NULL。
4. 使用相同原生 parser/rewrite/slot/read_cache 和进程内 ReadRequestHandler 读取，与 Databend 返回逐行对齐。
5. A 插入新行，更新两份重复行的 Decimal；B 独立插入新行。
6. A consuming read 全读完后 ROLLBACK；随后 A 仍读到全部 5 条变更，B 仍读到自己的 1 条变更。
7. 各自完整解码 → 原生 Table 写入（DELETE 在 INSERT 前）→ checkpoint → 原生持久化确认 → COMMIT。
8. 本地新行、Boolean false/NULL、热键两份更新后相同行均与 Databend 对齐；再次消费两个 Stream 都为空。
9. A 新增一列后再开始下一轮消费会返回 schema drift 错误；B 的 schema 和 checkpoint 保持独立。

实际测试中，两个来源共享本次运行 generation，各自维护 sequence；使用原生全局 `PersistencePoint::Persisted`，没有比较不同来源不可比的 generation。
这验证的是底层组合能力，尚未验证 HTTP 生产批次可读发布/故障隔离。测试 harness 复用了旧 loader（含一个额外未用空底表），不是最终 Runtime 启动方式。

## 发现并修复的协议问题

首轮真实测试失败：Databend BOOLEAN string fields 返回 `"1"/"0"`，而初版适配输出 `"true"/"false"`。
修复为 1/0，并保持接受 source boolean JSON/string 表达；之后原生测试和实库测试均通过。
失败日志保留于 `initial-boolean-wire-failure.log`，没有删除或改写。
该轮库 `readyset_gateway_multi_43a85d7ff402_{a,b}` 及中间成功轮次 `readyset_gateway_multi_6726d690d8f4_{a,b}` 也保留。

## 当前限制

- 生产 HTTP 入口仍为单表固定 schema；新 SourceCatalog/typed snapshot/typed Stream 尚未接入多表配置与调度。
- 尚未泛化真实 schema 的生产 marker/epoch fence，不能复用另一张表的标记证明本表可读。
- schema 检查仅检测观测到的漂移，不等于原子 DDL 协调/在线迁移。
- 首批类型限 Boolean、各宽度有/无符号整数、String、Decimal 精度 <=28 及 nullable。其他类型明确拒绝，不转 String/Float 蒙混支持。
- HTTP 层每表 readiness、staleness、故障隔离、共享预算压力、重启/最小权限等多表验收尚待完成。
- 没有 commit/push；旧 8001 服务未替换/重启，所有新测试库/Stream 保留。
