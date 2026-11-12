# ReadySet / Databend POC

该目录目前是独立实验组件，不是完整的 Databend 协议代理或生产复制器。
开发分支：`poc/databend-stream-m0`。

- [M0：快照与 Stream 衔接、消费与批次落地原子性](poc/README.md)
- [M1：静态全表加载与随机 key 点查](poc/M1.md)
- [M2-A：Stream 增量同步、UPDATE / DELETE 与重复投递](poc/M2.md)
- [声明式统一入口：Databend HTTP 本地查询与透传](poc/GATEWAY.md)

M1 的 `databend-m1` Rust binary 使用当前工程的 ReadySet server/client 实现，而不是旧构建产物：

```text
Databend 固定快照
    → m1_export.py 导出 JSONL
    → databend-m1 批量写 ReadySet 底表
    → 全量物化参数化点查
    → 随机遍历全表校验、重复随机/空结果/并发查询
```

全量数据由同步工具加载，不由业务应用按热点 key 填充。第一版刻意将源端导出和本地查询分开，
确保点查阶段没有 Databend 连接或查询回源。

M2-A 已接入真实 Standard Stream、源端原子 outbox、Databend generation/sequence 位点，
串行维护完整索引，并验证 UPDATE、DELETE、删除后重插和历史批次重复投递。
两轮独立实验各 142 条断言通过；旧批次不覆盖新值、不复活已删除记录。
M2-A 本身是实验编排器；后续 `poc/gateway.py` 已加入声明式 Databend HTTP 入口和后台自动捕获，
匹配模板且副本就绪时走 ReadySet，其余请求透传 Databend，分页和 query lifecycle 也可代理。
原生 `databend_driver` 的本地/源端混合连接已验证；这是 HTTP Query API 子集，尚非完整协议兼容。
仍无进程/节点崩溃恢复或 MySQL/PostgreSQL wire frontend，不宣称端到端 exactly-once 或生产 SLA。

本阶段只使用合成数据和独立 POC 数据库；没有改写用户已有业务表。
