# 第 1 阶段验证证据：readyset-databend 单二进制

- 基线：分支 `poc/databend-stream-m0` 的 `63cbe2ff679155d92cc0372c7040113372740750`，验证对象为其上的未提交改动。
- 远端独立 checkout：`wu@192.168.191.96:/data1/vmbox/readyset-single-binary`；授权 Databend `http://192.168.191.96:8000`，只使用新建的 POC 库。
- 原始结果目录：`/data1/vmbox/readyset-phase1-results-20261113-115236/`；`validate.sh` 是本次运行的完整脚本。
- 本地 19 个源码/配置文件与远端验证快照的 SHA256 逐一一致。
- **注意：** 这些结果针对阶段 1 当时的代码（有界的 oracle 同步路径）。此后同步改成了流式生产模式，
  最终代码的回归结果见 `../final_20261113/`，规模测试见 `../scale_*_20261113/`。

| 验证 | 结果 |
| --- | --- |
| `cargo fmt --check`、严格 Clippy（readyset-databend + readyset-server，all-targets，`-D warnings`） | 通过 |
| readyset-databend Rust 单测 | 24 通过 |
| readyset-server 单测（`integration::it_works` 过滤） | 15 通过 |
| Python 单测 | 45 通过 |
| 旧链路 gateway.py + 重构后的 databend-m2：IN/范围完整 live | 804/804 |
| 旧链路：旧 JSON 声明 live | 259/259 |
| M2 Stream live（1,000 行） | 142/142 |
| 真实 engine 结果预算回归 | 28/28 |
| **单二进制**：IN/范围完整 live（`--gateway-binary`） | **805/805** |
| **单二进制**：旧 JSON 声明 live | **260/260** |
| **单二进制**：客户端兼容 | bendsql 11/11，Python driver 0.34.0 / 0.34.3 各 11/11；JDBC 0.4.6 / 0.4.8 失败（已知，需 login/session 透传，第 3 阶段） |
| **Docker 最小权限端到端**（早期 `/tmp` 版脚本，见 `docker_e2e.log`） | 12/12 |
| **修改主键**（`../../pk_change_verify.py`，单二进制） | **382/382** |
| **Docker 最小权限端到端，仓库版脚本**（`../../../docker/e2e.sh`，新增停机期间改主键） | **13/13** |

修改主键的测试新建一个 20 行的库，声明点查、`id IN (?,?)`、`score=?`、`score>=? AND score<?` 四个缓存。
每个场景结束后，用整表范围查询、所有涉及主键的点查和 IN 查询、按 score 的等值查询，把本地结果和 Databend 直查结果逐一比对，
并核对副本行数。另建一个仅用于观察的 Stream，记录 Databend 对每个场景给出的原始变更行（见 `pk_change.log`）：

- 改成新主键、两行互换、批量平移、新旧主键重叠的平移、在事务内删一行再把另一行改成它的主键、复用之前删掉的主键、
  一次同时改主键和其他列：Stream 都给出“旧主键 DELETE + 新主键 INSERT”，同一个 `row_id`，`is_update` 为真；同步结果正确。
- 事务内改走再改回：Stream 没有变更行。改走再改回且换了 payload：只剩同一主键的一对 DELETE/INSERT。
- 事务内插入新行再改主键：只有一条 INSERT（新主键）。
- 把主键改成已存在的值（Databend 不强制唯一）：同步以 `duplicate source key` 停止，Stream 不推进（事务已回滚），
  `/readyz` 返回 503，读取全部转发给 Databend 且结果正确。

第一次运行时，测试脚本自己的“重叠平移”场景把 9..12 改成了 10..13，和已有的 13 冲突。同步按预期停止，
Stream 里保留了这批变更（已核对），所以这次失败的是测试设计，不是同步逻辑。之后改成 9..12 → 8..11 重新运行。

单二进制的 live 比旧链路多 1 项检查：引擎在同一进程内，故障注入改用仅测试时开启的 `POST /__fault/engine`，
多出的一项检查确认这个钩子被接受。

Docker 端到端（脚本现为 `../../../docker/e2e.sh`）使用临时角色和用户（业务库只有 `SELECT` + `CREATE`），结束后已删除。覆盖：
启动并就绪、Databend 侧只多出一个 Stream（没有新库、没有其他表）、bendsql 本地点查和 IN、外部 UPDATE/INSERT 同步、
经本服务转发的写入被 Databend 按权限拒绝、SIGTERM 正常退出、停机期间的写入和删除在同卷重启后可见。

已知限制见 `../../../docker/README.md` 第 5 节，自测命令见第 6 节。
