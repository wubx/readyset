# 收尾回归：生产模式单二进制（2026-11-13）

- 远端独立 checkout：`wu@192.168.191.96:/data1/vmbox/readyset-single-binary`（基线 `63cbe2ff`，加未提交改动）。
  授权 Databend `http://192.168.191.96:8000`（v1.2.962），每项测试都新建 POC 库。
- 原始结果：`/data1/vmbox/readyset-final-results-20261113-162353/`。本地副本是这里的 `*.log` 和 `*_report.json`，脚本是 `validate.sh`。
- 本地 24 个源码、配置和文档文件与远端逐一核对 SHA256，完全一致。
- 这次回归首次运行时 `cargo fmt --check` 报了格式差异。格式化之后在远端重跑了 fmt 检查、严格 Clippy 和 Rust 单测，都通过了。
  格式化不改变代码行为，所以下表的 live 结果仍然适用于格式化后的代码。

| 验证 | 结果 |
| --- | --- |
| `cargo fmt --check`（格式化后）、严格 Clippy（readyset-databend + readyset-server，all-targets，`-D warnings`） | 通过 |
| readyset-databend Rust 单测 / readyset-server 单测（`integration::it_works`） | 24 / 15 通过 |
| Python 单测 | 45 通过 |
| 旧链路 gateway.py + databend-m2：IN/范围完整 live / 旧 JSON 声明 / M2 Stream live / 引擎预算 | 804 / 259 / 142 / 28，全部通过 |
| 单二进制**生产模式**（默认）：IN/范围完整 live / 旧 JSON 声明 | **805 / 260** |
| 单二进制 **oracle 模式**：IN/范围完整 live / 旧 JSON 声明 | 805 / 260 |
| 修改主键，含重复主键、完全相同的行、`REPLACE INTO` 写回原值（生产模式） | **518 / 518** |
| 客户端兼容 | bendsql 0.34.3、Python driver 0.34.0 和 0.34.3 各 11/11；JDBC 0.4.6 和 0.4.8 失败（已知，需要 login/session 透传） |
| release 二进制打成 Docker 镜像（222 MB）+ 最小权限端到端（`docker/e2e.sh`，含停机期间改主键后同卷重启） | 13 / 13 |

千万行规模测试单独记录在 `../scale_databend_20261113/` 和 `../scale_readyset_20261113/`。
