# ReadySet / Databend 开发交接与验证说明

> 分工：**另一台机器继续开发、审查并提交；本机保留为 Databend 联调和验证环境。**
> 本文件仅记录交接流程，不代表已经同步、暂存、提交或推送代码。

## 1. 接手前先读

- 工作分支始终为 **`poc/databend-stream-m0`**，不要在 `main` 上开发或提交。
- 交接时本机仓库：`/data1/vmbox/readyset`。
- 交接基线 commit：**`9d06bba539c654c9f71450b701dec341367c89e1`**。
- 当前实现及实验记录**全部未提交**：既有 tracked 文件修改，也有大量 untracked 新文件。
- **只复制 `git diff` 会漏掉新 crate、同步脚本、声明匹配、HTTP 入口及证据。**
- 不要同步 `.git/`、`target/`、嵌套 `target/`、凭据、虚拟环境或 RocksDB 运行时状态。
- 不覆盖目标机已有工作，不执行 `git reset --hard`、`git clean -fd`，不使用带 `--delete` 的 rsync。
- 联调只允许访问已授权端点和新建独立 POC 数据库；不能修改业务表、重启外部服务。
- 不宣称生产 SLA、完整 Databend 协议兼容或端到端 exactly-once。

完整说明入口：

1. [组件概览](readyset-databend/README.md)
2. [M0：快照、Stream 边界和源端原子性](readyset-databend/poc/README.md)
3. [M1：全表加载和首次随机点查](readyset-databend/poc/M1.md)
4. [M2-A：增量、UPDATE / DELETE 和重复投递](readyset-databend/poc/M2.md)
5. [声明式 Databend HTTP 统一入口](readyset-databend/poc/GATEWAY.md)

## 2. 已实现什么

目标是 **Databend 的完整 serving/index 副本**，不是应用维护的热点 key 缓存：

```text
声明源表和查询模板
         │
         ├─ 固定 Stream 边界全表加载
         └─ 后台 Stream → 原子 outbox/meta → ReadySet 增量应用

Databend HTTP 客户端 → 统一入口
                       ├─ 模板匹配 + 安全 session + 副本就绪 → ReadySet
                       └─ 其他情况                         → Databend
```

- 使用本仓库当前源码构建的嵌入式 ReadySet，查询经 loopback TCP ReaderHandle RPC。
- 全量物化，不需要应用先查询某个 key；新增且从未访问的 key 也可本地返回。
- 本地查无记录直接返回空，**不以空结果为条件回源**。
- 源端事务同时消费 Stream、写 outbox 和 batch metadata。
- 使用专用 `DatabendPosition { generation, sequence }`，而不是冒用 MySQL 位点。
- 同一串行批次的行变更和底表 checkpoint 走同一持久化写路径；等待明确的 `Persisted`。
- 重复/旧批次在进入 dataflow 前跳过；跨 generation、缺号、截断、内容篡改拒绝应用。
- HTTP 透传保留原始 query body 和源端响应，支持 query 分页及生命周期代理，不自动重发 SQL。
- 已验证原生 `databend_driver 0.34.0` 的本地/源端混合连接。

ReadySet 原生就支持 `--query-caching=explicit` 与 `CREATE CACHE`/upstream fallback。
但 **Databend upstream、声明整表复制及 HTTP 入口是本 POC 新增的适配能力**，不是现有 SQL adapter 的原生 Databend 支持。

## 3. 代码地图与待提交范围

### 既有 tracked 文件修改

| 路径 | 内容 |
| --- | --- |
| `Cargo.toml` | 注册 workspace member `readyset-databend` |
| `Cargo.lock` | 新 crate 的依赖项；引用工作区已有依赖 |
| `replication-offset/src/lib.rs` | 追加 Databend 位点变体及比较、显示、解析处理 |
| `dataflow-state/src/persistent_state/mod.rs` | 仅新增 Databend 行/位点受控重开测试，未修改既有存储写逻辑 |

### 新文件/新目录

| 路径 | 内容 |
| --- | --- |
| `replication-offset/src/databend.rs` | generation/sequence 类型及序列化、比较测试 |
| `readyset-databend/Cargo.toml` | 实验 crate，`databend-m1` / `databend-m2` binary |
| `readyset-databend/src/lib.rs` | 批次模型、完整性检查、去重、串行 sink、持久化确认 |
| `readyset-databend/src/declaration.rs` | 保守 AST 查询声明校验与匹配 |
| `readyset-databend/src/bin/m1.rs` | M1 全量加载、首次点查、随机/并发正确性和阶段计时 |
| `readyset-databend/src/bin/m2.rs` | M2 engine 控制接口、声明 reader、本地查询和 barrier |
| `readyset-databend/poc/m0.py`、`m1_export.py`、`m2.py` | 源端边界实验、导出和增量实验 |
| `readyset-databend/poc/gateway.py` | loopback Databend HTTP 入口及后台增量捕获 |
| `readyset-databend/poc/gateway_verify.py` | 新建独立源表的 HTTP/原生 driver live 验证 |
| `readyset-databend/poc/test_*.py` | 离线回归 |
| `readyset-databend/poc/declarations.example.json` | 声明示例 |
| `readyset-databend/poc/*.md`、`README.md` | 各阶段结论、边界及复现 |
| `readyset-databend/poc/evidence/` | 精选、可复核的实验报告和日志 |
| `handout.md` | 本交接说明 |

交接时 `readyset-databend/` 整体约 3.4 MiB，精选证据适合一起同步。证据中有本机绝对路径和旧源码哈希，
它们是**历史运行的 provenance**，不要为了让路径“好看”而改写；换机复测时生成新证据。

## 4. 推荐同步方式：tracked patch + untracked 文件包

以下命令**由人工确认后执行**。不需要在本机先提交，也不会复制 `.git/` 或被 Git 忽略的构建产物。
所有临时交接包放到仓库外。执行期间先暂停代码编辑，保证 patch、文件包与哈希来自同一工作树状态。

### 4.1 在本机导出

在 `/data1/vmbox/readyset` 执行：

```bash
cd /data1/vmbox/readyset
set -euo pipefail

test "$(git branch --show-current)" = poc/databend-stream-m0
test "$(git rev-parse HEAD)" = 9d06bba539c654c9f71450b701dec341367c89e1
git status --short

export HANDOFF_DIR
HANDOFF_DIR="$(mktemp -d /tmp/readyset-databend-handoff.XXXXXX)"
git rev-parse HEAD > "$HANDOFF_DIR/BASE_COMMIT"

# HEAD 对比包括已暂存和未暂存的 tracked 修改。
git diff --binary HEAD -- \
  Cargo.toml Cargo.lock \
  replication-offset/src/lib.rs \
  dataflow-state/src/persistent_state/mod.rs \
  > "$HANDOFF_DIR/tracked.patch"

# 显式包含新增源码、文档和精选证据；Git 忽略的 target/、pyc 不会进入清单。
git ls-files --others --exclude-standard -z -- \
  handout.md readyset-databend replication-offset/src/databend.rs \
  > "$HANDOFF_DIR/new-files.list0"

tar --null --no-recursion -czf "$HANDOFF_DIR/new-files.tar.gz" \
  -T "$HANDOFF_DIR/new-files.list0"

# 为目标机最终工作树生成内容校验清单。
python3 - <<'PY'
import hashlib
import os
from pathlib import Path
import subprocess

out = Path(os.environ['HANDOFF_DIR'])
tracked = subprocess.check_output([
    'git', 'diff', '--name-only', '--diff-filter=ACMR', '-z', 'HEAD', '--',
    'Cargo.toml', 'Cargo.lock', 'replication-offset/src/lib.rs',
    'dataflow-state/src/persistent_state/mod.rs',
])
new = (out / 'new-files.list0').read_bytes()
paths = sorted({p.decode() for p in (tracked + new).split(b'\0') if p})
with (out / 'files.sha256').open('w') as f:
    for name in paths:
        f.write(hashlib.sha256(Path(name).read_bytes()).hexdigest() + '  ' + name + '\n')
print('Files:', len(paths))
PY

(cd "$HANDOFF_DIR" && sha256sum \
  BASE_COMMIT tracked.patch new-files.tar.gz new-files.list0 files.sha256 > SHA256SUMS)

printf '交接目录：%s\n' "$HANDOFF_DIR"
```

先检查 `git status --short`：如果将来增加了上表之外的改动，应同步更新导出范围，不能误以为这套固定清单包含任意后续变更。
这里的命令对应**本次交接**；已有 tracked 删除/重命名、新加入的其他路径也需要另行确认。

将打印出的目录复制到开发机，例如：

```bash
# 替换主机名、目录；不复制本机 .git，不带 --delete。
rsync -av /tmp/readyset-databend-handoff.XXXXXX/ \
  developer@DEV_HOST:/tmp/readyset-databend-handoff/
```

不要使用 `git archive HEAD` 代替：它既不会包含未提交 tracked 修改，也不会包含新增 untracked 文件。

### 4.2 在开发机导入

使用一个**新 clone 或确认干净、没有其他工作的 checkout**。仓库必须包含上述基线 commit；
若需要从 origin 获取基线，可先人工确认 remote 后 fetch，不要在有其他改动的目录直接套补丁。

```bash
cd /path/to/readyset
set -euo pipefail
HANDOFF_DIR=/tmp/readyset-databend-handoff

(cd "$HANDOFF_DIR" && sha256sum -c SHA256SUMS)
read -r BASE < "$HANDOFF_DIR/BASE_COMMIT"
git cat-file -e "${BASE}^{commit}"

# 目标 checkout 有任何工作就停止，改用新 clone；不要自动清理。
test -z "$(git status --porcelain)"

if git show-ref --verify --quiet refs/heads/poc/databend-stream-m0; then
  git switch poc/databend-stream-m0
else
  git switch -c poc/databend-stream-m0 "$BASE"
fi

test "$(git branch --show-current)" = poc/databend-stream-m0
test "$(git rev-parse HEAD)" = "$BASE"
test -z "$(git status --porcelain)"

git apply --check "$HANDOFF_DIR/tracked.patch"
# 先检查内容，应只有声明的 POC 源码/文档/证据路径。
tar -tzf "$HANDOFF_DIR/new-files.tar.gz"

git apply "$HANDOFF_DIR/tracked.patch"
tar -xzf "$HANDOFF_DIR/new-files.tar.gz"
sha256sum -c "$HANDOFF_DIR/files.sha256"
git diff --check
git status --short
```

此时目标机仍是**未提交工作树**，便于继续开发。不要将本机的旧 `target/` 或持久化状态复制过来当作可运行副本。

## 5. 开发机的构建与离线检查

### 工具链

- 仓库 `rust-toolchain.toml`：**Rust 1.99.0**；遵循仓库工具链，不擅自升级依赖。
- 本机 Python：3.12.3；脚本至少使用 Python 3.11 的 `Exception.add_note`，建议使用 Python 3.12。
- 需要 C/C++ 编译环境、clang/libclang、CMake、pkg-config 等 RocksDB 构建依赖。
- HTTP 入口本身只用 Python 标准库；**原生 driver live 验证**另需 `databend-driver==0.34.0`。
- 新机器首次构建需要获取 Cargo.lock 固定的 registry/git 依赖，不能依赖本机 Cargo cache。

本机 libclang 设置只是示例，换机后应指向实际安装路径：

```bash
# 本机的真实版本化文件：/usr/lib/llvm-18/lib/libclang.so.1
mkdir -p /tmp/readyset-m1-libclang
ln -sfn /usr/lib/llvm-18/lib/libclang.so.1 /tmp/readyset-m1-libclang/libclang.so
export LIBCLANG_PATH=/tmp/readyset-m1-libclang
```

不要在别的发行版上盲目照抄 LLVM 路径。使用新虚拟环境安装 driver（如需在开发机运行 live 验证）：

```bash
python3 -m venv /tmp/readyset-databend-venv
. /tmp/readyset-databend-venv/bin/activate
python -m pip install 'databend-driver==0.34.0'
```

### 无需访问 Databend 的检查

在仓库根目录执行：

```bash
cargo build --locked -p readyset-databend --bins -j 8
cargo test --locked -p readyset-databend -p replication-offset -j 8
cargo test --locked -p dataflow-state --lib \
  replication_offset_databend_rows_and_checkpoint_reopen -j 8
cargo test --locked -p dataflow-state --lib meta_ -j 8
cargo clippy --locked -p readyset-databend -p replication-offset \
  --all-targets -j 8 --no-deps -- -D warnings

python3 -B -m unittest discover -s readyset-databend/poc -p 'test_*.py' -v
cargo fmt --check -p readyset-databend -p replication-offset
rustfmt --edition 2021 --check dataflow-state/src/persistent_state/mod.rs
git diff --check
```

注意混合 edition：新 crate 和 `replication-offset` 是 2024，`dataflow-state` 是 2021。
不要用全局 `rustfmt --edition 2024` 格式化存储核心，避免带入无关 diff。

## 6. 本机联调环境与验证命令

### 已授权源端

- Endpoint：**`http://192.168.1.100:8000`**。
- User：`root`；本轮空密码可用。若使用密码，只从私有 `DATABEND_PASSWORD` 环境读取。
- 已验证版本：`Databend Query v1.2.962-652b12b49c`。
- 不因为连接失败改用其他 endpoint，不尝试业务库，不把凭据放入交接包、命令参数或 Git。

建议开发机先完成离线检查，**需要访问 Databend 的 live 实验继续在本机执行**。
所有 live 命令都会新建独立 POC 数据库，必须显式 `--confirm-create`。

### 声明式 HTTP 入口全链路

```bash
python3 -B readyset-databend/poc/gateway_verify.py \
  --endpoint http://192.168.1.100:8000 --user root --confirm-create
```

该命令自行：

1. 创建独立 `readyset_gateway_*` 数据库和合成源表。
2. 生成声明、启动 loopback HTTP 入口和本轮拥有的 ReadySet 子进程。
3. 验证本地匹配、透传分页、原生 driver、事务、后台更新/删除/新增 key。
4. 最后故意终止**本轮自己的 engine 子进程**，验证安全回源。
5. 关闭本轮 gateway，不重启外部服务。原始证据保留。

**最终 `ready=false`、`failed=Broken pipe` 是该故障注入的预期状态**。以顶层 `report.json` 的
`passed`、所有断言及进程退出结果判断，不要因此“修复”测试为静默忽略故障。
这不是恢复测试，不能据此声称崩溃后副本可以续跑。

### M2 增量/重放回归

```bash
python3 -B readyset-databend/poc/m2.py \
  --endpoint http://192.168.1.100:8000 --user root \
  --rows 1000 --confirm-create
```

### 持续运行 HTTP POC

使用已有的**新建独立 POC 源表**，准备符合固定 schema 的声明，参考
`readyset-databend/poc/declarations.example.json`：

```bash
python3 -B readyset-databend/poc/gateway.py \
  --endpoint http://192.168.1.100:8000 --user root \
  --declarations /path/to/declarations.json \
  --output /tmp/readyset-gateway-unique-run \
  --port 8001 --interval 1 --max-staleness 10 --confirm-create
```

- `output` 必须不存在；服务只监听 `127.0.0.1`，Basic 认证使用同一配置 principal。
- 源库必须为新建 `readyset_gateway_*` POC 库，不能填业务库。
- 启动时冻结声明并在访问源端前做 Rust AST 预检；声明不是热更新配置。
- `GET /health` 查看状态；`X-ReadySet-Route`/`X-ReadySet-Template` 查看路由。
- 不要把上一次存储目录复用为“恢复”；当前每次新 generation 都重新全量初始化。
- 停止自己启动的进程即可；不要清理他人的源端查询或重启共享 Databend。

M0/M1 的完整复现见各阶段文档。M1 的 10 万行 JSONL 只在本机 `/tmp`，换机不能假定其存在，应按 M1 文档重新导出。

## 7. 已验证结果与证据

以下是交接前已有结果，不是对新机器环境的保证：

| 阶段 | 结果 |
| --- | --- |
| M0 | 45/45 集成断言，136 条 SQL；snapshot/Stream 边界与源端原子性 |
| M1 | 10 万行，两轮共 242,000 次点查正确；首次任意 key 可用，无查询回源 |
| M2-A | 两轮各 142/142；UPDATE、DELETE、重复/旧批次重放正确 |
| HTTP 入口 | 两轮各 259/259；每轮 121 次 ReadySet query、14 次 Databend query，自动 checkpoint 12 |
| 当前 Python 回归 | 30/30 |
| 当前 crate Rust 回归 | 13/13：9 项 library + 4 项 M1 |
| 位点/存储 | `replication-offset` 118 项单元测试 + 3 doctest；受控重开 1 项及 metadata 兼容 3 项通过 |
| 静态检查 | Clippy、rustfmt、diff 检查通过 |

精选证据位于：

```text
readyset-databend/poc/evidence/
  20261112_075558/       # M0
  m1_20261112/           # M1
  m2_20261112/           # M2-A
  gateway_20261112/      # 最终 HTTP 两轮、原生路由、M2 回归、代码/binary 哈希
```

最终 HTTP 两轮原始目录：

```text
/tmp/readyset-databend-gateway/readyset_gateway_20261112_091519_712fab87/
/tmp/readyset-databend-gateway/readyset_gateway_20261112_091536_1d85f948/
```

最终 M2 回归：

```text
/tmp/readyset-databend-m2/readyset_m2_20261112_091241_d74da232/
```

这些 `/tmp` 文件和源端对象**不属于代码交接的必要内容**，也不是长期备份。
需要保留完整数据时另行归档；不要将庞大的 RocksDB 状态或 binary 加入 Git。

## 8. 不能踩的实现细节

1. **不能设置 `replication_enabled=false` 来“禁用内建复制”。** 当前代码将它视为 shallow-only，
   leader election 会清理 deep state。POC 保留默认配置、不设 upstream/CDC URL。
2. **`ReaderHandle::len()` 不等于增量删除后的现存行数。** 删除 key 可保留空槽；校验必须比较内容和删除 key。
3. **`PersistencePoint::UpTo(target)` 不证明 target 批次完整落盘。** 本 POC 等待明确 `Persisted`；
   使用 1 秒后台 WAL flush。同步 flush 的共享 WAL 水位有已知限制，详见 M2 文档。
4. **去重必须发生在 Table RPC 之前。** 只靠 RocksDB 忽略旧位点，不能证明下游 reader 没收到重复记录。
5. **Stream 输出不是有序 WAL。** 同批先撤回 DELETE before-image，再插入 INSERT；不要依赖返回行顺序。
6. **保留第一条错误。** Databend 失败事务最后的 COMMIT 可能返回成功但实际回滚；不能抹掉前面的错误。
7. **源表/Stream 重建必须切换 generation。** 不要比较两个 generation 的 sequence。
8. **不要把当前源表扫描当并发写下的 reader barrier。** 当前使用固定初始状态 + 已提交批次作为 oracle。
9. **会话/权限不能被模板匹配绕过。** 事务、特殊 settings、角色变化、未知状态和 tenant/warehouse headers 都不能无条件本地执行。
10. **输出目录、query ID、outbox 和元数据不是幂等重试协议。** 提交结果不确定不能自动重发 SQL。

## 9. 后续开发优先级与边界

建议先保证现有行为和兼容性，再扩展能力：

1. 补齐 Databend HTTP session/login/auth/driver 兼容测试，明确 API 子集；不要宣称全协议兼容。
2. 降低全表 barrier/全局串行锁对读延迟的影响，但保留正确的 reader 可服务边界。
3. 设计持久化 bootstrap、generation、controller/authority 恢复和 uncertain-commit resolution。
4. 按源提交、批次应用、落盘确认前后逐点注入进程退出，验证恢复，不只验证“失败后可回源”。
5. 消费者 fencing、批次大小策略、积压/lag 指标和安全 outbox GC。
6. 再扩展多表、通用 schema、更多索引字段、动态声明；当前固定四列与 id 点查不能被文档包装成通用功能。
7. 最后做持续写入、跨机原生 HTTP 查询的吞吐/P99/同步延迟评估。

当前仍有：9,000 行 POC 上限、12,000 历史 key、单批少于 10,000 行、串行全表 barrier、
24 小时实验进程上限、无 GC、无恢复、无 DDL 演进、单 principal 等限制，详见 GATEWAY.md。

## 10. 在开发机提交；本机接收已提交版本做验证

### 开发机提交前

- 确认仍在 `poc/databend-stream-m0`。
- 完成代码审查、离线测试；需要 live 复测时交回本机运行。
- 检查没有凭据、临时路径配置、binary、虚拟环境或 RocksDB 文件进入暂存区。
- 精选 `poc/evidence/` 可随代码提交；原始大数据不提交。
- 不把局部 loopback POC 阶段计时写成生产 SLA，不把失效回源写成恢复完成。

确认后可在**开发机**按明确路径暂存：

```bash
git add -- Cargo.toml Cargo.lock \
  replication-offset/src/lib.rs replication-offset/src/databend.rs \
  dataflow-state/src/persistent_state/mod.rs \
  readyset-databend handout.md

git diff --cached --check
git diff --cached --stat
# 人工审查完整 staged diff 后，再在开发机执行 git commit / git push。
```

本文件不要求也不会在验证机自动执行提交。是否拆分 M0/M1、位点/M2、HTTP 入口等 commit，由开发机审查后决定。

### 将开发机版本交回本机

**不要直接 `git pull` 覆盖本机现有未提交 POC 工作树。** 推荐在本机另建一个干净验证 clone，
同样使用 `poc/databend-stream-m0` 分支；原 `/data1/vmbox/readyset` 保留作为历史对照：

```bash
# 在本机仓库外操作，替换仓库地址，目录必须不存在。
git clone --single-branch --branch poc/databend-stream-m0 \
  '<REPOSITORY_URL>' /data1/vmbox/readyset-validation
cd /data1/vmbox/readyset-validation

test "$(git branch --show-current)" = poc/databend-stream-m0
git rev-parse HEAD
# 核对它正是开发机交付的 commit SHA，再构建并运行第 5/6 节检查。
```

若不能访问远端，可由开发机导出该分支的 **Git bundle**，在本机用 bundle 建立新的独立 clone；
只传已提交版本，不覆盖现有 `.git/`。之后每次交付都记录：

- 开发机 commit SHA、分支、变更目的。
- 实际构建的源码/binary 哈希及工具链。
- live 运行的新 POC 数据库、报告目录、顶层 passed 和失败细节。
- 是否有刻意故障注入、是否清理了本轮自己的测试进程。

这样开发/提交与联调验证分离，也能保证“验证的 binary 对应哪个 commit”可追溯。
