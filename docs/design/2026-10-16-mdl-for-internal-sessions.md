# TiDB MDL 与 internal session：问题与方案

> 状态：设计稿，待评审 ｜ 更新：2026-10-19 ｜ 代码基线：master @ 980923943d9
>
> 关联：[#71055](https://github.com/pingcap/tidb/issues/71055)（critical）· [#71064](https://github.com/pingcap/tidb/pull/71064)（upstream WIP）

## 一、摘要

- **问题**：MDL 不把 internal session 的事务算作 DDL 的阻塞者。TTL、MV 刷新、mlog purge 等后台功能通过 internal session 写用户表，因此不受 MDL 保护。#71055 已稳定复现：TTL 删除与 `ADD INDEX` 并发，产生孤儿索引；MV 这边，#72043 尝试用 advisory lock 绕开其中一种冲突，本方案不采用，改为从 MDL 本身修复。
- **方案**：internal session 访问用户表的事务参与 MDL，访问 `mysql` 库的事务继续豁免——从"按 session 类型豁免"改为"按表所在的库豁免"。另外为 TTL 扫描加超时、让 MV 原地刷新对 MV 表加锁，并改进可观测性。ANALYZE 和 IMPORT INTO 本来就有意不参与 MDL，维持不变。
- **代价**：TTL 会阻塞同表的 DDL，最长等到相关事务结束。核心改动约 4 个源文件，不加开关，回滚只需回退二进制。
- **待决**：`mysql` 库豁免与 upstream #71064 的做法不同，需要先对齐。

## 二、背景

MDL 是元数据锁：事务访问表时加锁，DDL 推进 schema 时要等持有旧版本锁的事务结束。具体分四步：

1. **加锁**：语句执行时对访问的表加锁，锁上记着当时的 schema 版本，记录在本节点的事务上下文里，事务结束时释放。snapshot / stale read、autocommit 只读语句、ANALYZE、IMPORT INTO 等不加锁。
2. **发布**：DDL owner 每推进一步，把 job 的版本和涉及的表写入 `mysql.tidb_mdl_info`。
3. **检查**：每个 TiDB 节点每 50ms 检查一遍本节点的 session，只要有事务持有相关表的旧版本锁，这个 job 在本节点就不放行。
4. **放行**：所有节点都放行后，DDL 才进入下一步。owner 的等待没有超时。

所以一个事务要挡住 DDL，必须同时满足三点：持有锁、所在 session 在检查范围内、没有被跳过。

## 三、问题

### 3.1 两层豁免

| 层 | 位置 | 效果 |
|---|---|---|
| 检查时跳过 | `RemoveLockDDLJobs` 开头的 `if sv.InRestrictedSQL { return }` | internal session 一律不算阻塞者 |
| 不在检查范围内 | `Server.CheckOldRunningTxn` 只遍历外部连接 | 即使删掉第一层，同 keyspace 下仍检查不到 internal session |

两层都要处理，只删一层没有效果。nextgen 的 crossKS 检查其实已经遍历 internal session，只是被第一层跳过了。

另外，ANALYZE 和 IMPORT INTO 在语句级不加锁（`skipLockMDL()`）。这是有意的设计，不属于本问题：

- **ANALYZE** 只读用户表，结果只写统计信息，不会造成数据与索引不一致；它往往运行很久，参与 MDL 反而会长时间阻塞 DDL。
- **IMPORT INTO** 要求目标表为空，导入过程本身还会执行 DDL（例如切换表模式），参与 MDL 会挡住自己。

除非发现具体的 corner case，否则维持现状。

### 3.2 后果

#71055 用 failpoint 让 TTL 删除在提交前暂停，同时执行 `ADD INDEX`。结果表里有 100,000 行，索引却有 110,240 条，多出的正好是那个未提交 TTL 批次的行数，`ADMIN CHECK` 报错。自然触发的时间窗口很小，但一旦发生就是数据与索引不一致。

### 3.3 影响面

| 功能 | 访问的表 | 事务形态 | 修复后 |
|---|---|---|---|
| TTL 扫描 | 用户表 | 每条 SELECT 一个事务；可能整段全表扫描，没有语句超时 | 参与 |
| TTL 删除 | 用户表 | 每批默认 100 行，亚秒级 | 参与 |
| MV 初始构建 | 读基表，写正在创建的 MV 表 | TiKV 上用 `IMPORT INTO` | 不需要，见 4.5 |
| MV 刷新（FAST、COMPLETE IN PLACE） | 读 mlog 和基表，写 MV 表 | 一个悲观事务，时长取决于变更量 | 参与；FAST 还需补加锁，见 4.5 |
| mlog purge | mlog 表 | 分批 autocommit 删除 | 参与 |
| stats 初始化、DDL 框架 | `mysql` 库 | stats 初始化是分钟级的显式事务 | 豁免 |

ANALYZE、IMPORT INTO 和 GC worker 不需要参与；LOAD DATA 走用户 session，已受保护。MV 刷新和 mlog purge 目前在特性分支 `feature/release-8.5-materialized-view` 上。

### 3.4 来历

MDL 从第一个 PR #37393（2022 年）起，就有意让 internal session 整体不参与：检查只遍历外部连接，所以 internal session 不需要加锁，这行跳过与此一致。如果让 internal session 既加锁又被检查，就会出现阻塞问题，例如 DDL 框架自己的 session、stats 初始化这类长事务挡住 DDL（见 4.3）。

这个取舍的前提是 internal session 只做系统内部的操作。TTL、MV 刷新等功能开始用 internal session 写用户表之后，这个前提就不成立了。2025 年 nextgen 的 crossKS 检查开始遍历 internal session，但被这行跳过挡住，实际仍不生效。

## 四、方案

### 4.1 判定规则

| | 用户表 | `mysql` 库 |
|---|---|---|
| 外部 session | 参与（不变） | 参与（不变） |
| internal session | **参与（新）** | 豁免（不变） |

不再按 session 类型豁免，因为这正是 #71055 的成因：任何新的后台功能默认都落在盲区里。按表所在的库判断后，新功能只要访问用户表，就自动受保护。

### 4.2 核心改动

| 文件 | 改动 |
|---|---|
| `pkg/sessionctx/variable/session.go` | 删除 `RemoveLockDDLJobs` 的 `InRestrictedSQL` 跳过；阻塞日志加上来源信息 |
| `pkg/server/server.go` | `CheckOldRunningTxn` 补充遍历 internal session。在互斥锁内拷贝 session 列表、锁外逐个检查，避免卡住 session 池 |
| `pkg/testkit/mocksessionmanager.go` | 测试用实现同步修改，否则新测试会假通过 |
| `pkg/planner/core/preprocess.go` | internal 语句访问 `mysql` 库时不加锁 |

最后一项的写法：

```go
// internal session 访问 mysql 库时不加 MDL 锁。这些表的 DDL 基本只出现在
// 升级中，若计入 stats 初始化这类长事务，升级 DDL 可能被拖住。
if sctx.GetSessionVars().InRestrictedSQL && metadef.IsSystemDB(dbName.L) {
	return tbl, nil
}
```

### 4.3 为什么豁免 `mysql` 库

去掉前两层后，所有 internal 事务会立刻参与检查——session 池借出 session 时本来就会把它纳入检查范围，不需要额外改动。系统表上的长事务也在其中：stats 初始化用一个显式事务读取全部 `mysql.stats_*`，大集群要几分钟；而升级中的 DDL 超时只有 75 秒，超时后进程直接退出。

按目前的升级流程，这两件事不会同时发生：升级由一个节点持锁执行，其他新版本节点在锁外等待，旧版本节点仍按旧逻辑运行。所以这是潜在风险，而不是已经发生的问题。豁免 `mysql` 库，等于保留了当初 internal session 不参与 MDL 的初衷（避免阻塞），只是把范围收窄到系统表：系统表的行为与今天完全一致，不必依赖升级流程的这些细节；代价是系统表上的 internal 事务仍不受保护，这一点也与今天一致。

判断必须用库名，不能用保留 ID：classic 里系统表的 ID 是普通分配的，不在保留区间。

upstream #71064 的测试要求系统表上的 internal 事务也阻塞 DDL，与本方案相反，需要在 PR 中说明并讨论。如果最终选择与 upstream 一致，去掉这一项即可，同时补一条"升级 DDL 不会被 stats 初始化拖到超时"的回归测试。

### 4.4 TTL

- **显式纳入检查范围**：`WithForceBlockGCSession` 改名为 `WithRegisteredSession`，TTL 全部改用它，保证进程刚启动时借出的 session 也能纳入检查范围。这与 #71064 一致。
- **扫描超时**：TTL 扫描可能整段全表扫描，又没有语句超时，它决定了 DDL 最长要等多久。本地测得 557 万行约 1.2 秒，外推到 1 亿行需要几十秒。新增扫描超时变量，超时后任务失败并重新调度；默认值需要参考真实集群的数据。删除批次很短，不需要处理。

### 4.5 MV

**初始构建不需要 MDL。** 构建只读基表，写入的是这个 DDL job 自己正在创建、尚不能被外部读写的 MV 表，不存在写入与 DDL 交错的问题。TiKV 上构建走 `IMPORT INTO`，不加锁，维持现状；非 TiKV 存储（实际只在测试中出现）走 `REPLACE INTO`，修复后会自动参与，没有必要但也无害。

**刷新和 purge 需要 MDL。** 以下基于特性分支 `feature/release-8.5-materialized-view`（8d99181c343）。

刷新和 purge 由后台的 `pkg/mvservice` 定时触发，也可以由用户语句触发。无论哪种，数据改动都在 `GetSysSession()` 取到的系统 session 上执行：`InRestrictedSQL` 为 true，借出时即进入 internal session 列表，所以和 TTL 一样被两层豁免挡在 MDL 之外。

- 原地刷新（FAST、COMPLETE IN PLACE、DELTA APPLY）是一个悲观事务：锁住 `mysql.tidb_mview_refresh_info` 中对应的行，读 mlog 和基表、写 MV 表，然后提交。
- purge 用一个悲观事务锁住 purge 信息行作为互斥，实际删除在另一个系统 session 里以 autocommit 方式分批执行。
- COMPLETE OUT OF PLACE 写影子表，再提交 cutover DDL 切换，不经过上面的刷新事务。

由此有两类风险：

- **在已建好的 MV 上 CREATE MLOG**（为嵌套 MV 做准备，当前分支尚不允许，#72043 才放开）：刷新按"没有 mlog"的元数据写 MV 表，这部分变更不会进入新 mlog。#72043 的 workaround 针对的就是这种情况。
- **在 MV 表上增删索引**（MV 表允许非唯一索引操作）：刷新写 MV 时并发 `ADD INDEX`，就是 #71055 那种孤儿索引。#72043 没有覆盖。

此外还有一个 MV 特有的缺口：**FAST 和 DELTA APPLY 刷新不对 MV 表加锁。** `RefreshMaterializedViewStmt` 的 view 名在 preprocess 中按 `inCreateOrDropTable` 处理，不走加锁逻辑；planner 再直接用 `is.TableByName` 取 MV 表。只有读 mlog 和基表的子查询走了正常加锁。所以即使去掉两层豁免，这两种刷新也挡不住 MV 表上的 DDL。COMPLETE IN PLACE 用普通的 `DELETE` / `INSERT` 写 MV 表，会正常加锁。

方案：

1. 把 4.2 的通用改动移植到特性分支后，purge 的分批删除和 COMPLETE IN PLACE 刷新自动受保护。refresh_info 和 purge 信息行在 `mysql` 库，按 4.3 豁免，purge 那个贯穿全程的互斥事务不会挡 DDL。
2. 原地刷新在 `BEGIN PESSIMISTIC` 之后立即对 MV 表加锁，后续的检查和写入都基于加锁时拿到的表结构。OUT OF PLACE 路径不开这个事务，提交 cutover DDL 时不会被自己挡住。
3. 不需要 #72043 那样的 advisory lock 和刷新前强制重新加载 schema。CREATE MLOG 遇到正在进行的刷新会等它结束，而不是直接被取消。

代价：MV 表、mlog 表和基表上的 DDL 要等正在进行的刷新事务结束，变更量大时可能较久。需要让路时可以用 `CANCEL MATERIALIZED VIEW ... JOB` 取消刷新。

不采用 #72043 的原因：它只覆盖 CREATE MLOG，MV 表上的索引操作仍不受保护；刷新进行中时 CREATE MLOG 会被直接取消、需要用户重试；并且在 DDL worker 里引入了跨步骤持有的 advisory lock。另外，#72043 还包含"允许在已建好的 MV 上建 mlog"等功能改动，这部分没有 MDL 保护时不安全，应等 PR-MV 合入后再放开。

### 4.6 可观测性与让路

今天几乎看不到 internal 阻塞者：连接 ID 恒为 0；阻塞日志要等事务超过 1 分钟才打 Info，而 TTL 事务通常不到 1 分钟；`tidb_mdl_view` 也不包含 internal session。改进：internal 阻塞者直接打 Info，日志带上来源和 startTS，并增加按来源分类的计数指标。

DDL 被 TTL 挡住时，可以执行 `SET GLOBAL tidb_ttl_job_enable = OFF`：约 15-20 秒后在途的 TTL 语句会被打断，代价是本轮 TTL 中止。

**注意**：对正在等待 MDL 的 DDL 执行 `ADMIN CANCEL DDL` 不会生效，因为取消检查只在执行 DDL 步骤时进行，等待期间不检查。这是已有问题，用户的长事务同样会触发，建议另行修复。

### 4.7 为什么不会自锁

1. **等待前已提交**：owner 提交完本步的事务才开始等待，等待期间不持有事务。
2. **版本只增不减**：DDL 推进之后开始的事务（包括 DDL 自己的构建事务）加的都是新版本的锁，不会被判为阻塞者。
3. **升级节点不检查自己**：升级在临时环境中执行，此时检查直接放行。
4. **等待有界**：节点收到新版本就重新加载，之后的新事务加的都是新版本的锁。DDL 最多等到当前的相关事务结束，连续的 TTL 批次不会无限拖延它。

## 五、兼容性

- **行为变化**：internal 事务会阻塞同表的 DDL。这正是要修的问题，不是回归。
- **滚动升级**：检查在各节点本地进行，新旧版本混跑没有问题，旧节点的行为不变。
- **不加开关**：nextgen 的 MDL 已固定开启，再加开关会带来切换期间的不一致（参见 #71106）。回滚只需回退二进制。
- **性能**：只有存在待放行的 DDL 时才会检查；借出中的 internal session 通常只有几十个，每轮开销远小于 1 毫秒。

修复后仍不参与 MDL 的有：internal 访问 `mysql` 库、ANALYZE、IMPORT INTO、bootstrap、GC worker，以及 BR 和 HTTP 接口里临时创建的 session。前几项都是有意为之；后两类目前只做短小的元数据操作，建议在代码中注明：不得在用户表上开事务。

## 六、备选方案

| 方案 | 结论 |
|---|---|
| 完全照搬 #71064，所有 session 参与 | 采用为主体，加上 `mysql` 库豁免 |
| 按表类别建模，并为 IMPORT INTO 等加显式加锁接口 | 采纳"按表类别豁免"的思路；IMPORT INTO 本来就不需要 MDL，新接口没有必要 |
| 只让打了标记的功能参与，分阶段放开 | 发布最稳，但新功能默认仍是盲区，与 #71055 是同一个问题 |
| 为 internal session 重新启用 schema checker | 不采用：upstream 已放弃；所有调用方都要处理报错重试；pipelined DML 依赖 MDL |

## 七、测试计划

用 `./tools/check/failpoint-go-test.sh` 运行，classic 和 nextgen 各跑一遍。新增测试要先在未修复的代码上失败。

1. `pkg/ddl/tests/metadatalock`：internal 事务挡住用户表 DDL，提交后放行且 `ADMIN CHECK` 通过；internal 事务不挡 `mysql` 库的 DDL；DDL 不会被自己挡住。现有的两个 MV 相关 MDL 测试保持通过。
2. `pkg/sessionctx/variable`：为 `RemoveLockDDLJobs` 补单元测试。
3. 升级：跑 `pkg/session/test/bootstraptest`，并补一条"stats 初始化期间，`mysql.stats_*` 上的 DDL 不被阻塞"。
4. `pkg/ttl/ttlworker`：扫描和删除各自的阻塞与释放；关闭 TTL 后约 20 秒内 DDL 完成；扫描超时后不重试。
5. `pkg/domain/crossks`：crossKS 路径下 internal 事务阻塞 DDL。
6. 在测试集群上复现 #71055，确认不再产生孤儿索引。
7. MV 特性分支：FAST 刷新进行中，在 MV 上 `ADD INDEX` 和 CREATE MLOG 都要等刷新提交，之后 `ADMIN CHECK` 通过、新 mlog 不缺变更；purge 进行中 `ALTER MATERIALIZED VIEW LOG` 加列要等当前批次结束；OUT OF PLACE 刷新不被自己的 cutover DDL 挡住。

## 八、交付与待定事项

| PR | 内容 |
|---|---|
| PR-1 | 4.2 的核心改动，以及测试 1、2、3、5；提交前先与 upstream 沟通 `mysql` 库豁免 |
| PR-2 | TTL 显式纳入检查范围与扫描超时，以及测试 4 |
| PR-3 | `ADMIN CANCEL` 在等待期间生效、`tidb_mdl_view` 纳入 internal session（可分别提交） |
| PR-MV | 在 MV 特性分支上移植 4.2 的通用改动，让原地刷新对 MV 表加锁，以及测试 7；取代 #72043 |

待定事项：

1. 与 #71064 对齐 `mysql` 库豁免（该 PR 已一个多月没有更新）。
2. TTL 扫描超时的默认值，需要真实集群的数据。
3. #72043 中"允许在已建好的 MV 上建 mlog"等功能改动，需要和作者沟通：拆掉 workaround 部分，等 PR-MV 合入后再合。

## 附录：验证与代码位置

方案经过多轮代码研究、三份独立设计的比较和四个方向的反驳检验，关键结论已于 2026-10-19 对照代码逐条复核。现有的两个 MV 相关 MDL 测试在 classic 和 nextgen 下均通过。本地测量：TTL 扫描 557 万行约 1.2 秒；持有一个 8 秒的事务时，并发 `ADD COLUMN` 等待了 6.8 秒，事务提交后立即完成。

主线行号基于 980923943d9，特性分支行号基于 8d99181c343，实现时请重新核对。MV 刷新和 purge 的结论来自阅读特性分支代码，尚未实际运行验证。

| 内容 | 位置 |
|---|---|
| 检查时跳过 | `pkg/sessionctx/variable/session.go:3938` |
| 检查遍历 session | `pkg/server/server.go:1300`，`pkg/domain/crossks/coordinator.go:66` |
| 加锁与不加锁 | `pkg/planner/core/preprocess.go:2214`，`:2359` |
| 检查循环 | `pkg/infoschema/issyncer/syncer.go:215` |
| session 池借出即纳入检查范围 | `pkg/session/syssession/pool.go:109`，`pkg/session/syssession/session.go:594` |
| TTL session 入口 | `pkg/ttl/ttlworker/session.go:48` |
| MV 构建 | `pkg/ddl/mview_worker.go:546` |
| stats 初始化的显式事务 | `pkg/statistics/handle/bootstrap.go:845` |
| 升级 DDL 超时退出 | `pkg/session/upgrade_def.go:802` |
| 升级锁 | `pkg/session/session.go:4790` |
| 特性分支：检查时跳过 | `pkg/session/session.go:4989` |
| 特性分支：检查遍历 session | `pkg/server/server.go:1141` |
| 特性分支：刷新、purge | `pkg/executor/materialized_view.go:4043`，`:2310` |
| 特性分支：FAST 刷新解析 MV 表 | `pkg/planner/core/planbuilder.go:3937`，`pkg/planner/core/preprocess.go:337` |
