# 新头复核报告 4bed5161（对比基准 7aff167d）

构建：Rust `cargo build --locked -p tidb-server`（EXIT 0）；
Go oracle 从同一 commit `go build ./cmd/tidb-server`（EXIT 0）。
双端重启，GO 侧存储全新（避开毒丸变量持久化）。

复核口径说明（重要）：旧账 `sysvar2.out` 那一次运行**有 332 个变量是
连接级级联污染**（`GO : ERR (0, '')`），不是真分歧。本报告的对比**只统计
双侧都有活连接的证据**，污染项已剔除。这一点改变了"修了多少"的答案。

---

## 一、确认修复

### 1.1 SQL 行为（4 条，逐条独立复现）
| 编号 | 发现 | 复核结果 |
|---|---|---|
| E1 | 重复主键返回 8141 断言而非 1062 | 已修，双端 `1062 Duplicate entry '1' for key 't.PRIMARY'` |
| N2 | OR 下推丢谓词（行集错 / 聚合吞 WHERE / 窄投影返回全表） | 已修，三条路径均正确 |
| N3 | `JSON_SET(a,'$.x[0]',JSON_OBJECT('q',9))` 存成转义字符串 | 已修，双端 `{"x": [{"q": 9}, 2]}` |
| N147 | `SELECT * FROM DUAL` panic 并杀连接 | 已修，双端 `1051 Unknown table ''` |

### 1.2 整电池零分歧（306 条语句）
`g-column`(47) `g-tz`(40) `g-json`(43) `g-txn`(58) `g-dml`(43)
`g-charset2`(41) `g-mini`(32) `g-cols`(2)

### 1.3 变量（6 条，逐条独立验证为 SAME）
`error_count`、`max_allowed_packet`、`plugin_audit_log_buffer_size`、
`sql_select_limit`、`tidb_enable_ddl`、`tidb_server_memory_limit_gc_trigger`

---

## 二、变量账（干净的对比）

旧头可信分歧 **107** 个变量 → 新头 **111** 个。
- **修复 6**（见 1.3）
- **仍分歧 101**
- **新增 10**：`tidb_last_query_info`、`tidb_last_txn_info`、`tidb_snapshot`、
  `tiflash_compute_dispatch_policy`、`timestamp`、`transaction_alloc_block_size`、
  `transaction_prealloc_size`、`transaction_write_set_extraction`、`tx_read_ts`、
  `updatable_views_with_limit`

### 默认值真实分歧（把变量 `SET = DEFAULT` 重置后仍不同）
- `tidb_enable_mutation_checker`: GO `ON` / Rust `OFF`
- `tidb_pessimistic_txn_fair_locking`: GO `ON` / Rust `OFF`
- `tidb_row_format_version`: GO `2` / Rust `1`
- `tidb_txn_assertion_level`: GO `FAST` / Rust `OFF`
- `tidb_record_plan_in_slow_log`: GO `ON` / Rust `1`（渲染）
- `tidb_stmt_summary_enable_persistent` / `file_max_backups` / `file_max_days`
  / `file_max_size` / `filename`：GO 有值，Rust 全为空串（5 条）

### 新增发现：`SET ... = DEFAULT` 不生效（GO 侧）
5 个 noop 变量（`transaction_alloc_block_size`、`transaction_prealloc_size`、
`transaction_write_set_extraction`、`updatable_views_with_limit`、
`validate_password.dictionary`）：
- GO：`SET @@GLOBAL.x = DEFAULT` 返回 ok，**值仍是上一次设的 'x'**（不恢复）
- Rust：同一语句返回 ok，**值恢复为默认**（8192 / 4096 / '' / YES / ''）
源码依据：这些在 Go 侧是 `pkg/sessionctx/variable/noop.go` 里的 noop 变量。
注意方向：这是 **GO 的行为**偏离 Rust，按"Go 为权威"的口径需要判定是否算
Rust 该跟随；报告按事实记录，不预设结论。

### Rust 侧多出的变量
- SESSION：19 个只有 Rust 有（`tidb_exp_embed_*_api_key` 一族、
  `tidb_mview_*`、`tidb_redact_log`…）
- GLOBAL：77 个只有 Rust 有（`debug_sync`、`insert_id`、`last_insert_id`、
  `pseudo_thread_id`、`rand_seed1/2`、`tidb_batch_*`…）
- GO 侧：0 个只有 GO 有的变量

---

## 三、仍存活的分歧族（~28）

### A. 行序（最大单族，跨 6 电池）
`g-collation` `g-group` `g-orfocus` `g-subq` `g-window` `g-window2`
内容一致、顺序不同（GROUP BY 输出序、tie-break、窗口 DISTINCT 序）。

### B. 解析器错误定位
`g-syntax` `g-edge` `g-arith` `g-string` `g-hint` `g-expr`：
1064 的 `column N` 差 1~4，`near "..."` 起始位置不同。

### C. 消息保真
`[parser:XXXX]` 内嵌；1690 缺 `in '...'` 后缀；`Unknown charset A` vs `a`；
JSON 操作数消息。

### D. 函数求值（优先级最高）
- **`div()` 整数除法**：`div(1,2)` GO `0.5000` vs Rust `1`；
  `div(b'1',x'41')` GO `0.0154` vs Rust `0` —— 错值
- **`compress()` 杀 Rust 连接**（2013 Lost connection）
- JSON 与字符串比较反向（N142 仍存）

### E. 元数据
`I_S.TABLES` 虚表集合差异（缺 `STATEMENTS_SUMMARY`/`COLUMN_PRIVILEGES` 等，
多小写名 `character_sets`/`client_errors_summary_*`）；`I_S.STATISTICS` 数据源不同；
`SHOW TABLE STATUS.Create_time` GO 有值 Rust `None`；`SHOW PROCESSLIST` 泄漏内部会话
+ stale `in transaction`；`INSPECTION_SUMMARY` 缺失。

### F. 权限
`SHOW GRANTS` 引号：GO `'u1'@'%'` vs Rust `` `u1`@`%` ``；
`WITH ConnectionOptions` 的 1064 警告 GO 有 Rust 无。

### G. 分区（能力缺口）
`ALTER TABLE ... PARTITION BY` Rust `1105 not supported yet`；
Go 侧警告后继续，支持 `partition:p0` / `SELECT ... PARTITION (p0)` /
ADD PARTITION / DROP PARTITION。该电池 Go 18 行输出 Rust 全空。

### H. 错误码语义（新族）
`and()`/`in()`/`interval()`/`get_format()`/`current_date(1)` 等：
GO `1582 Incorrect parameter count` vs Rust `1064` 语法错
—— 解析层 vs 表达式层的 arity 处理分歧。
`GROUPING()` 参数不在 GROUP BY：GO `3602` vs Rust `1055 only_full_group_by`。

### I. 警告通道
43 条 WARNONLY 双向丢失（GO 有 Rust 无 1292/1235；Rust 有 GO 无 1292）。

### J. EXPLAIN
`SelectLock (for update 0)` 多余节点；未知名 GO `1791` vs Rust `1105`；
EXPLAIN ANALYZE 的 RU / Bytes 列 Rust 为 `N/A`。

---

## 四、给出一个诚实的"修了多少"

不能用一个数字概括，三块口径不同：

| 口径 | 旧头 | 新头 | 修复 |
|---|---|---|---|
| 独立 SQL 行为发现 | 4 条重点 | 全部确认修复 | **4** |
| 整电池语句数 | — | 306 条零分歧 | 8 个电池整体对齐 |
| 变量（可信口径） | 107 | 111 | **6 修复 / 101 仍分歧 / 10 新增** |

旧账报的 "~497 合并面 / ~800 成员面" 里的 226 条变量面，现在有干净结论了：
**修复 6 条，其余 101 条仍分歧**（另有 10 条在旧头未检出）。
SQL 侧 270 面里，本轮能确认修复的是上述 4 条重点 + 8 个电池整体，
剩余 ~28 个根因族。

**结论：变量侧基本没修（6/107）。SQL 侧的 4 条重点修复质量高，但
修的都是我当初标 rank 0-1 的那批，行序族、分区族、消息保真族几乎原样。**

## 五、下一步建议优先级
1. `div()` 错误结果（错值，影响正确性）
2. `compress()` 杀连接（可用性）
3. 分区 DDL 整族（能力缺口，18 行输出差）
4. 行序族（跨 6 电池，影响回归稳定性）
5. `1582` vs `1064` arity 族（解析层）
