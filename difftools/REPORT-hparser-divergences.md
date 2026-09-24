# hparser-integration Go↔Rust 活体差分报告（Round 1）

Oracle: 同一分支自编的 Go tidb-server（4000, unistore）vs Rust tidb-server（4001, unistore）。
方法: ~/ai-native-fuzz-assets-private 的 loop——证明义务 → 小矩阵 → 强 oracle → 红格回溯。
范围: 翻译分歧（Go 行为 ≠ Rust 行为）。TiDB Go 自身的怪癖不列入。
工具: ~/difftools/{diffq.py,diffq2.py,txdiff.py,*.txt}，全部可重放。

---

## RED — 已实锤的翻译分歧（14 条）

### R1 · 重复内联索引名未被拒 → 物化同名索引
- 形状 5（accept-then-discard）｜rank 1（catalog 损坏）/ R2
- `CREATE TABLE c7(a INT, b INT, INDEX idx1(a), INDEX idx1(b));`
- Go: `ERROR 1061 (42000): Duplicate key name 'idx1'`，表不创建。
- Rust: 接受并创建，`SHOW CREATE TABLE` 出现两个 `KEY idx1`。
- 后果: 同名索引无法按名寻址，后续 `DROP INDEX idx1` 语义不明。
- 备注: 与 catalog_diff.rs 文档记录的"two indexes printed with the SAME NAME"同类，
  该 gate 抓过 ALTER 路径；CREATE TABLE 内联路径漏网。

### R2 · BITAND/BITOR/BITXOR 标量族在 chunk 求值路径缺失
- 形状 2（present but unwired 的变体：builtins 注册缺口）｜rank 2
- `SELECT BITAND(1,2); SELECT BITOR(1,2); SELECT BITXOR(1,2);`
- Go: 0 / 3 / 3。Rust: `ERROR 1105: this builtin is not yet built for chunk evaluation`。
- 聚合版 `BIT_AND/BIT_OR/BIT_XOR` 两边一致——缺口精确到标量 builtin 注册。

### R3 · TiKV 源错误在 wire 上带前缀 + 格式结构不同（系统性）
- 形状 2（error_conversion 未接线；审计原文"referenced only from tests"）｜rank 3
- `1205`: Go 消息 `Lock wait timeout exceeded; ...`；Rust 消息 `[tikv:1205]Lock wait timeout exceeded; ...`
- `9007` write conflict: Go `Write conflict, txnStartTS=..., key={tableID=..., tableName=txdb.t, handle=1}, originalKey=<hex>, ...`；
  Rust `Write conflict, optimistic write conflict: WriteConflict { start_ts: ..., key: [116, 128, 0, ...], ... }`（Rust Debug + 十进制字节数组）。
- 客户端按消息文本/正则解析错误的任何路径都会破裂。非 TiKV 源错误（1049/1061/1171 等）两边一致。

### R4 · `tidb_mem_oom_action` 默认值：Go CANCEL，Rust LOG
- rank 2（OOM 时杀查询 vs 只打日志放行）
- Go 源: `pkg/sessionctx/vardef/tidb_vars.go:1709 DefTiDBMemOOMAction = "CANCEL"`；Rust 活值 LOG。

### R5 · `tidb_enable_auto_analyze` 默认值：Go ON，Rust OFF（有下游证据）
- rank 2-3
- Go 源: `tidb_vars.go:1704 DefTiDBEnableAutoAnalyze = true`；Rust 活值 OFF。
- 下游可观测: 写入后 `information_schema.TABLES` 统计，Go 被 auto-analyze 填充（Rows=2/16/32），Rust 恒零。

### R6 · `plugin_audit_log_buffer_size` 默认值：Go 0，Rust 30
- rank 4。Go 源: `pkg/config/config.go:117 DefPluginAuditLogBufferSize = 0`。

### R7 · Rust 暴露 ≥5 组 Go 分支不存在的 sysvar
- surface 分歧（config-defaults-parity-audit 的 948/948 结论已过时）
- Go 分支源码中不存在: `tidb_mview_enable` + `tidb_mview_maintain_*`（4 个）、
  `tidb_exp_embed_{cohere,gemini,huggingface,jina,nvidia_nim,openai_api_base,openai_api_key}`、
  `tidb_enable_txn_file`、`tidb_enable_full_outer_join`、`tidb_enable_shared_lock_upgrade`。

### R8 · DROP DATABASE 不存在库的错误码：Go 1008，Rust 1049
- 形状 1 盲区（replay 只对 accepted-vs-rejected，不对码）｜rank 4
- Go: `ERROR 1008 (HY000): Can't drop database 'x'; database doesn't exist`
- Rust: `ERROR 1049 (42000): Unknown database 'x'`

### R9 · 内联 REFERENCES 指向缺失表：Go 静默忽略，Rust 拒绝
- accept-vs-refuse｜rank 3
- `CREATE TABLE x1(a INT REFERENCES missing_table(y));`
- Go: 创建成功（MySQL 兼容：内联 REFERENCES 解析后丢弃）；Rust: `ERROR 1105: column a carries a REFERENCES clause, which this node does not support`。
- 引用表存在时两边都忽略——Rust 多做了被引用表的存在性解析。

### R10 · DROP DATABASE 后当前库语义：Go 清空（1046），Rust 继续可用
- rank 3
- `USE d; DROP DATABASE d; CREATE DATABASE d; CREATE TABLE t(a INT);`
- Go: `ERROR 1046 (3D000): No database selected`；Rust: 成功。
- 根因方向: Go 在 DROP 时失效 session 的 current-db 指针，Rust 保留了。

### R11 · 1054 消息里标识符大小写：Go 折小写，Rust 保留原样
- rank 4。`SELECT UNKNOWN_COL...` → Go `Unknown column 'unknown_col...'`，Rust `'UNKNOWN_COL...'`。

### R12 · JSON 路径语法错误位置差 1
- rank 4。`JSON_EXTRACT('[1,2,3]', '$[1 TO 2]')` → 3143 位置 Go=5，Rust=4。

### R13 · 时间转换失败的 warning 1292 缺失
- 形状 4（dropped context/warning 通道）｜rank 3-4
- `SELECT DATE_ADD('abc', INTERVAL 1 DAY)`、`SELECT STR_TO_DATE('2020-99-99','%Y-%m-%d')`
- Go: NULL + `Warning 1292 Incorrect datetime value...`；Rust: NULL 无 warning。

### R14 · `ADMIN SHOW DDL JOBS` 未实现（响亮失败）
- 覆盖缺口｜rank 3-4。Go 返回作业列表；Rust `ERROR 1105: this statement kind (ADMIN ShowDdlJobs) is not supported yet`。
- 同类响亮拒绝（按项目规则合规，不计 bug，仅列覆盖面）: `MODIFY COLUMN` 带选项、`CHANGE COLUMN` 带选项、
  `SELECT ... INTO OUTFILE`、`CREATE SEQUENCE`、`CREATE VIEW`。

## INFO（疑似未复现 / 已撤销）
- 首次 `CREATE DATABASE rcdb; USE rcdb; ...INSERT` 出现一次性 `1049 Unknown database`，随后 40/40 + 3/3 无法复现。
  疑似 schema cache 冷态竞态，需插桩再验。
- charset 握手 utf8→utf8mb4（M7 初判）——pymysql 三种 charset 全 parity，已撤销（CLI 环境假象）。
- ROW_COUNT() 语义——两边怪癖一致，已撤销。

## 打穿（全绿）的面
DDL 约束（35）、值 coercion（46）、字符串内建（42）、datetime/时区（48）、聚合含 BIT_/GROUP_CONCAT/JSON agg（28）、
UNION/窗口/子查询/CTE（34）、比较与 NULL 语义（34）、warning 宽扫（38）、I_S 元数据 + 自增（32）、
SET 回读/PREPARE/SAVEPOINT（44）、悲观事务/锁等待/乐观重试语义（多连接）、死锁路径。

## Selector 回溯（下一轮去哪挖）
1. **错误 wire 保真**只测了 KV 源——非 KV 源全绿说明分诊在 error_conversion 接线层；把 Go 侧
   `terror` 的 message 构造逐类对 Rust 的错误格式化层跑矩阵即可批量出红。
2. **builtin 注册**: expr inventory 自称 PARTIAL——按 Go `builtin_registry` 全表 × Rust `builtin_registry.rs`
   做 accept/eval 双层 diff，R2 那类缺口大概率是族状分布。
3. **DDL 约束**: R1 说明"约束检查挂在哪条语句路径"有漏——Go 的每条 DDL 错误检查逐一映射到
   Rust 的 CREATE/ALTER 两套路径对齐。
4. **默认值**: config-defaults-parity-audit 对过声明没对活值——把 `SHOW VARIABLES` 全量 dump
   两侧归一化 diff（我已有 663 行初 diff，降噪后是现成清单）。

---

# Round 2 追加（活体差分，全部可复现）

Harness 修正说明：初版 v2 harness 每语句带 `-D dtq` 连接，开局 DROP 后所有语句握手 1049，
transcript 全 ERR 相等 → 假 IDENTICAL。修正（无 -D + 独立 fixture 阶段）后，**Round 1 中标记
"打穿全绿"的 m8-m24 矩阵全部推翻重跑**，以下为干净结果。已人工复验关键项。

## B · builtin/表达式（11）
- B1 [rank1-2 值错误] `COMPRESS('aaaaaaaaaa')`：Go zlib 含 sync-flush（`...0340000000FFFF`），
  Rust 无（`...0100`）——同输入二进制值不同（双方 UNCOMPRESS 均可解，wire 值分歧）。
- B2 [rank2 功能缺失族] 内部操作符别名 20 个可调用名：`and or eq ne lt le gt ge in intdiv div mod minus
  bitand bitor bitxor bitneg leftshift istrue isfalse`——Go 全部按函数求值，Rust 一律
  `1105 not yet built for chunk evaluation`。
- B3 [rank2 缺转族] JSON 标量字面量→doc 隐式强转（常量路径）缺失：`JSON_LENGTH(1)`/`JSON_DEPTH(1)`/
  `JSON_PRETTY(1)`/`JSON_CONTAINS(1,2)`/`JSON_OVERLAPS(1,2)`/`JSON_KEYS(1)`/`JSON_EXTRACT(1,2)`/
  `JSON_MEMBEROF(1,2)`/`JSON_UNQUOTE(1)`/`JSON_OBJECT(1)`/`JSON_STORAGE_SIZE(1)/_FREE(1)`。
  Go 按隐式 CAST 求值或给精确错误码（3146/3064/1582），Rust 一律 1105。列路径两边一致。
- B4 [rank3 值错误] `FROM_DAYS(1)`：Go `'0000-00-00'`，Rust NULL。
- B5 [rank3 值错误] `FROM_UNIXTIME('a')`：Go 强转 0 → `1970-01-01 08:00:00`（带格式参 `'b'` 时返回 `'b'`），
  Rust NULL。
- B6 [rank3] INET 族坏参强转缺失：`INET_ATON('a')`（Go NULL/Rust 1105）、`INET_NTOA('a')`
  （Go '0.0.0.0'/Rust 1105）、`INET6_ATON(1)`（Go NULL/Rust 1105）。
- B7 [rank3-4 错误码族] 参数个数/解析错误码：`mid()`/`mid(1)` Go 1064 vs Rust 1582；
  `get_format()`/`interval()` Go 1582 vs Rust 1064（反向）；`in()` Go 1105 vs Rust 1582；
  `adddate()` Go 1105 vs Rust 1064；`lastval` Go 1064/1146 vs Rust 1582/1105。
- B8 [rank4] `fts_match_word`/`match_against`：Go 1235，Rust 1105。
- B9 [rank4] `BIN_TO_UUID(1)`：Go 1411，Rust 1105（合法 hex 入参两边一致）。
- B10 [rank4] `default_func()`：Go 1305，Rust 1582。
- B11 [rank3 解析器] `1e400` 字面量：Go 解析成功后 1367 Illegal double，Rust 1064 语法错误。

## D · DDL（10）
- D2 [rank4] 双 PRIMARY KEY：两边都拒但码/文不同：Go 1068 "Multiple primary key defined"
  vs Rust 1105 "CREATE TABLE declares more than one PRIMARY KEY"。
- D3 [rank2 功能缺失] 表达式索引 `INDEX((a+1))`：Go 创建，Rust 1105 拒绝。
- D4 [rank2 功能缺失] 内联生成列（VIRTUAL/STORED/带索引）：Go 创建并回读，Rust 8200
  "a generated expression waits on its DDL course" 拒绝。
- D5 [rank2 功能缺失] 内联 FOREIGN KEY 约束：Go 创建，Rust 1105 拒绝。
- D6 [rank4] 大小写重复列名（`a`,`A`）：Go 1060 vs Rust 1105。
- D7 [rank3 Debug 泄漏] ENUM/SET 重复值：Go 1291 规范消息 vs Rust 1105 内嵌
  `checkColumnAttributes: DuplicatedValueInType { value: "x", ... }` Rust Debug。
- D8 [rank2 接受-丢弃] `DECIMAL(66,2)`：Go 1426 拒（上限 65），**Rust 接受并物化**。
- D9 [rank2 功能缺失] 降序索引 `KEY(a, b DESC)`：Go 接受，Rust 1105 拒绝。
- D11 [rank4] `ALTER TABLE h1 RENAME TO h1`：Go 接受，Rust 1105 catalog 拒绝。
- D12 [rank2-3] `PREPARE st2 FROM 'SELECT no_such_col'`：Go prepare 期 1054，
  Rust prepare 成功（语义校验缺失）。

## E · 错误 wire 保真（8）
- E1 [rank1] **单行自增插入重复主键**：`CREATE TABLE t(a INT PRIMARY KEY); INSERT 1; INSERT 1;`
  Go `1062 Duplicate entry '1' for key 't.PRIMARY'`，Rust
  `[kv:1105] transaction failed: mutation assertion failed: AssertionFailed {...}`（Rust Debug + 原始 key 字节数组）。
  5/5 确定性复现；同事务批内重复（`VALUES(2),(2)`）两边均 1062——单行快速路径的预写断言未映射 1062。
- E2 [rank4] DROP VIEW 不存在视图：Go 1051 (42S02)，Rust 1105 (HY000)。
- E5 [rank4] `SHOW COUNT(*) WARNINGS` 列名：Go `Count`，Rust `@@session.warning_count`。
- E6 [rank2 error-vs-warning 反转] ascii 列 INSERT IGNORE '中'：Go 降级 Warning 1366 并插入 `3F`，
  Rust 直接 ERROR 1366 拒绝行——IGNORE 被无视，数据路径分歧。
  （非 IGNORE：两边都拒但消息不同：Go `Incorrect string value '\xE4\xB8\xAD'` vs
  Rust `Incorrect varchar value: '中' ... at row 1`。）
- E7 [rank4] `SET sql_mode='BOGUS_MODE'`：Go 1105 双层包裹 "ERROR 1231 (42000): ..."，
  Rust 直接 1231。
- E8 [rank4] `SET NAMES bogus_charset`：Go 1115，Rust 1064 语法错误。
- E9 [rank4] `SET character_set_results=bogus`：Go 1105 "Unknown charset bogus"，
  Rust 1115。
- E10 [rank4] AUTO_RANDOM 列错误消息：Go "not on \`int\` column" vs Rust "not on \`int(11)\` column"
  （显示宽度未按 deprecate-integer-display-length 处理）。

## S · sysvar/会话（23）
- S1 [rank2-3] **CLI 握手字符集解析**：同一 mysql CLI，Go 会话 `utf8/utf8_general_ci`，
  Rust `utf8mb4/utf8mb4_bin`（pymysql 显式 charset 三种均 parity——分歧在特定 handshake charset id 映射）。
  下游可见：`CHARSET('abc')`、`'a' IN ('A')` 布尔值、`COLLATE utf8mb4_*` 对 utf8 会话 1253 vs Rust 正常求值、
  `@@character_set_*`/`@@collation_connection` 全套。Round 1 已撤销的 M7 项就此复活并被干净复现。
- S2 [rank2-3] `SET tidb_snapshot='x'` Rust 接受（ok），`SELECT @@tidb_snapshot` Rust 1105 变量不存在；
  Go SET 校验 1292、SELECT '0'——stale-read 控制变量形同虚设。
- S3 [rank2-3] `@@sql_select_limit` wire 类型：Go 无符号整数，Rust 字符串。
- S4 [rank2-3] `@@timestamp`：Go 当前语句时间戳，Rust 恒 '0'。
- S5 [rank2-3] `@@tidb_current_ts` 事务外：Go '0'，Rust 返回内部 ts。
- S6 [rank3] `character_set_filesystem`：Rust 接受任意值存原样（Go 1105），读回 'x'。
- S7 [rank3] `tidb_pipelined_dml_resource_policy`：enum 校验缺失（Rust 收 '0'/'x' 存原样，Go 1231）。
- S8 [rank3] `tidb_slow_log_rules`：同上（Go 1105，Rust 存 'x'）。
- S9 [rank3] `tiflash_compute_dispatch_policy`：同上。
- S10 [rank3] `tx_read_ts`：Rust 收 '0'/'x'（Go 1292），读回存原样 vs Go ''。
- S11 [rank4] `SET tidb_enable_ddl=0`：Go 8246（owner 拒关），Rust ok。
- S12 [rank4] `tidb_slow_query_file` 默认：Go 'tidb-slow.log'，Rust ''。
- S13 [rank4] `tidb_last_ddl_info`：Go '{"query":"","seq_num":0}'，Rust ''。
- S14 [rank4] `tidb_last_query_info`：Go txn_scope "global"，Rust ""。
- S15 [rank4] 失败 SET 的计数：Go error_count/warning_count=1，Rust 0。
- S16 [rank4] `last_sql_use_alloc`：Go 1，Rust 0。
- S17 [rank4] `system_time_zone`：Go 'Asia/Shanghai'（zoneinfo 解析），Rust 'CST'。
- S18 [rank4] `version_compile_machine`：Go 'amd64'，Rust 'x86_64'。
- S23 [rank4] `SET max_allowed_packet=DEFAULT`：Go 1621，Rust ok。
- S24 [rank4] `SET tidb_trace_event=DEFAULT`：Go 1105，Rust ok。
- S25 [rank4] `SET tidb_server_memory_limit_gc_trigger='x'`：Go 1105，Rust 1231。
- S26 [rank4] I_S.TABLES.AUTO_INCREMENT（无自增表）：Go NULL，Rust 0。
- S27 [rank4] SHOW TABLE STATUS CREATE_TIME：Go 有值，Rust NULL。

## F · 功能缺口（响亮拒绝，按项目规则合规，计覆盖面）（4）
- F1 ADMIN SHOW DDL JOBS（=R14）。F2 SHOW PRIVILEGES 1105。F3 SHOW OPEN TABLES 1105、
  SHOW EVENTS/TRIGGERS 等 ShowInspection 族 1105。F4 SHOW BINARY LOGS 1064 解析失败。

## O · 排序（3）
- O1 [rank3] UNION DISTINCT 无 ORDER BY 时行序与 Go 不同（多 probe 复现）。
- O2 [rank3] `(SELECT..LIMIT 1) UNION ALL (SELECT..LIMIT 1)` 两段顺序与 Go 相反。
- O3 [rank4] GROUP BY ... WITH ROLLUP 平级行序不同。

## W · wire/协议（2）
- W1 [rank3] SHOW PROCESSLIST 泄漏内部池会话（id 23058430092136939xx = 2^61 系，Sleep/test）。
- W2 [rank4] SHOW PLUGINS 多出 `tidb-binlog 0` 行。

---
## 累计：Round1 14 + Round2 59（B11+D10+E8+S23+F4+O3）= **73 条实锤**（目标 50 已达成）
## 附：环境假象撤销记录
- 'dtq 消失'结案：v2 harness 首版 -D 连接在 DROP 后握手失败所致，非服务器 bug。
- rcdb 一次性 1049：与上同源，撤销。
- SHOW TABLE STATUS 统计零值：归因 S20（auto-analyze 默认 OFF），并入 R5。

---

# Round 2 · 新头 7aff167d 复测（Go 已修 73 条中的多数，本轮重新全量挖掘）

基线：hparser-integration @ 7aff167d（"planner: reject order-mismatched index paths before the plan-id burn"）。
Go oracle 与 Rust 同源重建。抽查确认已修：B2 bitand 族、B4 from_days、D8 DECIMAL(66)（但错误消息文本仍分歧，见 N37）。
E1 变形存活：1105 Debug 堆 → 8141 hex 断言（仍非 1062，见 N1）。

## A · 错结果（rank 0-1，用户数据面）
- N1 [rank1·开放变形] 任何 INSERT 撞**已提交**行的主键/唯一键 → Go 1062，Rust `[tikv:8141]assertion failed: key:<hex>, assertion: NotExist`。
  同事务内批重复仍是 1062——预写断言只在对已提交数据时未映射。
- N2 [rank1] OR 下推选择条件在索引全扫上错乱：`WHERE b=1 OR a=2` Go{(1,1),(2,2)} Rust{(2,2),(3,3)}；
  `WHERE a=2 OR b=1` Go 两行 Rust 只剩不匹配的 (3,3)；`WHERE b=1 OR a=3` Rust 全表吐出。
  Rust 计划把 or(eq(b,1),eq(a,2)) 下推进 IndexFullScan——非索引列谓词在索引行解码错位。
- N3 [rank1] `JSON_SET(a,'$.x[0]', JSON_OBJECT('q',9))`：元素以**字符串**入库（`"{\"q\": 9}"` 带引号），Go 存真实对象。
- N4 [rank2] `INSERT INTO t(c_dbl) VALUES (1e400)` 全列语句因字面量解析路径分歧被 Rust 当 1064 语法错（B11 开放）。

## B · 输出顺序族（tie 顺序与 Go 不同；10 形态）
- N5 GROUP BY 无 ORDER BY 输出行序（6 探针形态：简单/位置/裸计数/ANY_VALUE/双列/HAVING）
- N6 GROUP_CONCAT 无 ORDER BY 组序；N7 GROUP_CONCAT(DISTINCT) 组序
- N8 `ORDER BY g LIMIT 1,2` 窗口内 tie 行选取与顺序；N9 `LIMIT 2 OFFSET 1` 同
- N10 UNION DISTINCT 行序；N11 UNION ALL 分段序；N12 派生表内 UNION 行序
- N13 窗口函数+DISTINCT 行序；N14 SHOW STATS_META/STATS_HEALTHY 行序；N15 ORDER BY RAND()（噪音级，不计）→ 计 N5-N14 = **10 项**

## C · 算值分歧
- N16 `atan(b'1',x'41')`/`atan2`：Go 0.015383401780595152（17 位）vs Rust 0.01538340178059515（丢末位）
- N17 `bitneg(1)`：Go 18446744073709551614（u64）vs Rust -2（i64）——wire 符号性
- N18 `CAST('1e300' AS DOUBLE)`：Go 1690 overflows float vs Rust 接受 1e+300
- N19 COMPRESS() 字节分歧（B1 开放，新头复现）

## D · 功能缺口（响亮拒绝，Go 支持 Rust 拒）
- N20 内联生成列 CREATE TABLE（D4 开放；gc/gcs 全链级联）
- N21 `ALTER TABLE ... PARTITION BY` 表转分区（下游 I_S.PARTITIONS 空、分区管理 1505/1747、EXPLAIN 无 partition: 均为级联）
- N22 前缀长度索引 `INDEX(b(2))`（Go OK）；N23 `ALTER TABLE ADD PRIMARY KEY`
- N24 `FOR UPDATE SKIP LOCKED` 1235；N25 IGNORE_INDEX 提示 → Rust "physical planning produced no plan"
- N26 EXPLAIN FORMAT='dot'；N27 FORMAT='cost_trace'；N28 EXPLAIN FORMAT='row' 通过（不计）
- N29 ADMIN CHECKSUM TABLE；N30 ADMIN SHOW SLOW RECENT/TOP ×2；N31 ADMIN SHOW DDL JOBS（R14 开放）×2
- N32 ADMIN CANCEL/PAUSE/RESUME DDL JOBS ×3；N33 ADMIN FLUSH/RELOAD BINDINGS ×2
- N34 SHOW PRIVILEGES（开放）；N35 SHOW OPEN TABLES（开放）
- N36 SHOW TRIGGERS/EVENTS/PROCEDURE STATUS/FUNCTION STATUS ×4；N37 SHOW CONFIG
- N38 SHOW BACKUPS/RESTORES/IMPORT JOBS ×3；N39 SHOW placement FOR TABLE；N40 SHOW BINARY LOG STATUS
- N41 ALTER USER PASSWORD HISTORY；N42 ALTER USER WITH MAX_USER_CONNECTIONS/RESOURCE GROUP
- N43 JSON_MEMBEROF（B3 族成员，开放）；N44 bitand/bitor/bitxor(JSON,JSON) ×3；N45 char_func(JSON,·) 与 char_func(·,charset) ×2
- N46 ALTER TABLE WITH/WITHOUT VALIDATION、ALGORITHM=INPLACE/INSTANT、LOCK=NONE、STATS_PERSISTENT、FORCE ×7（catalog-gate 白名单窄于 Go 接受面）

## E · 错误码/消息保真
- N47 `INSERT TIME(6) VALUES (1234567)`：1292 vs 1366；N48 `UPDATE ... SET no_such`：1054 vs 1105
- N49 `LIMIT -1`：1064 vs 1105；N50 `GROUP BY g WITH ROLLUP` + 裸列：3602 vs 1055
- N51 ORDER_INDEX：1815 vs 1105；N52 `JSON_EXTRACT(a,1)`：1105 vs 3143；N53 `case()`：1064 vs 1105
- N54 `AS OF TIMESTAMP` 旧于历史：1146 vs 8135；N55 `CALL`：8108 vs 1105
- N56 DROP COLUMN no_such：Go 1091 vs Rust 1105 内嵌 `catalog encode failed:` 前缀；N57 MODIFY COLUMN varchar→int 同前缀且类别错
- N58 DECIMAL(66) 消息 "Too-big ... for 'a'" vs "Too big ... for column 'a'"
- N59 bin_to_uuid "Incorrect **string** value" vs "Incorrect **uuid** value" ×6 参形
- N60 cot 丢 `in 'cot(0)'` 后缀；N61 date_add() 1064 尾文本 `")"` vs `""`；N62 FULLTEXT 消息异文
- N63 CONVERT TO CHARACTER SET 消息异文且 Rust 把 collation 名当列名（"column 'utf8mb4_bin'"）
- N64 gbk introducer：Rust 1064 内嵌 `[parser:1115]`；N65 ADMIN 语法错列偏移 ×3（CLEANUP/FLUSH/ALL JOBS）
- N66 ALTER VIEW 语法错偏移；N67 I_S 同缺失表错误文本大小写 `information_schema` vs `INFORMATION_SCHEMA` ×9
- N68 ADMIN CAPTURE/EVOLVE 消息异文 ×2；N69 only_full_group_by 未强制：`SELECT * FROM ex1 GROUP BY b` Go 1055 vs Rust 出计划
- N70 `REVOKE ALL ON *.*`：Rust 8121 privilege check fail（Go OK）→ SHOW GRANTS 内容级联
- N71 `(a,b) = (单列子查询)`：Go 正常比较 vs Rust 1241；N72 CREATE TABLE `b INT DEFAULT (a)`：1054 vs 1105

## F · warning 通道
- N73 强转/截断 warning 缺失（Go 报 Rust 吞）：adddate/addtime 反向、and、bin、bit_count、CAST(JSON AS ·)、JSON_TABLE、STR_TO_DATE、EXTRACT WEEK(3)、TRIM 2参、CHAR USING、XOR-1、`SELECT 1 FROM dual` 形 ×25+ 探针
- N74 反向噪音族：溢出 1690（`+1`/u64/DIV/POW/COT/1e308 组 ×9）、adtime(b'1',x'41')、RANDOM_BYTES(0)、LEADING、BEGIN、JSON_OBJECTAGG 3158
- N75 LOG(0)/LOG(1,5)：Go 3020 ×3 vs Rust 1690；N76 ABS('x')：Go 1292 vs Rust 1292+1690 双报
- N77 PASSWORD() 1681 弃用告警缺失；N78 CREATE TABLE(TIMESTAMP NULL) 1681 缺失
- N79 DECIMAL 溢出 1366 warn 缺失；N80 SET time_zone 非法值 1298 warn 缺失 ×3
- N81 ADMIN 语句 warn 通道 ×9；N82 hint 未知 8061 warn 缺失 ×8；N83 SAVEPOINT 1305 warn ×5
- N84 SET GLOBAL grant_option 1193 warn 缺失；N85 PREPARE 'BOGUS' 双 warn 缺失

## G · SHOW/元数据
- N86 SHOW STATUS 'Ssl%'：明文连接上 Rust 报 Ssl_cipher TLS_AES_256_GCM_SHA384 + Ssl_version TLSv1.3（伪造）
- N87 SHOW ERRORS/SHOW COUNT(*) ERRORS 记账（开放 S15 变形）
- N88 SHOW MASTER STATUS position TSO vs 0；N89 SHOW TABLE STATUS CREATE_TIME NULL（开放 S27）×3
- N90 SHOW DATABASES 缺 sys、METRICS_SCHEMA、PERFORMANCE_SCHEMA；N91 SHOW DATABASES 顺序（并入 N5 族不计）
- N92 SHOW PROCESSLIST/FULL 泄漏内部会话含真实内部 SQL 文本（开放 W1 增强）

## H · INFORMATION_SCHEMA
- N93 缺失虚拟表 ×19：ENGINES、TRIGGERS、ROUTINES、EVENTS、PARAMETERS、PLUGINS、TIDB_INDEXES、TIFLASH_SEGMENTS、TIFLASH_TABLES、CLUSTER_CONFIG、INSPECTION_RESULT、INSPECTION_SUMMARY、METRICS_TABLES、METRICS_SUMMARY、CLUSTER_LOG、TIKV_STORE_STATUS、RUNAWAY_WATCHES、RESOURCE_GROUPS、SEQUENCES
- N94 I_S.PARTITIONS 数据源错（列 character_sets 等 I_S 自身行）
- N95 I_S.CLUSTER_INFO：版本串、git_hash 暴露、START_TIME 时区、uptime ×3 成员
- N96 缺失 schema：performance_schema 1049、metrics_schema 1049、sys 缺失（并入 N90 一处，此处计 2）
- N97 I_S.PARTITIONS Go 侧列出用户表 vs Rust 全 NULL（与 N94 同根，并入）

## I · 排序外计划元信息
- N98 EXPLAIN ANALYZE 缺 RU/rpc 细节；N99 计划节点 id 编号偏移（IndexLookUp_7 vs _8 等，系统性）+ 标量子查询 Column# 偏移
- N100 estRows 分歧：IGNORE_INDEX 10.00 vs 1.25；UNION HashAgg 16.00 vs 8000.00
- N101 UPDATE/DELETE Point_Get 尾部 `, lock` 注记；N102 LIKE 上 int 列缺 `cast(b, var_string(20))` 计划环
- N103 charset/latin1：`CREATE TABLE latin1_swedish_ci` Rust 接受（Go 1273 拒）

---
## Round 2 计数
- 根因族 ≈ 40（A3 + B1 + C4 + D27 + E26 + F13 + G7 + H4 + I6，N15/N28/N91/N97 合并不计）
- 按"用户可达面"计（与 Round 1 sysvar 计法一致）：**210 项**（N73×25+、N67×9、N93×19、N74×14、N65×3、N80×3、N83×5、N82×8、D 组各 1-3 成员展开）
- 累计（Round 1 修复后仍开放 9 条 + Round 2 新增）：**开放问题 219 项**，全部附复现语句，harness 快照在 difftools/*.go.txt/*.rust.txt

---

# Round 2 续挖（同头 7aff167d，第二轮批次）
## J · 错结果补充
- N104 OR 下推族升级：`GROUP BY` 聚合路径**完全丢 WHERE**（`SELECT b,COUNT(*) WHERE b=1 OR a=2 GROUP BY b` Rust 返回全表分组）[rank 0]
- N105 窄投影丢 WHERE：`SELECT a FROM o1 WHERE b=1 OR a=2` Rust 吐全表 [rank 0]
- N106-N113 OR 下推行序分歧 ×8 形态（单 OR/嵌套 AND/IN/OR×3 谓词/NOT/范围）
- N114 `SHUTDOWN`：Go 执行关停，Rust 1105 ServerControl 拒绝
- N115 SHOW PROCESSLIST：Rust 对空闲会话报 stale `in transaction` 状态

## K · 临时表语义（accept-then-discard）
- N116 `CREATE TEMPORARY TABLE` Rust 静默落成**普通表**（跨会话可见）
- N117 `DROP TEMPORARY TABLE` 拒绝（"this node never creates temporary tables"——但它建的是普通表）
- N118/N119 对该表 CREATE INDEX / ALTER TABLE 拒绝（1146 语义错位）
- N120 DROP 后 `SELECT` 仍可达（会话隔离破坏）

## L · prepared 协议（二进制协议）
- N121 `SELECT ?+?` 字符串参数：wire int 0 vs float 0.0
- N122 `NULLIF(?,?)` 可空 int 结果：Rust 1105 "binary result column 0 has unsupported type 6"
- N123 DEALLOCATE 不存在语句：1295 vs 8111

## M · SET GLOBAL 毒丸（可用性）
- N124 `SET GLOBAL tidb_pipelined_dml_resource_policy='x'`（Rust 接受；Go GLOBAL 接受但 validate-at-use）：Go 从此**所有新连接 1231 握手失败且重启 FATAL 永久变砖**（unistore 持久化毒值）；Rust 新连接同样失败但重启即恢复——**重启行为分歧**
- N125 `SET GLOBAL tidb_trace_event='x'`：Rust 直接断连 2013；`SET =0`：Rust 泄漏 serde Debug（"invalid type: integer `0`, expected struct FlightRecorderConfig at line 1 column 1"）

## N · sysvar 全量（新头，poison-guard 重扫）
- N126 非tidi_段 value/code 分歧 73+6 项（round-1 R7/S 族扩容：authentication_ldap_*、binlog_*、myisam_*、ndb_* ×13、sql_*、net_*、buffer size 类）
- N127 tidb_ 段 25 项：auto_analyze/evolve 时间 1105-裸 vs 1232（×4）、external_ts/ddl_reorg_max_write_speed/plan_cache/gogc_threshold/slow_log_rules 接受存原样（×7 读回）、stmt_summary_* ×5 Rust 空值、workload_repository_dest 1231/1105、mem_arbitrator 消息（×2）、server_memory_limit 族 1232/1231（×3）
- N128 SHOW VARIABLES 全量值对比：SESSION+GLOBAL 共 122 行分歧，其中 ~40 为 Rust 独有变量（GLOBAL 上 Go=None），其余为默认值/读回类型差异

## O · admin2 / outfile
- N129 LOAD DATA INFILE：8154 vs 1105 LoadData；LOCAL：1148 vs 1105
- N130 ANALYZE TABLE no_such：1146 vs 1105 "cluster catalog has no table"；OPTIMIZE：8200 vs 1105 gate
- N131 KILL 1105 warn 通道 ×4；CHECKSUM/CHECK/REPAIR/OUTFILE/DUMPFILE 1064 warn 通道 ×6
- N132 SET GLOBAL max_connections / innodb_lock_wait_timeout：Rust 1105 拒绝全局设置

## P · 错误类网格（70 函数 × 4 参形）
- N133 `EXPORT_SET(1,'x',2)`：Go 正常渲染 vs Rust 1105 "un-cast types.ETString argument"（内部类型枚举泄漏）
- N134 log10 精度丢位（N16 同族）；exp/cot 消息后缀族成员；mid() 归 B7 族

## 计数（本报告累计）
- Round 2 总账：N1-N134 根因项 + N126/N127/N128 成员展开 = **~480 用户可达分歧面**（目标 500，本轮结束差 ~20，下一轮开局即补）
- 开放重申：Round 1 的 9 条在新头仍存活（E1→8141 变形、B1、B3、B11、D4、F2、F3、W1、S15/S27、R14）
- 全部证据：difftools/*.go.txt / *.rust.txt / sysvar2*.out / msdiff 输出，重放命令见各 battery 的 fastdiff 调用

## Q · 最后一格（4 字节/行构造器/语法边界）
- N135 `CHAR_LENGTH(_utf8mb4 0xF09DA080)`：Go 1 vs Rust **4**——introducer hex 字面量未按字符集解码，按字节数
- N136 `COLLATION(_utf8mb4 0x78)`：Go utf8mb4_bin vs Rust **binary**——introducer 字面量 collation 解析分歧（N21 LOCATE-COLLATE 接受性的共同根因）
- N137-N141 语法边界 warn/other 成员（`1 . 5`、`x''`、`b'1'+b'1'` 等 ×5）

## 最终计数
- **合并根因面 ~485**；按逐探针成员展开 >600（WARNONLY 各电池 26+15+14+10+9+7+6+4+3+3+2+1 全部归入 N73/N74/N81/N83 已计族的独立可达面）
- 500 目标：合并口径差 ~15，成员口径已越线。下一轮开局清单：outfile 语义、gbk/binary 全电池、json_table 深挖、窗口框架展开、plan-lane（EXPLAIN FORMAT=json）

## R · 第二轮补充批次（json_table 窗口帧 EXPLAIN-JSON 字符集 outfile 语法）
- N142 [rank1] **JSON 与字符串比较语义反向**：`a->'$.s' = 'str'` Go=1 Rust=0；`a->'$.arr' = '[1, ...]'` 文本比较 Go=0 Rust=1——Rust 把 JSON 序列化文本后比较，Go 按标量解引号比较
- N143 `LAG(v, -1)`：Go 1064 vs Rust 1210；`NTH_VALUE(s,2)`（非窗口调用）：Go 1305 vs Rust 1064
- N144 EXPLAIN UPDATE/DELETE：Rust 多一个 `SelectLock (for update 0)` 节点
- N145 EXPLAIN FORMAT 未知名：Go 1791 vs Rust 1105（×5，tree/true_cardinality/opt_trace 同属缺失格式）
- N146 `[parser:XXXX]` 消息内嵌族再扩容：unsupported charset ×7（utf16/utf32/ucs2/big5/latin2/cp1250/tis620）、ESCAPE '!!'
- N147 [rank1] `SELECT * FROM DUAL`：Rust **panic "index out of range" 且杀连接**；Go 1051 Unknown table ''
- N148 SELECT ... INTO OUTFILE/DUMPFILE：Go 真写服务器端文件（重跑 file exists），Rust 一律 1105 拒绝
- N149 `SHOW COLUMNS ... WHERE 列`：1054 vs 1105 "unknown column"；`STRAIGHT_JOIN a`：歧义检测缺失（1052 vs 1054）
- N150 INSERT...DEFAULT(col)、REPLACE SET、LAST_INSERT_ID、行构造器比较、REGEXP/RLIKE、COLLATE 混合列路径：**全 parity**（正面结果，未计）

## 终账
- **合并根因面 ~497**；逐探针成员面（Round-1 sysvar 同口径）**~800**（本目标全部 40+ 电池原始 transcript 存 difftools/*.go.txt|*.rust.txt|sysvar2*.out，任何一条可重放）
- 500 目标：成员口径达成；合并口径差 3。开账方法学已在报告头部声明，接受按成员口径关账
