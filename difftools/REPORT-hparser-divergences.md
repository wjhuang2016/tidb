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
