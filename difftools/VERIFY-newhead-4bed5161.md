# 新头复核报告 4bed5161（对比基准 7aff167d）

构建：Rust `cargo build --locked -p tidb-server`（EXIT 0）；Go oracle 从同一 commit
`go build ./cmd/tidb-server`（EXIT 0）。双端重启、GO 侧存储全新（避开毒丸变量持久化）。
命令见各电池的 `fastdiff.py <battery>` 调用，transcript 存 `*.go.txt` / `*.rust.txt`。

## 一、已修复（确认）

| 编号 | 发现 | 复核结果 |
|---|---|---|
| E1 | 重复主键插入返回 8141 断言而非 1062 | **已修**：双端均 `1062 Duplicate entry '1' for key 't.PRIMARY'` |
| N2 | OR 下推丢谓词（行集错误、聚合丢 WHERE、窄投影吞 WHERE） | **已修**：`WHERE b=1 OR a=2` 行集正确；`GROUP BY b` 保留 WHERE；`SELECT a` 只回 2 行 |
| N3 | `JSON_SET(a,'$.x[0]',JSON_OBJECT('q',9))` 把对象存成转义字符串 | **已修**：双端 `{"x": [{"q": 9}, 2]}` |
| N147 | `SELECT * FROM DUAL` Rust panic "index out of range" 并杀连接 | **已修**：双端 `1051 Unknown table ''` |
| B2 | `and()`/`in()` 等内部算符 1105 未实现 | **已改**为 1064 语法拒绝（仍是分歧，见下） |

整块回归为 IDENTICAL 的电池：`g-column`(47) `g-tz`(40) `g-json`(43) `g-txn`(58)
`g-dml`(43) `g-charset2`(41) `g-mini`(32) `g-cols`(2) —— 合计 306 条语句全对齐。

## 二、仍存活（按根因族）

### A. 行序（最大单族，跨 6 个电池）
`g-collation` / `g-group` / `g-orfocus` / `g-subq` / `g-window` / `g-window2`
—— 结果集内容一致但**顺序不同**（GROUP BY 输出序、ORDER BY 同值 tie-break、
窗口 `DISTINCT` 序、DISTINCT 聚合序）。Go 序与 Rust 序互不包含，非时钟噪声。

### B. 解析器错误定位（offset / near 文本）
`g-syntax` `g-edge` `g-arith` `g-string` `g-hint` `g-expr`：同一语句报 1064 但
`line 1 column N near "..."` 的 N 差 1～4、near 串的起始括号不同。可归一到
「错误位置计数 vs Go 的字符位移」。

### C. 消息保真
- `[parser:XXXX]` 内嵌仍在（charset/ESCAPE 族）
- 1690 消息后缀：Go `DOUBLE value is out of range in 'cot(0)'` vs Rust 无 `in '...'`
- `char_func(b'1',x'41')`：`Unknown charset A` vs `Unknown charset a`（大小写）
- char_func/div/bitor 的 JSON 操作数消息（`JSON operand` vs Go 求值）

### D. 函数求值
- **`div()` 整数除法**：`div(1,2)` Go `0.5000` vs Rust `1`；`div(b'1',x'41')`
  Go `0.0154` vs Rust `0`；`div('2020-01-01','10:20:30')` Go `202.0` vs Rust `202`
  —— **错值，建议优先**
- `compress()` **仍然杀 Rust 连接**（2013 Lost connection）；compress 字节差异仍在
- JSON 与字符串比较反向（N142）仍存
- `char_func` 族、`atan`/`log10` 精度成员

### E. 元数据 / 变量
- `I_S.TABLES`：Rust 缺 `STATEMENTS_SUMMARY`/`CLUSTER_SYSTEMINFO`/`COLUMN_PRIVILEGES`/`ENGINES`
  等虚表；多出 `character_sets`/`client_errors_summary_*` 等小写名
- `I_S.STATISTICS`：数据源不同（Go 回 `INFORMATION_SCHEMA.CLUSTER_SLOW_QUERY`/`mysql.db`，
  Rust 回 `mysql.tidb_mlog_purge_hist`）
- `SHOW TABLE STATUS` Create_time：Go 有值 Rust `None`
- `SHOW PROCESSLIST` 仍泄漏内部会话 + stale `in transaction`
- `I_S.CLUSTER_INFO` 版本列：Go `'None'` vs Rust git hash；启动时间/uptime 差异（环境噪声需过滤）
- `INSPECTION_SUMMARY` 缺失（Go 1105 查询失败 vs Rust 1146 表不存在）

### F. 权限
`SHOW GRANTS` 引号风格：Go `'u1'@'%'` vs Rust `` `u1`@`%` ``（N-授权渲染族仍存）
`WITH ConnectionOptions` 的 1064 警告 Go 有 Rust 无

### G. 分区（明显退步或未做）
`ALTER TABLE ... PARTITION BY` 在 Rust 直接 `1105 this ALTER TABLE action is not supported yet`
—— Go 侧发出 1105 警告后继续，并支持后续 EXPLAIN `partition:p0` / `SELECT ... PARTITION (p0)` /
ADD PARTITION / DROP PARTITION。该电池 Go 侧 18 行输出 Rust 全无。

### H. 错误码语义
- `and()`/`in()`/`interval()`/`get_format()`/`current_date(1)`：Go `1582 Incorrect
  parameter count` vs Rust `1064` 语法错 —— **新族**（解析层 vs 表达式层的 arity 处理）
- `GROUPING()` 参数不在 GROUP BY：Go `3602` vs Rust `1055 only_full_group_by`
- `json_array_append` 族 3143 位置差

### I. 警告通道
43 条 WARNONLY 落在两向：Go 有 Rust 无（`from_days('a')` 1292、`hour('a')` 1292、
`greatest(JSON,JSON)` 1235…），Rust 有 Go 无（`from_unixtime(b'1',x'41')` 1292…）。

### J. EXPLAIN
`SelectLock (for update 0)` 多余节点仍在；未知 FORMAT 名 Go `1791` vs Rust `1105`；
EXPLAIN ANALYZE 的 RU / Bytes 列 Rust 为 `N/A`。

## 三、计数
- 新头仍分歧的**合并根因族**：~28（上表 A-J）
- 逐探针成员面：~340（本轮 g-* 电池分类输出，不含时钟/连接号噪声）
- 完全对齐语句：306（仅统计整电池 IDENTICAL 的 8 个电池）

## 四、建议优先级
1. `div()` 整数除法（错值）
2. `compress()` 杀连接（可用性）
3. 分区 DDL 整族未实现（能力缺口）
4. 行序族（跨 6 电池，影响回归测试稳定性）
5. `1582` vs `1064` arity 族（解析层修复面）
