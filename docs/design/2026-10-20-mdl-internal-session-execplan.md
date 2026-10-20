# Make internal sessions participate in MDL for user tables

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` (this file) at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

Transactions running on internal sessions (TTL workers, materialized view maintenance, statistics, ...) currently never block DDL, because metadata lock (MDL) census skips them in two places. That allows a DDL to change a user table while such a transaction still writes it, producing e.g. orphan index entries (issue #71055). After this change, an internal transaction that accesses a **user table** blocks concurrent DDL on that table until it finishes, exactly like a user connection. Internal transactions that only touch tables in the `mysql` schema keep today's behavior, so the DDL framework and the stats bootstrap cannot block upgrade DDL.

You can observe the change with the regression tests added here: with an internal session holding a transaction on `test.t`, `ALTER TABLE test.t ...` waits; with the transaction on a table in the `mysql` schema, the DDL proceeds.

Design background: `docs/design/2026-10-16-mdl-for-internal-sessions.md`.

## Progress

- [x] Remove the `InRestrictedSQL` early return in `RemoveLockDDLJobs` and add attribution fields to the blocking log.
- [x] Census registered internal sessions in `Server.CheckOldRunningTxn`.
- [x] Census registered internal sessions in `MockSessionManager.CheckOldRunningTxn`.
- [x] Keep `mysql`-schema tables exempt for internal sessions in `tryLockMDLAndUpdateSchemaIfNecessary`.
- [x] Add regression tests (unit + `pkg/ddl/tests/metadatalock`).
- [x] Targeted tests pass on classic and nextgen; `make lint` passes for the changed files (the repository-wide target fails on an unrelated untracked probe test, see Surprises).
- [x] Bazel BUILD metadata regenerated with scoped `gazelle` and `tazel` (new `mdl_test.go`, `//pkg/kv` dep, `shard_count` 40 -> 42).

## Surprises & Discoveries

- Observation: `AdvancedSessionPool.Get()` already registers the checked-out session through `TransferOwner` → `onBecameOwner` → `infosync.StoreInternalSession`, so removing the two census-side exemptions is enough for TTL to participate; no per-feature registration change is needed.
  Evidence: `pkg/session/syssession/pool.go:109-140`, `pkg/session/syssession/session.go:594-603`.
- Observation: classic kernels do not assign reserved IDs to `mysql` system tables, so the `mysql` exemption must key on the schema name (`metadef.IsSystemDB`), not on `metadef.IsReservedID`.
  Evidence: `pkg/session/bootstrap.go:415-431` creates system tables with plain `CREATE TABLE`.
- Observation: `ADMIN CANCEL DDL` does not interrupt `WaitVersionSynced`; this is a pre-existing gap and out of scope here.
- Observation: `ExecuteInternal` on an internal session panics when the context has no `RequestSourceTypeKey`. Tests must wrap the context with `kv.WithInternalSourceType`.
  Evidence: `pkg/session/session.go:5863`; fixed in the new tests with `kv.WithInternalSourceType(ctx, kv.InternalTxnTTL)`.
- Observation: repository-wide `make lint` fails on `pkg/executor/test/writetest/pessimistic_fk_cascade_orphan_probe_test.go`, an untracked file that predates this change. Running `revive` on the changed files reports no problems.

## Decision Log

- Decision: remove both census-side exemptions and add a schema-scoped exemption in the lock-registration path, instead of tagging individual internal workloads.
  Rationale: new internal workloads that touch user tables are protected by default; only `mysql`-schema access stays exempt, which keeps upgrade/bootstrap behavior identical to today.
  Date/Author: 2026-10-20 / wjhuang2016

- Decision: copy the internal session list under `sessionMapMutex` and inspect sessions outside the lock.
  Rationale: `RemoveLockDDLJobs` may block on a session's `TxnCtxMu`; holding `sessionMapMutex` during that would block pool `Get`/`Put`.
  Date/Author: 2026-10-20 / wjhuang2016

## Outcomes & Retrospective

Internal transactions now block DDL on the user tables they access, and keep the old behavior on `mysql`-schema tables. The regression test fails on the unmodified tree (`--- FAIL: TestMDLInternalSessionBlocksDDL`) and passes with the change. Remaining work from the design (TTL scan timeout, MV refresh lock, `ADMIN CANCEL` during MDL wait) is intentionally left for follow-up PRs, as split in `docs/design/2026-10-16-mdl-for-internal-sessions.md`.

## Context and Orientation

- `pkg/sessionctx/variable/session.go` — `RemoveLockDDLJobs` decides whether a session's transaction blocks a DDL job. `TxnCtx.relatedTableForMDL` maps table ID to the schema version the transaction locked; entries are written by `pkg/planner/core/preprocess.go` (`tryLockMDLAndUpdateSchemaIfNecessary`).
- `pkg/server/server.go` — `Server.CheckOldRunningTxn` is the MDL census for ordinary deployments. It iterated only `s.clients` (external connections).
- `pkg/testkit/mocksessionmanager.go` — the census used by tests.
- `pkg/domain/crossks/coordinator.go` — cross-keyspace census; it already iterates registered internal sessions, so no change is required.
- `pkg/infoschema/issyncer/syncer.go` — runs the census every 50ms and reports per-node versions to the DDL owner.

Internal session: a session created by TiDB itself (session pools in `pkg/domain`, `pkg/ddl/session`, `pkg/session/syssession`), marked with `SessionVars.InRestrictedSQL = true`.

## Plan of Work

1. In `pkg/sessionctx/variable/session.go`, delete the `if sv.InRestrictedSQL { return }` guard at the top of `RemoveLockDDLJobs`. Extend the "old running transaction block DDL" log with `internal`, `startTS`, and log it at Info for sessions without a connection ID (rate-limited by the caller).
2. In `pkg/server/server.go`, after the existing `s.clients` loop, snapshot `s.internalSessions` under `s.sessionMapMutex` and call `variable.RemoveLockDDLJobs` for every entry that exposes `GetSessionVars()`.
3. Mirror step 2 in `pkg/testkit/mocksessionmanager.go` so tests exercise the real code path.
4. In `pkg/planner/core/preprocess.go`, at the top of `tryLockMDLAndUpdateSchemaIfNecessary` (after the existing `defer`), return early when the session is internal and `metadef.IsSystemDB(dbName.L)`. This preserves today's behavior for `mysql`-schema tables.
5. Add `TestMDLInternalSessionBlocksDDL` and `TestMDLInternalSessionDoesNotBlockSystemTableDDL` to `pkg/ddl/tests/metadatalock/mdl_test.go`, and a unit test for `RemoveLockDDLJobs` in `pkg/sessionctx/variable/mdl_test.go`.

## Concrete Steps

Working directory: repository root.

    go test ./pkg/sessionctx/variable -run TestRemoveLockDDLJobsForInternalSession -tags=intest,deadlock
    ./tools/check/failpoint-go-test.sh pkg/ddl/tests/metadatalock -run 'TestMDLInternalSession' -v
    ./tools/check/failpoint-go-test.sh pkg/ddl/tests/metadatalock -tags=intest,deadlock,nextgen -run 'TestMDLInternalSession' -v
    make lint

Expected: the new tests pass after the change. `TestMDLInternalSessionBlocksDDL` fails on the unmodified tree because the DDL completes while the internal transaction is open.

## Validation and Acceptance

- With an internal session executing `BEGIN OPTIMISTIC; INSERT INTO test.t ...` and keeping the transaction open, `ALTER TABLE test.t ADD COLUMN` stays in `WaitVersionSynced` and only finishes after `COMMIT`.
- With an internal session executing the same statements against a table in the `mysql` schema, `ALTER TABLE` on that table finishes while the transaction is open.
- Existing MDL tests (`TestMDLBasic*`, `TestMDLCreateMaterializedViewLog*`) keep passing.

## Idempotence and Recovery

Source edits are ordinary and can be re-applied or reverted with `git checkout -- <file>`. Tests create their own mock clusters; no cleanup is required. No persistent state or configuration changes.

## Artifacts and Notes

    $ ./tools/check/failpoint-go-test.sh pkg/ddl/tests/metadatalock -run 'TestMDLInternalSession' -v
    --- PASS: TestMDLInternalSessionBlocksDDL (0.96s)
    --- PASS: TestMDLInternalSessionDoesNotBlockSystemTableDDL (0.41s)
    ok      github.com/pingcap/tidb/pkg/ddl/tests/metadatalock      2.650s

    $ ./tools/check/failpoint-go-test.sh pkg/ddl/tests/metadatalock -tags=intest,deadlock,nextgen -run 'TestMDLInternalSession' -v
    --- PASS: TestMDLInternalSessionBlocksDDL (0.65s)
    --- PASS: TestMDLInternalSessionDoesNotBlockSystemTableDDL (0.11s)

    $ ./tools/check/failpoint-go-test.sh pkg/ddl/tests/metadatalock -run 'TestMDL'
    ok      github.com/pingcap/tidb/pkg/ddl/tests/metadatalock      78.194s

    $ ./tools/check/failpoint-go-test.sh pkg/sessionctx/variable -run '.*'
    ok      github.com/pingcap/tidb/pkg/sessionctx/variable        0.426s

Without the source fix (tests kept in place):

    --- FAIL: TestMDLInternalSessionBlocksDDL (0.44s)
    FAIL    github.com/pingcap/tidb/pkg/ddl/tests/metadatalock      1.721s

## Interfaces and Dependencies

- `variable.RemoveLockDDLJobs(sv *variable.SessionVars, jobs map[int64]*mdldef.JobMDL, printLog bool)` keeps its signature; only its early-return behavior changes.
- `Server.CheckOldRunningTxn(jobs map[int64]*mdldef.JobMDL)` keeps its signature; it now also inspects registered internal sessions.
- `tryLockMDLAndUpdateSchemaIfNecessary(ctx context.Context, sctx base.PlanContext, dbName ast.CIStr, tbl table.Table, is infoschema.InfoSchema) (table.Table, error)` keeps its signature.
