// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type backfillSchedulerHandle struct {
	d             *ddl
	db            *model.DBInfo
	index         *model.IndexInfo
	job           *model.Job
	bc            ingest.BackendCtx
	ptbl          table.PhysicalTable
	jc            *JobContext
	eleTypeKey    []byte
	totalRowCnt   int64
	isPartition   bool
	stepForImport bool
}

// BackfillGlobalMeta is the global task meta for backfilling index.
type BackfillGlobalMeta struct {
	Job        model.Job `json:"job"`
	EleID      int64     `json:"ele_id"`
	EleTypeKey []byte    `json:"ele_type_key"`
}

// BackfillSubTaskMeta is the sub-task meta for backfilling index.
type BackfillSubTaskMeta struct {
	PhysicalTableID int64  `json:"physical_table_id"`
	StartKey        []byte `json:"start_key"`
	EndKey          []byte `json:"end_key"`
}

// BackfillMinimalTask is the minimal-task for backfilling index.
type BackfillMinimalTask struct {
}

// IsMinimalTask implements the MinimalTask interface.
func (b *BackfillMinimalTask) IsMinimalTask() {
}

// NewBackfillSchedulerHandle creates a new backfill scheduler.
func NewBackfillSchedulerHandle(taskMeta []byte, d *ddl, stepForImport bool) (scheduler.Scheduler, error) {
	bh := &backfillSchedulerHandle{d: d}

	bgm := &BackfillGlobalMeta{}
	err := json.Unmarshal(taskMeta, bgm)
	if err != nil {
		return nil, err
	}

	bh.eleTypeKey = bgm.EleTypeKey
	jobMeta := &bgm.Job
	bh.job = jobMeta

	db, tbl, err := d.getTableByTxn(d.store, jobMeta.SchemaID, jobMeta.TableID)
	if err != nil {
		return nil, err
	}
	bh.isPartition = tbl.Meta().GetPartitionInfo() != nil
	bh.db = db

	physicalTable := tbl.(table.PhysicalTable)
	bh.ptbl = physicalTable

	indexInfo := model.FindIndexInfoByID(tbl.Meta().Indices, bgm.EleID)
	if indexInfo == nil {
		logutil.BgLogger().Warn("[ddl-ingest] cannot init cop request sender",
			zap.Int64("table ID", tbl.Meta().ID), zap.Int64("index ID", bgm.EleID))
		return nil, errors.New("cannot find index info")
	}
	bh.index = indexInfo

	if stepForImport {
		bh.stepForImport = true
		return bh, nil
	}

	d.setDDLLabelForTopSQL(jobMeta.ID, jobMeta.Query)
	d.setDDLSourceForDiagnosis(jobMeta.ID, jobMeta.Type)
	jobCtx := d.jobContext(jobMeta.ID)
	bh.jc = jobCtx

	return bh, nil
}

// InitSubtaskExecEnv implements the Scheduler interface.
func (b *backfillSchedulerHandle) InitSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("[ddl] lightning init subtask exec env")
	d := b.d

	bc, err := ingest.LitBackCtxMgr.Register(d.ctx, b.index.Unique, b.job.ID, d.etcdCli)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] lightning register error", zap.Error(err))
		return err
	}
	b.bc = bc
	if b.stepForImport {
		return b.doImportWithDistributedLock(ctx, ingest.FlushModeForceGlobal)
	}
	return nil
}

func acquireLock(ctx context.Context, se *concurrency.Session, key string) error {
	mu := concurrency.NewMutex(se, key)
	err := mu.Lock(ctx)
	if err != nil {
		return err
	}
	return nil
}

func releaseLock(ctx context.Context, se *concurrency.Session, key string) error {
	mu := concurrency.NewMutex(se, key)
	err := mu.Unlock(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (b *backfillSchedulerHandle) doImportWithDistributedLock(ctx context.Context, mode ingest.FlushMode) error {
	distLockKey := fmt.Sprintf("/tidb/distributeLock/%d/%d", b.job.ID, b.index.ID)
	se, _ := concurrency.NewSession(b.d.etcdCli)
	err := acquireLock(ctx, se, distLockKey)
	if err != nil {
		return err
	}
	logutil.BgLogger().Info("[ddl] acquire lock success")
	defer func() {
		err = releaseLock(ctx, se, distLockKey)
		if err != nil {
			logutil.BgLogger().Warn("[ddl] release lock error", zap.Error(err))
		}
		logutil.BgLogger().Info("[ddl] release lock success")
		err = se.Close()
		if err != nil {
			logutil.BgLogger().Warn("[ddl] close session error", zap.Error(err))
		}
	}()

	_, _, err = b.bc.Flush(b.index.ID, ingest.FlushModeForceGlobal)
	if err != nil {
		if common.ErrFoundDuplicateKeys.Equal(err) {
			err = convertToKeyExistsErr(err, b.index, b.ptbl.Meta())
		}
		logutil.BgLogger().Error("[ddl] flush error", zap.Error(err))
		return err
	}
	return nil
}

// SplitSubtask implements the Scheduler interface.
func (b *backfillSchedulerHandle) SplitSubtask(ctx context.Context, subtask []byte) ([]proto.MinimalTask, error) {
	logutil.BgLogger().Info("[ddl] lightning split subtask")

	if b.stepForImport {
		return nil, nil
	}

	fnCtx, fnCancel := context.WithCancel(context.Background())
	defer fnCancel()

	go func() {
		select {
		case <-ctx.Done():
			b.d.notifyReorgWorkerJobStateChange(b.job)
		case <-fnCtx.Done():
		}
	}()

	d := b.d
	sm := &BackfillSubTaskMeta{}
	err := json.Unmarshal(subtask, sm)
	if err != nil {
		logutil.BgLogger().Error("[ddl] unmarshal error", zap.Error(err))
		return nil, err
	}

	var startKey, endKey kv.Key
	var tbl table.PhysicalTable

	currentVer, err1 := getValidCurrentVersion(d.store)
	if err1 != nil {
		return nil, errors.Trace(err1)
	}

	if !b.isPartition {
		startKey, endKey = sm.StartKey, sm.EndKey
		tbl = b.ptbl
	} else {
		pid := sm.PhysicalTableID
		parTbl := b.ptbl.(table.PartitionedTable)
		startKey, endKey, err = getTableRange(b.jc, d.ddlCtx, parTbl.GetPartition(pid), currentVer.Ver, b.job.Priority)
		if err != nil {
			logutil.BgLogger().Error("[ddl] get table range error", zap.Error(err))
			return nil, err
		}
		tbl = parTbl.GetPartition(pid)
	}

	mockReorgInfo := &reorgInfo{Job: b.job, d: d.ddlCtx}
	elements := make([]*meta.Element, 0)
	elements = append(elements, &meta.Element{ID: b.index.ID, TypeKey: meta.IndexElementKey})
	mockReorgInfo.elements = elements
	mockReorgInfo.currElement = mockReorgInfo.elements[0]

	ingestScheduler := newIngestBackfillScheduler(d.ctx, mockReorgInfo, d.sessPool, tbl, true)
	defer ingestScheduler.close(true)

	consumer := newResultConsumer(d.ddlCtx, mockReorgInfo, nil, true)
	consumer.run(ingestScheduler, startKey, &b.totalRowCnt)

	err = ingestScheduler.setupWorkers()
	if err != nil {
		logutil.BgLogger().Error("[ddl] setup workers error", zap.Error(err))
		return nil, err
	}

	taskIDAlloc := newTaskIDAllocator()
	for {
		kvRanges, err := splitTableRanges(b.ptbl, d.store, startKey, endKey, backfillTaskChanSize)
		if err != nil {
			return nil, err
		}
		if len(kvRanges) == 0 {
			break
		}

		logutil.BgLogger().Info("[ddl] start backfill workers to reorg record",
			zap.Int("workerCnt", ingestScheduler.currentWorkerSize()),
			zap.Int("regionCnt", len(kvRanges)),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)))

		sendTasks(ingestScheduler, consumer, tbl, kvRanges, mockReorgInfo, taskIDAlloc)
		if consumer.shouldAbort() {
			break
		}
		rangeEndKey := kvRanges[len(kvRanges)-1].EndKey
		startKey = rangeEndKey.Next()
		if startKey.Cmp(endKey) >= 0 {
			break
		}
	}
	ingestScheduler.close(false)

	if consumer.getResult() != nil {
		return nil, consumer.getResult()
	}

	if b.isPartition {
		return nil, b.doImportWithDistributedLock(ctx, ingest.FlushModeForceGlobal)
	}
	return nil, b.doImportWithDistributedLock(ctx, ingest.FlushModeForceLocalAndCheckDiskQuota)
}

// OnSubtaskFinished implements the Scheduler interface.
func (*backfillSchedulerHandle) OnSubtaskFinished(_ context.Context, meta []byte) ([]byte, error) {
	return meta, nil
}

// CleanupSubtaskExecEnv implements the Scheduler interface.
func (b *backfillSchedulerHandle) CleanupSubtaskExecEnv(context.Context) error {
	logutil.BgLogger().Info("[ddl] lightning cleanup subtask exec env")

	if b.isPartition || b.stepForImport {
		b.bc.Unregister(b.job.ID, b.index.ID)
	}
	return nil
}

// Rollback implements the Scheduler interface.
func (b *backfillSchedulerHandle) Rollback(context.Context) error {
	return nil
}

// BackFillSubtaskExecutor is the executor for backfill subtask.
type BackFillSubtaskExecutor struct {
	Task proto.MinimalTask
}

// Run implements the Executor interface.
func (b *BackFillSubtaskExecutor) Run(ctx context.Context) error {
	return nil
}

// BackfillTaskType is the type of backfill task.
const BackfillTaskType = "backfill"