// Copyright 2022 PingCAP, Inc.
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

package ingest

import (
	"context"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	lightning "github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tikv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/generic"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// BackendCtx is the backend context for add index reorg task.
type BackendCtx interface {
	Register(jobID, indexID int64, schemaName, tableName string) (Engine, error)
	Unregister(jobID, indexID int64)

	CollectRemoteDuplicateRows(indexID int64, unique bool, tbl table.Table) error
	UnsafeImportAndReset(ctx context.Context, indexID int64, regionSplitSize, regionSplitKeys int64) error
	FinishImport(indexID int64, unique bool, tbl table.Table) error
	ResetWorkers(jobID, indexID int64)
	Flush(indexID int64) (imported bool, err error)
	Done() bool
	SetDone()
}

// litBackendCtx store a backend info for add index reorg task.
type litBackendCtx struct {
	generic.SyncMap[int64, *engineInfo]
	MemRoot  MemRoot
	DiskRoot DiskRoot
	jobID    int64
	backend  *local.Backend
	ctx      context.Context
	cfg      *lightning.Config
	sysVars  map[string]string
	diskRoot DiskRoot
	done     bool
}

func (bc *litBackendCtx) CollectRemoteDuplicateRows(indexID int64, unique bool, tbl table.Table) error {
	if !unique {
		return nil
	}

	errorMgr := errormanager.New(nil, bc.cfg, log.Logger{Logger: logutil.BgLogger()})
	// backend must be a local backend.
	// todo: when we can separate local backend completely from tidb backend, will remove this cast.
	//nolint:forcetypeassert
	dupeController := bc.backend.GetDupeController(bc.cfg.TikvImporter.RangeConcurrency*2, errorMgr)
	hasDupe, err := dupeController.CollectRemoteDuplicateRows(bc.ctx, tbl, tbl.Meta().Name.L, &encode.SessionOptions{
		SQLMode: mysql.ModeStrictAllTables,
		SysVars: bc.sysVars,
		IndexID: indexID,
	})
	if err != nil {
		logutil.BgLogger().Error(LitInfoRemoteDupCheck, zap.Error(err),
			zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
		return err
	} else if hasDupe {
		logutil.BgLogger().Error(LitErrRemoteDupExistErr,
			zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
		return tikv.ErrKeyExists
	}
	return nil
}

// FinishImport imports all the key-values in engine into the storage, collects the duplicate errors if any, and
// removes the engine from the backend context.
func (bc *litBackendCtx) FinishImport(indexID int64, unique bool, tbl table.Table) error {
	ei, exist := bc.Load(indexID)
	if !exist {
		return dbterror.ErrIngestFailed.FastGenByArgs("ingest engine not found")
	}

	err := ei.ImportAndClean()
	if err != nil {
		return err
	}

	// Check remote duplicate value for the index.
	if unique {
		errorMgr := errormanager.New(nil, bc.cfg, log.Logger{Logger: logutil.BgLogger()})
		// backend must be a local backend.
		// todo: when we can separate local backend completely from tidb backend, will remove this cast.
		//nolint:forcetypeassert
		dupeController := bc.backend.GetDupeController(bc.cfg.TikvImporter.RangeConcurrency*2, errorMgr)
		hasDupe, err := dupeController.CollectRemoteDuplicateRows(bc.ctx, tbl, tbl.Meta().Name.L, &encode.SessionOptions{
			SQLMode: mysql.ModeStrictAllTables,
			SysVars: bc.sysVars,
			IndexID: ei.indexID,
		})
		if err != nil {
			logutil.BgLogger().Error(LitInfoRemoteDupCheck, zap.Error(err),
				zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
			return err
		} else if hasDupe {
			logutil.BgLogger().Error(LitErrRemoteDupExistErr,
				zap.String("table", tbl.Meta().Name.O), zap.Int64("index ID", indexID))
			return tikv.ErrKeyExists
		}
	}
	return nil
}

// UnsafeImportAndReset implements BackendCtx.UnsafeImportAndReset interface.
func (bc *litBackendCtx) UnsafeImportAndReset(ctx context.Context, indexID int64, regionSplitSize, regionSplitKeys int64) error {
	ei, exist := bc.Load(indexID)
	if !exist {
		return dbterror.ErrIngestFailed.FastGenByArgs("ingest engine not found")
	}
	return bc.backend.UnsafeImportAndReset(ctx, ei.uuid, regionSplitSize, regionSplitKeys)
}

const importThreshold = 0.85

// Flush checks the disk quota and imports the current key-values in engine to the storage.
func (bc *litBackendCtx) Flush(indexID int64) (imported bool, err error) {
	ei, exist := bc.Load(indexID)
	if !exist {
		logutil.BgLogger().Error(LitErrGetEngineFail, zap.Int64("index ID", indexID))
		return false, dbterror.ErrIngestFailed.FastGenByArgs("ingest engine not found")
	}

	err = bc.diskRoot.UpdateUsageAndQuota()
	if err != nil {
		logutil.BgLogger().Error(LitErrUpdateDiskStats, zap.Int64("index ID", indexID))
		return false, err
	}

	err = ei.Flush()
	if err != nil {
		return false, err
	}

	release := ei.acquireFlushLock()
	if release != nil && bc.diskRoot.CurrentUsage() >= uint64(importThreshold*float64(bc.diskRoot.MaxQuota())) {
		defer release()
		logutil.BgLogger().Info(LitInfoUnsafeImport, zap.Int64("index ID", indexID),
			zap.Uint64("current disk usage", bc.diskRoot.CurrentUsage()),
			zap.Uint64("max disk quota", bc.diskRoot.MaxQuota()))
		err = bc.backend.UnsafeImportAndReset(bc.ctx, ei.uuid, int64(lightning.SplitRegionSize)*int64(lightning.MaxSplitRegionSizeRatio), int64(lightning.SplitRegionKeys))
		if err != nil {
			logutil.BgLogger().Error(LitErrIngestDataErr, zap.Int64("index ID", indexID),
				zap.Error(err), zap.Uint64("current disk usage", bc.diskRoot.CurrentUsage()),
				zap.Uint64("max disk quota", bc.diskRoot.MaxQuota()))
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// Done returns true if the lightning backfill is done.
func (bc *litBackendCtx) Done() bool {
	return bc.done
}

// SetDone sets the done flag.
func (bc *litBackendCtx) SetDone() {
	bc.done = true
}
