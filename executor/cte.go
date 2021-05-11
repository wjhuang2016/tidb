// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
)

// Following diagram describes how CTEExec works.
//
// `iterInTbl` is shared by `CTEExec` and `CTETableReaderExec`.
// `CTETableReaderExec` reads data from `iterInTbl`,
// and its output will be stored `iterOutTbl` by `CTEExec`.
//
// When an iteration ends, `CTEExec` will move all data from `iterOutTbl` into `iterInTbl`,
// which will be the input for new iteration.
// At the end of each iteration, data in `iterOutTbl` will also be added into `resTbl`.
// `resTbl` stores data of all iteration.
//                                   +----------+
//                                   |iterOutTbl|
//       CTEExec ------------------->|          |
//          |                        +----+-----+
//    -------------                       |
//    |           |                       v
// other op     other op             +----------+
// (seed)       (recursive)          |  resTbl  |
//                  ^                |          |
//                  |                +----------+
//            CTETableReaderExec
//                   ^
//                   |               +----------+
//                   +---------------+iterInTbl |
//                                   |          |
//                                   +----------+

var _ Executor = &CTEExec{}

type CTEExec struct {
	baseExecutor

	seedExec      Executor
	recursiveExec Executor

	// resTbl and iterInTbl are shared by all CTEExec which reference to same CTE.
	// iterInTbl is also shared by CTETableReaderExec.
	resTbl     CTEStorage
	iterInTbl  CTEStorage
	iterOutTbl CTEStorage

	// idx of chunk to read from resTbl.
	chkIdx int

	// UNION ALL or UNION DISTINCT.
	isDistinct bool
	curIter    int
}

func (e *CTEExec) Open(ctx context.Context) (err error) {
    e.reset()
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	if e.seedExec == nil {
		return errors.Trace(errors.New("seedExec for CTEExec is nil"))
	}
	if err = e.seedExec.Open(ctx); err != nil {
		return err
	}

	seedTypes := e.seedExec.base().retFieldTypes
	if err = e.resTbl.OpenAndRef(seedTypes, e.maxChunkSize); err != nil {
		return err
	}
	if err = e.iterInTbl.OpenAndRef(seedTypes, e.maxChunkSize); err != nil {
		return err
	}

	if e.recursiveExec != nil {
		if err = e.recursiveExec.Open(ctx); err != nil {
			return err
		}
		if err = e.iterOutTbl.OpenAndRef(seedTypes, e.maxChunkSize); err != nil {
			return err
		}
        setupCTEStorageTracker(e.iterOutTbl, e.ctx)
	}
	return nil
}

func (e *CTEExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	e.resTbl.Lock()
    defer func() {
        e.resTbl.Unlock()
    }()
	if !e.resTbl.Done() {
        setupCTEStorageTracker(e.resTbl, e.ctx)
        setupCTEStorageTracker(e.iterInTbl, e.ctx)

        if err = e.computeSeedPart(ctx); err != nil {
            // Don't put it in defer.
            // Because it should be called only when the filling is not completed.
            if err1 := e.reopenTbls(); err1 != nil {
                return err1
            }
            return err
        }
        if err = e.computeRecursivePart(ctx); err != nil {
            if err1 := e.reopenTbls(); err1 != nil {
                return err1
            }
            return err
        }
    }
    e.resTbl.SetDone()

	if e.chkIdx < e.resTbl.NumChunks() {
		res, err := e.resTbl.GetChunk(e.chkIdx)
		if err != nil {
			return err
		}
		// Need to copy chunk to make sure upper operator will not change chunk in resTbl.
		req.SwapColumns(res.CopyConstruct())
		e.chkIdx++
	}
	return nil
}

func (e *CTEExec) Close() (err error) {
    e.reset()
	if err = e.seedExec.Close(); err != nil {
		return err
	}
	if e.recursiveExec != nil {
		if err = e.recursiveExec.Close(); err != nil {
			return err
		}
        if err = e.iterOutTbl.DerefAndClose(); err != nil {
            return err
        }
	}
	if err = e.resTbl.DerefAndClose(); err != nil {
		return err
	}
	if err = e.iterInTbl.DerefAndClose(); err != nil {
		return err
	}

	return e.baseExecutor.Close()
}

func (e *CTEExec) computeSeedPart(ctx context.Context) (err error) {
    e.curIter = 0
    e.iterInTbl.SetIter(e.curIter)
    if e.curIter >= e.ctx.GetSessionVars().CTEMaxRecursionDepth {
        return ErrCTEMaxRecursionDepth.GenWithStackByArgs(e.curIter + 1)
    }
    for {
        chk := newFirstChunk(e.seedExec)
        if err = Next(ctx, e.seedExec, chk); err != nil {
            return err
        }
        if chk.NumRows() == 0 {
            break
        }
        if chk, err = e.iterInTbl.Add(chk); err != nil {
            return err
        }
        if _, err = e.resTbl.Add(chk); err != nil {
            return err
        }
    }
    e.curIter++

    // TODO: This means iterInTbl fill done. But too tricky.
    close(e.iterInTbl.GetBegCh())
    return nil
}

func (e *CTEExec) computeRecursivePart(ctx context.Context) (err error) {
    if e.recursiveExec == nil || e.iterInTbl.NumChunks() == 0 {
        return nil
    }

    for {
        chk := newFirstChunk(e.recursiveExec)
        if err = Next(ctx, e.recursiveExec, chk); err != nil {
            return err
        }
        if chk.NumRows() == 0 {
            e.iterInTbl.ResetData()
            for i := 0; i < e.iterOutTbl.NumChunks(); i++ {
                if chk, err = e.iterOutTbl.GetChunk(i); err != nil {
                    return err
                }
                if chk, err = e.resTbl.Add(chk); err != nil {
                    return err
                }
                if _, err = e.iterInTbl.Add(chk); err != nil {
                    return err
                }
            }
            if err = e.iterOutTbl.ResetData(); err != nil {
                return err
            }
            if e.iterInTbl.NumChunks() == 0 {
                break
            } else {
                if e.curIter >= e.ctx.GetSessionVars().CTEMaxRecursionDepth {
                    return ErrCTEMaxRecursionDepth.GenWithStackByArgs(e.curIter + 1)
                }
                // Next iteration begins. Need use iterOutTbl as input of next iteration.
                e.curIter++
                e.iterInTbl.SetIter(e.curIter)
                // Make sure iterInTbl is setup before Close/Open,
                // because some executors will read iterInTbl in Open() (like IndexLookupJoin).
                if err = e.recursiveExec.Close(); err != nil {
                    return err
                }
                if err = e.recursiveExec.Open(ctx); err != nil {
                    return err
                }
            }
        } else {
            if _, err = e.iterOutTbl.Add(chk); err != nil {
                return err
            }
        }
    }
    return nil
}

func (e *CTEExec) reset() {
    e.curIter = 0
    e.chkIdx = 0
}

func (e *CTEExec) reopenTbls() (err error) {
    if err := e.resTbl.Reopen(); err != nil {
        return err
    }
    if err := e.iterInTbl.Reopen(); err != nil {
        return err
    }
    return nil
}

func setupCTEStorageTracker(tbl CTEStorage, ctx sessionctx.Context) {
    memTracker := tbl.GetMemTracker()
    memTracker.SetLabel(memory.LabelForCTEStorage)
    memTracker.AttachTo(ctx.GetSessionVars().StmtCtx.MemTracker)

    diskTracker := tbl.GetDiskTracker()
    diskTracker.SetLabel(memory.LabelForCTEStorage)
    diskTracker.AttachTo(ctx.GetSessionVars().StmtCtx.DiskTracker)

    if config.GetGlobalConfig().OOMUseTmpStorage {
        actionSpill := tbl.ActionSpill()
        ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(actionSpill)
    }
}
