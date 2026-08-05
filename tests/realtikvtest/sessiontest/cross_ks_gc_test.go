// Copyright 2026 PingCAP, Inc.
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

package sessiontest

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	clitutil "github.com/tikv/client-go/v2/util"
)

func TestCrossKSRuntimeGCLoopStartedBySystemDomain(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only runs in nextgen kernel")
	}

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockRuntimeGCLoopConfig",
		func(interval, idleTimeout *time.Duration) {
			*interval = 50 * time.Millisecond
			*idleTimeout = 100 * time.Millisecond
		})

	const targetKS = "keyspace2"
	_, systemDom := realtikvtest.CreateMockStoreAndDomainAndSetup(t,
		realtikvtest.WithKeyspaceName(keyspace.System),
		realtikvtest.WithAllocPort(true))
	// Bootstrap the target keyspace so its runtime can be acquired, then release the bootstrap domain
	// before the GC loop opens and closes its own runtime.
	t.Run("bootstrap target keyspace", func(t *testing.T) {
		realtikvtest.CreateMockStoreAndDomainAndSetup(t,
			realtikvtest.WithKeyspaceName(targetKS),
			realtikvtest.WithKeepSystemStore(true),
			realtikvtest.WithAllocPort(true))
	})

	handle, err := systemDom.AcquireKSRuntime(targetKS, "test/cross-ks-gc-loop")
	require.NoError(t, err)
	require.Contains(t, systemDom.GetCrossKSMgr().GetAllKeyspace(), targetKS)

	handle.Release()

	require.Eventually(t, func() bool {
		return !slices.Contains(systemDom.GetCrossKSMgr().GetAllKeyspace(), targetKS)
	}, 5*time.Second, 20*time.Millisecond)
}

func TestCrossKSRuntimeGCReacquireBeforeStoreClose(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only runs in nextgen kernel")
	}

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/mockRuntimeGCLoopConfig",
		func(interval, idleTimeout *time.Duration) {
			*interval = 50 * time.Millisecond
			*idleTimeout = 100 * time.Millisecond
		})

	const targetKS = "keyspace3"
	_, systemDom := realtikvtest.CreateMockStoreAndDomainAndSetup(t,
		realtikvtest.WithKeyspaceName(keyspace.System),
		realtikvtest.WithAllocPort(true))
	t.Run("bootstrap target keyspace", func(t *testing.T) {
		realtikvtest.CreateMockStoreAndDomainAndSetup(t,
			realtikvtest.WithKeyspaceName(targetKS),
			realtikvtest.WithKeepSystemStore(true),
			realtikvtest.WithAllocPort(true))
	})

	first, err := systemDom.AcquireKSRuntime(targetKS, "DXF/scheduler/100")
	require.NoError(t, err)
	firstStore := first.Store()
	markerKey := kv.Key("cross-ks-gc-race-marker")
	markerValue := []byte("alive")
	txn, err := firstStore.Begin()
	require.NoError(t, err)
	require.NoError(t, txn.Set(markerKey, markerValue))
	require.NoError(t, txn.Commit(context.Background()))
	readMarker := func(store kv.Storage) ([]byte, error) {
		txn, err := store.Begin()
		if err != nil {
			return nil, err
		}
		defer txn.Rollback()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		entry, err := txn.Get(ctx, markerKey)
		return entry.Value, err
	}

	closeReached := make(chan struct{})
	allowClose := make(chan struct{})
	var blockCloseOnce sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore",
		func(needClose *bool) {
			if !*needClose {
				return
			}
			blockCloseOnce.Do(func() {
				close(closeReached)
				<-allowClose
			})
		})

	first.Release()
	select {
	case <-closeReached:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime GC did not reach store close")
	}

	second, err := systemDom.AcquireKSRuntime(targetKS, "DXF/scheduler/101")
	require.NoError(t, err)
	require.Same(t, firstStore, second.Store(), "the TiKV driver cache reuses the not-yet-closed store")
	got, err := readMarker(second.Store())
	require.NoError(t, err)
	require.Equal(t, markerValue, got)
	require.NoError(t, dxfutil.CheckTaskRuntime(second, targetKS))
	taskCtx := clitutil.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	taskMgr := storage.NewTaskManager(second.SysSessionPool())
	require.NoError(t, taskMgr.InitMeta(taskCtx, "cross-ks-gc-probe:4000", ""))
	taskID, err := taskMgr.CreateTask(taskCtx, "cross-ks-gc-race-task", proto.TaskTypeExample,
		targetKS, 1, "", 0, proto.ExtraParams{}, []byte("persisted-task"))
	require.NoError(t, err)
	taskBeforeClose, err := taskMgr.GetTaskByID(taskCtx, taskID)
	require.NoError(t, err)
	require.Equal(t, []byte("persisted-task"), taskBeforeClose.Meta)
	closeState, ok := second.Store().(interface {
		Closed() <-chan struct{}
		IsClose() bool
	})
	require.True(t, ok)
	require.False(t, closeState.IsClose())

	close(allowClose)
	select {
	case <-closeState.Closed():
	case <-time.After(5 * time.Second):
		t.Fatal("the old runtime did not close the store used by the new runtime")
	}
	require.True(t, closeState.IsClose(), "the new runtime now owns a closed TiKV store")
	require.NoError(t, dxfutil.CheckTaskRuntime(second, targetKS),
		"the identity-only task runtime check does not detect a closed store")
	taskQueryCtx, cancelTaskQuery := context.WithTimeout(
		clitutil.WithInternalSourceType(context.Background(), kv.InternalDistTask), 2*time.Second)
	_, taskQueryErr := taskMgr.GetTaskByID(taskQueryCtx, taskID)
	cancelTaskQuery()
	require.Error(t, taskQueryErr, "a DXF task-table read unexpectedly succeeded on the closed runtime")
	require.NotErrorIs(t, taskQueryErr, storage.ErrTaskNotFound,
		"the persisted task unexpectedly became a normal not-found result")
	require.ErrorContains(t, taskQueryErr, "rpcClient is closed")
	executor, err := systemDom.AcquireKSRuntime(targetKS, "DXF/executor/101")
	require.NoError(t, err)
	require.Same(t, second.Store(), executor.Store(),
		"the executor of the same task reuses the poisoned runtime")
	require.NoError(t, dxfutil.CheckTaskRuntime(executor, targetKS))
	executor.Release()

	second.Release()
	require.Eventually(t, func() bool {
		return !slices.Contains(systemDom.GetCrossKSMgr().GetAllKeyspace(), targetKS)
	}, 5*time.Second, 20*time.Millisecond)

	third, err := systemDom.AcquireKSRuntime(targetKS, "test/cross-ks-gc-race-control")
	require.NoError(t, err)
	require.NotSame(t, firstStore, third.Store(), "reacquiring after close must create a fresh store")
	got, err = readMarker(third.Store())
	require.NoError(t, err)
	require.Equal(t, markerValue, got)
	thirdTaskCtx, cancelThirdTaskQuery := context.WithTimeout(
		clitutil.WithInternalSourceType(context.Background(), kv.InternalDistTask), 2*time.Second)
	taskAfterReopen, err := storage.NewTaskManager(third.SysSessionPool()).GetTaskByID(thirdTaskCtx, taskID)
	cancelThirdTaskQuery()
	require.NoError(t, err)
	require.Equal(t, taskBeforeClose.Meta, taskAfterReopen.Meta)
	third.Release()
}
