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

package variable

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema/issyncer/mdldef"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

// TestRemoveLockDDLJobsForInternalSession verifies that transactions on internal
// sessions participate in MDL. Before the fix RemoveLockDDLJobs returned early for
// `InRestrictedSQL` sessions, so such transactions never blocked a DDL job.
func TestRemoveLockDDLJobsForInternalSession(t *testing.T) {
	sv := NewSessionVars(nil)
	sv.TxnCtx.StartTS = oracle.GoTimeToTS(time.Now())
	sv.InRestrictedSQL = true

	newJobs := func() map[int64]*mdldef.JobMDL {
		return map[int64]*mdldef.JobMDL{
			1: {Ver: 20, TableIDs: map[int64]struct{}{100: {}}},
		}
	}

	sv.GetRelatedTableForMDL().Store(int64(100), int64(10))
	jobs := newJobs()
	RemoveLockDDLJobs(sv, jobs, false)
	require.Empty(t, jobs, "an internal session holding an older schema version must block the DDL job")

	sv.ClearRelatedTableForMDL()
	sv.GetRelatedTableForMDL().Store(int64(100), int64(30))
	jobs = newJobs()
	RemoveLockDDLJobs(sv, jobs, false)
	require.Len(t, jobs, 1, "a newer schema version must not block the DDL job")
}
