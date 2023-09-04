// Copyright 2018 PingCAP, Inc.
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

package implementation

import (
	"testing"

	"github.com/pingcap/tidb/domain"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
)

func TestBaseImplementation(t *testing.T) {
	defer view.Stop()
	sctx := plannercore.MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()
	p := plannercore.PhysicalLimit{}.Init(sctx, nil, 0, nil)
	impl := &baseImpl{plan: p}
	require.Equal(t, p, impl.GetPlan())

	cost := impl.CalcCost(10, []memo.Implementation{}...)
	require.Equal(t, 0.0, cost)
	require.Equal(t, 0.0, impl.GetCost())

	impl.SetCost(6.0)
	require.Equal(t, 6.0, impl.GetCost())
}
