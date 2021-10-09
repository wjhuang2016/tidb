package ddl

import (
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"sync"
)

// workerPool is used to new worker.
type workerPool struct {
	mu struct {
		sync.Mutex
		closed bool
	}
	resPool *pools.ResourcePool
}

func newDDLWorkerPool(resPool *pools.ResourcePool) *workerPool {
	return &workerPool{resPool: resPool}
}

// get gets workerPool from context resource pool.
// Please remember to call put after you finished using workerPool.
func (sg *workerPool) get() (*worker, error) {
	if sg.resPool == nil {
		return nil, nil
	}

	sg.mu.Lock()
	if sg.mu.closed {
		sg.mu.Unlock()
		return nil, errors.Errorf("workerPool is closed.")
	}
	sg.mu.Unlock()

	// no need to protect sg.resPool
	resource, err := sg.resPool.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}

	worker := resource.(*worker)
	return worker, nil
}

// put returns workerPool to context resource pool.
func (sg *workerPool) put(wk *worker) {
	if sg.resPool == nil {
		return
	}

	// no need to protect sg.resPool, even the sg.resPool is closed, the ctx still need to
	// put into resPool, because when resPool is closing, it will wait all the ctx returns, then resPool finish closing.
	sg.resPool.Put(wk)
}

// close clean up the sessionPool.
func (sg *workerPool) close() {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	// prevent closing resPool twice.
	if sg.mu.closed || sg.resPool == nil {
		return
	}
	logutil.BgLogger().Info("[ddl] closing sessionPool")
	sg.resPool.Close()
	sg.mu.closed = true
}
