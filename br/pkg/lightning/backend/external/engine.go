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

package external

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/tidb/pkg/metrics"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// during test on ks3, we found that we can open about 8000 connections to ks3,
// bigger than that, we might receive "connection reset by peer" error, and
// the read speed will be very slow, still investigating the reason.
// Also open too many connections will take many memory in kernel, and the
// test is based on k8s pod, not sure how it will behave on EC2.
// but, ks3 supporter says there's no such limit on connections.
// And our target for global sort is AWS s3, this default value might not fit well.
// TODO: adjust it according to cloud storage.
const maxCloudStorageConnections = 1000

type memKVsAndBuffers struct {
	mu           sync.Mutex
	keys         [][]byte
	values       [][]byte
	memKVBuffers []*membuf.Buffer
}

// Engine stored sorted key/value pairs in an external storage.
type Engine struct {
	storage         storage.ExternalStorage
	dataFiles       []string
	statsFiles      []string
	startKey        []byte
	endKey          []byte
	splitKeys       [][]byte
	regionSplitSize int64
	bufPool         *membuf.Pool

	memKVsAndBuffers memKVsAndBuffers

	// checkHotspot is true means we will check hotspot file when using MergeKVIter.
	// if hotspot file is detected, we will use multiple readers to read data.
	// if it's false, MergeKVIter will read each file using 1 reader.
	// this flag also affects the strategy of loading data, either:
	// 	less load routine + check and read hotspot file concurrently (add-index uses this one)
	// 	more load routine + read each file using 1 reader (import-into uses this one)
	checkHotspot          bool
	mergerIterConcurrency int

	keyAdapter         common.KeyAdapter
	duplicateDetection bool
	duplicateDB        *pebble.DB
	dupDetectOpt       common.DupDetectOpt
	workerConcurrency  int
	ts                 uint64

	totalKVSize  int64
	totalKVCount int64

	importedKVSize  *atomic.Int64
	importedKVCount *atomic.Int64
}

// NewExternalEngine creates an (external) engine.
func NewExternalEngine(
	storage storage.ExternalStorage,
	dataFiles []string,
	statsFiles []string,
	startKey []byte,
	endKey []byte,
	splitKeys [][]byte,
	regionSplitSize int64,
	keyAdapter common.KeyAdapter,
	duplicateDetection bool,
	duplicateDB *pebble.DB,
	dupDetectOpt common.DupDetectOpt,
	workerConcurrency int,
	ts uint64,
	totalKVSize int64,
	totalKVCount int64,
	checkHotspot bool,
) common.Engine {
	return &Engine{
		storage:            storage,
		dataFiles:          dataFiles,
		statsFiles:         statsFiles,
		startKey:           startKey,
		endKey:             endKey,
		splitKeys:          splitKeys,
		regionSplitSize:    regionSplitSize,
		bufPool:            membuf.NewPool(),
		checkHotspot:       checkHotspot,
		keyAdapter:         keyAdapter,
		duplicateDetection: duplicateDetection,
		duplicateDB:        duplicateDB,
		dupDetectOpt:       dupDetectOpt,
		workerConcurrency:  workerConcurrency,
		ts:                 ts,
		totalKVSize:        totalKVSize,
		totalKVCount:       totalKVCount,
		importedKVSize:     atomic.NewInt64(0),
		importedKVCount:    atomic.NewInt64(0),
	}
}

func split[T any](in []T, groupNum int) [][]T {
	if len(in) == 0 {
		return nil
	}
	if groupNum <= 0 {
		groupNum = 1
	}
	ceil := (len(in) + groupNum - 1) / groupNum
	ret := make([][]T, 0, groupNum)
	l := len(in)
	for i := 0; i < l; i += ceil {
		if i+ceil > l {
			ret = append(ret, in[i:])
		} else {
			ret = append(ret, in[i:i+ceil])
		}
	}
	return ret
}

func (e *Engine) getAdjustedConcurrency() int {
	if e.checkHotspot {
		// estimate we will open at most 1000 files, so if e.dataFiles is small we can
		// try to concurrently process ranges.
		adjusted := maxCloudStorageConnections / len(e.dataFiles)
		return min(adjusted, 8)
	}
	adjusted := min(e.workerConcurrency, maxCloudStorageConnections/len(e.dataFiles))
	return max(adjusted, 1)
}

const concurrentReadThreshold = 4 * 1024 * 1024

func checkConcurrentFiles(ctx context.Context, storage storage.ExternalStorage, statsFiles []string, startKey, endKey []byte) ([]uint64, []uint64, error) {
	result := make([]uint64, len(statsFiles))
	startOffs, err := seekPropsOffsets(ctx, startKey, statsFiles, storage, false)
	if err != nil {
		return nil, nil, err
	}
	endOffs, err := seekPropsOffsets(ctx, endKey, statsFiles, storage, false)
	if err != nil {
		return nil, nil, err
	}
	for i := range statsFiles {
		result[i] = (endOffs[i] - startOffs[i]) / uint64(ConcurrentReaderBufferSizePerConc)
		if result[i] < 16 {
			result[i] = 1
		} else {
			logutil.Logger(ctx).Info("found hotspot file in checkConcurrentFiles",
				zap.String("filename", statsFiles[i]),
			)
		}
	}
	return result, startOffs, nil
}

func readOneFile(
	ctx context.Context,
	storage storage.ExternalStorage,
	dataFile string,
	startKey, endKey []byte,
	startOffset uint64,
	concurrency uint64,
	bufPool *membuf.Pool,
	output *memKVsAndBuffers,
) error {
	readAndSortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read_one_file")

	ts := time.Now()

	rd, err := newKVReader(ctx, dataFile, storage, startOffset, 64*1024)
	if err != nil {
		return err
	}
	defer rd.Close()
	if concurrency > 1 {
		rd.byteReader.enableConcurrentRead(
			storage,
			dataFile,
			int(concurrency)*2,
			ConcurrentReaderBufferSizePerConc,
			bufPool.NewBuffer(),
		)
		err = rd.byteReader.switchConcurrentMode(true)
		if err != nil {
			return err
		}
	}

	// this buffer is associated with data slices and will return to caller
	memBuf := bufPool.NewBuffer()
	keys := make([][]byte, 0, 1024)
	values := make([][]byte, 0, 1024)

	var prevKey []byte
	for {
		k, v, err := rd.nextKV()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if bytes.Compare(k, startKey) < 0 {
			continue
		}
		if bytes.Compare(k, endKey) >= 0 {
			break
		}
		if prevKey != nil && bytes.Compare(prevKey, k) >= 0 {
			log.FromContext(ctx).Error("kv is not in increasing order", zap.ByteString("prevKey", prevKey), zap.ByteString("key", k))
		}
		prevKey = slices.Clone(k)
		// TODO(lance6716): we are copying every KV from rd's buffer to memBuf, can we
		// directly read into memBuf?
		keys = append(keys, memBuf.AddBytes(k))
		values = append(values, memBuf.AddBytes(v))
	}
	readAndSortDurHist.Observe(time.Since(ts).Seconds())
	output.mu.Lock()
	output.keys = append(output.keys, keys...)
	output.values = append(output.values, values...)
	output.memKVBuffers = append(output.memKVBuffers, memBuf)
	output.mu.Unlock()
	return nil
}

func readAllData(
	ctx context.Context,
	storage storage.ExternalStorage,
	dataFiles, statsFiles []string,
	startKey, endKey []byte,
	bufPool *membuf.Pool,
	output *memKVsAndBuffers,
) error {
	logutil.Logger(ctx).Info("enter readAllData",
		zap.Int("data-file-count", len(dataFiles)),
		zap.Int("stat-file-count", len(statsFiles)),
		zap.Binary("start-key", startKey),
		zap.Binary("end-key", endKey),
	)

	concurrencys, startOffsets, err := checkConcurrentFiles(
		ctx,
		storage,
		statsFiles,
		startKey,
		endKey,
	)
	if err != nil {
		return err
	}
	var eg errgroup.Group
	for i := range dataFiles {
		i := i
		eg.Go(func() error {
			return readOneFile(
				ctx,
				storage,
				dataFiles[i],
				startKey,
				endKey,
				startOffsets[i],
				concurrencys[i],
				bufPool,
				output,
			)
		})
	}
	return eg.Wait()
}

func (e *Engine) loadBatchRegionData(ctx context.Context, startKey, endKey []byte, outCh chan<- common.DataAndRange) error {
	readAndSortRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("read_and_sort")
	readAndSortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read_and_sort")
	readRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("read")
	readDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read")
	sortRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("sort")
	sortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("sort")

	readStart := time.Now()
	err := readAllData(
		ctx,
		e.storage,
		e.dataFiles,
		e.statsFiles,
		startKey,
		endKey,
		e.bufPool,
		&e.memKVsAndBuffers,
	)
	if err != nil {
		return err
	}
	readSecond := time.Since(readStart).Seconds()
	readDurHist.Observe(readSecond)
	logutil.Logger(ctx).Info("reading external storage in loadBatchRegionData",
		zap.Duration("cost time", time.Since(readStart)))
	sortStart := time.Now()
	sorty.MaxGor = uint64(e.workerConcurrency * 2)
	sorty.Sort(len(e.memKVsAndBuffers.keys), func(i, k, r, s int) bool {
		if bytes.Compare(e.memKVsAndBuffers.keys[i], e.memKVsAndBuffers.keys[k]) < 0 { // strict comparator like < or >
			if r != s {
				e.memKVsAndBuffers.keys[r], e.memKVsAndBuffers.keys[s] = e.memKVsAndBuffers.keys[s], e.memKVsAndBuffers.keys[r]
				e.memKVsAndBuffers.values[r], e.memKVsAndBuffers.values[s] = e.memKVsAndBuffers.values[s], e.memKVsAndBuffers.values[r]
			}
			return true
		}
		return false
	})
	sortSecond := time.Since(sortStart).Seconds()
	sortDurHist.Observe(sortSecond)
	logutil.Logger(ctx).Info("sorting in loadBatchRegionData",
		zap.Duration("cost time", time.Since(sortStart)))

	readAndSortSecond := time.Since(readStart).Seconds()
	readAndSortDurHist.Observe(readAndSortSecond)

	size := 0
	for i := range e.memKVsAndBuffers.keys {
		size += len(e.memKVsAndBuffers.keys[i]) + len(e.memKVsAndBuffers.values[i])
	}
	readAndSortRateHist.Observe(float64(size) / 1024.0 / 1024.0 / readAndSortSecond)
	readRateHist.Observe(float64(size) / 1024.0 / 1024.0 / readSecond)
	sortRateHist.Observe(float64(size) / 1024.0 / 1024.0 / sortSecond)

	newBuf := make([]*membuf.Buffer, 0, len(e.memKVsAndBuffers.memKVBuffers))
	copy(newBuf, e.memKVsAndBuffers.memKVBuffers)
	data := e.buildIngestData(e.memKVsAndBuffers.keys, e.memKVsAndBuffers.values, newBuf)

	// release the reference of e.memKVsAndBuffers
	e.memKVsAndBuffers.keys = nil
	e.memKVsAndBuffers.values = nil
	e.memKVsAndBuffers.memKVBuffers = nil

	sendFn := func(dr common.DataAndRange) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case outCh <- dr:
		}
		return nil
	}
	return sendFn(common.DataAndRange{
		Data: data,
		Range: common.Range{
			Start: startKey,
			End:   endKey,
		},
	})
}

// LoadIngestData loads the data from the external storage to memory in [start,
// end) range, so local backend can ingest it. The used byte slice of ingest data
// are allocated from Engine.bufPool and must be released by
// MemoryIngestData.DecRef().
func (e *Engine) LoadIngestData(
	ctx context.Context,
	regionRanges []common.Range,
	outCh chan<- common.DataAndRange,
) error {
	regionBatchSize := 40
	for i := 0; i < len(regionRanges); i += regionBatchSize {
		err := e.loadBatchRegionData(ctx, regionRanges[i].Start, regionRanges[min(i+regionBatchSize, len(regionRanges))-1].End, outCh)
		if err != nil {
			return err
		}
	}
	return nil

	//concurrency := e.getAdjustedConcurrency()
	//rangeGroups := split(regionRanges, concurrency)
	//
	//logutil.Logger(ctx).Info("load ingest data",
	//	zap.Int("concurrency", concurrency),
	//	zap.Int("ranges", len(regionRanges)),
	//	zap.Int("range-groups", len(rangeGroups)),
	//	zap.Int("data-files", len(e.dataFiles)),
	//	zap.Bool("check-hotspot", e.checkHotspot),
	//)
	//e.mergerIterConcurrency = concurrency
	//eg, egCtx := errgroup.WithContext(ctx)
	//for _, ranges := range rangeGroups {
	//	ranges := ranges
	//	eg.Go(func() error {
	//		iter, err := e.createMergeIter(egCtx, ranges[0].Start)
	//		if err != nil {
	//			return errors.Trace(err)
	//		}
	//		defer iter.Close()
	//
	//		if !iter.Next() {
	//			return iter.Error()
	//		}
	//		for _, r := range ranges {
	//			err := e.loadIngestData(egCtx, iter, r.Start, r.End, outCh)
	//			if err != nil {
	//				return errors.Trace(err)
	//			}
	//		}
	//		return nil
	//	})
	//}
	//return eg.Wait()
}

func (e *Engine) buildIngestData(keys, values [][]byte, bufs []*membuf.Buffer) *MemoryIngestData {
	return &MemoryIngestData{
		keyAdapter:         e.keyAdapter,
		duplicateDetection: e.duplicateDetection,
		duplicateDB:        e.duplicateDB,
		dupDetectOpt:       e.dupDetectOpt,
		keys:               keys,
		values:             values,
		ts:                 e.ts,
		memBufs:            bufs,
		refCnt:             atomic.NewInt64(0),
		importedKVSize:     e.importedKVSize,
		importedKVCount:    e.importedKVCount,
	}
}

// LargeRegionSplitDataThreshold is exposed for test.
var LargeRegionSplitDataThreshold = int(config.SplitRegionSize)

//// loadIngestData loads the data from the external storage to memory in [start,
//// end) range, and if the range is large enough, it will return multiple data.
//// The input `iter` should be called Next() before calling this function.
//func (e *Engine) loadIngestData(
//	ctx context.Context,
//	iter *MergeKVIter,
//	start, end []byte,
//	outCh chan<- common.DataAndRange) error {
//	if bytes.Equal(start, end) {
//		return errors.Errorf("start key and end key must not be the same: %s",
//			hex.EncodeToString(start))
//	}
//
//	readRateHist := metrics.GlobalSortReadFromCloudStorageRate.WithLabelValues("read_and_sort")
//	readDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read_and_sort")
//	sendFn := func(dr common.DataAndRange) error {
//		select {
//		case <-ctx.Done():
//			return ctx.Err()
//		case outCh <- dr:
//		}
//		return nil
//	}
//
//	loadStartTs, batchStartTs := time.Now(), time.Now()
//	keys := make([][]byte, 0, 1024)
//	values := make([][]byte, 0, 1024)
//	memBuf := e.bufPool.NewBuffer()
//	cnt := 0
//	size := 0
//	curStart := start
//
//	// there should be a key that just exceeds the end key in last loadIngestData
//	// invocation.
//	k, v := iter.Key(), iter.Value()
//	if len(k) > 0 {
//		keys = append(keys, memBuf.AddBytes(k))
//		values = append(values, memBuf.AddBytes(v))
//		cnt++
//		size += len(k) + len(v)
//	}
//
//	for iter.Next() {
//		k, v = iter.Key(), iter.Value()
//		if bytes.Compare(k, start) < 0 {
//			continue
//		}
//		if bytes.Compare(k, end) >= 0 {
//			break
//		}
//		// as we keep KV data in memory, to avoid OOM, we only keep at most 1
//		// DataAndRange for each loadIngestData and regionJobWorker routine(channel
//		// is unbuffered).
//		if size > LargeRegionSplitDataThreshold {
//			readRateHist.Observe(float64(size) / 1024.0 / 1024.0 / time.Since(batchStartTs).Seconds())
//			readDurHist.Observe(time.Since(batchStartTs).Seconds())
//			curKey := slices.Clone(k)
//			if err := sendFn(common.DataAndRange{
//				Data:  e.buildIngestData(keys, values, memBuf),
//				Range: common.Range{Start: curStart, End: curKey},
//			}); err != nil {
//				return errors.Trace(err)
//			}
//			keys = make([][]byte, 0, 1024)
//			values = make([][]byte, 0, 1024)
//			size = 0
//			curStart = curKey
//			batchStartTs = time.Now()
//			memBuf = e.bufPool.NewBuffer()
//		}
//
//		keys = append(keys, memBuf.AddBytes(k))
//		values = append(values, memBuf.AddBytes(v))
//		cnt++
//		size += len(k) + len(v)
//	}
//	if iter.Error() != nil {
//		return errors.Trace(iter.Error())
//	}
//	if len(keys) > 0 {
//		readRateHist.Observe(float64(size) / 1024.0 / 1024.0 / time.Since(batchStartTs).Seconds())
//		readDurHist.Observe(time.Since(batchStartTs).Seconds())
//		if err := sendFn(common.DataAndRange{
//			Data:  e.buildIngestData(keys, values, memBuf),
//			Range: common.Range{Start: curStart, End: end},
//		}); err != nil {
//			return errors.Trace(err)
//		}
//	}
//	logutil.Logger(ctx).Info("load data from external storage",
//		zap.Duration("cost time", time.Since(loadStartTs)),
//		zap.Int("iterated count", cnt))
//	return nil
//}

func (e *Engine) createMergeIter(ctx context.Context, start kv.Key) (*MergeKVIter, error) {
	logger := logutil.Logger(ctx)

	var offsets []uint64
	if len(e.statsFiles) == 0 {
		offsets = make([]uint64, len(e.dataFiles))
		logger.Info("no stats files",
			zap.String("startKey", hex.EncodeToString(start)))
	} else {
		offs, err := seekPropsOffsets(ctx, start, e.statsFiles, e.storage, e.checkHotspot)
		if err != nil {
			return nil, errors.Trace(err)
		}
		offsets = offs
		logger.Debug("seek props offsets",
			zap.Uint64s("offsets", offsets),
			zap.String("startKey", hex.EncodeToString(start)),
			zap.Strings("dataFiles", e.dataFiles),
			zap.Strings("statsFiles", e.statsFiles))
	}
	iter, err := NewMergeKVIter(ctx, e.dataFiles, offsets, e.storage, 64*1024, e.checkHotspot, e.mergerIterConcurrency)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return iter, nil
}

// KVStatistics returns the total kv size and total kv count.
func (e *Engine) KVStatistics() (totalKVSize int64, totalKVCount int64) {
	return e.totalKVSize, e.totalKVCount
}

// ImportedStatistics returns the imported kv size and imported kv count.
func (e *Engine) ImportedStatistics() (importedSize int64, importedKVCount int64) {
	return e.importedKVSize.Load(), e.importedKVCount.Load()
}

// ID is the identifier of an engine.
func (e *Engine) ID() string {
	return "external"
}

// GetKeyRange implements common.Engine.
func (e *Engine) GetKeyRange() (startKey []byte, endKey []byte, err error) {
	return e.startKey, e.endKey, nil
}

// SplitRanges split the ranges by split keys provided by external engine.
func (e *Engine) SplitRanges(
	startKey, endKey []byte,
	_, _ int64,
	_ log.Logger,
) ([]common.Range, error) {
	splitKeys := e.splitKeys
	ranges := make([]common.Range, 0, len(splitKeys)+1)
	ranges = append(ranges, common.Range{Start: startKey})
	for i := 0; i < len(splitKeys); i++ {
		ranges[len(ranges)-1].End = splitKeys[i]
		var endK []byte
		if i < len(splitKeys)-1 {
			endK = splitKeys[i+1]
		}
		ranges = append(ranges, common.Range{Start: splitKeys[i], End: endK})
	}
	ranges[len(ranges)-1].End = endKey
	return ranges, nil
}

// Close implements common.Engine.
func (e *Engine) Close() error {
	if e.bufPool != nil {
		e.bufPool.Destroy()
		e.bufPool = nil
	}
	return nil
}

// Reset resets the memory buffer pool.
func (e *Engine) Reset() error {
	if e.bufPool != nil {
		e.bufPool.Destroy()
		e.bufPool = membuf.NewPool()
	}
	return nil
}

// MemoryIngestData is the in-memory implementation of IngestData.
type MemoryIngestData struct {
	keyAdapter         common.KeyAdapter
	duplicateDetection bool
	duplicateDB        *pebble.DB
	dupDetectOpt       common.DupDetectOpt

	keys   [][]byte
	values [][]byte
	ts     uint64

	memBufs         []*membuf.Buffer
	refCnt          *atomic.Int64
	importedKVSize  *atomic.Int64
	importedKVCount *atomic.Int64
}

var _ common.IngestData = (*MemoryIngestData)(nil)

func (m *MemoryIngestData) firstAndLastKeyIndex(lowerBound, upperBound []byte) (int, int) {
	firstKeyIdx := 0
	if len(lowerBound) > 0 {
		lowerBound = m.keyAdapter.Encode(nil, lowerBound, common.MinRowID)
		firstKeyIdx = sort.Search(len(m.keys), func(i int) bool {
			return bytes.Compare(lowerBound, m.keys[i]) <= 0
		})
		if firstKeyIdx == len(m.keys) {
			return -1, -1
		}
	}

	lastKeyIdx := len(m.keys) - 1
	if len(upperBound) > 0 {
		upperBound = m.keyAdapter.Encode(nil, upperBound, common.MinRowID)
		i := sort.Search(len(m.keys), func(i int) bool {
			reverseIdx := len(m.keys) - 1 - i
			return bytes.Compare(upperBound, m.keys[reverseIdx]) > 0
		})
		if i == len(m.keys) {
			// should not happen
			return -1, -1
		}
		lastKeyIdx = len(m.keys) - 1 - i
	}
	return firstKeyIdx, lastKeyIdx
}

// GetFirstAndLastKey implements IngestData.GetFirstAndLastKey.
func (m *MemoryIngestData) GetFirstAndLastKey(lowerBound, upperBound []byte) ([]byte, []byte, error) {
	firstKeyIdx, lastKeyIdx := m.firstAndLastKeyIndex(lowerBound, upperBound)
	if firstKeyIdx < 0 || firstKeyIdx > lastKeyIdx {
		return nil, nil, nil
	}
	firstKey, err := m.keyAdapter.Decode(nil, m.keys[firstKeyIdx])
	if err != nil {
		return nil, nil, err
	}
	lastKey, err := m.keyAdapter.Decode(nil, m.keys[lastKeyIdx])
	if err != nil {
		return nil, nil, err
	}
	return firstKey, lastKey, nil
}

type memoryDataIter struct {
	keys   [][]byte
	values [][]byte

	firstKeyIdx int
	lastKeyIdx  int
	curIdx      int
}

// First implements ForwardIter.
func (m *memoryDataIter) First() bool {
	if m.firstKeyIdx < 0 {
		return false
	}
	m.curIdx = m.firstKeyIdx
	return true
}

// Valid implements ForwardIter.
func (m *memoryDataIter) Valid() bool {
	return m.firstKeyIdx <= m.curIdx && m.curIdx <= m.lastKeyIdx
}

// Next implements ForwardIter.
func (m *memoryDataIter) Next() bool {
	m.curIdx++
	return m.Valid()
}

// Key implements ForwardIter.
func (m *memoryDataIter) Key() []byte {
	return m.keys[m.curIdx]
}

// Value implements ForwardIter.
func (m *memoryDataIter) Value() []byte {
	return m.values[m.curIdx]
}

// Close implements ForwardIter.
func (m *memoryDataIter) Close() error {
	return nil
}

// Error implements ForwardIter.
func (m *memoryDataIter) Error() error {
	return nil
}

type memoryDataDupDetectIter struct {
	iter           *memoryDataIter
	dupDetector    *common.DupDetector
	err            error
	curKey, curVal []byte
}

// First implements ForwardIter.
func (m *memoryDataDupDetectIter) First() bool {
	if m.err != nil || !m.iter.First() {
		return false
	}
	m.curKey, m.curVal, m.err = m.dupDetector.Init(m.iter)
	return m.Valid()
}

// Valid implements ForwardIter.
func (m *memoryDataDupDetectIter) Valid() bool {
	return m.err == nil && m.iter.Valid()
}

// Next implements ForwardIter.
func (m *memoryDataDupDetectIter) Next() bool {
	if m.err != nil {
		return false
	}
	key, val, ok, err := m.dupDetector.Next(m.iter)
	if err != nil {
		m.err = err
		return false
	}
	if !ok {
		return false
	}
	m.curKey, m.curVal = key, val
	return true
}

// Key implements ForwardIter.
func (m *memoryDataDupDetectIter) Key() []byte {
	return m.curKey
}

// Value implements ForwardIter.
func (m *memoryDataDupDetectIter) Value() []byte {
	return m.curVal
}

// Close implements ForwardIter.
func (m *memoryDataDupDetectIter) Close() error {
	return m.dupDetector.Close()
}

// Error implements ForwardIter.
func (m *memoryDataDupDetectIter) Error() error {
	return m.err
}

// NewIter implements IngestData.NewIter.
func (m *MemoryIngestData) NewIter(ctx context.Context, lowerBound, upperBound []byte) common.ForwardIter {
	firstKeyIdx, lastKeyIdx := m.firstAndLastKeyIndex(lowerBound, upperBound)
	iter := &memoryDataIter{
		keys:        m.keys,
		values:      m.values,
		firstKeyIdx: firstKeyIdx,
		lastKeyIdx:  lastKeyIdx,
	}
	if !m.duplicateDetection {
		return iter
	}
	logger := log.FromContext(ctx)
	detector := common.NewDupDetector(m.keyAdapter, m.duplicateDB.NewBatch(), logger, m.dupDetectOpt)
	return &memoryDataDupDetectIter{
		iter:        iter,
		dupDetector: detector,
	}
}

// GetTS implements IngestData.GetTS.
func (m *MemoryIngestData) GetTS() uint64 {
	return m.ts
}

// IncRef implements IngestData.IncRef.
func (m *MemoryIngestData) IncRef() {
	m.refCnt.Inc()
}

// DecRef implements IngestData.DecRef.
func (m *MemoryIngestData) DecRef() {
	if m.refCnt.Dec() == 0 {
		for _, b := range m.memBufs {
			b.Destroy()
		}
	}
}

// Finish implements IngestData.Finish.
func (m *MemoryIngestData) Finish(totalBytes, totalCount int64) {
	m.importedKVSize.Add(totalBytes)
	m.importedKVCount.Add(totalCount)

}
