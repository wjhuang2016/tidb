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
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/docker/go-units"
	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

var (
	multiFileStatNum = 500

	// MergeSortOverlapThreshold is the threshold of overlap between sorted kv files.
	// if the overlap ratio is greater than this threshold, we will merge the files.
	MergeSortOverlapThreshold int64 = 1000
	// MergeSortFileCountStep is the step of file count when we split the sorted kv files.
	MergeSortFileCountStep = 1000
)

const (
	// DefaultMemSizeLimit is the default memory size limit for writer.
	DefaultMemSizeLimit = 256 * size.MB
)

// rangePropertiesCollector collects range properties for each range. The zero
// value of rangePropertiesCollector is not ready to use, should call reset()
// first.
type rangePropertiesCollector struct {
	props        []*rangeProperty
	currProp     *rangeProperty
	propSizeDist uint64
	propKeysDist uint64
}

func (rc *rangePropertiesCollector) reset() {
	rc.props = rc.props[:0]
	rc.currProp = &rangeProperty{}
}

// encode encodes rc.props to a byte slice.
func (rc *rangePropertiesCollector) encode() []byte {
	b := make([]byte, 0, 1024)
	return encodeMultiProps(b, rc.props)
}

// WriterSummary is the summary of a writer.
type WriterSummary struct {
	WriterID string
	Seq      int
	// Min and Max are the min and max key written by this writer, both are
	// inclusive, i.e. [Min, Max].
	// will be empty if no key is written.
	Min                tidbkv.Key
	Max                tidbkv.Key
	TotalSize          uint64
	MultipleFilesStats []MultipleFilesStat
}

// OnCloseFunc is the callback function when a writer is closed.
type OnCloseFunc func(summary *WriterSummary)

// dummyOnCloseFunc is a dummy OnCloseFunc.
func dummyOnCloseFunc(*WriterSummary) {}

// WriterBuilder builds a new Writer.
type WriterBuilder struct {
	memSizeLimit    uint64
	writeBatchCount uint64
	propSizeDist    uint64
	propKeysDist    uint64
	onClose         OnCloseFunc
	keyDupeEncoding bool
	// This mutex is used to make sure the writer is flushed mutually exclusively in a TiDB server.
	mu *sync.Mutex

	bufferPool *membuf.Pool
}

// NewWriterBuilder creates a WriterBuilder.
func NewWriterBuilder() *WriterBuilder {
	return &WriterBuilder{
		memSizeLimit:    DefaultMemSizeLimit,
		writeBatchCount: 8 * 1024,
		propSizeDist:    1 * size.MB,
		propKeysDist:    8 * 1024,
		onClose:         dummyOnCloseFunc,
	}
}

// SetMemorySizeLimit sets the memory size limit of the writer. When accumulated
// data size exceeds this limit, the writer will flush data as a file to external
// storage.
func (b *WriterBuilder) SetMemorySizeLimit(size uint64) *WriterBuilder {
	b.memSizeLimit = size
	return b
}

// SetWriterBatchCount sets the batch count of the writer.
func (b *WriterBuilder) SetWriterBatchCount(count uint64) *WriterBuilder {
	b.writeBatchCount = count
	return b
}

// SetPropSizeDistance sets the distance of range size for each property.
func (b *WriterBuilder) SetPropSizeDistance(dist uint64) *WriterBuilder {
	b.propSizeDist = dist
	return b
}

// SetPropKeysDistance sets the distance of range keys for each property.
func (b *WriterBuilder) SetPropKeysDistance(dist uint64) *WriterBuilder {
	b.propKeysDist = dist
	return b
}

// SetOnCloseFunc sets the callback function when a writer is closed.
func (b *WriterBuilder) SetOnCloseFunc(onClose OnCloseFunc) *WriterBuilder {
	if onClose == nil {
		onClose = dummyOnCloseFunc
	}
	b.onClose = onClose
	return b
}

// SetBufferPool sets the buffer pool of the writer.
func (b *WriterBuilder) SetBufferPool(bufferPool *membuf.Pool) *WriterBuilder {
	b.bufferPool = bufferPool
	return b
}

// SetKeyDuplicationEncoding sets if the writer can distinguish duplicate key.
func (b *WriterBuilder) SetKeyDuplicationEncoding(val bool) *WriterBuilder {
	b.keyDupeEncoding = val
	return b
}

// SetMutex sets the mutex of the writer.
func (b *WriterBuilder) SetMutex(mu *sync.Mutex) *WriterBuilder {
	b.mu = mu
	return b
}

// Build builds a new Writer. The files writer will create are under the prefix
// of "{prefix}/{writerID}".
func (b *WriterBuilder) Build(
	store storage.ExternalStorage,
	prefix string,
	writerID string,
) *Writer {
	bp := b.bufferPool
	if bp == nil {
		bp = membuf.NewPool()
	}
	filenamePrefix := filepath.Join(prefix, writerID)
	keyAdapter := common.KeyAdapter(common.NoopKeyAdapter{})
	if b.keyDupeEncoding {
		keyAdapter = common.DupDetectKeyAdapter{}
	}
	ret := &Writer{
		rc: &rangePropertiesCollector{
			props:        make([]*rangeProperty, 0, 1024),
			currProp:     &rangeProperty{},
			propSizeDist: b.propSizeDist,
			propKeysDist: b.propKeysDist,
		},
		memSizeLimit: b.memSizeLimit,
		store:        store,
		kvBuffer:     newKVBuf(b.memSizeLimit),
		//writeBatch:     make([]common.KvPair, 0, b.writeBatchCount),
		currentSeq:     0,
		filenamePrefix: filenamePrefix,
		keyAdapter:     keyAdapter,
		writerID:       writerID,
		kvStore:        nil,
		onClose:        b.onClose,
		closed:         false,
		multiFileStats: make([]MultipleFilesStat, 1),
		fileMinKeys:    make([]tidbkv.Key, 0, multiFileStatNum),
		fileMaxKeys:    make([]tidbkv.Key, 0, multiFileStatNum),
		shareMu:        b.mu,
	}
	ret.multiFileStats[0].Filenames = make([][2]string, 0, multiFileStatNum)
	return ret
}

// MultipleFilesStat is the statistic information of multiple files (currently
// every 500 files). It is used to estimate the data overlapping, and per-file
// statistic information maybe too big to loaded into memory.
type MultipleFilesStat struct {
	MinKey            tidbkv.Key  `json:"min-key"`
	MaxKey            tidbkv.Key  `json:"max-key"`
	Filenames         [][2]string `json:"filenames"` // [dataFile, statFile]
	MaxOverlappingNum int64       `json:"max-overlapping-num"`
}

func (m *MultipleFilesStat) build(startKeys, endKeys []tidbkv.Key) {
	if len(startKeys) == 0 {
		return
	}
	m.MinKey = startKeys[0]
	m.MaxKey = endKeys[0]
	for i := 1; i < len(startKeys); i++ {
		if m.MinKey.Cmp(startKeys[i]) > 0 {
			m.MinKey = startKeys[i]
		}
		if m.MaxKey.Cmp(endKeys[i]) < 0 {
			m.MaxKey = endKeys[i]
		}
	}

	points := make([]Endpoint, 0, len(startKeys)*2)
	for _, k := range startKeys {
		points = append(points, Endpoint{Key: k, Tp: InclusiveStart, Weight: 1})
	}
	for _, k := range endKeys {
		points = append(points, Endpoint{Key: k, Tp: InclusiveEnd, Weight: 1})
	}
	m.MaxOverlappingNum = GetMaxOverlapping(points)
}

// GetMaxOverlappingTotal assume the most overlapping case from given stats and
// returns the overlapping level.
func GetMaxOverlappingTotal(stats []MultipleFilesStat) int64 {
	points := make([]Endpoint, 0, len(stats)*2)
	for _, stat := range stats {
		points = append(points, Endpoint{Key: stat.MinKey, Tp: InclusiveStart, Weight: stat.MaxOverlappingNum})
	}
	for _, stat := range stats {
		points = append(points, Endpoint{Key: stat.MaxKey, Tp: InclusiveEnd, Weight: stat.MaxOverlappingNum})
	}

	return GetMaxOverlapping(points)
}

type kvPos struct {
	bufIdx int
	offset int
	length int
}

// Writer is used to write data into external storage.
type Writer struct {
	store          storage.ExternalStorage
	writerID       string
	currentSeq     int
	filenamePrefix string
	keyAdapter     common.KeyAdapter

	kvStore *KeyValueStore
	rc      *rangePropertiesCollector

	memSizeLimit uint64

	kvBuffer *kvBuf
	byteBufs [][]byte
	kvPoss   []kvPos
	kvSize   int64

	onClose OnCloseFunc
	closed  bool

	// Statistic information per batch.
	batchSize  uint64
	memorySize uint64

	// Statistic information per 500 batches.
	multiFileStats []MultipleFilesStat
	fileMinKeys    []tidbkv.Key
	fileMaxKeys    []tidbkv.Key

	// Statistic information per writer.
	minKey    tidbkv.Key
	maxKey    tidbkv.Key
	totalSize uint64
	// This mutex is used to make sure the writer is flushed mutually exclusively in a TiDB server.
	shareMu *sync.Mutex
}

// WriteRow implements ingest.Writer.
func (w *Writer) WriteRow(ctx context.Context, idxKey, idxVal []byte, handle tidbkv.Handle) error {
	keyAdapter := w.keyAdapter

	var rowID []byte
	if handle != nil {
		rowID = handle.Encoded()
	}
	encodedKeyLen := keyAdapter.EncodedLen(idxKey, rowID)
	length := encodedKeyLen + len(idxVal) + 8*2
	base, buf, off, allocated := w.kvBuffer.Alloc(length)
	if !allocated {
		if err := w.flushKVs(ctx, false); err != nil {
			return err
		}
		base, buf, off, _ = w.kvBuffer.Alloc(length)
	}
	binary.BigEndian.AppendUint64(buf[:0], uint64(encodedKeyLen))
	keyAdapter.Encode(buf[8:], idxKey, rowID)
	binary.BigEndian.AppendUint64(buf[8+encodedKeyLen:], uint64(len(idxVal)))
	copy(buf[8*2+encodedKeyLen:], idxVal)

	if len(w.byteBufs) == 0 || &(w.byteBufs[len(w.byteBufs)-1]) != &base {
		w.byteBufs = append(w.byteBufs, base)
	}
	w.kvPoss = append(w.kvPoss, kvPos{bufIdx: len(w.byteBufs) - 1, offset: off, length: length})
	w.kvSize += int64(encodedKeyLen + len(idxVal))
	w.batchSize += uint64(length)
	w.memorySize += uint64(length) + uint64(unsafe.Sizeof(kvPos{}))
	return nil
}

// LockForWrite implements ingest.Writer.
// Since flushKVs is thread-safe in external storage writer,
// this is implemented as noop.
func (w *Writer) LockForWrite() func() {
	return func() {}
}

// Close closes the writer.
func (w *Writer) Close(ctx context.Context) error {
	if w.closed {
		return errors.Errorf("writer %s has been closed", w.writerID)
	}
	w.closed = true
	defer w.kvBuffer.destroy()
	err := w.flushKVs(ctx, true)
	if err != nil {
		return err
	}
	// remove the trailing empty MultipleFilesStat
	w.multiFileStats = w.multiFileStats[:len(w.multiFileStats)-1]

	logutil.Logger(ctx).Info("close writer",
		zap.String("writerID", w.writerID),
		zap.String("minKey", hex.EncodeToString(w.minKey)),
		zap.String("maxKey", hex.EncodeToString(w.maxKey)))

	w.byteBufs = nil
	w.kvPoss = nil

	w.onClose(&WriterSummary{
		WriterID:           w.writerID,
		Seq:                w.currentSeq,
		Min:                w.minKey,
		Max:                w.maxKey,
		TotalSize:          w.totalSize,
		MultipleFilesStats: w.multiFileStats,
	})
	return nil
}

func (w *Writer) recordMinMax(newMin, newMax tidbkv.Key, size uint64) {
	if len(w.minKey) == 0 || newMin.Cmp(w.minKey) < 0 {
		w.minKey = newMin.Clone()
	}
	if len(w.maxKey) == 0 || newMax.Cmp(w.maxKey) > 0 {
		w.maxKey = newMax.Clone()
	}
	w.totalSize += size
}

func (w *Writer) flushKVs(ctx context.Context, fromClose bool) (err error) {
	if len(w.kvPoss) == 0 {
		return nil
	}
	if w.shareMu != nil {
		w.shareMu.Lock()
		defer w.shareMu.Unlock()
	}

	logger := logutil.Logger(ctx)
	dataFile, statFile, dataWriter, statWriter, err := w.createStorageWriter(ctx)
	if err != nil {
		return err
	}

	var (
		savedBytes                  uint64
		statSize                    int
		sortDuration, writeDuration time.Duration
		writeStartTime              time.Time
	)
	savedBytes = w.batchSize
	startTs := time.Now()

	getSpeed := func(n uint64, dur float64, isBytes bool) string {
		if dur == 0 {
			return "-"
		}
		if isBytes {
			return units.BytesSize(float64(n) / dur)
		}
		return units.HumanSize(float64(n) / dur)
	}
	kvCnt := len(w.kvPoss)
	defer func() {
		w.currentSeq++
		err1, err2 := dataWriter.Close(ctx), statWriter.Close(ctx)
		if err != nil {
			return
		}
		if err1 != nil {
			logger.Error("close data writer failed", zap.Error(err1))
			err = err1
			return
		}
		if err2 != nil {
			logger.Error("close stat writer failed", zap.Error(err2))
			err = err2
			return
		}
		writeDuration = time.Since(writeStartTime)
		logger.Info("flush kv",
			zap.Uint64("bytes", savedBytes),
			zap.Int("kv-cnt", kvCnt),
			zap.Int("stat-size", statSize),
			zap.Duration("sort-time", sortDuration),
			zap.Duration("write-time", writeDuration),
			zap.String("sort-speed(kv/s)", getSpeed(uint64(kvCnt), sortDuration.Seconds(), false)),
			zap.String("write-speed(bytes/s)", getSpeed(savedBytes, writeDuration.Seconds(), true)),
			zap.String("writer-id", w.writerID),
		)
		metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("write").Observe(writeDuration.Seconds())
		metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("write").Observe(float64(savedBytes) / 1024.0 / 1024.0 / writeDuration.Seconds())
		metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("sort_and_write").Observe(time.Since(startTs).Seconds())
		metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("sort_and_write").Observe(float64(savedBytes) / 1024.0 / 1024.0 / time.Since(startTs).Seconds())
	}()

	sortStart := time.Now()
	if w.shareMu != nil {
		sorty.MaxGor = min(8, uint64(variable.GetDDLReorgWorkerCounter()))
		sorty.Sort(len(w.kvPoss), func(i, j, r, s int) bool {
			posi, posj := w.kvPoss[i], w.kvPoss[j]
			if bytes.Compare(w.byteBufs[posi.bufIdx][posi.offset+8:], w.byteBufs[posj.bufIdx][posj.offset+8:]) < 0 {
				if r != s {
					w.kvPoss[r], w.kvPoss[s] = w.kvPoss[s], w.kvPoss[r]
				}
				return true
			}
			return false
		})
	} else {
		slices.SortFunc(w.kvPoss, func(i, j kvPos) int {
			bufI := w.byteBufs[i.bufIdx]
			bufJ := w.byteBufs[j.bufIdx]
			return bytes.Compare(bufI[i.offset+8:], bufJ[j.offset+8:])
		})
	}
	sortDuration = time.Since(sortStart)

	writeStartTime = time.Now()
	metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("sort").Observe(sortDuration.Seconds())
	metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("sort").Observe(float64(savedBytes) / 1024.0 / 1024.0 / sortDuration.Seconds())
	w.kvStore, err = NewKeyValueStore(ctx, dataWriter, w.rc)
	if err != nil {
		return err
	}

	for _, pair := range w.kvPoss {
		err = w.kvStore.AddKeyValue(w.byteBufs[pair.bufIdx][pair.offset : pair.offset+pair.length])
		if err != nil {
			return err
		}
	}

	w.kvStore.Close()
	encodedStat := w.rc.encode()
	statSize = len(encodedStat)
	_, err = statWriter.Write(ctx, encodedStat)
	if err != nil {
		return err
	}

	minKey, maxKey := w.getKey(0), w.getKey(len(w.kvPoss)-1)
	w.recordMinMax(minKey, maxKey, uint64(w.kvSize))

	// maintain 500-batch statistics

	l := len(w.multiFileStats)
	w.multiFileStats[l-1].Filenames = append(w.multiFileStats[l-1].Filenames,
		[2]string{dataFile, statFile},
	)
	w.fileMinKeys = append(w.fileMinKeys, tidbkv.Key(minKey).Clone())
	w.fileMaxKeys = append(w.fileMaxKeys, tidbkv.Key(maxKey).Clone())
	if fromClose || len(w.multiFileStats[l-1].Filenames) == multiFileStatNum {
		w.multiFileStats[l-1].build(w.fileMinKeys, w.fileMaxKeys)
		w.multiFileStats = append(w.multiFileStats, MultipleFilesStat{
			Filenames: make([][2]string, 0, multiFileStatNum),
		})
		w.fileMinKeys = w.fileMinKeys[:0]
		w.fileMaxKeys = w.fileMaxKeys[:0]
	}

	w.byteBufs = w.byteBufs[:0]
	w.kvPoss = w.kvPoss[:0]
	w.kvSize = 0
	w.kvBuffer.reset()
	w.rc.reset()
	w.batchSize = 0
	w.memorySize = 0
	return nil
}

func (w *Writer) getKey(idx int) []byte {
	byteBuf := w.byteBufs[w.kvPoss[idx].bufIdx]
	keyLen := binary.BigEndian.Uint64(byteBuf[w.kvPoss[idx].offset : w.kvPoss[idx].offset+8])
	return byteBuf[8 : 8+keyLen]
}

func (w *Writer) createStorageWriter(ctx context.Context) (
	dataFile, statFile string,
	data, stats storage.ExternalFileWriter,
	err error,
) {
	dataPath := filepath.Join(w.filenamePrefix, strconv.Itoa(w.currentSeq))
	dataWriter, err := w.store.Create(ctx, dataPath, &storage.WriterOption{Concurrency: 20})
	if err != nil {
		return "", "", nil, nil, err
	}
	statPath := filepath.Join(w.filenamePrefix+statSuffix, strconv.Itoa(w.currentSeq))
	statsWriter, err := w.store.Create(ctx, statPath, &storage.WriterOption{Concurrency: 20})
	if err != nil {
		_ = dataWriter.Close(ctx)
		return "", "", nil, nil, err
	}
	return dataPath, statPath, dataWriter, statsWriter, nil
}

// EngineWriter implements backend.EngineWriter interface.
type EngineWriter struct {
	w *Writer
}

// NewEngineWriter creates a new EngineWriter.
func NewEngineWriter(w *Writer) *EngineWriter {
	return &EngineWriter{w: w}
}

// AppendRows implements backend.EngineWriter interface.
func (e *EngineWriter) AppendRows(ctx context.Context, _ []string, rows encode.Rows) error {
	kvs := kv.Rows2KvPairs(rows)
	if len(kvs) == 0 {
		return nil
	}
	for _, item := range kvs {
		err := e.w.WriteRow(ctx, item.Key, item.Val, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// IsSynced implements backend.EngineWriter interface.
func (e *EngineWriter) IsSynced() bool {
	// only used when saving checkpoint
	return true
}

// Close implements backend.EngineWriter interface.
func (e *EngineWriter) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	return nil, e.w.Close(ctx)
}
