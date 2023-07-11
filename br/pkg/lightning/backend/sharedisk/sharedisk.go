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

package sharedisk

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/keyspace"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var ReadByteForTest atomic.Uint64
var ReadTimeForTest atomic.Uint64
var ReadIOCnt atomic.Uint64

type rangeOffsets struct {
	Size uint64
	Keys uint64
}

type RangeProperty struct {
	Key    []byte
	offset uint64
	rangeOffsets
}

// RangePropertiesCollector collects range properties for each range.
type RangePropertiesCollector struct {
	props               []*RangeProperty
	currProp            *RangeProperty
	lastOffsets         rangeOffsets
	lastKey             []byte
	currentOffsets      rangeOffsets
	propSizeIdxDistance uint64
	propKeysIdxDistance uint64
}

func (rc *RangePropertiesCollector) reset() {
	rc.props = rc.props[:0]
	rc.currProp = &RangeProperty{
		rangeOffsets: rangeOffsets{},
	}
	rc.lastOffsets = rangeOffsets{}
	rc.lastKey = nil
	rc.currentOffsets = rangeOffsets{}
}

func (rc *RangePropertiesCollector) Encode() []byte {
	b := make([]byte, 0, 1024)
	idx := 0
	for _, p := range rc.props {
		// Size.
		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(28+len(p.Key)))
		idx += 4

		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(len(p.Key)))
		idx += 4
		b = append(b, p.Key...)
		idx += len(p.Key)

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.Size)
		idx += 8

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.Keys)
		idx += 8

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.offset)
		idx += 8
	}
	return b
}

func Decode2RangeProperty(data []byte) (*RangeProperty, error) {
	rp := &RangeProperty{}
	keyLen := binary.BigEndian.Uint32(data[0:4])
	rp.Key = data[4 : 4+keyLen]
	rp.Size = binary.BigEndian.Uint64(data[4+keyLen : 12+keyLen])
	rp.Keys = binary.BigEndian.Uint64(data[12+keyLen : 20+keyLen])
	rp.offset = binary.BigEndian.Uint64(data[20+keyLen : 28+keyLen])
	return rp, nil
}

func NewEngine(sizeDist, keyDist uint64) *Engine {
	return &Engine{
		rc: &RangePropertiesCollector{
			// TODO(tangenta): decide the preserved size of props.
			props:               nil,
			currProp:            &RangeProperty{},
			propSizeIdxDistance: sizeDist,
			propKeysIdxDistance: keyDist,
		},
	}
}

type Engine struct {
	rc *RangePropertiesCollector
}

var WriteBatchSize = 8 * 1024
var MemQuota = 8 * 1024

type OnCloseFunc func(writerID int, seq int, min tidbkv.Key, max tidbkv.Key)

func NewWriter(ctx context.Context, externalStorage storage.ExternalStorage,
	prefix string, writerID int, onClose OnCloseFunc) *Writer {
	// TODO(tangenta): make it configurable.
	engine := NewEngine(2048, 256)
	pool := membuf.NewPool()
	filePrefix := filepath.Join(prefix, strconv.Itoa(writerID))
	return &Writer{
		ctx:               ctx,
		engine:            engine,
		memtableSizeLimit: MemQuota,
		keyAdapter:        &local.NoopKeyAdapter{},
		exStorage:         externalStorage,
		memBufPool:        pool,
		kvBuffer:          pool.NewBuffer(),
		writeBatch:        make([]common.KvPair, 0, WriteBatchSize),
		currentSeq:        0,
		tikvCodec:         keyspace.CodecV1,
		filenamePrefix:    filePrefix,
		writerID:          writerID,
		kvStore:           nil,
		onClose:           onClose,
		closed:            false,
	}
}

// Writer is used to write data into external storage.
type Writer struct {
	ctx context.Context
	sync.Mutex
	engine            *Engine
	memtableSizeLimit int
	keyAdapter        local.KeyAdapter
	exStorage         storage.ExternalStorage

	// bytes buffer for writeBatch
	memBufPool *membuf.Pool
	kvBuffer   *membuf.Buffer
	writeBatch []common.KvPair
	batchSize  int

	currentSeq int
	onClose    OnCloseFunc
	closed     bool

	tikvCodec      tikv.Codec
	filenamePrefix string
	writerID       int
	minKey         tidbkv.Key
	maxKey         tidbkv.Key

	kvStore *KeyValueStore
}

type DataFileReader struct {
	ctx context.Context

	name      string
	exStorage storage.ExternalStorage

	fileMaxOffset    uint64
	readBuffer       []byte
	bufferMaxOffset  uint64
	currBufferOffset uint64
	currFileOffset   uint64
	kvBoundaryOffset uint64
	init             bool
}

func (dr *DataFileReader) getMoreDataFromStorage() (bool, error) {
	//logutil.BgLogger().Info("getMoreDataFromStorage")

	start := dr.currFileOffset
	end := dr.currFileOffset + dr.kvBoundaryOffset
	if end > dr.fileMaxOffset {
		end = dr.fileMaxOffset
	}
	if start == end {
		return false, nil
	}
	// Move the unread data to the beginning of the buffer.
	unreadLen := len(dr.readBuffer) - int(dr.kvBoundaryOffset)
	for i := 0; i < unreadLen; i++ {
		dr.readBuffer[i] = dr.readBuffer[int(dr.kvBoundaryOffset)+i]
	}
	startTime := time.Now()
	nBytes, err := storage.ReadPartialFileDirectly(dr.ctx, dr.exStorage, dr.name, start, end, dr.readBuffer[unreadLen:])
	elapsed := time.Since(startTime).Microseconds()
	ReadByteForTest.Add(nBytes)
	ReadTimeForTest.Add(uint64(elapsed))
	ReadIOCnt.Add(1)
	//logutil.BgLogger().Info("read data", zap.Any("name", dr.name), zap.Any("bytes cnt", maxOffset), zap.Any("elasp", elasp))
	if err != nil {
		return false, err
	}
	dr.bufferMaxOffset = nBytes + uint64(unreadLen)
	dr.currBufferOffset = 0
	dr.currFileOffset = dr.currFileOffset + nBytes
	return true, nil
}

func (dr *DataFileReader) GetNextKV() ([]byte, []byte, error) {
	if !dr.init {
		maxOffset, err := storage.GetFileMaxOffset(dr.ctx, dr.exStorage, dr.name)
		if err != nil {
			return nil, nil, err
		}
		dr.fileMaxOffset = maxOffset
		dr.kvBoundaryOffset = uint64(len(dr.readBuffer))
		get, err := dr.getMoreDataFromStorage()
		if err != nil {
			return nil, nil, err
		}
		if !get {
			return nil, nil, nil
		}

		dr.init = true
	}
	dr.kvBoundaryOffset = dr.currBufferOffset
	if dr.bufferMaxOffset < dr.currBufferOffset+8 {
		get, err := dr.getMoreDataFromStorage()
		if err != nil {
			return nil, nil, err
		}
		if !get {
			return nil, nil, nil
		}
		return dr.GetNextKV()
	}
	keyLen := binary.BigEndian.Uint64(dr.readBuffer[dr.currBufferOffset:])
	dr.currBufferOffset += 8

	if dr.bufferMaxOffset < dr.currBufferOffset+keyLen {
		_, err := dr.getMoreDataFromStorage()
		if err != nil {
			return nil, nil, err
		}
		return dr.GetNextKV()
	}
	key := dr.readBuffer[dr.currBufferOffset : dr.currBufferOffset+keyLen]
	dr.currBufferOffset += keyLen

	if dr.bufferMaxOffset < dr.currBufferOffset+8 {
		_, err := dr.getMoreDataFromStorage()
		if err != nil {
			return nil, nil, err
		}
		return dr.GetNextKV()
	}
	valLen := binary.BigEndian.Uint64(dr.readBuffer[dr.currBufferOffset:])
	dr.currBufferOffset += 8

	if dr.bufferMaxOffset < dr.currBufferOffset+valLen {
		_, err := dr.getMoreDataFromStorage()
		if err != nil {
			return nil, nil, err
		}
		return dr.GetNextKV()
	}
	val := dr.readBuffer[dr.currBufferOffset : dr.currBufferOffset+valLen]
	dr.currBufferOffset += valLen
	return key, val, nil
}

type statFileReader struct {
	ctx context.Context

	name      string
	exStorage storage.ExternalStorage

	fileMaxOffset    uint64
	bufferMaxOffset  uint64
	readBuffer       []byte
	currBufferOffset uint64
	currFileOffset   uint64
	kvBoundaryOffset uint64
	init             bool
}

func (sr *statFileReader) getMoreDataFromStorage() (bool, error) {
	//logutil.BgLogger().Info("getMoreDataFromStorage")

	start := sr.currFileOffset
	end := sr.currFileOffset + sr.kvBoundaryOffset
	if end > sr.fileMaxOffset {
		end = sr.fileMaxOffset
	}
	if start == end {
		return false, nil
	}
	// Move the unread data to the beginning of the buffer
	unreadLen := len(sr.readBuffer) - int(sr.kvBoundaryOffset)
	for i := 0; i < unreadLen; i++ {
		sr.readBuffer[i] = sr.readBuffer[int(sr.kvBoundaryOffset)+i]
	}
	nBytes, err := storage.ReadPartialFileDirectly(sr.ctx, sr.exStorage, sr.name, start, end, sr.readBuffer[unreadLen:])
	if err != nil {
		return false, err
	}
	sr.bufferMaxOffset = nBytes + uint64(unreadLen)
	sr.currBufferOffset = 0
	sr.currFileOffset = sr.currFileOffset + nBytes
	return true, nil
}

func (sr *statFileReader) GetNextProp() (*RangeProperty, error) {
	if !sr.init {
		maxOffset, err := storage.GetFileMaxOffset(sr.ctx, sr.exStorage, sr.name)
		if err != nil {
			return nil, err
		}
		sr.fileMaxOffset = maxOffset
		sr.kvBoundaryOffset = uint64(len(sr.readBuffer))
		get, err := sr.getMoreDataFromStorage()
		if err != nil {
			return nil, err
		}
		if !get {
			return nil, nil
		}
		sr.init = true
	}
	if sr.bufferMaxOffset < sr.currBufferOffset+4 {
		get, err := sr.getMoreDataFromStorage()
		if err != nil {
			return nil, err
		}
		if !get {
			return nil, nil
		}
	}
	propLen := binary.BigEndian.Uint32(sr.readBuffer[sr.currBufferOffset:])
	sr.currBufferOffset += 4

	if sr.bufferMaxOffset < sr.currBufferOffset+uint64(propLen) {
		get, err := sr.getMoreDataFromStorage()
		if err != nil {
			return nil, err
		}
		if !get {
			return nil, nil
		}
	}
	propBytes := sr.readBuffer[sr.currBufferOffset : sr.currBufferOffset+uint64(propLen)]
	sr.currBufferOffset += uint64(propLen)
	return Decode2RangeProperty(propBytes)
}

//func openLocalWriter(cfg *backend.LocalWriterConfig) (*Writer, error) {
//	w := &Writer{
//		engine:             engine,
//		memtableSizeLimit:  cacheSize,
//		kvBuffer:           kvBuffer,
//		isKVSorted:         cfg.IsKVSorted,
//		tikvCodec:          tikvCodec,
//	}
//	// pre-allocate a long enough buffer to avoid a lot of runtime.growslice
//	// this can help save about 3% of CPU.
//	if !w.isKVSorted {
//		w.writeBatch = make([]common.KvPair, units.MiB)
//	}
//	//engine.localWriters.Store(w, nil)
//	return w, nil
//}

// AppendRows appends rows to the external storage.
func (w *Writer) AppendRows(ctx context.Context, columnNames []string, rows encode.Rows) error {
	kvs := kv.Rows2KvPairs(rows)
	if len(kvs) == 0 {
		return nil
	}

	//if w.engine.closed.Load() {
	//	return errorEngineClosed
	//}

	for i := range kvs {
		kvs[i].Key = w.tikvCodec.EncodeKey(kvs[i].Key)
	}

	w.Lock()
	defer w.Unlock()

	keyAdapter := w.keyAdapter
	for _, pair := range kvs {
		w.batchSize += len(pair.Key) + len(pair.Val)
		buf := w.kvBuffer.AllocBytes(keyAdapter.EncodedLen(pair.Key, pair.RowID))
		key := keyAdapter.Encode(buf[:0], pair.Key, pair.RowID)
		val := w.kvBuffer.AddBytes(pair.Val)
		w.writeBatch = append(w.writeBatch, common.KvPair{Key: key, Val: val})
		if w.batchSize >= w.memtableSizeLimit {
			if err := w.flushKVs(ctx); err != nil {
				return err
			}
			w.writeBatch = w.writeBatch[:0]
			w.kvBuffer.Reset()
			w.batchSize = 0
		}
	}

	return nil
}

func (w *Writer) IsSynced() bool {
	return false
}

func (w *Writer) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	if w.closed {
		return status(true), nil
	}
	logutil.BgLogger().Info("close writer", zap.Int("writerID", w.writerID),
		zap.String("minKey", hex.EncodeToString(w.minKey)), zap.String("maxKey", hex.EncodeToString(w.maxKey)))
	w.closed = true
	defer w.memBufPool.Destroy()
	err := w.flushKVs(ctx)
	if err != nil {
		return status(false), err
	}
	err = w.kvStore.Finish()
	if err != nil {
		return status(false), err
	}
	w.onClose(w.writerID, w.currentSeq, w.minKey, w.maxKey)
	return status(true), nil
}

func (w *Writer) recordMinMax(newMin, newMax tidbkv.Key) {
	if len(w.minKey) == 0 || newMin.Cmp(w.minKey) < 0 {
		w.minKey = newMin.Clone()
	}
	if len(w.maxKey) == 0 || newMax.Cmp(w.maxKey) > 0 {
		w.maxKey = newMax.Clone()
	}
}

type status bool

func (s status) Flushed() bool {
	return bool(s)
}

func CheckDataCnt(file string, exStorage storage.ExternalStorage) error {
	iter, err := NewMergeIter(context.Background(), []string{file}, []uint64{0}, exStorage, 4096)
	if err != nil {
		return err
	}
	var cnt int
	var firstKey, lastKey tidbkv.Key
	for iter.Next() {
		cnt++
		if len(firstKey) == 0 {
			firstKey = iter.Key()
			firstKey = firstKey.Clone()
		}
		lastKey = iter.Key()
	}
	lastKey = lastKey.Clone()
	logutil.BgLogger().Info("check data cnt", zap.Int("cnt", cnt),
		zap.Strings("name", PrettyFileNames([]string{file})),
		zap.String("first", hex.EncodeToString(firstKey)), zap.String("last", hex.EncodeToString(lastKey)))
	return iter.Error()
}

func (w *Writer) flushKVs(ctx context.Context) error {
	dataWriter, statWriter, err := w.createStorageWriter()
	if err != nil {
		return err
	}
	//defer func() {
	//	err := CheckDataCnt(filepath.Join(w.filenamePrefix, strconv.Itoa(w.currentSeq-1)), w.exStorage)
	//	if err != nil {
	//		logutil.BgLogger().Error("check data cnt failed", zap.Error(err))
	//	}
	//}()
	defer func() {
		dataWriter.Close(w.ctx)
		statWriter.Close(w.ctx)
	}()
	w.currentSeq++

	slices.SortFunc(w.writeBatch, func(i, j common.KvPair) bool {
		return bytes.Compare(i.Key, j.Key) < 0
	})

	w.kvStore, err = Create(w.ctx, dataWriter, statWriter)
	w.kvStore.rc = w.engine.rc

	for i := 0; i < len(w.writeBatch); i++ {
		err = w.kvStore.AddKeyValue(w.writeBatch[i].Key, w.writeBatch[i].Val)
		if err != nil {
			return err
		}
	}

	if w.engine.rc.currProp.Keys > 0 {
		newProp := *w.engine.rc.currProp
		w.engine.rc.props = append(w.engine.rc.props, &newProp)
	}
	_, err = statWriter.Write(w.ctx, w.engine.rc.Encode())
	if err != nil {
		return err
	}

	w.recordMinMax(w.writeBatch[0].Key, w.writeBatch[len(w.writeBatch)-1].Key)

	w.engine.rc.reset()
	return nil
}

func (w *Writer) createStorageWriter() (storage.ExternalFileWriter, storage.ExternalFileWriter, error) {
	dataPath := filepath.Join(w.filenamePrefix, strconv.Itoa(w.currentSeq))
	statPath := filepath.Join(w.filenamePrefix+"_stat", strconv.Itoa(w.currentSeq))
	dataWriter, err := w.exStorage.Create(w.ctx, dataPath)
	logutil.BgLogger().Info("new data writer", zap.Any("name", dataPath))
	if err != nil {
		return nil, nil, err
	}
	statWriter, err := w.exStorage.Create(w.ctx, statPath)
	logutil.BgLogger().Info("new stat writer", zap.Any("name", statPath))
	if err != nil {
		return nil, nil, err
	}
	return dataWriter, statWriter, nil
}

func PrettyFileNames(files []string) []string {
	names := make([]string, 0, len(files))
	for _, f := range files {
		dir, file := filepath.Split(f)
		names = append(names, fmt.Sprintf("%s/%s", filepath.Base(dir), file))
	}
	return names
}
