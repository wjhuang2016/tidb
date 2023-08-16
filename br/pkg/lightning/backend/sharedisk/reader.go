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
	"context"
	"encoding/binary"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"io"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
)

type kvReader struct {
	byteReader *byteReader
	key        []byte
	val        []byte
}

func newKVReader(
	ctx context.Context,
	name string,
	store storage.ExternalStorage,
	initFileOffset uint64,
	bufSize int,
) (*kvReader, error) {
	sr, err := openStoreReaderAndSeek(ctx, store, name, initFileOffset)
	if err != nil {
		return nil, err
	}
	br, err := newByteReader(ctx, sr, bufSize)
	if err != nil {
		br.Close()
		return nil, err
	}
	return &kvReader{
		byteReader: br,
	}, nil
}

func (r *kvReader) nextKV() (key, val []byte, err error) {
	r.byteReader.reset()
	lenBytes, err := r.byteReader.readNBytes(8)
	if err != nil {
		return nil, nil, err
	}
	keyLen := int(binary.BigEndian.Uint64(*lenBytes))
	keyPtr, err := r.byteReader.readNBytes(keyLen)
	if err != nil {
		return nil, nil, noEOF(err)
	}
	lenBytes, err = r.byteReader.readNBytes(8)
	if err != nil {
		return nil, nil, noEOF(err)
	}
	valLen := int(binary.BigEndian.Uint64(*lenBytes))
	valPtr, err := r.byteReader.readNBytes(valLen)
	if err != nil {
		return nil, nil, noEOF(err)
	}
	return *keyPtr, *valPtr, nil
}

// noEOF converts the EOF error to io.ErrUnexpectedEOF.
func noEOF(err error) error {
	if err == io.EOF {
		logutil.BgLogger().Warn("unexpected EOF", zap.Error(errors.Trace(err)))
		return io.ErrUnexpectedEOF
	}
	return err
}

func (r *kvReader) Close() error {
	return r.byteReader.Close()
}

type statsReader struct {
	byteReader *byteReader
	propBytes  []byte
}

func newStatsReader(ctx context.Context, store storage.ExternalStorage, name string, bufSize int) (*statsReader, error) {
	sr, err := openStoreReaderAndSeek(ctx, store, name, 0)
	if err != nil {
		return nil, err
	}
	br, err := newByteReader(ctx, sr, bufSize)
	if err != nil {
		return nil, err
	}
	return &statsReader{
		byteReader: br,
	}, nil
}

func (r *statsReader) nextProp() (*RangeProperty, error) {
	r.byteReader.reset()
	lenBytes, err := r.byteReader.readNBytes(4)
	if err != nil {
		return nil, err
	}
	propLen := int(binary.BigEndian.Uint32(*lenBytes))
	propBytes, err := r.byteReader.readNBytes(propLen)
	if err != nil {
		return nil, noEOF(err)
	}
	return decodeProp(*propBytes)
}

func (r *statsReader) Close() error {
	return r.byteReader.Close()
}

func decodeProp(data []byte) (*RangeProperty, error) {
	rp := &RangeProperty{}
	keyLen := binary.BigEndian.Uint32(data[0:4])
	rp.Key = data[4 : 4+keyLen]
	rp.Size = binary.BigEndian.Uint64(data[4+keyLen : 12+keyLen])
	rp.Keys = binary.BigEndian.Uint64(data[12+keyLen : 20+keyLen])
	rp.offset = binary.BigEndian.Uint64(data[20+keyLen : 28+keyLen])
	rp.WriterID = int(binary.BigEndian.Uint32(data[28+keyLen : 32+keyLen]))
	rp.DataSeq = int(binary.BigEndian.Uint32(data[32+keyLen : 36+keyLen]))
	return rp, nil
}

// byteReader provides structured reading on a byte stream of external storage.
type byteReader struct {
	ctx           context.Context
	storageReader storage.ReadSeekCloser

	buf       []byte
	bufOffset int

	isEOF bool

	retPointers []*[]byte
}

func openStoreReaderAndSeek(
	ctx context.Context,
	store storage.ExternalStorage,
	name string,
	initFileOffset uint64,
) (storage.ReadSeekCloser, error) {
	storageReader, err := store.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	_, err = storageReader.Seek(int64(initFileOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	return storageReader, nil
}

func newByteReader(ctx context.Context, storageReader storage.ReadSeekCloser, bufSize int) (*byteReader, error) {
	r := &byteReader{
		ctx:           ctx,
		storageReader: storageReader,
		buf:           make([]byte, bufSize),
		bufOffset:     0,
	}
	return r, r.reload()
}

// readNBytes reads the next n bytes from the reader and returns a buffer slice containing those bytes.
// The returned slice (pointer) can not be used after r.reset. In the same interval of r.reset,
// byteReader guarantees that the returned slice (pointer) will point to the same content
// though the slice may be changed.
func (r *byteReader) readNBytes(n int) (*[]byte, error) {
	b := r.next(n)
	readLen := len(b)
	if readLen == n {
		ret := &b
		r.retPointers = append(r.retPointers, ret)
		return ret, nil
	}
	// If the reader has fewer than n bytes remaining in current buffer,
	// `auxBuf` is used as a container instead.
	auxBuf := make([]byte, n)
	copy(auxBuf, b)
	for readLen < n {
		r.cloneSlices()
		err := r.reload()
		if err != nil {
			return nil, err
		}
		b = r.next(n - readLen)
		copy(auxBuf[readLen:], b)
		readLen += len(b)
	}
	return &auxBuf, nil
}

func (r *byteReader) reset() {
	for i := range r.retPointers {
		r.retPointers[i] = nil
	}
	r.retPointers = r.retPointers[:0]
}

func (r *byteReader) cloneSlices() {
	for i := range r.retPointers {
		copied := make([]byte, len(*r.retPointers[i]))
		copy(copied, *r.retPointers[i])
		*r.retPointers[i] = copied
		r.retPointers[i] = nil
	}
	r.retPointers = r.retPointers[:0]
}

func (r *byteReader) eof() bool {
	return r.isEOF && len(r.buf) == r.bufOffset
}

func (r *byteReader) next(n int) []byte {
	end := mathutil.Min(r.bufOffset+n, len(r.buf))
	ret := r.buf[r.bufOffset:end]
	r.bufOffset += len(ret)
	return ret
}

func (r *byteReader) reload() error {
	startTime := time.Now()
	nBytes, err := io.ReadFull(r.storageReader, r.buf[0:])
	if err == io.EOF {
		r.isEOF = true
		return err
	} else if err == io.ErrUnexpectedEOF {
		r.isEOF = true
	} else if err != nil {
		logutil.Logger(r.ctx).Warn("other error during reading from external storage", zap.Error(err))
		return err
	}
	elapsed := time.Since(startTime).Microseconds()
	ReadByteForTest.Add(uint64(nBytes))
	ReadTimeForTest.Add(uint64(elapsed))
	readRate := float64(nBytes) / 1024.0 / 1024.0 / (float64(time.Since(startTime).Microseconds()) / 1000000.0)
	if time.Since(startTime) > time.Millisecond {
		log.Info("s3 read rate", zap.Any("res", readRate), zap.Any("bytes", nBytes), zap.Any("time", time.Since(startTime)))
	}
	//log.Info("s3 read rate", zap.Any("res", readRate), zap.Any("bytes", nBytes), zap.Any("time", time.Since(startTime)))
	metrics.GlobalSortSharedDiskRate.WithLabelValues("read").Observe(readRate)
	metrics.GlobalSortSharedDiskThroughput.WithLabelValues("read").Add(float64(nBytes) / 1024.0 / 1024.0)
	ReadIOCnt.Add(1)
	r.bufOffset = 0
	if nBytes < len(r.buf) {
		// The last batch.
		r.buf = r.buf[:nBytes]
	}
	return nil
}

func (r *byteReader) Close() error {
	return r.storageReader.Close()
}
