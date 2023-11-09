// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"context"
	"encoding/binary"

	"github.com/pingcap/tidb/br/pkg/storage"
)

// we use uint64 to store the length of key and value.
const lengthBytes = 8

// KeyValueStore stores key-value pairs and maintains the range properties.
type KeyValueStore struct {
	dataWriter storage.ExternalFileWriter

	rc        *rangePropertiesCollector
	ctx       context.Context
	offset    uint64
	memBuffer []byte
}

// NewKeyValueStore creates a new KeyValueStore. The data will be written to the
// given dataWriter and range properties will be maintained in the given
// rangePropertiesCollector.
func NewKeyValueStore(
	ctx context.Context,
	dataWriter storage.ExternalFileWriter,
	rangePropertiesCollector *rangePropertiesCollector,
) (*KeyValueStore, error) {
	kvStore := &KeyValueStore{
		dataWriter: dataWriter,
		ctx:        ctx,
		rc:         rangePropertiesCollector,
		memBuffer:  make([]byte, 0, 1024*8),
	}
	return kvStore, nil
}

// addEncodedData saves encoded key-value pairs to the KeyValueStore.
// data layout: keyLen + key + valueLen + value. If the accumulated
// size or key count exceeds the given distance, a new range property will be
// appended to the rangePropertiesCollector with current status.
// `key` must be in strictly ascending order for invocations of a KeyValueStore.
func (s *KeyValueStore) addEncodedData(key []byte, val []byte) error {
	if len(s.memBuffer) > 5*1024*1024 {
		_, err := s.dataWriter.Write(s.ctx, s.memBuffer)
		if err != nil {
			return err
		}
		s.memBuffer = s.memBuffer[:0]
	}

	s.memBuffer = binary.BigEndian.AppendUint64(s.memBuffer, uint64(len(key)))
	s.memBuffer = binary.BigEndian.AppendUint64(s.memBuffer, uint64(len(val)))
	s.memBuffer = append(s.memBuffer, key...)
	s.memBuffer = append(s.memBuffer, val...)

	if len(s.rc.currProp.firstKey) == 0 {
		s.rc.currProp.firstKey = key
	}
	s.rc.currProp.lastKey = key

	s.offset += uint64(lengthBytes*2 + len(key) + len(val))
	s.rc.currProp.size += uint64(len(key) + len(val))
	s.rc.currProp.keys++

	if s.rc.currProp.size >= s.rc.propSizeDist ||
		s.rc.currProp.keys >= s.rc.propKeysDist {
		newProp := *s.rc.currProp
		s.rc.props = append(s.rc.props, &newProp)

		s.rc.currProp.firstKey = nil
		s.rc.currProp.offset = s.offset
		s.rc.currProp.keys = 0
		s.rc.currProp.size = 0
	}

	return nil
}

// Close closes the KeyValueStore and append the last range property.
func (s *KeyValueStore) Close() error {
	if len(s.memBuffer) > 0 {
		_, err := s.dataWriter.Write(s.ctx, s.memBuffer)
		if err != nil {
			return err
		}
		s.memBuffer = s.memBuffer[:0]
	}
	if s.rc.currProp.keys > 0 {
		newProp := *s.rc.currProp
		s.rc.props = append(s.rc.props, &newProp)
	}
	return nil
}

const statSuffix = "_stat"
