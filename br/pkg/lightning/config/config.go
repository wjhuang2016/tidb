// Copyright 2019 PingCAP, Inc.
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

package config

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/BurntSushi/toml"
	"github.com/docker/go-units"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/mathutil"
	filter "github.com/pingcap/tidb/util/table-filter"
	router "github.com/pingcap/tidb/util/table-router"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// constants for config items
const (
	// ImportMode defines mode of import for tikv.
	ImportMode = "import"
	// NormalMode defines mode of normal for tikv.
	NormalMode = "normal"

	// BackendTiDB is a constant for choosing the "TiDB" backend in the configuration.
	BackendTiDB = "tidb"
	// BackendLocal is a constant for choosing the "Local" backup in the configuration.
	// In this mode, we write & sort kv pairs with local storage and directly write them to tikv.
	BackendLocal = "local"

	// CheckpointDriverMySQL is a constant for choosing the "MySQL" checkpoint driver in the configuration.
	CheckpointDriverMySQL = "mysql"
	// CheckpointDriverFile is a constant for choosing the "File" checkpoint driver in the configuration.
	CheckpointDriverFile = "file"

	// ReplaceOnDup indicates using REPLACE INTO to insert data
	ReplaceOnDup = "replace"
	// IgnoreOnDup indicates using INSERT IGNORE INTO to insert data
	IgnoreOnDup = "ignore"
	// ErrorOnDup indicates using INSERT INTO to insert data, which would violate PK or UNIQUE constraint
	ErrorOnDup = "error"

	// KVWriteBatchSize batch size when write to TiKV.
	// this is the default value of linux send buffer size(net.ipv4.tcp_wmem) too.
	KVWriteBatchSize        = 16 * units.KiB
	DefaultRangeConcurrency = 16

	defaultDistSQLScanConcurrency     = 15
	defaultBuildStatsConcurrency      = 20
	defaultIndexSerialScanConcurrency = 20
	defaultChecksumTableConcurrency   = 2
	DefaultTableConcurrency           = 6
	defaultIndexConcurrency           = 2
	DefaultRegionCheckBackoffLimit    = 1800
	DefaultRegionSplitBatchSize       = 4096

	// defaultMetaSchemaName is the default database name used to store lightning metadata
	defaultMetaSchemaName     = "lightning_metadata"
	defaultTaskInfoSchemaName = "lightning_task_info"
	defaultMaxErrorRecords    = 100

	// autoDiskQuotaLocalReservedSpeed is the estimated size increase per
	// millisecond per write thread the local backend may gain on all engines.
	// This is used to compute the maximum size overshoot between two disk quota
	// checks, if the first one has barely passed.
	//
	// With cron.check-disk-quota = 1m, region-concurrency = 40, this should
	// contribute 2.3 GiB to the reserved size.
	// autoDiskQuotaLocalReservedSpeed uint64 = 1 * units.KiB

	DefaultEngineMemCacheSize      = 512 * units.MiB
	DefaultLocalWriterMemCacheSize = 128 * units.MiB

	defaultCSVDataCharacterSet       = "binary"
	defaultCSVDataInvalidCharReplace = utf8.RuneError

	DefaultSwitchTiKVModeInterval = 5 * time.Minute
)

var (
	supportedStorageTypes = []string{"file", "local", "s3", "noop", "gcs", "gs"}

	defaultFilter = []string{
		"*.*",
		"!mysql.*",
		"!sys.*",
		"!INFORMATION_SCHEMA.*",
		"!PERFORMANCE_SCHEMA.*",
		"!METRICS_SCHEMA.*",
		"!INSPECTION_SCHEMA.*",
	}
)

// GetDefaultFilter gets the default table filter used in Lightning.
// It clones the original default filter,
// so that the original value won't be changed when the returned slice's element is changed.
func GetDefaultFilter() []string {
	return append([]string{}, defaultFilter...)
}

// DBStore is the database connection information.
type DBStore struct {
	Host       string    `toml:"host" json:"host"`
	Port       int       `toml:"port" json:"port"`
	User       string    `toml:"user" json:"user"`
	Psw        string    `toml:"password" json:"-"`
	StatusPort int       `toml:"status-port" json:"status-port"`
	PdAddr     string    `toml:"pd-addr" json:"pd-addr"`
	StrSQLMode string    `toml:"sql-mode" json:"sql-mode"`
	TLS        string    `toml:"tls" json:"tls"`
	Security   *Security `toml:"security" json:"security"`

	SQLMode          mysql.SQLMode `toml:"-" json:"-"`
	MaxAllowedPacket uint64        `toml:"max-allowed-packet" json:"max-allowed-packet"`

	DistSQLScanConcurrency     int               `toml:"distsql-scan-concurrency" json:"distsql-scan-concurrency"`
	BuildStatsConcurrency      int               `toml:"build-stats-concurrency" json:"build-stats-concurrency"`
	IndexSerialScanConcurrency int               `toml:"index-serial-scan-concurrency" json:"index-serial-scan-concurrency"`
	ChecksumTableConcurrency   int               `toml:"checksum-table-concurrency" json:"checksum-table-concurrency"`
	Vars                       map[string]string `toml:"-" json:"vars"`

	IOTotalBytes *atomic.Uint64 `toml:"-" json:"-"`
	UUID         string         `toml:"-" json:"-"`
}

// Config is the configuration.
type Config struct {
	TaskID int64 `toml:"-" json:"id"`

	App  Lightning `toml:"lightning" json:"lightning"`
	TiDB DBStore   `toml:"tidb" json:"tidb"`

	Checkpoint   Checkpoint          `toml:"checkpoint" json:"checkpoint"`
	Mydumper     MydumperRuntime     `toml:"mydumper" json:"mydumper"`
	TikvImporter TikvImporter        `toml:"tikv-importer" json:"tikv-importer"`
	PostRestore  PostRestore         `toml:"post-restore" json:"post-restore"`
	Cron         Cron                `toml:"cron" json:"cron"`
	Routes       []*router.TableRule `toml:"routes" json:"routes"`
	Security     Security            `toml:"security" json:"security"`

	BWList filter.MySQLReplicationRules `toml:"black-white-list" json:"black-white-list"`
}

// String implements fmt.Stringer interface.
func (cfg *Config) String() string {
	bytes, err := json.Marshal(cfg)
	if err != nil {
		log.L().Error("marshal config to json error", log.ShortError(err))
	}
	return string(bytes)
}

// ToTLS creates a common.TLS from the config.
func (cfg *Config) ToTLS() (*common.TLS, error) {
	hostPort := net.JoinHostPort(cfg.TiDB.Host, strconv.Itoa(cfg.TiDB.StatusPort))
	return common.NewTLS(
		cfg.Security.CAPath,
		cfg.Security.CertPath,
		cfg.Security.KeyPath,
		hostPort,
		cfg.Security.CABytes,
		cfg.Security.CertBytes,
		cfg.Security.KeyBytes,
	)
}

// Lightning is the root configuration of lightning.
type Lightning struct {
	TableConcurrency  int    `toml:"table-concurrency" json:"table-concurrency"`
	IndexConcurrency  int    `toml:"index-concurrency" json:"index-concurrency"`
	RegionConcurrency int    `toml:"region-concurrency" json:"region-concurrency"`
	IOConcurrency     int    `toml:"io-concurrency" json:"io-concurrency"`
	CheckRequirements bool   `toml:"check-requirements" json:"check-requirements"`
	MetaSchemaName    string `toml:"meta-schema-name" json:"meta-schema-name"`

	MaxError           MaxError `toml:"max-error" json:"max-error"`
	MaxErrorRecords    int64    `toml:"max-error-records" json:"max-error-records"`
	TaskInfoSchemaName string   `toml:"task-info-schema-name" json:"task-info-schema-name"`
}

// PostOpLevel represents the level of post-operation.
type PostOpLevel int

// PostOpLevel constants.
const (
	OpLevelOff PostOpLevel = iota
	OpLevelOptional
	OpLevelRequired
)

// UnmarshalTOML implements toml.Unmarshaler interface.
func (t *PostOpLevel) UnmarshalTOML(v interface{}) error {
	switch val := v.(type) {
	case bool:
		if val {
			*t = OpLevelRequired
		} else {
			*t = OpLevelOff
		}
	case string:
		return t.FromStringValue(val)
	default:
		return errors.Errorf("invalid op level '%v', please choose valid option between ['off', 'optional', 'required']", v)
	}
	return nil
}

// MarshalText implements encoding.TextMarshaler interface.
func (t PostOpLevel) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

// FromStringValue parse command line parameter.
func (t *PostOpLevel) FromStringValue(s string) error {
	switch strings.ToLower(s) {
	//nolint:goconst // This 'false' and other 'false's aren't the same.
	case "off", "false":
		*t = OpLevelOff
	case "required", "true":
		*t = OpLevelRequired
	case "optional":
		*t = OpLevelOptional
	default:
		return errors.Errorf("invalid op level '%s', please choose valid option between ['off', 'optional', 'required']", s)
	}
	return nil
}

// MarshalJSON implements json.Marshaler interface.
func (t *PostOpLevel) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (t *PostOpLevel) UnmarshalJSON(data []byte) error {
	return t.FromStringValue(strings.Trim(string(data), `"`))
}

// String returns the string representation of the level.
func (t PostOpLevel) String() string {
	switch t {
	case OpLevelOff:
		return "off"
	case OpLevelOptional:
		return "optional"
	case OpLevelRequired:
		return "required"
	default:
		panic(fmt.Sprintf("invalid post process type '%d'", t))
	}
}

// CheckpointKeepStrategy represents the strategy to keep checkpoint data.
type CheckpointKeepStrategy int

const (
	// CheckpointRemove remove checkpoint data
	CheckpointRemove CheckpointKeepStrategy = iota
	// CheckpointRename keep by rename checkpoint file/db according to task id
	CheckpointRename
	// CheckpointOrigin keep checkpoint data unchanged
	CheckpointOrigin
)

// UnmarshalTOML implements toml.Unmarshaler interface.
func (t *CheckpointKeepStrategy) UnmarshalTOML(v interface{}) error {
	switch val := v.(type) {
	case bool:
		if val {
			*t = CheckpointRename
		} else {
			*t = CheckpointRemove
		}
	case string:
		return t.FromStringValue(val)
	default:
		return errors.Errorf("invalid checkpoint keep strategy '%v', please choose valid option between ['remove', 'rename', 'origin']", v)
	}
	return nil
}

// MarshalText implements encoding.TextMarshaler interface.
func (t CheckpointKeepStrategy) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

// FromStringValue parser command line parameter.
func (t *CheckpointKeepStrategy) FromStringValue(s string) error {
	switch strings.ToLower(s) {
	//nolint:goconst // This 'false' and other 'false's aren't the same.
	case "remove", "false":
		*t = CheckpointRemove
	case "rename", "true":
		*t = CheckpointRename
	case "origin":
		*t = CheckpointOrigin
	default:
		return errors.Errorf("invalid checkpoint keep strategy '%s', please choose valid option between ['remove', 'rename', 'origin']", s)
	}
	return nil
}

// MarshalJSON implements json.Marshaler interface.
func (t *CheckpointKeepStrategy) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (t *CheckpointKeepStrategy) UnmarshalJSON(data []byte) error {
	return t.FromStringValue(strings.Trim(string(data), `"`))
}

// String implements fmt.Stringer interface.
func (t CheckpointKeepStrategy) String() string {
	switch t {
	case CheckpointRemove:
		return "remove"
	case CheckpointRename:
		return "rename"
	case CheckpointOrigin:
		return "origin"
	default:
		panic(fmt.Sprintf("invalid post process type '%d'", t))
	}
}

// MaxError configures the maximum number of acceptable errors per kind.
type MaxError struct {
	// Syntax is the maximum number of syntax errors accepted.
	// When tolerated, the file chunk causing syntax error will be skipped, and adds 1 to the counter.
	// TODO Currently this is hard-coded to zero.
	Syntax atomic.Int64 `toml:"syntax" json:"-"`

	// Charset is the maximum number of character-set conversion errors accepted.
	// When tolerated, and `data-invalid-char-replace` is not changed from "\ufffd",
	// every invalid byte in the source file will be converted to U+FFFD and adds 1 to the counter.
	// Note that a failed conversion a column's character set (e.g. UTF8-to-GBK conversion)
	// is counted as a type error, not a charset error.
	// TODO character-set conversion is not yet implemented.
	Charset atomic.Int64 `toml:"charset" json:"-"`

	// Type is the maximum number of type errors accepted.
	// This includes strict-mode errors such as zero in dates, integer overflow, character string too long, etc.
	// In TiDB backend, this also includes all possible SQL errors raised from INSERT,
	// such as unique key conflict when `on-duplicate` is set to `error`.
	// When tolerated, the row causing the error will be skipped, and adds 1 to the counter.
	// The default value is zero, which means that such errors are not tolerated.
	Type atomic.Int64 `toml:"type" json:"type"`

	// Conflict is the maximum number of unique key conflicts in local backend accepted.
	// When tolerated, every pair of conflict adds 1 to the counter.
	// Those pairs will NOT be deleted from the target. Conflict resolution is performed separately.
	// The default value is max int64, which means conflict errors will be recorded as much as possible.
	// Sometime the actual number of conflict record logged will be greater than the value configured here,
	// because conflict error data are recorded batch by batch.
	// If the limit is reached in a single batch, the entire batch of records will be persisted before an error is reported.
	Conflict atomic.Int64 `toml:"conflict" json:"conflict"`
}

// UnmarshalTOML implements toml.Unmarshaler interface.
func (cfg *MaxError) UnmarshalTOML(v interface{}) error {
	defaultValMap := map[string]int64{
		"syntax":   0,
		"charset":  math.MaxInt64,
		"type":     0,
		"conflict": -1,
	}
	// set default value first
	cfg.Syntax.Store(defaultValMap["syntax"])
	cfg.Charset.Store(defaultValMap["charset"])
	cfg.Type.Store(defaultValMap["type"])
	cfg.Conflict.Store(defaultValMap["conflict"])
	switch val := v.(type) {
	case int64:
		// ignore val that is smaller than 0
		if val >= 0 {
			// only set type error
			cfg.Type.Store(val)
		}
		return nil
	case map[string]interface{}:
		// support stuff like `max-error = { charset = 1000, type = 1000 }`.
		getVal := func(k string, v interface{}) int64 {
			defaultVal, ok := defaultValMap[k]
			if !ok {
				return 0
			}
			iVal, ok := v.(int64)
			if !ok || iVal < 0 {
				return defaultVal
			}
			return iVal
		}
		for k, v := range val {
			switch k {
			case "type":
				cfg.Type.Store(getVal(k, v))
			case "conflict":
				cfg.Conflict.Store(getVal(k, v))
			}
		}
		return nil
	default:
		return errors.Errorf("invalid max-error '%v', should be an integer or a map of string:int64", v)
	}
}

// PausePDSchedulerScope the scope when pausing pd schedulers.
type PausePDSchedulerScope string

// constants for PausePDSchedulerScope.
const (
	// PausePDSchedulerScopeTable pause scheduler by adding schedule=deny label to target key range of the table.
	PausePDSchedulerScopeTable PausePDSchedulerScope = "table"
	// PausePDSchedulerScopeGlobal pause scheduler by remove global schedulers.
	// schedulers removed includes:
	// 	- balance-leader-scheduler
	// 	- balance-hot-region-scheduler
	// 	- balance-region-scheduler
	// 	- shuffle-leader-scheduler
	// 	- shuffle-region-scheduler
	// 	- shuffle-hot-region-scheduler
	// and we also set configs below:
	// 	- max-merge-region-keys = 0
	// 	- max-merge-region-size = 0
	// 	- leader-schedule-limit = min(40, <store-count> * <current value of leader-schedule-limit>)
	// 	- region-schedule-limit = min(40, <store-count> * <current value of region-schedule-limit>)
	// 	- max-snapshot-count = min(40, <store-count> * <current value of max-snapshot-count>)
	// 	- enable-location-replacement = false
	// 	- max-pending-peer-count = math.MaxInt32
	// see br/pkg/pdutil/pd.go for more detail.
	PausePDSchedulerScopeGlobal PausePDSchedulerScope = "global"
)

// DuplicateResolutionAlgorithm is the config type of how to resolve duplicates.
type DuplicateResolutionAlgorithm int

const (
	// DupeResAlgNone doesn't detect duplicate.
	DupeResAlgNone DuplicateResolutionAlgorithm = iota

	// DupeResAlgRecord only records duplicate records to `lightning_task_info.conflict_error_v1` table on the target TiDB.
	DupeResAlgRecord

	// DupeResAlgRemove records all duplicate records like the 'record' algorithm and remove all information related to the
	// duplicated rows. Users need to analyze the lightning_task_info.conflict_error_v1 table to add back the correct rows.
	DupeResAlgRemove

	// DupeResAlgErr reports an error and stops the import process.
	// Note: this value is only used for internal.
	DupeResAlgErr
)

// UnmarshalTOML implements the toml.Unmarshaler interface.
func (dra *DuplicateResolutionAlgorithm) UnmarshalTOML(v interface{}) error {
	if val, ok := v.(string); ok {
		return dra.FromStringValue(val)
	}
	return errors.Errorf("invalid duplicate-resolution '%v', please choose valid option between ['record', 'none', 'remove']", v)
}

// MarshalText implements the encoding.TextMarshaler interface.
func (dra DuplicateResolutionAlgorithm) MarshalText() ([]byte, error) {
	return []byte(dra.String()), nil
}

// FromStringValue parses the string value to the DuplicateResolutionAlgorithm.
func (dra *DuplicateResolutionAlgorithm) FromStringValue(s string) error {
	switch strings.ToLower(s) {
	case "record":
		*dra = DupeResAlgRecord
	case "none":
		*dra = DupeResAlgNone
	case "remove":
		*dra = DupeResAlgRemove
	default:
		return errors.Errorf("invalid duplicate-resolution '%s', please choose valid option between ['record', 'none', 'remove']", s)
	}
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (dra *DuplicateResolutionAlgorithm) MarshalJSON() ([]byte, error) {
	return []byte(`"` + dra.String() + `"`), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (dra *DuplicateResolutionAlgorithm) UnmarshalJSON(data []byte) error {
	return dra.FromStringValue(strings.Trim(string(data), `"`))
}

// String implements the fmt.Stringer interface.
func (dra DuplicateResolutionAlgorithm) String() string {
	switch dra {
	case DupeResAlgRecord:
		return "record"
	case DupeResAlgNone:
		return "none"
	case DupeResAlgRemove:
		return "remove"
	default:
		panic(fmt.Sprintf("invalid duplicate-resolution type '%d'", dra))
	}
}

// CompressionType is the config type of compression algorithm.
type CompressionType int

const (
	// CompressionNone means no compression.
	CompressionNone CompressionType = iota
	// CompressionGzip means gzip compression.
	CompressionGzip
)

// UnmarshalTOML implements toml.Unmarshaler.
func (t *CompressionType) UnmarshalTOML(v interface{}) error {
	if val, ok := v.(string); ok {
		return t.FromStringValue(val)
	}
	return errors.Errorf("invalid compression-type '%v', please choose valid option between ['gzip']", v)
}

// MarshalText implements encoding.TextMarshaler.
func (t CompressionType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

// FromStringValue parses a string to CompressionType.
func (t *CompressionType) FromStringValue(s string) error {
	switch strings.ToLower(s) {
	case "":
		*t = CompressionNone
	case "gz", "gzip":
		*t = CompressionGzip
	default:
		return errors.Errorf("invalid compression-type '%s', please choose valid option between ['gzip']", s)
	}
	return nil
}

// MarshalJSON implements json.Marshaler.
func (t *CompressionType) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *CompressionType) UnmarshalJSON(data []byte) error {
	return t.FromStringValue(strings.Trim(string(data), `"`))
}

// String implements fmt.Stringer.
func (t CompressionType) String() string {
	switch t {
	case CompressionGzip:
		return "gzip"
	case CompressionNone:
		return ""
	default:
		panic(fmt.Sprintf("invalid compression type '%d'", t))
	}
}

// PostRestore has some options which will be executed after kv restored.
type PostRestore struct {
	Checksum          PostOpLevel `toml:"checksum" json:"checksum"`
	Analyze           PostOpLevel `toml:"analyze" json:"analyze"`
	Level1Compact     bool        `toml:"level-1-compact" json:"level-1-compact"`
	PostProcessAtLast bool        `toml:"post-process-at-last" json:"post-process-at-last"`
	Compact           bool        `toml:"compact" json:"compact"`
	ChecksumViaSQL    bool        `toml:"checksum-via-sql" json:"checksum-via-sql"`
}

// StringOrStringSlice can unmarshal a TOML string as string slice with one element.
type StringOrStringSlice []string

// UnmarshalTOML implements the toml.Unmarshaler interface.
func (s *StringOrStringSlice) UnmarshalTOML(in interface{}) error {
	switch v := in.(type) {
	case string:
		*s = []string{v}
	case []interface{}:
		*s = make([]string, 0, len(v))
		for _, vv := range v {
			vs, ok := vv.(string)
			if !ok {
				return errors.Errorf("invalid string slice '%v'", in)
			}
			*s = append(*s, vs)
		}
	default:
		return errors.Errorf("invalid string slice '%v'", in)
	}
	return nil
}

// CSVConfig is the config for CSV files.
type CSVConfig struct {
	// Separator, Delimiter and Terminator should all be in utf8mb4 encoding.
	Separator         string              `toml:"separator" json:"separator"`
	Delimiter         string              `toml:"delimiter" json:"delimiter"`
	Terminator        string              `toml:"terminator" json:"terminator"`
	Null              StringOrStringSlice `toml:"null" json:"null"`
	Header            bool                `toml:"header" json:"header"`
	HeaderSchemaMatch bool                `toml:"header-schema-match" json:"header-schema-match"`
	TrimLastSep       bool                `toml:"trim-last-separator" json:"trim-last-separator"`
	NotNull           bool                `toml:"not-null" json:"not-null"`
	// deprecated, use `escaped-by` instead.
	BackslashEscape bool `toml:"backslash-escape" json:"backslash-escape"`
	// EscapedBy has higher priority than BackslashEscape, currently it must be a single character if set.
	EscapedBy string `toml:"escaped-by" json:"escaped-by"`

	// hide these options for lightning configuration file, they can only be used by LOAD DATA
	// https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-field-line-handling
	StartingBy     string `toml:"-" json:"-"`
	AllowEmptyLine bool   `toml:"-" json:"-"`
	// For non-empty Delimiter (for example quotes), null elements inside quotes are not considered as null except for
	// `\N` (when escape-by is `\`). That is to say, `\N` is special for null because it always means null.
	QuotedNullIsText bool `toml:"-" json:"-"`
	// ref https://dev.mysql.com/doc/refman/8.0/en/load-data.html
	// > If the field begins with the ENCLOSED BY character, instances of that character are recognized as terminating a
	// > field value only if followed by the field or line TERMINATED BY sequence.
	// This means we will meet unescaped quote in a quoted field
	// > The "BIG" boss      -> The "BIG" boss
	// This means we will meet unescaped quote in a unquoted field
	UnescapedQuote bool `toml:"-" json:"-"`
}

// MydumperRuntime is the runtime config for mydumper.
type MydumperRuntime struct {
	ReadBlockSize    ByteSize         `toml:"read-block-size" json:"read-block-size"`
	BatchSize        ByteSize         `toml:"batch-size" json:"batch-size"`
	BatchImportRatio float64          `toml:"batch-import-ratio" json:"batch-import-ratio"`
	SourceID         string           `toml:"source-id" json:"source-id"`
	SourceDir        string           `toml:"data-source-dir" json:"data-source-dir"`
	CharacterSet     string           `toml:"character-set" json:"character-set"`
	CSV              CSVConfig        `toml:"csv" json:"csv"`
	MaxRegionSize    ByteSize         `toml:"max-region-size" json:"max-region-size"`
	Filter           []string         `toml:"filter" json:"filter"`
	FileRouters      []*FileRouteRule `toml:"files" json:"files"`
	// Deprecated: only used to keep the compatibility.
	NoSchema         bool             `toml:"no-schema" json:"no-schema"`
	CaseSensitive    bool             `toml:"case-sensitive" json:"case-sensitive"`
	StrictFormat     bool             `toml:"strict-format" json:"strict-format"`
	DefaultFileRules bool             `toml:"default-file-rules" json:"default-file-rules"`
	IgnoreColumns    AllIgnoreColumns `toml:"ignore-data-columns" json:"ignore-data-columns"`
	// DataCharacterSet is the character set of the source file. Only CSV files are supported now. The following options are supported.
	//   - utf8mb4
	//   - GB18030
	//   - GBK: an extension of the GB2312 character set and is also known as Code Page 936.
	//   - latin1: IANA Windows1252
	//   - binary: no attempt to convert the encoding.
	// Leave DataCharacterSet empty will make it use `binary` by default.
	DataCharacterSet string `toml:"data-character-set" json:"data-character-set"`
	// DataInvalidCharReplace is the replacement characters for non-compatible characters, which shouldn't duplicate with the separators or line breaks.
	// Changing the default value will result in increased parsing time. Non-compatible characters do not cause an increase in error.
	DataInvalidCharReplace string `toml:"data-invalid-char-replace" json:"data-invalid-char-replace"`
}

// AllIgnoreColumns is a slice of IgnoreColumns.
type AllIgnoreColumns []*IgnoreColumns

// IgnoreColumns is the config for ignoring columns.
type IgnoreColumns struct {
	DB          string   `toml:"db" json:"db"`
	Table       string   `toml:"table" json:"table"`
	TableFilter []string `toml:"table-filter" json:"table-filter"`
	Columns     []string `toml:"columns" json:"columns"`
}

// ColumnsMap returns a map of columns.
func (ic *IgnoreColumns) ColumnsMap() map[string]struct{} {
	columnMap := make(map[string]struct{}, len(ic.Columns))
	for _, c := range ic.Columns {
		columnMap[c] = struct{}{}
	}
	return columnMap
}

// GetIgnoreColumns gets Ignore config by schema name/regex and table name/regex.
func (igCols AllIgnoreColumns) GetIgnoreColumns(db string, table string, caseSensitive bool) (*IgnoreColumns, error) {
	if !caseSensitive {
		db = strings.ToLower(db)
		table = strings.ToLower(table)
	}
	for i, ig := range igCols {
		if ig.DB == db && ig.Table == table {
			return igCols[i], nil
		}
		f, err := filter.Parse(ig.TableFilter)
		if err != nil {
			return nil, common.ErrInvalidConfig.GenWithStack("invalid table filter %s in ignore columns", strings.Join(ig.TableFilter, ","))
		}
		if f.MatchTable(db, table) {
			return igCols[i], nil
		}
	}
	return &IgnoreColumns{Columns: make([]string, 0)}, nil
}

// FileRouteRule is the rule for routing files.
type FileRouteRule struct {
	Pattern     string `json:"pattern" toml:"pattern" yaml:"pattern"`
	Path        string `json:"path" toml:"path" yaml:"path"`
	Schema      string `json:"schema" toml:"schema" yaml:"schema"`
	Table       string `json:"table" toml:"table" yaml:"table"`
	Type        string `json:"type" toml:"type" yaml:"type"`
	Key         string `json:"key" toml:"key" yaml:"key"`
	Compression string `json:"compression" toml:"compression" yaml:"compression"`
	// unescape the schema/table name only used in lightning's internal logic now.
	Unescape bool `json:"-" toml:"-" yaml:"-"`
	// TODO: DataCharacterSet here can override the same field in [mydumper.csv] with a higher level.
	// This could provide users a more flexible usage to configure different files with
	// different data charsets.
	// DataCharacterSet string `toml:"data-character-set" json:"data-character-set"`
}

// TikvImporter is the config for tikv-importer.
type TikvImporter struct {
	// Deprecated: only used to keep the compatibility.
	Addr        string `toml:"addr" json:"addr"`
	Backend     string `toml:"backend" json:"backend"`
	OnDuplicate string `toml:"on-duplicate" json:"on-duplicate"`
	MaxKVPairs  int    `toml:"max-kv-pairs" json:"max-kv-pairs"`
	// deprecated
	SendKVPairs             int                          `toml:"send-kv-pairs" json:"send-kv-pairs"`
	SendKVSize              ByteSize                     `toml:"send-kv-size" json:"send-kv-size"`
	CompressKVPairs         CompressionType              `toml:"compress-kv-pairs" json:"compress-kv-pairs"`
	RegionSplitSize         ByteSize                     `toml:"region-split-size" json:"region-split-size"`
	RegionSplitKeys         int                          `toml:"region-split-keys" json:"region-split-keys"`
	RegionSplitBatchSize    int                          `toml:"region-split-batch-size" json:"region-split-batch-size"`
	RegionSplitConcurrency  int                          `toml:"region-split-concurrency" json:"region-split-concurrency"`
	RegionCheckBackoffLimit int                          `toml:"region-check-backoff-limit" json:"region-check-backoff-limit"`
	SortedKVDir             string                       `toml:"sorted-kv-dir" json:"sorted-kv-dir"`
	DiskQuota               ByteSize                     `toml:"disk-quota" json:"disk-quota"`
	RangeConcurrency        int                          `toml:"range-concurrency" json:"range-concurrency"`
	DuplicateResolution     DuplicateResolutionAlgorithm `toml:"duplicate-resolution" json:"duplicate-resolution"`
	IncrementalImport       bool                         `toml:"incremental-import" json:"incremental-import"`
	KeyspaceName            string                       `toml:"keyspace-name" json:"keyspace-name"`
	AddIndexBySQL           bool                         `toml:"add-index-by-sql" json:"add-index-by-sql"`

	EngineMemCacheSize      ByteSize `toml:"engine-mem-cache-size" json:"engine-mem-cache-size"`
	LocalWriterMemCacheSize ByteSize `toml:"local-writer-mem-cache-size" json:"local-writer-mem-cache-size"`
	StoreWriteBWLimit       ByteSize `toml:"store-write-bwlimit" json:"store-write-bwlimit"`
	// default is PausePDSchedulerScopeTable to compatible with previous version(>= 6.1)
	PausePDSchedulerScope PausePDSchedulerScope `toml:"pause-pd-scheduler-scope" json:"pause-pd-scheduler-scope"`
}

// Checkpoint is the config for checkpoint.
type Checkpoint struct {
	Schema           string                    `toml:"schema" json:"schema"`
	DSN              string                    `toml:"dsn" json:"-"` // DSN may contain password, don't expose this to JSON.
	MySQLParam       *common.MySQLConnectParam `toml:"-" json:"-"`   // For some security reason, we use MySQLParam instead of DSN.
	Driver           string                    `toml:"driver" json:"driver"`
	Enable           bool                      `toml:"enable" json:"enable"`
	KeepAfterSuccess CheckpointKeepStrategy    `toml:"keep-after-success" json:"keep-after-success"`
}

// Cron is the config for cron.
type Cron struct {
	SwitchMode     Duration `toml:"switch-mode" json:"switch-mode"`
	LogProgress    Duration `toml:"log-progress" json:"log-progress"`
	CheckDiskQuota Duration `toml:"check-disk-quota" json:"check-disk-quota"`
}

// Security is the config for security.
type Security struct {
	CAPath   string `toml:"ca-path" json:"ca-path"`
	CertPath string `toml:"cert-path" json:"cert-path"`
	KeyPath  string `toml:"key-path" json:"key-path"`
	// RedactInfoLog indicates that whether enabling redact log
	RedactInfoLog bool `toml:"redact-info-log" json:"redact-info-log"`

	TLSConfig                *tls.Config `toml:"-" json:"-"`
	AllowFallbackToPlaintext bool        `toml:"-" json:"-"`

	// When DM/engine uses lightning as a library, it can directly pass in the content
	CABytes   []byte `toml:"-" json:"-"`
	CertBytes []byte `toml:"-" json:"-"`
	KeyBytes  []byte `toml:"-" json:"-"`
}

// BuildTLSConfig builds the tls config which is used by SQL drier later.
func (sec *Security) BuildTLSConfig() error {
	if sec == nil || sec.TLSConfig != nil {
		return nil
	}

	tlsConfig, err := util.NewTLSConfig(
		util.WithCAPath(sec.CAPath),
		util.WithCertAndKeyPath(sec.CertPath, sec.KeyPath),
		util.WithCAContent(sec.CABytes),
		util.WithCertAndKeyContent(sec.CertBytes, sec.KeyBytes),
	)
	if err != nil {
		return errors.Trace(err)
	}
	sec.TLSConfig = tlsConfig
	return nil
}

// Duration which can be deserialized from a TOML string.
// Implemented as https://github.com/BurntSushi/toml#using-the-encodingtextunmarshaler-interface
type Duration struct {
	time.Duration
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return errors.Trace(err)
}

// MarshalText implements encoding.TextMarshaler.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// MarshalJSON implements json.Marshaler.
func (d *Duration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, d.Duration)), nil
}

// Charset defines character set
type Charset int

// Charset constants
const (
	Binary Charset = iota
	UTF8MB4
	GB18030
	GBK
	Latin1
	ASCII
)

// String return the string value of charset
func (c Charset) String() string {
	switch c {
	case Binary:
		return "binary"
	case UTF8MB4:
		return "utf8mb4"
	case GB18030:
		return "gb18030"
	case GBK:
		return "gbk"
	case Latin1:
		return "latin1"
	case ASCII:
		return "ascii"
	default:
		return "unknown_charset"
	}
}

// ParseCharset parser character set for string
func ParseCharset(dataCharacterSet string) (Charset, error) {
	switch strings.ToLower(dataCharacterSet) {
	case "", "binary":
		return Binary, nil
	case "utf8", "utf8mb4":
		return UTF8MB4, nil
	case "gb18030":
		return GB18030, nil
	case "gbk":
		return GBK, nil
	case "latin1":
		return Latin1, nil
	case "ascii":
		return ASCII, nil
	default:
		return Binary, errors.Errorf("found unsupported data-character-set: %s", dataCharacterSet)
	}
}

// NewConfig creates a new Config.
func NewConfig() *Config {
	return &Config{
		App: Lightning{
			RegionConcurrency: runtime.NumCPU(),
			TableConcurrency:  0,
			IndexConcurrency:  0,
			IOConcurrency:     5,
			CheckRequirements: true,
			MaxError: MaxError{
				Conflict: *atomic.NewInt64(-1),
			},
			TaskInfoSchemaName: defaultTaskInfoSchemaName,
		},
		Checkpoint: Checkpoint{
			Enable: true,
		},
		TiDB: DBStore{
			Host:                       "127.0.0.1",
			User:                       "root",
			StatusPort:                 10080,
			StrSQLMode:                 "ONLY_FULL_GROUP_BY,NO_AUTO_CREATE_USER",
			MaxAllowedPacket:           defaultMaxAllowedPacket,
			BuildStatsConcurrency:      defaultBuildStatsConcurrency,
			DistSQLScanConcurrency:     defaultDistSQLScanConcurrency,
			IndexSerialScanConcurrency: defaultIndexSerialScanConcurrency,
			ChecksumTableConcurrency:   defaultChecksumTableConcurrency,
		},
		Cron: Cron{
			SwitchMode:     Duration{Duration: DefaultSwitchTiKVModeInterval},
			LogProgress:    Duration{Duration: 5 * time.Minute},
			CheckDiskQuota: Duration{Duration: 1 * time.Minute},
		},
		Mydumper: MydumperRuntime{
			ReadBlockSize: ReadBlockSize,
			CSV: CSVConfig{
				Separator:         ",",
				Delimiter:         `"`,
				Header:            true,
				HeaderSchemaMatch: true,
				NotNull:           false,
				Null:              []string{`\N`},
				BackslashEscape:   true,
				EscapedBy:         `\`,
				TrimLastSep:       false,
			},
			StrictFormat:           false,
			MaxRegionSize:          MaxRegionSize,
			Filter:                 GetDefaultFilter(),
			DataCharacterSet:       defaultCSVDataCharacterSet,
			DataInvalidCharReplace: string(defaultCSVDataInvalidCharReplace),
		},
		TikvImporter: TikvImporter{
			Backend:                 "",
			MaxKVPairs:              4096,
			SendKVPairs:             32768,
			SendKVSize:              KVWriteBatchSize,
			RegionSplitSize:         0,
			RegionSplitBatchSize:    DefaultRegionSplitBatchSize,
			RegionSplitConcurrency:  runtime.GOMAXPROCS(0),
			RegionCheckBackoffLimit: DefaultRegionCheckBackoffLimit,
			DiskQuota:               ByteSize(math.MaxInt64),
			DuplicateResolution:     DupeResAlgNone,
			PausePDSchedulerScope:   PausePDSchedulerScopeTable,
		},
		PostRestore: PostRestore{
			Checksum:          OpLevelRequired,
			Analyze:           OpLevelOptional,
			PostProcessAtLast: true,
			ChecksumViaSQL:    true,
		},
	}
}

// LoadFromGlobal resets the current configuration to the global settings.
func (cfg *Config) LoadFromGlobal(global *GlobalConfig) error {
	if err := cfg.LoadFromTOML(global.ConfigFileContent); err != nil {
		return err
	}

	cfg.TiDB.Host = global.TiDB.Host
	cfg.TiDB.Port = global.TiDB.Port
	cfg.TiDB.User = global.TiDB.User
	cfg.TiDB.Psw = global.TiDB.Psw
	cfg.TiDB.StatusPort = global.TiDB.StatusPort
	cfg.TiDB.PdAddr = global.TiDB.PdAddr
	cfg.Mydumper.NoSchema = global.Mydumper.NoSchema
	cfg.Mydumper.SourceDir = global.Mydumper.SourceDir
	cfg.Mydumper.Filter = global.Mydumper.Filter
	cfg.TikvImporter.Backend = global.TikvImporter.Backend
	cfg.TikvImporter.SortedKVDir = global.TikvImporter.SortedKVDir
	cfg.Checkpoint.Enable = global.Checkpoint.Enable
	cfg.PostRestore.Checksum = global.PostRestore.Checksum
	cfg.PostRestore.Analyze = global.PostRestore.Analyze
	cfg.App.CheckRequirements = global.App.CheckRequirements
	cfg.Security = global.Security
	cfg.Mydumper.IgnoreColumns = global.Mydumper.IgnoreColumns
	return nil
}

// LoadFromTOML overwrites the current configuration by the TOML data
// If data contains toml items not in Config and GlobalConfig, return an error
// If data contains toml items not in Config, thus won't take effect, warn user
func (cfg *Config) LoadFromTOML(data []byte) error {
	// bothUnused saves toml items not belong to Config nor GlobalConfig
	var bothUnused []string
	// warnItems saves legal toml items but won't effect
	var warnItems []string

	dataStr := string(data)

	// Here we load toml into cfg, and rest logic is check unused keys
	metaData, err := toml.Decode(dataStr, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	unusedConfigKeys := metaData.Undecoded()
	if len(unusedConfigKeys) == 0 {
		return nil
	}

	// Now we deal with potential both-unused keys of Config and GlobalConfig struct

	metaDataGlobal, err := toml.Decode(dataStr, &GlobalConfig{})
	if err != nil {
		return errors.Trace(err)
	}

	// Key type returned by metadata.Undecoded doesn't have a equality comparison,
	// we convert them to string type instead, and this conversion is identical
	unusedGlobalKeys := metaDataGlobal.Undecoded()
	unusedGlobalKeyStrs := make(map[string]struct{})
	for _, key := range unusedGlobalKeys {
		unusedGlobalKeyStrs[key.String()] = struct{}{}
	}

iterateUnusedKeys:
	for _, key := range unusedConfigKeys {
		keyStr := key.String()
		switch keyStr {
		// these keys are not counted as decoded by toml decoder, but actually they are decoded,
		// because the corresponding unmarshal logic handles these key's decoding in a custom way
		case "lightning.max-error.type",
			"lightning.max-error.conflict":
			continue iterateUnusedKeys
		}
		if _, found := unusedGlobalKeyStrs[keyStr]; found {
			bothUnused = append(bothUnused, keyStr)
		} else {
			warnItems = append(warnItems, keyStr)
		}
	}

	if len(bothUnused) > 0 {
		return errors.Errorf("config file contained unknown configuration options: %s",
			strings.Join(bothUnused, ", "))
	}

	// Warn that some legal field of config file won't be overwritten, such as lightning.file
	if len(warnItems) > 0 {
		log.L().Warn("currently only per-task configuration can be applied, global configuration changes can only be made on startup",
			zap.Strings("global config changes", warnItems))
	}

	return nil
}

// Adjust fixes the invalid or unspecified settings to reasonable valid values.
func (cfg *Config) Adjust(ctx context.Context) error {
	// Reject problematic CSV configurations.
	csv := &cfg.Mydumper.CSV
	if len(csv.Separator) == 0 {
		return common.ErrInvalidConfig.GenWithStack("`mydumper.csv.separator` must not be empty")
	}

	if len(csv.Delimiter) > 0 && (strings.HasPrefix(csv.Separator, csv.Delimiter) || strings.HasPrefix(csv.Delimiter, csv.Separator)) {
		return common.ErrInvalidConfig.GenWithStack("`mydumper.csv.separator` and `mydumper.csv.delimiter` must not be prefix of each other")
	}

	if len(csv.EscapedBy) > 1 {
		return common.ErrInvalidConfig.GenWithStack("`mydumper.csv.escaped-by` must be empty or a single character")
	}
	if csv.BackslashEscape && csv.EscapedBy == "" {
		csv.EscapedBy = `\`
	}
	if !csv.BackslashEscape && csv.EscapedBy == `\` {
		csv.EscapedBy = ""
	}

	// keep compatibility with old behaviour
	if !csv.NotNull && len(csv.Null) == 0 {
		csv.Null = []string{""}
	}

	if len(csv.EscapedBy) > 0 {
		if csv.Separator == csv.EscapedBy {
			return common.ErrInvalidConfig.GenWithStack("cannot use '%s' both as CSV separator and `mydumper.csv.escaped-by`", csv.EscapedBy)
		}
		if csv.Delimiter == csv.EscapedBy {
			return common.ErrInvalidConfig.GenWithStack("cannot use '%s' both as CSV delimiter and `mydumper.csv.escaped-by`", csv.EscapedBy)
		}
		if csv.Terminator == csv.EscapedBy {
			return common.ErrInvalidConfig.GenWithStack("cannot use '%s' both as CSV terminator and `mydumper.csv.escaped-by`", csv.EscapedBy)
		}
	}

	// adjust file routing
	for _, rule := range cfg.Mydumper.FileRouters {
		if filepath.IsAbs(rule.Path) {
			relPath, err := filepath.Rel(cfg.Mydumper.SourceDir, rule.Path)
			if err != nil {
				return common.ErrInvalidConfig.Wrap(err).
					GenWithStack("cannot find relative path for file route path %s", rule.Path)
			}
			// ".." means that this path is not in source dir, so we should return an error
			if strings.HasPrefix(relPath, "..") {
				return common.ErrInvalidConfig.GenWithStack(
					"file route path '%s' is not in source dir '%s'", rule.Path, cfg.Mydumper.SourceDir)
			}
			rule.Path = relPath
		}
	}

	// enable default file route rule if no rules are set
	if len(cfg.Mydumper.FileRouters) == 0 {
		cfg.Mydumper.DefaultFileRules = true
	}

	if len(cfg.Mydumper.DataCharacterSet) == 0 {
		cfg.Mydumper.DataCharacterSet = defaultCSVDataCharacterSet
	}
	charset, err1 := ParseCharset(cfg.Mydumper.DataCharacterSet)
	if err1 != nil {
		return common.ErrInvalidConfig.Wrap(err1).GenWithStack("invalid `mydumper.data-character-set`")
	}
	if charset == GBK || charset == GB18030 {
		log.L().Warn(
			"incompatible strings may be encountered during the transcoding process and will be replaced, please be aware of the risk of not being able to retain the original information",
			zap.String("source-character-set", charset.String()),
			zap.ByteString("invalid-char-replacement", []byte(cfg.Mydumper.DataInvalidCharReplace)))
	}

	mustHaveInternalConnections, err := cfg.AdjustCommon()
	if err != nil {
		return err
	}

	// mydumper.filter and black-white-list cannot co-exist.
	if cfg.HasLegacyBlackWhiteList() {
		log.L().Warn("the config `black-white-list` has been deprecated, please replace with `mydumper.filter`")
		if !common.StringSliceEqual(cfg.Mydumper.Filter, defaultFilter) {
			return common.ErrInvalidConfig.GenWithStack("`mydumper.filter` and `black-white-list` cannot be simultaneously defined")
		}
	}

	for _, rule := range cfg.Routes {
		if !cfg.Mydumper.CaseSensitive {
			rule.ToLower()
		}
		if err := rule.Valid(); err != nil {
			return common.ErrInvalidConfig.Wrap(err).GenWithStack("file route rule is invalid")
		}
	}

	lowerCaseScope := strings.ToLower(string(cfg.TikvImporter.PausePDSchedulerScope))
	cfg.TikvImporter.PausePDSchedulerScope = PausePDSchedulerScope(lowerCaseScope)
	if cfg.TikvImporter.PausePDSchedulerScope != PausePDSchedulerScopeTable &&
		cfg.TikvImporter.PausePDSchedulerScope != PausePDSchedulerScopeGlobal {
		return common.ErrInvalidConfig.GenWithStack("pause-pd-scheduler-scope is invalid, allowed value include: table, global")
	}

	if err := cfg.CheckAndAdjustTiDBPort(ctx, mustHaveInternalConnections); err != nil {
		return err
	}
	cfg.AdjustMydumper()
	cfg.AdjustCheckPoint()
	return cfg.CheckAndAdjustFilePath()
}

// AdjustCommon adjusts the common configurations.
func (cfg *Config) AdjustCommon() (bool, error) {
	if cfg.TikvImporter.Backend == "" {
		return false, common.ErrInvalidConfig.GenWithStack("tikv-importer.backend must not be empty!")
	}
	cfg.TikvImporter.Backend = strings.ToLower(cfg.TikvImporter.Backend)
	mustHaveInternalConnections := true
	switch cfg.TikvImporter.Backend {
	case BackendTiDB:
		cfg.DefaultVarsForTiDBBackend()
		mustHaveInternalConnections = false
		cfg.PostRestore.Checksum = OpLevelOff
		cfg.PostRestore.Analyze = OpLevelOff
		cfg.PostRestore.Compact = false
		if cfg.TikvImporter.OnDuplicate == "" {
			cfg.TikvImporter.OnDuplicate = ReplaceOnDup
		}
		cfg.TikvImporter.DuplicateResolution = DupeResAlgNone
	case BackendLocal:
		// force turn off pre-dedup for local backend
		cfg.TikvImporter.OnDuplicate = ""
		if cfg.TikvImporter.RegionSplitBatchSize <= 0 {
			return mustHaveInternalConnections, common.ErrInvalidConfig.GenWithStack("`tikv-importer.region-split-batch-size` got %d, should be larger than 0", cfg.TikvImporter.RegionSplitBatchSize)
		}
		if cfg.TikvImporter.RegionSplitConcurrency <= 0 {
			return mustHaveInternalConnections, common.ErrInvalidConfig.GenWithStack("`tikv-importer.region-split-concurrency` got %d, should be larger than 0", cfg.TikvImporter.RegionSplitConcurrency)
		}
		// RegionConcurrency > NumCPU is meaningless.
		cpuCount := runtime.NumCPU()
		if cfg.App.RegionConcurrency > cpuCount {
			cfg.App.RegionConcurrency = cpuCount
		}
		cfg.DefaultVarsForImporterAndLocalBackend()
		if err := cfg.CheckAndAdjustForLocalBackend(); err != nil {
			return mustHaveInternalConnections, err
		}
	default:
		return mustHaveInternalConnections, common.ErrInvalidConfig.GenWithStack("unsupported `tikv-importer.backend` (%s)", cfg.TikvImporter.Backend)
	}

	if cfg.App.MaxErrorRecords == 0 {
		maxErr := cfg.App.MaxError
		// Compatible with the old behavior that records all syntax,charset,type errors.
		maxAccepted := mathutil.Max(maxErr.Syntax.Load(), maxErr.Charset.Load(), maxErr.Type.Load())
		if maxAccepted > defaultMaxErrorRecords {
			cfg.App.MaxErrorRecords = maxAccepted
		} else {
			cfg.App.MaxErrorRecords = defaultMaxErrorRecords
		}
	}

	if cfg.App.MaxError.Conflict.Load() == -1 {
		if cfg.TikvImporter.Backend == BackendTiDB {
			// in versions before v7.3, tidb backend will treat "duplicate entry"
			// as type error which default is 0. So we set it to 0 to keep compatible.
			cfg.App.MaxError.Conflict.Store(0)
		} else {
			cfg.App.MaxError.Conflict.Store(math.MaxInt64)
		}
	}

	cfg.TikvImporter.OnDuplicate = strings.ToLower(cfg.TikvImporter.OnDuplicate)
	switch cfg.TikvImporter.OnDuplicate {
	case ReplaceOnDup, IgnoreOnDup, ErrorOnDup, "":
	default:
		return mustHaveInternalConnections, common.ErrInvalidConfig.GenWithStack(
			"unsupported `tikv-importer.on-duplicate` (%s)", cfg.TikvImporter.OnDuplicate)
	}

	var err error
	cfg.TiDB.SQLMode, err = mysql.GetSQLMode(cfg.TiDB.StrSQLMode)
	if err != nil {
		return mustHaveInternalConnections, common.ErrInvalidConfig.Wrap(err).GenWithStack("`mydumper.tidb.sql_mode` must be a valid SQL_MODE")
	}

	if err := cfg.CheckAndAdjustSecurity(); err != nil {
		return mustHaveInternalConnections, err
	}
	return mustHaveInternalConnections, err
}

// CheckAndAdjustForLocalBackend checks and adjusts the configurations for local backend.
func (cfg *Config) CheckAndAdjustForLocalBackend() error {
	if cfg.TikvImporter.EngineMemCacheSize == 0 {
		cfg.TikvImporter.EngineMemCacheSize = DefaultEngineMemCacheSize
	}
	if cfg.TikvImporter.LocalWriterMemCacheSize == 0 {
		cfg.TikvImporter.LocalWriterMemCacheSize = DefaultLocalWriterMemCacheSize
	}

	if cfg.TikvImporter.IncrementalImport && cfg.TikvImporter.AddIndexBySQL {
		return common.ErrInvalidConfig.
			GenWithStack("tikv-importer.add-index-using-ddl cannot be used with tikv-importer.incremental-import")
	}

	if cfg.TikvImporter.IncrementalImport && cfg.TikvImporter.OnDuplicate != "" {
		return common.ErrInvalidConfig.
			GenWithStack("tikv-importer.on-duplicate cannot be used with tikv-importer.incremental-import")
	}
	if cfg.TikvImporter.DuplicateResolution != DupeResAlgNone && cfg.TikvImporter.OnDuplicate != "" {
		return common.ErrInvalidConfig.
			GenWithStack("tikv-importer.on-duplicate cannot be used with tikv-importer.duplicate-resolution")
	}

	if len(cfg.TikvImporter.SortedKVDir) == 0 {
		return common.ErrInvalidConfig.GenWithStack("tikv-importer.sorted-kv-dir must not be empty!")
	}

	storageSizeDir := filepath.Clean(cfg.TikvImporter.SortedKVDir)
	sortedKVDirInfo, err := os.Stat(storageSizeDir)

	switch {
	case os.IsNotExist(err):
		return nil
	case err == nil:
		if !sortedKVDirInfo.IsDir() {
			return common.ErrInvalidConfig.
				GenWithStack("tikv-importer.sorted-kv-dir ('%s') is not a directory", storageSizeDir)
		}
	default:
		return common.ErrInvalidConfig.Wrap(err).GenWithStack("invalid tikv-importer.sorted-kv-dir")
	}

	return nil
}

// DefaultVarsForTiDBBackend sets the default values for TiDB backend.
func (cfg *Config) DefaultVarsForTiDBBackend() {
	if cfg.App.TableConcurrency == 0 {
		cfg.App.TableConcurrency = cfg.App.RegionConcurrency
	}
	if cfg.App.IndexConcurrency == 0 {
		cfg.App.IndexConcurrency = cfg.App.RegionConcurrency
	}
}

// DefaultVarsForImporterAndLocalBackend sets the default values for importer and local backend.
func (cfg *Config) DefaultVarsForImporterAndLocalBackend() {
	if cfg.App.IndexConcurrency == 0 {
		cfg.App.IndexConcurrency = defaultIndexConcurrency
	}
	if cfg.App.TableConcurrency == 0 {
		cfg.App.TableConcurrency = DefaultTableConcurrency
	}

	if len(cfg.App.MetaSchemaName) == 0 {
		cfg.App.MetaSchemaName = defaultMetaSchemaName
	}
	if cfg.TikvImporter.RangeConcurrency == 0 {
		cfg.TikvImporter.RangeConcurrency = DefaultRangeConcurrency
	}
	if cfg.TiDB.BuildStatsConcurrency == 0 {
		cfg.TiDB.BuildStatsConcurrency = defaultBuildStatsConcurrency
	}
	if cfg.TiDB.IndexSerialScanConcurrency == 0 {
		cfg.TiDB.IndexSerialScanConcurrency = defaultIndexSerialScanConcurrency
	}
	if cfg.TiDB.ChecksumTableConcurrency == 0 {
		cfg.TiDB.ChecksumTableConcurrency = defaultChecksumTableConcurrency
	}
}

// CheckAndAdjustTiDBPort checks and adjusts the TiDB port and PD address.
func (cfg *Config) CheckAndAdjustTiDBPort(ctx context.Context, mustHaveInternalConnections bool) error {
	// automatically determine the TiDB port & PD address from TiDB settings
	if mustHaveInternalConnections && (cfg.TiDB.Port <= 0 || len(cfg.TiDB.PdAddr) == 0) {
		tls, err := cfg.ToTLS()
		if err != nil {
			return err
		}

		var settings tidbcfg.Config
		err = tls.GetJSON(ctx, "/settings", &settings)
		if err != nil {
			return common.ErrInvalidConfig.Wrap(err).GenWithStack("cannot fetch settings from TiDB, please manually fill in `tidb.port` and `tidb.pd-addr`")
		}
		if cfg.TiDB.Port <= 0 {
			cfg.TiDB.Port = int(settings.Port)
		}
		if len(cfg.TiDB.PdAddr) == 0 {
			pdAddrs := strings.Split(settings.Path, ",")
			cfg.TiDB.PdAddr = pdAddrs[0] // FIXME support multiple PDs once importer can.
		}
	}

	if cfg.TiDB.Port <= 0 {
		return common.ErrInvalidConfig.GenWithStack("invalid `tidb.port` setting")
	}

	if mustHaveInternalConnections && len(cfg.TiDB.PdAddr) == 0 {
		return common.ErrInvalidConfig.GenWithStack("invalid `tidb.pd-addr` setting")
	}
	return nil
}

// CheckAndAdjustFilePath checks and adjusts the file path.
func (cfg *Config) CheckAndAdjustFilePath() error {
	var u *url.URL

	// An absolute Windows path like "C:\Users\XYZ" would be interpreted as
	// an URL with scheme "C" and opaque data "\Users\XYZ".
	// Therefore, we only perform URL parsing if we are sure the path is not
	// an absolute Windows path.
	// Here we use the `filepath.VolumeName` which can identify the "C:" part
	// out of the path. On Linux this method always return an empty string.
	// On Windows, the drive letter can only be single letters from "A:" to "Z:",
	// so this won't mistake "S3:" as a Windows path.
	if len(filepath.VolumeName(cfg.Mydumper.SourceDir)) == 0 {
		var err error
		u, err = url.Parse(cfg.Mydumper.SourceDir)
		if err != nil {
			return common.ErrInvalidConfig.Wrap(err).GenWithStack("cannot parse `mydumper.data-source-dir` %s", cfg.Mydumper.SourceDir)
		}
	} else {
		u = &url.URL{}
	}

	// convert path and relative path to a valid file url
	if u.Scheme == "" {
		if cfg.Mydumper.SourceDir == "" {
			return common.ErrInvalidConfig.GenWithStack("`mydumper.data-source-dir` is not set")
		}
		if !common.IsDirExists(cfg.Mydumper.SourceDir) {
			return common.ErrInvalidConfig.GenWithStack("'%s': `mydumper.data-source-dir` does not exist", cfg.Mydumper.SourceDir)
		}
		absPath, err := filepath.Abs(cfg.Mydumper.SourceDir)
		if err != nil {
			return common.ErrInvalidConfig.Wrap(err).GenWithStack("covert data-source-dir '%s' to absolute path failed", cfg.Mydumper.SourceDir)
		}
		u.Path = filepath.ToSlash(absPath)
		u.Scheme = "file"
		cfg.Mydumper.SourceDir = u.String()
	}

	found := false
	for _, t := range supportedStorageTypes {
		if u.Scheme == t {
			found = true
			break
		}
	}
	if !found {
		return common.ErrInvalidConfig.GenWithStack(
			"unsupported data-source-dir url '%s', supported storage types are %s",
			cfg.Mydumper.SourceDir, strings.Join(supportedStorageTypes, ","))
	}
	return nil
}

// AdjustCheckPoint adjusts the checkpoint.
func (cfg *Config) AdjustCheckPoint() {
	if len(cfg.Checkpoint.Schema) == 0 {
		cfg.Checkpoint.Schema = "tidb_lightning_checkpoint"
	}
	if len(cfg.Checkpoint.Driver) == 0 {
		cfg.Checkpoint.Driver = CheckpointDriverFile
	}
	if len(cfg.Checkpoint.DSN) == 0 {
		switch cfg.Checkpoint.Driver {
		case CheckpointDriverMySQL:
			param := common.MySQLConnectParam{
				Host:                     cfg.TiDB.Host,
				Port:                     cfg.TiDB.Port,
				User:                     cfg.TiDB.User,
				Password:                 cfg.TiDB.Psw,
				SQLMode:                  mysql.DefaultSQLMode,
				MaxAllowedPacket:         defaultMaxAllowedPacket,
				TLSConfig:                cfg.TiDB.Security.TLSConfig,
				AllowFallbackToPlaintext: cfg.TiDB.Security.AllowFallbackToPlaintext,
			}
			cfg.Checkpoint.MySQLParam = &param
		case CheckpointDriverFile:
			cfg.Checkpoint.DSN = "/tmp/" + cfg.Checkpoint.Schema + ".pb"
		}
	} else {
		// try to remove allowAllFiles
		mysqlCfg, err := gomysql.ParseDSN(cfg.Checkpoint.DSN)
		if err != nil {
			return
		}
		mysqlCfg.AllowAllFiles = false
		cfg.Checkpoint.DSN = mysqlCfg.FormatDSN()
	}
}

// AdjustMydumper adjusts the mydumper config.
func (cfg *Config) AdjustMydumper() {
	if cfg.Mydumper.BatchImportRatio < 0.0 || cfg.Mydumper.BatchImportRatio >= 1.0 {
		cfg.Mydumper.BatchImportRatio = DefaultBatchImportRatio
	}
	if cfg.Mydumper.ReadBlockSize <= 0 {
		cfg.Mydumper.ReadBlockSize = ReadBlockSize
	}
	if len(cfg.Mydumper.CharacterSet) == 0 {
		cfg.Mydumper.CharacterSet = "auto"
	}

	if len(cfg.Mydumper.IgnoreColumns) != 0 {
		// Tolower columns cause we use Name.L to compare column in tidb.
		for _, ig := range cfg.Mydumper.IgnoreColumns {
			cols := make([]string, len(ig.Columns))
			for i, col := range ig.Columns {
				cols[i] = strings.ToLower(col)
			}
			ig.Columns = cols
		}
	}
}

// CheckAndAdjustSecurity checks and adjusts the security config.
func (cfg *Config) CheckAndAdjustSecurity() error {
	if cfg.TiDB.Security == nil {
		cfg.TiDB.Security = &cfg.Security
	}

	switch cfg.TiDB.TLS {
	case "skip-verify", "preferred":
		if cfg.TiDB.Security.TLSConfig == nil {
			/* #nosec G402 */
			cfg.TiDB.Security.TLSConfig = &tls.Config{
				MinVersion:         tls.VersionTLS10,
				InsecureSkipVerify: true,
				NextProtos:         []string{"h2", "http/1.1"}, // specify `h2` to let Go use HTTP/2.
			}
			cfg.TiDB.Security.AllowFallbackToPlaintext = true
		}
	case "cluster":
		if len(cfg.Security.CAPath) == 0 {
			return common.ErrInvalidConfig.GenWithStack("cannot set `tidb.tls` to 'cluster' without a [security] section")
		}
	case "", "false":
		cfg.TiDB.TLS = "false"
		return nil
	default:
		return common.ErrInvalidConfig.GenWithStack("unsupported `tidb.tls` config %s", cfg.TiDB.TLS)
	}
	return nil
}

// HasLegacyBlackWhiteList checks whether the deprecated [black-white-list] section
// was defined.
func (cfg *Config) HasLegacyBlackWhiteList() bool {
	return len(cfg.BWList.DoTables) != 0 || len(cfg.BWList.DoDBs) != 0 || len(cfg.BWList.IgnoreTables) != 0 || len(cfg.BWList.IgnoreDBs) != 0
}
