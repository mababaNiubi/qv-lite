package tsdb

import (
	"context"
	"encoding/json"
	"github.com/mababaNiubi/qv-lite/container"
	"os"
	"path/filepath"
	"time"

	"github.com/mababaNiubi/variant"
)

type WalConfig struct {
	// MaxCacheSize is the maximum size of the WAL cache in bytes. Default 64M.
	MaxCacheSize int64 `json:"max_cache_size"`
	// MaxFileNumber is the maximum number of WAL files.
	MaxFileNumber int `json:"max_file_number"`
	// CloseBuffer disables the WAL write buffer when true.
	CloseBuffer bool `json:"close_buffer"`
	// MaxBufferBatchSize is the maximum number of entries to buffer in memory
	// before sorting by timestamp and flushing to the WAL file. Default 10000.
	MaxBufferBatchSize int `json:"max_buffer_batch_size"`
}

type Config struct {
	// Path is the path to the database.
	Path string `json:"path"`
	// WalConfig groups WAL-related settings.
	WalConfig WalConfig `json:"wal_config"`
	// maxSegmentSize is the maximum size of a segment in bytes.
	// Default 64M
	MaxSegmentSize int64 `json:"max_segment_size"`
	// MaxSegmentTimeInterval is the maximum time interval of a segment.
	// Default 0 no restrictions
	MaxSegmentTimeInterval int64 `json:"max_segment_time_interval"`
	//Maximum storage time, data much larger than the current time is not allowed to be stored
	//Default 1h(s)
	MaxStorageTime int64 `json:"max_storage_time"`
	// ExpirationMinuteTime is the expiration time of the data in minutes
	// Default 0 is doing not expire data
	ExpirationMinuteTime int64 `json:"data_expiration_time"`
	// DedupWindowMs is the deduplication window in milliseconds.
	// If the same value is written for a tag within this window, the write is skipped.
	// Default 0 disables deduplication.
	DedupWindowMs int64 `json:"dedup_window_ms"`
	// MinIntervalMs is the minimum time interval between consecutive writes in milliseconds.
	// If a new data point is too close to the previous one (regardless of value), it is skipped.
	// Default 0 disables this check.
	MinIntervalMs int64 `json:"min_interval_ms"`
	// SecondaryCompressionName is the block compression algorithm: "zstd", "lz4", "snappy", "gzip", "none".
	// Default "zstd".
	SecondaryCompressionName string `json:"secondary_compression_name"`
}

const DefaultTableName = "default"

type DB struct {
	tableInfos []TableInfo
	Config
	ssTables    container.SyncMap[string, *ssTable]
	tableNumber uint64
	ctx         context.Context
	cancel      context.CancelFunc
}

func (db *DB) resolveTableName(name string) string {
	if name == "" {
		return DefaultTableName
	}
	return name
}

func (db *DB) ensureDefaultTable() error {
	if _, ok := db.ssTables.Load(DefaultTableName); ok {
		return nil
	}
	return db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{
			Name: DefaultTableName,
			Type: ColumnTypeUnknown,
		},
	})
}

func Open(config Config, ctx context.Context) (*DB, error) {
	if len(config.Path) == 0 {
		config.Path = "./qvLite-data"
	}
	if config.WalConfig.MaxCacheSize <= 0 {
		config.WalConfig.MaxCacheSize = 64 * 1024 * 1024
	}
	if config.WalConfig.MaxBufferBatchSize <= 0 {
		config.WalConfig.MaxBufferBatchSize = 10000
	}
	if config.SecondaryCompressionName == "" {
		config.SecondaryCompressionName = "zstd"
	}
	db := &DB{
		Config: config,
	}
	db.ctx, db.cancel = context.WithCancel(ctx)
	err := db.BuildTable()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) BuildTable() error {
	metaFilePath := filepath.Join(db.Path, tableInfoFile)
	fileData, err := os.ReadFile(metaFilePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if fileData != nil {
		if err := json.Unmarshal(fileData, &db.tableInfos); err != nil {
			return err
		}
	}
	for i := range db.tableInfos {
		tableName := db.tableInfos[i].Name
		table, err := mewSSTable(
			db.tableInfos[i],
			filepath.Join(db.Path, tableName),
			db.MaxSegmentSize,
			db.MaxSegmentTimeInterval*int64(time.Second),
			db.ExpirationMinuteTime*int64(time.Minute),
			db.DedupWindowMs*int64(time.Millisecond),
			db.MinIntervalMs*int64(time.Millisecond),
			db.SecondaryCompressionName,
			db.WalConfig)
		if err != nil {
			return err
		}
		db.ssTables.Store(tableName, table)
		db.tableNumber++
	}
	return nil
}

func (db *DB) CreateTable(tableConfig TableInfo) error {
	for _, tableInfo := range db.tableInfos {
		if tableInfo.Name == tableConfig.Name {
			return ErrorTableExists
		}
	}
	if tableConfig.FloatPrecision == 0 {
		tableConfig.FloatPrecision = 4
	}
	db.tableInfos = append(db.tableInfos, tableConfig)
	table, err := mewSSTable(
		tableConfig,
		filepath.Join(db.Path, tableConfig.Name),
		db.MaxSegmentSize,
		db.MaxSegmentTimeInterval*int64(time.Second),
		db.ExpirationMinuteTime*int64(time.Minute),
		db.DedupWindowMs*int64(time.Millisecond),
		db.MinIntervalMs*int64(time.Millisecond),
		db.SecondaryCompressionName, db.WalConfig,
	)
	if err != nil {
		return err
	}
	db.ssTables.Store(tableConfig.Name, table)
	db.tableNumber++
	// Persist table metadata to disk.
	marshal, err := json.Marshal(&db.tableInfos)
	if err != nil {
		return err
	}
	create, err := os.Create(filepath.Join(db.Path, tableInfoFile))
	if err != nil {
		return err
	}
	defer create.Close()
	_, err = create.Write(marshal)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) Close() error {
	var errChan = make(chan error, 1)
	defer close(errChan)
	db.ssTables.Range(func(key string, value *ssTable) bool {
		err := value.Close()
		if err != nil {
			errChan <- err
			return false
		}
		return true
	})
	select {
	case err := <-errChan:
		return err
	default:
	}
	db.cancel()
	return nil
}

func (db *DB) check(timestamp int64, value variant.Variant) error {
	if value.IsEmpty() {
		return ErrorValueIsEmpty
	}
	if db.MaxStorageTime != 0 {
		// Reject data with timestamps too far beyond the current time.
		if time.Now().UnixNano()+db.MaxStorageTime*int64(time.Second) < timestamp {
			return ErrorTimeOut
		}
	}
	return nil
}

// Write writes a data point to the specified table and tag. Returns whether the data was actually written.
// An empty tableName writes to the default table, which is auto-created on first use.
func (db *DB) Write(tableName string, tag string, timestamp int64, value variant.Variant) (bool, error) {
	tableName = db.resolveTableName(tableName)
	if err := db.check(timestamp, value); err != nil {
		return false, err
	}
	table, ok := db.ssTables.Load(tableName)
	if !ok {
		if tableName == DefaultTableName {
			if err := db.ensureDefaultTable(); err != nil {
				return false, err
			}
			table, _ = db.ssTables.Load(tableName)
		} else {
			return false, ErrorTableNotExists
		}
	}
	return table.Write(tag, timestamp, value)
}

func (db *DB) Query(tableName string, tag string, startTime int64, endTime int64, maxNumber int64, polymerization uint8, cond any) ([]Point, error) {
	tableName = db.resolveTableName(tableName)
	// Clamp query range to the expiration time window.
	if db.ExpirationMinuteTime != 0 {
		startTime = max(startTime, time.Now().Add(-time.Duration(db.ExpirationMinuteTime)*time.Minute).UnixNano())
		endTime = min(endTime, time.Now().Add(time.Duration(db.ExpirationMinuteTime)*time.Minute).UnixNano())
	}
	table, ok := db.ssTables.Load(tableName)
	if !ok {
		return nil, nil
	}
	if maxNumber == 0 {
		maxNumber = 10000
	}
	// For queries spanning less than 1 hour, read all data directly.
	if endTime-startTime > int64(time.Hour/time.Millisecond) {
		return table.QueryLimitNumber(tag, startTime, endTime, maxNumber, polymerization, cond)
	}
	return table.Query(tag, startTime, endTime, cond)
}

func (db *DB) QueryAll(tableName string, tag string, startTime int64, endTime int64, cond any) ([]Point, error) {
	tableName = db.resolveTableName(tableName)
	if db.ExpirationMinuteTime != 0 {
		startTime = max(startTime, time.Now().Add(-time.Duration(db.ExpirationMinuteTime)*time.Minute).UnixNano())
		endTime = min(endTime, time.Now().Add(time.Duration(db.ExpirationMinuteTime)*time.Minute).UnixNano())
	}
	table, ok := db.ssTables.Load(tableName)
	if !ok {
		return nil, nil
	}
	return table.Query(tag, startTime, endTime, cond)
}

// QueryLatest returns the most recent data point for the specified tag.
// An empty tableName queries the default table.
func (db *DB) QueryLatest(tableName string, tag string) (*Point, error) {
	tableName = db.resolveTableName(tableName)
	table, ok := db.ssTables.Load(tableName)
	if !ok {
		return nil, ErrorTableNotExists
	}
	return table.QueryLatest(tag)
}
