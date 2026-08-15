package tsdb

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/mababaNiubi/qv-lite/container"

	"github.com/mababaNiubi/variant"
)

type WalConfig struct {
	// MaxFileSize is the maximum size of the WAL cache in bytes. Default 64M.
	MaxFileSize int64 `json:"max_file_size"`
	// MaxFileNumber is the maximum number of WAL files.
	MaxFileNumber int `json:"max_file_number"`
	// CloseBuffer disables the WAL write buffer when true.
	CloseBuffer bool `json:"close_buffer"`
	// MaxBufferBatchSize is the maximum number of entries to buffer in memory
	// before sorting by timestamp and flushing to the WAL file. Default 10000.
	MaxBufferBatchSize int `json:"max_buffer_batch_size"`
}

func (config *WalConfig) setDefaultValues() {
	if config.MaxFileSize <= 0 {
		config.MaxFileSize = 64 * 1024 * 1024
	}
	if config.MaxBufferBatchSize <= 0 {
		config.MaxBufferBatchSize = 4096
	}
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

	// AsyncFlush enables asynchronous flushCache processing. When true, WAL data
	// is encoded and flushed to disk segments in a background goroutine instead
	// of blocking the write path. Failed flushes are retried on the next trigger
	// since the data stays in the WAL until a successful flush. Default false.
	AsyncFlush bool `json:"async_flush"`
	// AsyncCleanup enables asynchronous periodic cleanup of expired data files.
	// When true, expired segments are removed by a background goroutine on a
	// fixed interval instead of inline during each write. Default false.
	AsyncCleanup bool `json:"async_cleanup"`
	// CleanupIntervalSeconds is the interval, in seconds, between periodic
	// cleanup sweeps when AsyncCleanup is enabled. An initial sweep runs
	// immediately on startup. Default 60.
	CleanupIntervalSeconds int64 `json:"cleanup_interval_seconds"`
}

const DefaultTableName = "default"

type DB struct {
	tableInfos []TableInfo
	Config
	ssTables container.SyncMap[string, *ssTable]
	// tableCache resolves recent (tableName → table) hits without touching
	// ssTables' map on every Write. Table mappings are permanent once created.
	tableCache container.StringKeyCache[*ssTable]
	ctx        context.Context
	cancel     context.CancelFunc
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
	if config.SecondaryCompressionName == "" {
		config.SecondaryCompressionName = "zstd"
	}
	config.WalConfig.setDefaultValues()
	if config.CleanupIntervalSeconds <= 0 {
		config.CleanupIntervalSeconds = 60
	}
	db := &DB{
		Config:     config,
		tableCache: *container.NewStringKeyCache[*ssTable](tableCacheSlots),
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
	config := WalConfig{
		CloseBuffer:        db.WalConfig.CloseBuffer,
		MaxFileNumber:      db.WalConfig.MaxFileNumber,
		MaxBufferBatchSize: db.WalConfig.MaxBufferBatchSize,
		MaxFileSize:        db.WalConfig.MaxFileSize,
	}
	if len(db.tableInfos) > 0 {
		config.MaxFileSize = db.WalConfig.MaxFileSize / int64(len(db.tableInfos))
		config.MaxBufferBatchSize = db.WalConfig.MaxBufferBatchSize / len(db.tableInfos)
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
			db.MaxStorageTime*int64(time.Second),
			db.SecondaryCompressionName,
			config,
			db.ctx,
			db.AsyncFlush,
			db.AsyncCleanup,
			time.Duration(db.CleanupIntervalSeconds)*time.Second)
		if err != nil {
			return err
		}
		db.ssTables.Store(tableName, table)
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
	config := WalConfig{
		CloseBuffer:        db.WalConfig.CloseBuffer,
		MaxFileNumber:      db.WalConfig.MaxFileNumber,
		MaxBufferBatchSize: db.WalConfig.MaxBufferBatchSize,
		MaxFileSize:        db.WalConfig.MaxFileSize,
	}
	if len(db.tableInfos) > 0 {
		config.MaxFileSize = db.WalConfig.MaxFileSize / int64(len(db.tableInfos))
		config.MaxBufferBatchSize = db.WalConfig.MaxBufferBatchSize / len(db.tableInfos)
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
		db.MaxStorageTime*int64(time.Second),
		db.SecondaryCompressionName, config,
		db.ctx,
		db.AsyncFlush,
		db.AsyncCleanup,
		time.Duration(db.CleanupIntervalSeconds)*time.Second)
	if err != nil {
		return err
	}
	db.ssTables.Store(tableConfig.Name, table)
	db.ssTables.Range(func(k string, v *ssTable) bool {
		v.walFile.updateWalConfig(config)
		return true
	})
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
	var err error
	db.ssTables.Range(func(key string, value *ssTable) bool {
		err = value.Close()
		if err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	db.cancel()
	return nil
}

func (db *DB) getTable(tableName string) (*ssTable, error) {
	tableName = db.resolveTableName(tableName)
	if table, ok := db.tableCache.Lookup(tableName); ok {
		return table, nil
	}
	table, ok := db.ssTables.Load(tableName)
	if !ok {
		if tableName == DefaultTableName {
			if err := db.ensureDefaultTable(); err != nil {
				return nil, err
			}
			table, _ = db.ssTables.Load(tableName)
		} else {
			return nil, ErrorTableNotExists
		}
	}
	db.tableCache.Store(tableName, table)
	return table, nil
}

// Write writes a data point to the specified table and tag. Returns whether the data was actually written.
// An empty tableName writes to the default table, which is auto-created on first use.
func (db *DB) Write(tableName string, tag string, timestamp int64, value variant.Variant) (bool, error) {
	table, err := db.getTable(tableName)
	if err != nil {
		return false, err
	}
	return table.Write(tag, timestamp, value)
}

// WriteBatch writes multiple data points to the specified table in a single batch.
// This acquires the WAL mutex once instead of once per point, reducing lock contention.
// An empty tableName writes to the default table.
func (db *DB) WriteBatch(tableName string, points []TagPoint) (int, error) {
	if len(points) == 0 {
		return 0, nil
	}
	table, err := db.getTable(tableName)
	if err != nil {
		return 0, err
	}
	return table.WriteBatch(points)
}

func (db *DB) Query(tableName string, tag string, startTime int64, endTime int64, windowSize int64, polymerization uint8, cond any) ([]Point, error) {
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
	// windowSize <= 0 means return all raw data without aggregation.
	if windowSize <= 0 {
		return table.Query(tag, startTime, endTime, cond)
	}
	return table.QueryWindow(tag, startTime, endTime, windowSize, polymerization, cond)
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
