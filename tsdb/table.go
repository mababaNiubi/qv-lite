package tsdb

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mababaNiubi/variant"
)

type tagCode uint32

type ssTable struct {
	tableInfo TableInfo
	dirPath   string // Table directory path.
	*Meta
	fragmentation          fileSegmentList
	columns                []*ssColumn
	maxSegmentSize         int64
	maxSegmentTimeInterval int64
	expirationMinuteTime   int64
	maxStorageTime         int64
	walFile                WalFile
	batcher                *tableBatcher
	flushMute              sync.Mutex
	queryMute              sync.RWMutex // serializes queries with flush commit+truncate
	flushEpoch             uint64
	flushTouched           []*ssColumn

	// tagCacheSlots 是此表 Meta.tagCache 的槽数(2 的幂), 来自 Config.TagCacheSlots。
	tagCacheSlots int

	// Asynchronous processing (enabled via Config).
	asyncFlush      bool
	asyncCleanup    bool
	cleanupInterval time.Duration
	ctx             context.Context
	cancel          context.CancelFunc
	flushWg         sync.WaitGroup // tracks in-flight async flush goroutines
	cleanupDone     chan struct{}  // closed when the cleanup goroutine exits
	asyncErr        atomic.Value   // stores error from async flush
}

func mewSSTable(tableInfo TableInfo, dirPath string, maxSegmentSize, maxSegmentTimeInterval,
	expirationMinuteTime int64, dedupWindowMs, minIntervalMs, maxStorageTime int64, compressionName string, walConfig WalConfig,
	tagCacheSlots int, parentCtx context.Context, asyncFlush, asyncCleanup bool, cleanupInterval time.Duration,
	ingestConfig IngestConfig) (*ssTable, error) {
	s := &ssTable{
		tableInfo:              tableInfo,
		dirPath:                dirPath,
		columns:                make([]*ssColumn, 0),
		maxSegmentSize:         maxSegmentSize,
		expirationMinuteTime:   expirationMinuteTime,
		maxSegmentTimeInterval: maxSegmentTimeInterval,
		maxStorageTime:         maxStorageTime,
		tagCacheSlots:          tagCacheSlots,
		asyncFlush:             asyncFlush,
		asyncCleanup:           asyncCleanup,
		cleanupInterval:        cleanupInterval,
	}
	s.ctx, s.cancel = context.WithCancel(parentCtx)
	// Cancel the derived context if construction fails so it is not leaked.
	success := false
	defer func() {
		if !success {
			s.cancel()
		}
	}()
	var err error
	err = s.BuildColumn()
	if err != nil {
		return nil, err
	}
	s.fragmentation.readerCache = newReaderCache(defaultMaxOpenReaders)
	err = s.fragmentation.BuildFragmentation(s.dirPath, CompressorByName(compressionName))
	if err != nil {
		return nil, err
	}
	s.walFile, err = NewWalFile(s.dirPath, dedupWindowMs, minIntervalMs, walConfig)
	if err != nil {
		return nil, err
	}
	// Handle file corruption caused by an abnormal interruption during writes.
	lastPoints, err := s.fragmentation.InspectLastBlockIndex(&s.tableInfo)
	if err != nil {
		return nil, err
	}
	for k, lp := range lastPoints {
		s.walFile.SetLastPoint(k, lp.Tms, lp.V)
	}
	s.batcher = newTableBatcher(s, ingestConfig)
	// Start the background cleanup loop when async cleanup is enabled.
	if s.asyncCleanup {
		s.cleanupDone = make(chan struct{})
		go s.runCleanupLoop()
	}
	success = true
	return s, nil
}

func (s *ssTable) Write(tag string, timestamp int64, value variant.Variant) (bool, error) {
	if s.walFile == nil || s.batcher == nil {
		return false, ErrorWALCacheIsNil
	}
	if err := s.validateWrite(timestamp, value, time.Now().UnixNano()); err != nil {
		return false, err
	}
	if err := s.batcher.add(tag, timestamp, value); err != nil {
		return false, err
	}
	return true, nil
}

// WriteBatch validates and appends multiple raw-tag points to the sharded
// table buffer. Tag resolution and WAL I/O happen on the background worker.
func (s *ssTable) WriteBatch(points []TagPoint) (int, error) {
	if s.walFile == nil || s.batcher == nil {
		return 0, ErrorWALCacheIsNil
	}
	now := time.Now().UnixNano()
	for _, p := range points {
		if err := s.validateWrite(p.Timestamp, p.Value, now); err != nil {
			return 0, err
		}
	}
	if err := s.batcher.addBatch(points); err != nil {
		return 0, err
	}
	return len(points), nil
}

func (s *ssTable) validateWrite(timestamp int64, value variant.Variant, now int64) error {
	if value.IsEmpty() {
		return ErrorValueIsEmpty
	}
	if s.maxStorageTime != 0 && now+s.maxStorageTime < timestamp {
		return ErrorTimeOut
	}
	return nil
}

// commitRawWriteBatch runs off the caller path. It resolves each distinct tag
// once, sorts only out-of-order tag buffers, persists any newly created tag
// codes to Meta, and only then appends the prepared tag runs to the WAL.
func (s *ssTable) commitRawWriteBatch(batch *frozenWriteBatch) (int, error) {
	runs, err := s.batcher.prepareRawWriteBatch(batch)
	if err != nil {
		return 0, err
	}
	// Durability ordering: codes created by prepareRawWriteBatch must be fsynced
	// before the WAL bytes that reference them reach the OS. Otherwise a crash
	// leaves WAL points whose codes Meta cannot resolve, breaking recovery. One
	// Meta fsync per batch; a no-op when the batch introduced no new tags.
	if err := s.Meta.FlushPending(); err != nil {
		return 0, err
	}
	results, err := s.walFile.WriteRuns(runs)
	if err == ErrorWALCacheFull {
		if flushErr := s.flushBlocking(); flushErr != nil {
			return results, flushErr
		}
		results, err = s.walFile.WriteRuns(runs)
	}
	if err != nil {
		return results, err
	}
	if s.walFile.NeedFlush() {
		if err = s.maybeFlush(); err != nil {
			return results, err
		}
	}
	return results, nil
}

func (s *ssTable) flushCache() error {
	// Hold queryMute for the entire flush so a concurrent query cannot open
	// a segment that is mid-transaction (TxActive). The reader's crash
	// recovery would truncate such a file, corrupting the flush.
	s.queryMute.Lock()
	defer s.queryMute.Unlock()

	// Flush already accepted WAL bytes before encoding complete files.
	if s.walFile != nil {
		_ = s.walFile.FlushPending()
	}

	completeCount, needsSort := s.walFile.completeFileState()
	if completeCount == 0 {
		return nil
	}

	var entries []flushEntry
	lastReadFile := -1
	lastReadOffset := int64(0)
	repairReadError := func(consumed int, readErr error) error {
		truncateSize := int64(0)
		if lastReadFile == consumed {
			truncateSize = lastReadOffset
		}
		if err := s.walFile.retainWalFilePrefix(consumed, truncateSize); err != nil {
			return err
		}
		return readErr
	}

	consumed := completeCount
	if needsSort {
		entries = make([]flushEntry, 0, 4096)
		var readErr error
		consumed, readErr = s.walFile.forEachCompleteFile(completeCount, func(fileIndex int, tag tagCode, timestamp int64, value variant.Variant, offset int64) bool {
			entries = append(entries, flushEntry{tag: tag, timestamp: timestamp, value: value})
			lastReadFile = fileIndex
			lastReadOffset = offset
			return true
		})
		if readErr != nil {
			return repairReadError(consumed, readErr)
		}
		if len(entries) == 0 {
			s.walFile.truncate(consumed)
			return nil
		}
		sort.SliceStable(entries, func(i, j int) bool {
			if entries[i].tag != entries[j].tag {
				return entries[i].tag < entries[j].tag
			}
			return entries[i].timestamp < entries[j].timestamp
		})
	}

	if err := s.fragmentation.OpenTransaction(); err != nil {
		return err
	}

	s.flushEpoch++
	if s.flushEpoch == 0 {
		// Only reachable after 2^64 flushes. Keep zero reserved for columns that
		// have never participated in a flush.
		for _, column := range s.columns {
			column.lastFlushEpoch = 0
		}
		s.flushEpoch = 1
	}
	epoch := s.flushEpoch
	s.flushTouched = s.flushTouched[:0]

	rollbackTransaction := func() error {
		for _, column := range s.flushTouched {
			column.Reset()
		}
		return s.fragmentation.RollbackLastCommitTransaction()
	}

	var processErr error
	processEntry := func(tag tagCode, timestamp int64, value variant.Variant) bool {
		if tag == 0 || int(tag) > len(s.columns) {
			processErr = ErrorWALDataCorruption
			return false
		}
		column := s.columns[tag-1]
		if column.lastFlushEpoch != epoch {
			column.lastFlushEpoch = epoch
			s.flushTouched = append(s.flushTouched, column)
		}
		glowNot, err := column.Write(timestamp, value)
		if err != nil {
			processErr = err
			return false
		}
		if !glowNot {
			if _, err := column.glowWrite(&s.fragmentation); err != nil {
				processErr = err
				return false
			}
		}
		return true
	}

	if needsSort {
		for i := range entries {
			entry := &entries[i]
			if !processEntry(entry.tag, entry.timestamp, entry.value) {
				break
			}
		}
	} else {
		var readErr error
		consumed, readErr = s.walFile.forEachCompleteFile(completeCount, func(fileIndex int, tag tagCode, timestamp int64, value variant.Variant, offset int64) bool {
			lastReadFile = fileIndex
			lastReadOffset = offset
			return processEntry(tag, timestamp, value)
		})
		if processErr != nil {
			_ = rollbackTransaction()
			return processErr
		}
		if readErr != nil {
			if err := rollbackTransaction(); err != nil {
				return err
			}
			return repairReadError(consumed, readErr)
		}
	}
	if processErr != nil {
		_ = rollbackTransaction()
		return processErr
	}
	if len(s.flushTouched) == 0 {
		if err := rollbackTransaction(); err != nil {
			return err
		}
		s.walFile.truncate(consumed)
		return nil
	}

	// Flush remaining encoder data to disk.
	var err error
	for _, column := range s.flushTouched {
		_, err = column.glowWrite(&s.fragmentation)
		if err != nil {
			break
		}
	}
	if err != nil {
		_ = rollbackTransaction()
		return err
	}
	// Commit the data segments and truncate the consumed WAL files.
	err = s.fragmentation.CommitTransactionFileSegment()
	if err != nil {
		_ = rollbackTransaction()
		return err
	}
	// Truncate WAL after successful flush.
	s.walFile.truncate(consumed)
	return nil
}

// flushAndCleanup encodes buffered WAL data to disk segments and, when cleanup
// is handled inline, evicts expired segments. It must be called while holding
// flushMute.
func (s *ssTable) flushAndCleanup() error {
	if err := s.flushCache(); err != nil {
		return err
	}
	// Inline cleanup only when the background cleanup loop is not running.
	if !s.asyncCleanup && s.expirationMinuteTime != 0 {
		s.fragmentation.Remove(time.Now().UnixNano() - s.expirationMinuteTime)
	}
	return nil
}

// flushBlocking acquires flushMute with a blocking lock and drains all
// complete WAL files. Used as backpressure when the WAL is full instead
// of returning ErrorWALCacheFull to the caller.
func (s *ssTable) flushBlocking() error {
	// Surface any prior async flush error before waiting.
	if v := s.asyncErr.Load(); v != nil {
		return v.(error)
	}
	s.flushMute.Lock()
	defer s.flushMute.Unlock()
	for s.walFile.NeedFlush() {
		if err := s.flushAndCleanup(); err != nil {
			return err
		}
	}
	return nil
}

// maybeFlush triggers a flush when the WAL cache exceeds the threshold. In
// async mode the encoding runs in a background goroutine; otherwise it runs
// inline, blocking the caller. At most one flush runs at a time per table:
// flushMute serializes access, and the write path uses a non-blocking TryLock
// so concurrent writers skip when a flush is already in progress.
func (s *ssTable) maybeFlush() error {
	if !s.walFile.NeedFlush() {
		return nil
	}
	if !s.flushMute.TryLock() {
		return nil
	}
	if s.asyncFlush {
		// Re-check under lock: another goroutine may have flushed already.
		if s.walFile.NeedFlush() {
			s.flushWg.Add(1)
			go func() {
				defer s.flushWg.Done()
				defer s.flushMute.Unlock()
				// Drain loop: keep flushing until the WAL is back to a single
				// file. Concurrent writes may add files between iterations, so
				// re-check NeedFlush after each successful flush.
				for {
					if err := s.flushAndCleanup(); err != nil {
						s.setAsyncErr(err)
						return
					}
					if !s.walFile.NeedFlush() {
						return
					}
				}
			}()
		} else {
			s.flushMute.Unlock()
		}
		return nil
	}
	// Synchronous path (default): flush inline.
	defer s.flushMute.Unlock()
	if s.walFile.NeedFlush() {
		return s.flushAndCleanup()
	}
	return nil
}

// runCleanupLoop periodically removes expired data segments in the background.
// It exits when the table context is cancelled (e.g. on Close).
func (s *ssTable) runCleanupLoop() {
	defer close(s.cleanupDone)
	if s.expirationMinuteTime == 0 {
		return
	}
	interval := s.cleanupInterval
	if interval <= 0 {
		interval = 60 * time.Second
	}
	// Initial sweep: clean up files that expired while the database was down.
	s.cleanupExpired()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.cleanupExpired()
		}
	}
}

// cleanupExpired removes segments older than the expiration window. It acquires
// flushMute to avoid racing with a concurrent flushCache: glowWrite reads the
// segment list via GetLastFragmentation without the segment mutex, so cleanup
// must not restructure the list at the same time.
func (s *ssTable) cleanupExpired() {
	s.flushMute.Lock()
	defer s.flushMute.Unlock()
	s.fragmentation.Remove(time.Now().UnixNano() - s.expirationMinuteTime)
}

func (s *ssTable) setAsyncErr(err error) {
	s.asyncErr.Store(err)
}

func (s *ssTable) BuildColumn() error {
	s.flushMute.Lock()
	defer s.flushMute.Unlock()
	meta, err := NewMeta(s.dirPath, s.tagCacheSlots)
	if err != nil {
		return err
	}
	s.Meta = meta
	s.columns = make([]*ssColumn, s.MaxPointDict)
	s.Meta.Range(func(k string, u tagCode) bool {
		// tag 编码 1 起始（addTag 自增），columns 数组 0 起始（columns[tag-1]）。
		s.columns[u-1] = newSSColumn(u, &s.tableInfo, s.maxSegmentSize, s.maxSegmentTimeInterval)
		return true
	})
	return nil
}

func (s *ssTable) CreateColumn(tag string) (tagCode, error) {
	s.flushMute.Lock()
	defer s.flushMute.Unlock()

	// Double-check: another goroutine may have created this tag while we waited.
	if code, ok := s.Meta.Load(tag); ok {
		return code, nil
	}

	if s.MaxPointDict == maxColumnTag {
		return 0, ErrorPointQuantityExceedsLimit
	}

	code, err := s.Meta.addTag(tag)
	if err != nil {
		return 0, err
	}
	for i := tagCode(len(s.columns)); i < code; i++ {
		s.columns = append(s.columns, newSSColumn(i+1, &s.tableInfo, s.maxSegmentSize, s.maxSegmentTimeInterval))
	}
	return code, nil
}

func (s *ssTable) Query(tag string, startTime int64, endTime int64, cond any) ([]Point, error) {
	if s.batcher != nil {
		if err := s.batcher.Flush(); err != nil {
			return nil, err
		}
	}
	code, ok := s.Meta.Load(tag)
	if !ok {
		return nil, ErrorTagNotFound
	}
	var points pointCollector
	err := s.forEachQueryPoint(code, startTime, endTime, compileCond(cond), nil, func(p Point) bool {
		points.append(p)
		return true
	})
	if err != nil {
		return nil, err
	}
	return points.result(), nil
}

// QueryIter streams query results for one tag in time order. The returned
// iterator owns the table's query lock and must be closed by the caller.
// opts controls limit/offset; nil means unbounded.
func (s *ssTable) QueryIter(ctx context.Context, tag string, startTime int64, endTime int64, cond any, opts *QueryOptions) (PointIter, error) {
	if s.batcher != nil {
		if err := s.batcher.Flush(); err != nil {
			return nil, err
		}
	}
	code, ok := s.Meta.Load(tag)
	if !ok {
		return nil, ErrorTagNotFound
	}
	return s.queryIter(ctx, code, startTime, endTime, compileCond(cond), opts), nil
}

// QueryLatest returns the most recent data point for the specified tag.
func (s *ssTable) QueryLatest(tag string) (*Point, error) {
	if s.batcher != nil {
		if err := s.batcher.Flush(); err != nil {
			return nil, err
		}
	}
	code, ok := s.Meta.Load(tag)
	if !ok {
		return nil, ErrorTagNotFound
	}
	tms, value, ok := s.walFile.GetTagMaxTimestamp(code)
	if !ok {
		return nil, ErrorNoDataForTag
	}
	return &Point{
		Tms: tms,
		V:   value,
	}, nil
}

// isNumericType checks whether a variant's runtime type supports arithmetic aggregation.
func isNumericType(v variant.Variant) bool {
	switch v.Type() {
	case variant.TypeInt64, variant.TypeUInt64, variant.TypeFloat64:
		return true
	default:
		return false
	}
}

// QueryWindow queries data for a tag within a time range, aggregating within fixed-size windows.
// windowSize is the aggregation window in nanoseconds. fusion controls aggregation: 0=avg, 1=min, 2=max.
// It consumes a single two-phase disk+WAL stream, so windows span the flush
// boundary correctly and no full result set is ever materialized.
func (s *ssTable) QueryWindow(tag string, startTime int64, endTime int64, windowSize int64, fusion uint8, cond any) ([]Point, error) {
	if s.batcher != nil {
		if err := s.batcher.Flush(); err != nil {
			return nil, err
		}
	}
	code, ok := s.Meta.Load(tag)
	if !ok {
		return nil, ErrorTagNotFound
	}
	var interval = windowSize

	targetValue := variant.NewEmpty()
	var targetTms, varCount, lastTms int64
	var windowNumeric bool
	var windowType variant.Type
	// resetWindow begins a new aggregation window at the given point.
	resetWindow := func(tms int64, v variant.Variant) {
		lastTms = tms
		targetTms = tms
		targetValue = v
		varCount = 1
		windowNumeric = isNumericType(v)
		windowType = v.Type()
	}

	points := make([]Point, 0, 64)
	var aggErr error
	err := s.forEachQueryPoint(code, startTime, endTime, compileCond(cond), nil, func(p Point) bool {
		if aggErr != nil {
			return false
		}
		tms, v := p.Tms, p.V
		if lastTms == 0 {
			resetWindow(tms, v)
			return true
		}
		if tms-lastTms >= interval {
			points = append(points, Point{Tms: targetTms, V: targetValue})
			resetWindow(tms, v)
			return true
		}
		// If the window started with a non-numeric value, skip all aggregation
		// and keep the first value. Otherwise only aggregate numeric values.
		if !windowNumeric || !isNumericType(v) {
			return true
		}
		switch fusion {
		case MinFusion:
			if targetValue.Comparable(v) {
				targetValue = v
				targetTms = tms
			}
		case MaxFusion:
			if !targetValue.Comparable(v) {
				targetValue = v
				targetTms = tms
			}
		default:
			varCount++
			targetTms = targetTms + (tms-targetTms)/varCount
			// Numeric fast path: for a uniform Int64/Float64 window, mirror the
			// generic Reduce/Divide/Increase exactly (same IEEE/int64
			// operations, same wrap-on-overflow semantics) without the per-point
			// variant machinery. Mixed-type or UInt64 windows keep the generic
			// path.
			if v.Type() == windowType && (windowType == variant.TypeInt64 || windowType == variant.TypeFloat64) {
				if windowType == variant.TypeInt64 {
					ti, _ := targetValue.AsInt64()
					vi, _ := v.AsInt64()
					targetValue = variant.NewInt64(ti + (vi-ti)/varCount)
				} else {
					tf, _ := targetValue.AsFloat64()
					vf, _ := v.AsFloat64()
					targetValue = variant.NewFloat64(tf + (vf-tf)/float64(varCount))
				}
				return true
			}
			reduceVariant, err := v.Reduce(targetValue)
			if err != nil {
				aggErr = err
				return false
			}
			divideValue, err := reduceVariant.Divide(variant.NewInt64(varCount))
			if err != nil {
				aggErr = err
				return false
			}
			targetValue, err = targetValue.Increase(divideValue)
			if err != nil {
				aggErr = err
				return false
			}
		}
		return true
	})
	if aggErr != nil {
		return points, aggErr
	}
	if err != nil {
		return points, err
	}
	if lastTms != 0 {
		points = append(points, Point{
			Tms: targetTms,
			V:   targetValue,
		})
	}
	return points, nil
}

func (s *ssTable) Close() error {
	// Stop accepting new points and commit both active and queued MemTable
	// batches before shutting down WAL/segment workers.
	var batchErr error
	if s.batcher != nil {
		batchErr = s.batcher.Close()
	}
	// Stop background goroutines and wait for any in-flight async flush so that
	// files are not closed mid-transaction.
	if s.cancel != nil {
		s.cancel()
	}
	s.flushWg.Wait()
	if s.cleanupDone != nil {
		<-s.cleanupDone
	}
	// Close the WAL after the table batcher has committed every accepted point.
	var closeErr error
	if s.walFile != nil {
		closeErr = s.walFile.Close()
	}
	// Persist block-level indexes for all segments (including the last one).
	s.fragmentation.Range(func(fs, _ FileSegment) bool {
		_ = fs.PersistIndex()
		_ = fs.Cleanup()
		return true
	})
	if s.fragmentation.readerCache != nil {
		s.fragmentation.readerCache.closeAll()
	}
	if s.walFile != nil {
		s.walFile.removeOrphanedFiles()
	}
	var metaErr error
	if s.Meta != nil {
		metaErr = s.Meta.Close()
	}
	if closeErr != nil {
		return closeErr
	}
	if batchErr != nil {
		return batchErr
	}
	if metaErr != nil {
		return metaErr
	}
	// Surface the last asynchronous flush error, if any.
	if v := s.asyncErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}
