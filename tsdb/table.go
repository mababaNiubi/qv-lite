package tsdb

import (
	"context"
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
	flushMute              sync.Mutex
	queryMute              sync.RWMutex // serializes queries with flush commit+truncate

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
	parentCtx context.Context, asyncFlush, asyncCleanup bool, cleanupInterval time.Duration) (*ssTable, error) {
	s := &ssTable{
		tableInfo:              tableInfo,
		dirPath:                dirPath,
		columns:                make([]*ssColumn, 0),
		maxSegmentSize:         maxSegmentSize,
		expirationMinuteTime:   expirationMinuteTime,
		maxSegmentTimeInterval: maxSegmentTimeInterval,
		maxStorageTime:         maxStorageTime,
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
	// Persist tag metadata before any WAL bytes reach the OS, so tag codes are
	// always durable ahead of the points referencing them.
	s.walFile.SetPreFlush(s.Meta.FlushPending)
	// Handle file corruption caused by an abnormal interruption during writes.
	lastPoints, err := s.fragmentation.InspectLastBlockIndex(&s.tableInfo)
	if err != nil {
		return nil, err
	}
	for k, lp := range lastPoints {
		s.walFile.SetLastPoint(k, lp.Tms, lp.V)
	}
	// Start the background cleanup loop when async cleanup is enabled.
	if s.asyncCleanup {
		s.cleanupDone = make(chan struct{})
		go s.runCleanupLoop()
	}
	success = true
	return s, nil
}

func (s *ssTable) Write(tag string, timestamp int64, value variant.Variant) (bool, error) {
	if s.walFile == nil {
		return false, ErrorWALCacheIsNil
	}
	if value.IsEmpty() {
		return false, ErrorValueIsEmpty
	}
	if s.maxStorageTime != 0 && time.Now().UnixNano()+s.maxStorageTime < timestamp {
		// Reject data with timestamps too far beyond the current time.
		return false, ErrorTimeOut
	}
	// Look up the column index. Meta.Load serves hot tags from its cache.
	code, ok := s.Meta.Load(tag)
	if !ok {
		var err error
		code, err = s.CreateColumn(tag)
		if err != nil {
			return false, err
		}
	}
	// Write to WAL cache.
	ok, _, err := s.walFile.Write(code, timestamp, value)
	if err != nil {
		if err == ErrorWALCacheFull {
			// Backpressure: flush synchronously, then retry.
			if flushErr := s.flushBlocking(); flushErr != nil {
				return ok, flushErr
			}
			ok, _, err = s.walFile.Write(code, timestamp, value)
		}
		return ok, err
	}
	// Flush to disk when the cache exceeds the size limit.
	if s.walFile.NeedFlush() {
		if err = s.maybeFlush(); err != nil {
			return ok, err
		}
	}
	return ok, nil
}

// WriteBatch writes multiple data points under a single WAL mutex acquisition,
// reducing lock contention compared to calling Write repeatedly.
func (s *ssTable) WriteBatch(points []TagPoint) (int, error) {
	if s.walFile == nil {
		return 0, ErrorWALCacheIsNil
	}
	// Resolve all tag codes, creating columns for new tags.
	entries := make([]walDataEntry, len(points))
	for i, p := range points {
		code, ok := s.Meta.Load(p.Tag)
		if !ok {
			var err error
			code, err = s.CreateColumn(p.Tag)
			if err != nil {
				return 0, err
			}
		}
		if p.Value.IsEmpty() {
			return 0, ErrorValueIsEmpty
		}
		if s.maxStorageTime != 0 && time.Now().UnixNano()+s.maxStorageTime < p.Timestamp {
			// Reject data with timestamps too far beyond the current time.
			return 0, ErrorTimeOut
		}
		entries[i] = walDataEntry{
			Key:       code,
			Timestamp: p.Timestamp,
			Value:     p.Value,
		}
	}

	results, err := s.walFile.WriteBatch(entries)
	if err != nil {
		if err == ErrorWALCacheFull {
			// Backpressure: flush synchronously, then retry.
			if flushErr := s.flushBlocking(); flushErr != nil {
				return results, flushErr
			}
			results, err = s.walFile.WriteBatch(entries)
		}
		return results, err
	}

	// Check flush once after the batch.
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

	// Flush the active WAL file's unflushed chunk first so that
	// complete files only contain flushed data during encoding.
	if s.walFile != nil {
		_ = s.walFile.FlushPending()
	}

	// Open or create a data segment.
	var err, readErr error
	err = s.fragmentation.OpenTransaction()
	if err != nil {
		return err
	}
	// Iterate over WAL data and write to column encoders.
	readSize := int64(0)
	// Track the position of the last successfully read entry for error recovery.
	errIndex := 0
	var consumed int
	consumed, err = s.walFile.forEachCompleteFile(func(fileIndex int, tag tagCode, timestamp int64, value variant.Variant, offset int64) bool {
		column := s.columns[tag-1]
		// Find the column for this tag and write the data point.
		glowNot := true
		glowNot, readErr = column.Write(timestamp, value)
		if !glowNot {
			// Flush encoder data to disk.
			var needNewFile bool
			needNewFile, readErr = column.glowWrite(&s.fragmentation)
			if readErr != nil {
				return false
			}
			// Create a new data segment if the disk size limit is exceeded.
			if needNewFile {
				_ = s.fragmentation.PersistLastIndex()
				readErr = s.fragmentation.AddTransactionSegment()
				if readErr != nil {
					return false
				}
			}
		}
		if readErr != nil {
			errIndex = fileIndex
			return false
		}
		readSize = offset
		return true
	})
	if err != nil || readErr != nil {
		// Reset all encoder state.
		for _, column := range s.columns {
			column.Reset()
		}
		// Roll back all data segments.
		errRollback := s.fragmentation.RollbackLastCommitTransaction()
		if errRollback != nil {
			return errRollback
		}
		// Truncate WAL to the last valid position on read error.
		err2 := s.walFile.retainWalFilePrefix(errIndex, readSize)
		if err2 != nil {
			return err2
		}
		if readErr != nil {
			return readErr
		}
		return err
	}
	// Flush remaining encoder data to disk.
	for _, column := range s.columns {
		var needNewFile bool
		needNewFile, err = column.glowWrite(&s.fragmentation)
		if err != nil {
			break
		}
		if needNewFile {
			_ = s.fragmentation.PersistLastIndex()
			err = s.fragmentation.AddTransactionSegment()
			if err != nil {
				break
			}
		}
	}
	// Commit the data segments and truncate the consumed WAL files.
	err = s.fragmentation.CommitTransactionFileSegment()
	if err != nil {
		// On commit failure, roll back to protect data integrity.
		errRollback := s.fragmentation.RollbackLastCommitTransaction()
		if errRollback != nil {
			return errRollback
		}
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
	meta, err := NewMeta(s.dirPath)
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

func (s *ssTable) queryCache(code tagCode, startTime int64, endTime int64, evalCond ConditionFilter) ([]Point, error) {
	allPoints, err := s.walFile.ReadByTime(code, startTime, endTime)
	if err != nil {
		return nil, err
	}
	points := make([]Point, 0, len(allPoints))
	for i := range allPoints {
		condition, err := evalCond(allPoints[i].V)
		if err != nil {
			return nil, err
		}
		if condition {
			points = append(points, allPoints[i])
		}
	}
	return points, nil
}

// forEachBlock iterates over matching data blocks. When the index is available,
// random access is preferred; otherwise, a sequential scan is used.
func (s *ssTable) forEachBlock(code tagCode, startTime, endTime int64, handle func(head *SegmentHeader, timeData, valueData []byte) error) error {
	var err error
	s.fragmentation.RangeFromTime(startTime, endTime, func(fs FileSegment) bool {
		idx := fs.GetIndex()
		if idx != nil && len(idx.Blocks) > 0 {
			if startTime > idx.MaxTime || endTime < idx.MinTime {
				return true
			}
			matching := make([]BlockIndexEntry, 0, 5)
			for i := range idx.Blocks {
				b := &idx.Blocks[i]
				if b.Attribute != code || startTime > b.MaxTime || endTime < b.MinTime {
					continue
				}
				matching = append(matching, *b)
			}
			if len(matching) > len(idx.Blocks)/2 || len(matching) > 100 {
				err = s.scanSegment(fs, code, startTime, endTime, handle)
				return true
			}
			for i := range matching {
				head, td, vd, err2 := fs.ReadAt(matching[i].Offset, &s.tableInfo)
				if err2 != nil || head == nil {
					continue
				}
				if err = handle(head, td, vd); err != nil {
					return false
				}
			}
			fs.CloseReader()
			return true
		}
		err = s.scanSegment(fs, code, startTime, endTime, handle)
		return true
	})
	return err
}

// scanSegment sequentially scans a segment for matching blocks.
func (s *ssTable) scanSegment(fs FileSegment, code tagCode, startTime, endTime int64, handle func(head *SegmentHeader, timeData, valueData []byte) error) error {
	if e := fs.OpenReader(); e != nil {
		return e
	}
	defer fs.CloseReader()
	for {
		head, td, vd, e := fs.NextReadFilter(code, startTime, endTime, &s.tableInfo)
		if e != nil || head == nil {
			return e
		}
		if e = handle(head, td, vd); e != nil {
			return e
		}
	}
}

func (s *ssTable) queryDisk(code tagCode, startTime int64, endTime int64, evalCond ConditionFilter) ([]Point, error) {
	var points pointCollector
	pack := NewPointDiskPack(s.tableInfo.Structure, startTime, endTime)
	err := s.forEachBlock(code, startTime, endTime, func(head *SegmentHeader, compressedTimeData, compressedValueData []byte) error {
		pack.Reset()
		if e := pack.AddSegment(compressedTimeData, compressedValueData); e != nil {
			return e
		}
		for pack.Next() {
			tms, value := pack.Read()
			ok, e := evalCond(value)
			if e != nil {
				return e
			}
			if ok {
				points.append(Point{Tms: tms, V: value})
			}
		}
		return nil
	})
	return points.result(), err
}

func (s *ssTable) Query(tag string, startTime int64, endTime int64, cond any) ([]Point, error) {
	code, ok := s.Meta.Load(tag)
	if !ok {
		return nil, ErrorTagNotFound
	}
	evalCond := CompileCondition(cond)
	s.queryMute.RLock()
	cachePoints, err := s.queryCache(code, startTime, endTime, evalCond)
	if err != nil {
		s.queryMute.RUnlock()
		return nil, err
	}
	disk, err := s.queryDisk(code, startTime, endTime, evalCond)
	s.queryMute.RUnlock()
	if err != nil {
		return nil, err
	}
	if len(cachePoints) == 0 {
		return disk, nil
	}
	result := make([]Point, 0, len(disk)+len(cachePoints))
	result = append(result, disk...)
	result = append(result, cachePoints...)
	return result, nil
}

// QueryLatest returns the most recent data point for the specified tag.
func (s *ssTable) QueryLatest(tag string) (*Point, error) {
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
func (s *ssTable) QueryWindow(tag string, startTime int64, endTime int64, windowSize int64, fusion uint8, cond any) ([]Point, error) {
	code, ok := s.Meta.Load(tag)
	if !ok {
		return nil, ErrorTagNotFound
	}
	var interval = windowSize

	s.queryMute.RLock()
	defer s.queryMute.RUnlock()

	evalCond := CompileCondition(cond)
	targetValue := variant.NewEmpty()
	var targetTms, varCount, lastTms int64
	var windowNumeric bool
	// resetWindow begins a new aggregation window at the given point.
	resetWindow := func(tms int64, v variant.Variant) {
		lastTms = tms
		targetTms = tms
		targetValue = v
		varCount = 1
		windowNumeric = isNumericType(v)
	}

	slideFunc := func(pack PointPack) ([]Point, error) {
		fgPoints := make([]Point, 0, 100)
		for pack.Next() {
			tms, v := pack.Read()
			condition, err := evalCond(v)
			if err != nil {
				return nil, err
			}
			if !condition {
				continue
			}
			if lastTms == 0 {
				resetWindow(tms, v)
				continue
			}
			if tms-lastTms >= interval {
				fgPoints = append(fgPoints, Point{Tms: targetTms, V: targetValue})
				resetWindow(tms, v)
				continue
			}
			// If the window started with a non-numeric value, skip all aggregation
			// and keep the first value. Otherwise only aggregate numeric values.
			if !windowNumeric || !isNumericType(v) {
				continue
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
				reduceVariant, err := v.Reduce(targetValue)
				if err != nil {
					return nil, err
				}
				divideValue, err := reduceVariant.Divide(variant.NewInt64(varCount))
				if err != nil {
					return nil, err
				}
				targetValue, err = targetValue.Increase(divideValue)
				if err != nil {
					return nil, err
				}
			}
		}

		return fgPoints, nil
	}

	var err error
	pack := NewPointDiskPack(s.tableInfo.Structure, startTime, endTime)
	points := make([]Point, 0)
	err = s.forEachBlock(code, startTime, endTime, func(head *SegmentHeader, compressedTimeData, compressedValueData []byte) error {
		pack.Reset()
		if e := pack.AddSegment(compressedTimeData, compressedValueData); e != nil {
			return e
		}
		ps, e := slideFunc(pack)
		if e != nil {
			return e
		}
		points = append(points, ps...)
		return nil
	})
	if err != nil {
		return points, err
	}

	cachePoints, err := s.walFile.ReadByTime(code, startTime, endTime)
	if err != nil {
		return nil, err
	}

	ps, err := slideFunc(NewPointCachePack(cachePoints))
	if err != nil {
		return points, err
	}
	points = append(points, ps...)
	if lastTms != 0 {
		points = append(points, Point{
			Tms: targetTms,
			V:   targetValue,
		})
	}
	return points, err
}

func (s *ssTable) Close() error {
	// Stop background goroutines and wait for any in-flight async flush so that
	// files are not closed mid-transaction.
	if s.cancel != nil {
		s.cancel()
	}
	s.flushWg.Wait()
	if s.cleanupDone != nil {
		<-s.cleanupDone
	}
	// Close the WAL. Its internal flushPending flushes the active chunk and
	// may rotate the file, creating a new complete file. We must drain again
	// so that file is also encoded and truncated, otherwise its data would
	// appear in both segments and the WAL on reopen.
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
	if closeErr != nil {
		return closeErr
	}
	if s.Meta != nil {
		if err := s.Meta.Close(); err != nil {
			return err
		}
	}
	// Surface the last asynchronous flush error, if any.
	if v := s.asyncErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}
