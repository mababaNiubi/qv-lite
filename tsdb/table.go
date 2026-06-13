package tsdb

import (
	"sync"
	"time"

	"github.com/mababaNiubi/variant"
)

type tagCode uint32

type ssTable struct {
	mute      sync.RWMutex
	tableInfo TableInfo
	dirPath   string // Table directory path.
	*Meta
	fragmentation          fileSegmentList
	columnMap              map[tagCode]*ssColumn
	maxSegmentSize         int64
	maxSegmentTimeInterval int64
	expirationMinuteTime   int64
	walFile                WalFile
	flushMute              sync.Mutex
}

func mewSSTable(tableInfo TableInfo, dirPath string, maxSegmentSize, maxSegmentTimeInterval,
	expirationMinuteTime int64, dedupWindowMs, minIntervalMs int64, compressionName string, walConfig WalConfig) (*ssTable, error) {
	if walConfig.MaxCacheSize <= 0 {
		walConfig.MaxCacheSize = 1024 * 1024 * 64
	}
	s := &ssTable{
		tableInfo:              tableInfo,
		dirPath:                dirPath,
		columnMap:              make(map[tagCode]*ssColumn),
		maxSegmentSize:         maxSegmentSize,
		expirationMinuteTime:   expirationMinuteTime,
		maxSegmentTimeInterval: maxSegmentTimeInterval,
	}
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
	s.walFile, err = NewWalFile(s.dirPath, walConfig.CloseBuffer, walConfig.MaxCacheSize, walConfig.MaxFileNumber, dedupWindowMs, minIntervalMs, walConfig.MaxBufferBatchSize)
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
	return s, nil
}

func (s *ssTable) Write(tag string, timestamp int64, value variant.Variant) (bool, error) {
	if s.walFile == nil {
		return false, ErrorWALCacheIsNil
	}
	// Look up the column index.
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
		return ok, err
	}
	// Flush to disk when the cache exceeds the size limit.
	if s.walFile.NeedFlush() {
		if s.flushMute.TryLock() {
			defer s.flushMute.Unlock()
			if s.walFile.NeedFlush() {
				err = s.flushCache()
				if err != nil {
					return ok, err
				}
				// Clean up expired data.
				if s.expirationMinuteTime != 0 {
					s.fragmentation.Remove(time.Now().UnixNano() - s.expirationMinuteTime)
				}
			}
		}
	}
	return ok, nil
}

func (s *ssTable) flushCache() error {
	// Flush pending WAL entries so they are visible to forEachCompleteFile.
	if err := s.walFile.FlushPending(); err != nil {
		return err
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
	err = s.walFile.forEachCompleteFile(func(fileIndex int, tag tagCode, timestamp int64, value variant.Variant, offset int64) bool {
		column, ok := s.columnMap[tag]
		if !ok {
			return true
		}
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
		for _, column := range s.columnMap {
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
	for _, column := range s.columnMap {
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
	// Commit the data segments.
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
	s.walFile.truncate()
	return nil
}

func (s *ssTable) BuildColumn() error {
	s.mute.Lock()
	defer s.mute.Unlock()
	meta, err := NewMeta(s.dirPath)
	if err != nil {
		return err
	}
	s.Meta = meta
	s.Meta.Range(func(k string, u tagCode) bool {
		s.columnMap[u] = newSSColumn(u, &s.tableInfo, s.maxSegmentSize, s.maxSegmentTimeInterval)
		return true
	})
	return nil
}

func (s *ssTable) CreateColumn(tag string) (tagCode, error) {
	if s.MaxPointDict == maxColumnTag {
		return 0, ErrorPointQuantityExceedsLimit
	}
	s.mute.Lock()
	defer s.mute.Unlock()
	pointDict, err := s.Meta.addTag(tag)
	if err != nil {
		return 0, err
	}
	s.flushMute.Lock()
	s.columnMap[s.MaxPointDict] = newSSColumn(s.MaxPointDict, &s.tableInfo, s.maxSegmentSize, s.maxSegmentTimeInterval)
	s.flushMute.Unlock()
	return pointDict, nil
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
	s.mute.RLock()
	code, ok := s.Meta.Load(tag)
	s.mute.RUnlock()
	if !ok {
		return nil, ErrorTagNotFound
	}
	evalCond := CompileCondition(cond)
	cachePoints, err := s.queryCache(code, startTime, endTime, evalCond)
	if err != nil {
		return nil, err
	}
	disk, err := s.queryDisk(code, startTime, endTime, evalCond)
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

// QueryLimitNumber queries data for a tag within a time range, limited to maxNumber points.
// fusion controls aggregation: 0=avg, 1=min, 2=max.
func (s *ssTable) QueryLimitNumber(tag string, startTime int64, endTime int64, maxNumber int64, fusion uint8, cond any) ([]Point, error) {
	code, ok := s.Meta.Load(tag)
	if !ok {
		return nil, ErrorTagNotFound
	}
	tms, _, ok := s.walFile.GetTagMaxTimestamp(code)
	if ok {
		endTime = min(endTime, tms)
	}
	var interval = (endTime - startTime) / maxNumber

	evalCond := CompileCondition(cond)
	targetValue := variant.NewEmpty()
	var targetTms, count, lastTms, pointsLen int64
	var windowNumeric bool
	// resetWindow begins a new aggregation window at the given point.
	resetWindow := func(tms int64, v variant.Variant) {
		lastTms = tms
		targetTms = tms
		targetValue = v
		count = 1
		windowNumeric = isNumericType(v)
		n := maxNumber - pointsLen
		if n > 0 {
			interval = (endTime - lastTms) / n
		}
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
				pointsLen++
				if pointsLen >= maxNumber-1 {
					return fgPoints, nil
				}
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
				count++
				targetTms = targetTms + (tms-targetTms)/count
				reduceVariant, err := v.Reduce(targetValue)
				if err != nil {
					return nil, err
				}
				divideValue, err := reduceVariant.Divide(variant.NewInt64(count))
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
	points := make([]Point, 0, maxNumber)
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
		return s.walFile.Close()
	}
	return nil
}
