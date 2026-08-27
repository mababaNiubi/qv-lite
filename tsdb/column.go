package tsdb

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/mababaNiubi/variant"
)

func newSSColumn(index tagCode, tableInfo *TableInfo, maxSize int64, maxSegmentTimeInterval int64) *ssColumn {
	if tableInfo == nil {
		tableInfo = &TableInfo{
			ColumnAttribute: ColumnAttribute{
				Type:      ColumnTypeUnknown,
				Structure: make([]ColumnAttribute, 0),
			},
		}
	}
	if maxSize == 0 {
		maxSize = maxSegmentSize
	}
	// maxSegmentTimeInterval is already expressed in nanoseconds by DB.Open.
	// Keep zero as "unlimited"; silently replacing it with a duration here both
	// violated the public Config contract and previously mixed seconds with
	// nanoseconds.
	sc := &ssColumn{
		index:                  index,
		tableInfo:              tableInfo,
		maxSegmentSize:         maxSize,
		maxSegmentTimeInterval: maxSegmentTimeInterval,
		tmsCompressor:          NewTimeEncoder(encoderInitCap),
	}
	switch tableInfo.Type {
	case ColumnTypeUnknown:
		sc.valueCompressor = NewAdaptColumnEncoder(tableInfo.FloatPrecision, encoderInitCap)
		//sc.valueCompressor = NewUnknownEncoder(tableInfo.FloatPrecision, batchSize)
	case ColumnTypeBool:
		sc.valueCompressor = NewBooleanEncoder(encoderInitCap)
	case ColumnTypeFloat:
		sc.valueCompressor = NewFloatEncoder(tableInfo.FloatPrecision, encoderInitCap)
	case ColumnTypeInt:
		sc.valueCompressor = NewIntegerEncoder(encoderInitCap)
	case ColumnTypeString:
		sc.valueCompressor = NewStringEncoder(encoderInitCap)
	case ColumnTypeStructure:
		sc.valueCompressor = NewColumnEncoder(tableInfo.Structure, encoderInitCap)
	default:
		sc.valueCompressor = &JsonEncoder{}

	}
	return sc
}

type ssColumn struct {
	index                  tagCode
	tableInfo              *TableInfo
	maxTms                 int64
	valueCompressor        Encoder
	tmsCompressor          *TimeEncoder
	maxSegmentSize         int64
	maxSegmentTimeInterval int64

	// preTms/preVariant save a (timestamp, value) pair rejected by the value
	// encoder due to a type change. They are flushed on the next Write call
	// after the caller restructures (glow), preventing both time and value loss.
	preTms     int64
	preVariant variant.Variant
}

func (s *ssColumn) Write(timestamp int64, value variant.Variant) (bool, error) {
	// Flush a previously rejected (timestamp, value) pair first.
	if s.preTms != 0 {
		prevTs := s.preTms
		prevVal := s.preVariant
		s.preTms = 0
		s.preVariant = variant.NewEmpty()

		s.maxTms = prevTs
		s.tmsCompressor.Write(prevTs)
		if !s.valueCompressor.Write(prevVal) {
			// Still incompatible — re-save and let the caller glow again.
			s.preTms = prevTs
			s.preVariant = prevVal
			return false, nil
		}
	}

	ok := s.valueCompressor.Write(value)
	if !ok {
		// Value rejected due to type change; save both time and value
		// so the caller can flush the current segment and retry.
		s.preTms = timestamp
		s.preVariant = value
		return false, nil
	}
	s.maxTms = timestamp
	s.tmsCompressor.Write(timestamp)
	return true, nil
}

// glowWrite flushes buffered data to disk without committing the segment.
func (s *ssColumn) glowWrite(fileSegments *fileSegmentList) (bool, error) {
	if fileSegments == nil || s.tmsCompressor == nil || s.valueCompressor == nil || s.tmsCompressor.Length() == 0 {
		return false, nil
	}
	w := fileSegments.GetLastFragmentation()
	if w == nil {
		return false, nil
	}
	maxTms := s.maxTms
	s.maxTms = 0
	// If data exceeds the limit, flush the segment to disk.
	// Compress data.
	compressedTimeData, err := s.tmsCompressor.Bytes()
	if err != nil || len(compressedTimeData) <= 1 {
		return false, err
	}
	compressedValueData, err := s.valueCompressor.Bytes()
	if err != nil || len(compressedValueData) <= 1 {
		return false, err
	}
	minTime := s.tmsCompressor.GetMinTime()
	header := &SegmentHeader{
		MinTime:   minTime,
		MaxTime:   maxTms,
		Attribute: s.index,
		DataSize:  int64(len(compressedValueData) + len(compressedTimeData)),
		Crc:       crc32.ChecksumIEEE(compressedValueData),
	}

	// Build the complete data block.
	blockOffset := w.Size()
	if s.tableInfo.Type != ColumnTypeStructure {
		header.DataSize += 8
		headerBuf := encodeSegmentHeader(header)
		if _, err := w.Write(headerBuf[:]); err != nil {
			return false, err
		}
		var lenBuf [8]byte
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(compressedValueData)))
		if _, err := w.Write(lenBuf[:]); err != nil {
			return false, err
		}
	} else {
		headerBuf := encodeSegmentHeader(header)
		if _, err := w.Write(headerBuf[:]); err != nil {
			return false, err
		}
	}
	_, err = w.Write(compressedValueData)
	if err != nil {
		return false, err
	}
	_, err = w.Write(compressedTimeData)
	if err != nil {
		return false, err
	}
	fileIndex := w.GetIndex()
	beyondSegmentTime := false
	if fileIndex != nil {
		if fileIndex.MinTime == 0 || minTime < fileIndex.MinTime {
			fileIndex.MinTime = minTime
		}
		if maxTms > fileIndex.MaxTime {
			fileIndex.MaxTime = maxTms
		}
		fileIndex.appendBlock(BlockIndexEntry{
			Attribute: s.index,
			MinTime:   minTime,
			MaxTime:   maxTms,
			Offset:    blockOffset,
			DataSize:  header.DataSize,
		})
		beyondSegmentTime = segmentTimeExceeded(fileIndex.MinTime, fileIndex.MaxTime, s.maxSegmentTimeInterval)
	} else {
		beyondSegmentTime = segmentTimeExceeded(w.GetMinTms(), maxTms, s.maxSegmentTimeInterval)
	}

	s.tmsCompressor.Reset()
	s.valueCompressor.Reset()
	if w.PhysicalSize() >= s.maxSegmentSize || beyondSegmentTime {
		return true, nil
	}
	return false, nil
}

// segmentTimeExceeded reports whether [minTime, maxTime] has reached the
// configured segment span. interval <= 0 disables time-based rotation. The
// subtraction is only evaluated for an ordered range, avoiding signed overflow
// for malformed/reversed timestamps.
func segmentTimeExceeded(minTime, maxTime, interval int64) bool {
	return interval > 0 && maxTime >= minTime && uint64(maxTime)-uint64(minTime) >= uint64(interval)
}

func (s *ssColumn) Reset() {
	if s.tmsCompressor == nil || s.valueCompressor == nil || s.tmsCompressor.Length() == 0 {
		return
	}
	s.preTms = 0
	s.preVariant = variant.NewEmpty()
	s.tmsCompressor.Reset()
	s.valueCompressor.Reset()
}
