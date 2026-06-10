package tsdb

import (
	"errors"
	"fmt"
)

// Table-level errors
var (
	ErrorTableNotExists = errors.New("table not exists")
	ErrorTableExists    = errors.New("table already exists")
	ErrorTagNotFound    = errors.New("tag not found")
	ErrorNoDataForTag   = errors.New("no data for tag")
)

// Data validation errors
var (
	ErrorTimeOut                   = errors.New("timestamp out of range")
	ErrorValueIsEmpty              = errors.New("value is empty")
	ErrorUnsupportedNaN            = errors.New("unsupported value: NaN")
	ErrorUnsupportedInf            = errors.New("unsupported value: Inf")
	ErrorPointQuantityExceedsLimit = errors.New("point quantity exceeds limit")
)

// WAL errors
var (
	ErrorWALClose            = errors.New("WAL has been closed")
	ErrorWALCacheIsNil       = errors.New("walCache is nil")
	ErrorWALCacheFull        = errors.New("you are writing too fast, the wal file cache is full")
	ErrorWALDataCorruption   = errors.New("wal data corruption occurred")
	ErrorWALDataMayBeDamaged = errors.New("wal data may be damaged")
	ErrorFilePathEmpty       = errors.New("file path is empty")
)

// File/block errors
var (
	ErrorInvalidFileFormat      = errors.New("invalid file format")
	ErrorInvalidSeekMode        = errors.New("invalid seek mode")
	ErrorNegativePosition       = errors.New("negative position")
	ErrorInvalidTruncationPoint = errors.New("invalid truncation point")
	ErrorBlockCRCMismatch       = errors.New("block data crc mismatch")
	ErrorCRCCheckFailed         = errors.New("CRC check failed")
	ErrorReaderNotOpened        = errors.New("reader not opened")
	ErrorWriterNotOpen          = errors.New("writer is not open or has been closed")
	ErrorNoTransaction          = errors.New("no transaction")
)

// Condition errors
var (
	ErrorEmptyLogicalCondition = errors.New("logical condition has no sub-conditions")
)

// Errorf helpers for format-string errors that are used in multiple places.
func errorUnknownValueCompressionType(t byte) error {
	return fmt.Errorf("unknown value compression type: %v", t)
}
