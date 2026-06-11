// Package tsdb Segment compression: encoder/decoder interfaces and on-disk layout.
//
// A file segment consists of a sequence of blocks, each containing a header
// followed by compressed timestamp and value data for one column:
//
//	SegmentHeader (big-endian):
//	  [0:8]   MinTime: int64 BE
//	  [8:16]  MaxTime: int64 BE
//	  [16:20] Attribute: tagCode BE (column identifier)
//	  [20:28] DataSize: int64 BE (value + time byte length; +8 for non-struct)
//	  [28:32] Crc: uint32 BE — CRC32 of the value data
//
//	Block payload:
//	  For non-struct columns:
//	    [0:8]   valueLen: uint64 BE — byte length of compressed value data
//	    [8..]   compressed value data (marker byte + sub-encoder payload)
//	    [..]    compressed time data (TimeEncoder output)
//
//	  For struct columns (ColumnTypeStructure):
//	    [0..]   compressed value data
//	    [..]    compressed time data
//
// Each sub-encoder (int, float, string, bool, json, column, adapt) writes its
// own marker byte as the first byte of its output. See decoderForMarker() for
// the dispatch table.
package tsdb

import (
	"github.com/mababaNiubi/variant"
)

// ZigZagEncode converts a int64 to a uint64 by zig zagging negative and positive values
// across even and odd numbers.  Eg. [0,-1,1,-2] becomes [0, 1, 2, 3].
func ZigZagEncode(x int64) uint64 {
	return uint64(uint64(x<<1) ^ uint64(int64(x)>>63))
}

// ZigZagDecode converts a previously zigzag encoded uint64 back to a int64.
func ZigZagDecode(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
}

const (
	tableInfoFile   = "table.json"
	metaFile        = "meta.json"
	dataSuffix      = ".tsb"
	dataPath        = "data"
	indexFileSuffix = ".idx"
	indexMagic      = 0x49445801 // "IDX" + version 1
	maxSegmentSize  = 64 * 1024 * 1024
	maxColumnTag    = 255 * 255 * 255 * 255
)

type Encoder interface {
	Write(v variant.Variant) bool
	Bytes() ([]byte, error)
	Reset()
}

type Decoder interface {
	SetBytes(b []byte)
	Next() bool
	Read() variant.Variant
	Error() error
}

// ColumnReader is an optional interface implemented by decoders that can read a
// single column value by name, avoiding the per-row map allocation of Read().
type ColumnReader interface {
	ReadColumn(name string) (variant.Variant, bool)
}

// Sub-encoder marker bytes. Each encoder's Bytes() output begins with one of
// these markers so the decoder can identify the encoding type.
const (
	intUncompressed            = iota // 0: raw 8-byte integers
	intCompressedSimple               // 1: simple8b-packed integers
	intCompressedRLE                  // 2: run-length encoded integers
	floatCompressedXDMI               // 3: XOR-delta mantissa-ignore compressed floats
	jsonCompressed                    // 4: LZ4-compressed variant binary
	stringCompressedSnappy            // 5: Snappy-compressed strings
	booleanCompressedBitPacked        // 6: bit-packed booleans
	booleanCompressedRLETrue          // 7: RLE all-true
	booleanCompressedRLEFalse         // 8: RLE all-false
	columnCompressed                  // 9: fixed-schema column encoding
	adaptColumnCompressed             // 10: self-describing adaptive column encoding
)

var emptyVariant = variant.NewEmpty()
