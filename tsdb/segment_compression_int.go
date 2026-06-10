package tsdb

// Code source: modified by influxDB

import (
	"encoding/binary"
	"fmt"

	"github.com/jwilder/encoding/simple8b"
	"github.com/mababaNiubi/variant"
)

// IntegerEncoder encoding: delta + zigzag + simple8b (or RLE, or raw).
//
// Encoding pipeline:
//  1. Delta-encode: each value is stored as (value - previous).
//  2. ZigZag-encode: maps signed int64 to unsigned uint64 so small negatives
//     become small positives (e.g. -1 → 1, 1 → 2).
//  3. Choose strategy based on value range:
//     a) RLE — all deltas identical (intCompressedRLE)
//     b) simple8b — all deltas ≤ simple8b.MaxValue (intCompressedSimple)
//     c) uncompressed — otherwise (intUncompressed)
//
// Binary layout per strategy:
//
//	RLE (intCompressedRLE):
//	  [0]    marker: intCompressedRLE (1 byte, 4 high bits)
//	  [1:9]  first: uint64 BE — the first zigzag-encoded value
//	  [9..]  delta: uvarint — the repeated zigzag delta
//	  [..]   count: uvarint — how many times the delta repeats
//
//	simple8b (intCompressedSimple):
//	  [0]    marker: intCompressedSimple (1 byte)
//	  [1:9]  first: uint64 BE — the first zigzag-encoded value (uncompressed)
//	  [9..]  N × uint64 BE — simple8b-encoded words containing deltas 2..N
//
//	uncompressed (intUncompressed):
//	  [0]    marker: intUncompressed (1 byte)
//	  [1..]  N × uint64 BE — each zigzag-encoded delta value
type IntegerEncoder struct {
	prev   int64
	rle    bool
	values []uint64
	err    error
}

// NewIntegerEncoder returns a new integer TimeEncoder with an initial buffer of values sized at sz.
func NewIntegerEncoder() *IntegerEncoder {
	return &IntegerEncoder{
		rle:    true,
		values: make([]uint64, 0, 256),
	}
}

// Flush is no-op
func (e *IntegerEncoder) Flush() {}

// Reset sets the TimeEncoder back to its initial state.
func (e *IntegerEncoder) Reset() {
	e.prev = 0
	e.rle = true
	e.values = e.values[:0]
}

// Write encodes v to the underlying buffers.
func (e *IntegerEncoder) Write(value variant.Variant) bool {
	v, err := value.AsInt64()
	if err != nil {
		e.err = err
		return true
	}
	delta := v - e.prev
	e.prev = v
	enc := ZigZagEncode(delta)
	if len(e.values) > 1 {
		e.rle = e.rle && e.values[len(e.values)-1] == enc
	}

	e.values = append(e.values, enc)
	return true
}

// Bytes returns a copy of the underlying buffer.
func (e *IntegerEncoder) Bytes() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	// Only run-length encode if it could reduce storage size.
	if e.rle && len(e.values) > 2 {
		return e.encodeRLE()
	}

	for _, v := range e.values {
		// Value is too large to encode using packed format
		if v > simple8b.MaxValue {
			return e.encodeUncompressed()
		}
	}

	return e.encodePacked()
}

func (e *IntegerEncoder) encodeRLE() ([]byte, error) {
	// Large varints can take up to 10 bytes.  We're storing 3 + 1
	// type byte.
	var b [31]byte

	// 4 high bits used for the encoding type
	b[0] = intCompressedRLE

	i := 1
	// The first value
	binary.BigEndian.PutUint64(b[i:], e.values[0])
	i += 8
	// The first delta
	i += binary.PutUvarint(b[i:], e.values[1])
	// The number of times the delta is repeated
	i += binary.PutUvarint(b[i:], uint64(len(e.values)-1))

	return b[:i], nil
}

func (e *IntegerEncoder) encodePacked() ([]byte, error) {
	if len(e.values) == 0 {
		return nil, nil
	}

	// Encode all but the first value.  Fist value is written unencoded
	// using 8 bytes.
	encoded, err := simple8b.EncodeAll(e.values[1:])
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1+(len(encoded)+1)*8)
	// 4 high bits of first byte store the encoding type for the block
	b[0] = intCompressedSimple

	// Write the first value since it's not part of the encoded values
	binary.BigEndian.PutUint64(b[1:9], e.values[0])

	// Write the encoded values
	for i, v := range encoded {
		binary.BigEndian.PutUint64(b[9+i*8:9+i*8+8], v)
	}
	return b, nil
}

func (e *IntegerEncoder) encodeUncompressed() ([]byte, error) {
	if len(e.values) == 0 {
		return nil, nil
	}

	b := make([]byte, 1+len(e.values)*8)
	// 4 high bits of first byte store the encoding type for the block
	b[0] = intUncompressed

	for i, v := range e.values {
		binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], v)
	}
	return b, nil
}

// IntegerDecoder decodes a byte slice into int64s.
type IntegerDecoder struct {
	// 240 is the maximum number of values that can be encoded into a single uint64 using simple8b
	values [240]uint64
	bytes  []byte
	i      int
	n      int
	prev   int64
	first  bool

	// The first value for a run-length encoded byte slice
	rleFirst uint64

	// The delta value for a run-length encoded byte slice
	rleDelta uint64
	encoding byte
	err      error
}

// SetBytes sets the underlying byte slice of the decoder.
func (d *IntegerDecoder) SetBytes(b []byte) {
	if len(b) > 0 {
		d.encoding = b[0]
		d.bytes = b[1:]
	} else {
		d.encoding = 0
		d.bytes = nil
	}

	d.i = 0
	d.n = 0
	d.prev = 0
	d.first = true

	d.rleFirst = 0
	d.rleDelta = 0
	d.err = nil
}

// Next returns true if there are any values remaining to be decoded.
func (d *IntegerDecoder) Next() bool {
	if d.i >= d.n && len(d.bytes) == 0 {
		return false
	}

	d.i++

	if d.i >= d.n {
		switch d.encoding {
		case intUncompressed:
			d.decodeUncompressed()
		case intCompressedSimple:
			d.decodePacked()
		case intCompressedRLE:
			d.decodeRLE()
		default:
			d.err = fmt.Errorf("unknown encoding %v", d.encoding)
		}
	}
	return d.err == nil && d.i < d.n
}

// Error returns the last error encountered by the decoder.
func (d *IntegerDecoder) Error() error {
	return d.err
}

// Read returns the next value from the decoder.
func (d *IntegerDecoder) Read() variant.Variant {
	switch d.encoding {
	case intCompressedRLE:
		return variant.NewInt64(ZigZagDecode(d.rleFirst) + int64(d.i)*ZigZagDecode(d.rleDelta))
	default:
		v := ZigZagDecode(d.values[d.i])
		// v is the delta encoded value, we need to add the prior value to get the original
		v = v + d.prev
		d.prev = v
		return variant.NewInt64(v)
	}
}

func (d *IntegerDecoder) decodeRLE() {
	if len(d.bytes) == 0 {
		return
	}

	if len(d.bytes) < 8 {
		d.err = fmt.Errorf("IntegerDecoder: not enough data to decode RLE starting value")
		return
	}

	var i, n int

	// Next 8 bytes is the starting value
	first := binary.BigEndian.Uint64(d.bytes[i : i+8])
	i += 8

	// Next 1-10 bytes is the delta value
	value, n := binary.Uvarint(d.bytes[i:])
	if n <= 0 {
		d.err = fmt.Errorf("IntegerDecoder: invalid RLE delta value")
		return
	}
	i += n

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(d.bytes[i:])
	if n <= 0 {
		d.err = fmt.Errorf("IntegerDecoder: invalid RLE repeat value")
		return
	}

	// Store the first value and delta value so we do not need to allocate
	// a large values slice.  We can compute the value at position d.i on
	// demand.
	d.rleFirst = first
	d.rleDelta = value
	d.n = int(count) + 1
	d.i = 0

	// We've process all the bytes
	d.bytes = nil
}

func (d *IntegerDecoder) decodePacked() {
	if len(d.bytes) == 0 {
		return
	}

	if len(d.bytes) < 8 {
		d.err = fmt.Errorf("IntegerDecoder: not enough data to decode packed value")
		return
	}

	v := binary.BigEndian.Uint64(d.bytes[0:8])
	// The first value is always unencoded
	if d.first {
		d.first = false
		d.n = 1
		d.values[0] = v
	} else {
		n, err := simple8b.Decode(&d.values, v)
		if err != nil {
			// Should never happen, only error that could be returned is if the value to be decoded was not
			// actually encoded by simple8b TimeEncoder.
			d.err = fmt.Errorf("failed to decode value %v: %v", v, err)
		}

		d.n = n
	}
	d.i = 0
	d.bytes = d.bytes[8:]
}

func (d *IntegerDecoder) decodeUncompressed() {
	if len(d.bytes) == 0 {
		return
	}

	if len(d.bytes) < 8 {
		d.err = fmt.Errorf("IntegerDecoder: not enough data to decode uncompressed value")
		return
	}

	d.values[0] = binary.BigEndian.Uint64(d.bytes[0:8])
	d.i = 0
	d.n = 1
	d.bytes = d.bytes[8:]
}
