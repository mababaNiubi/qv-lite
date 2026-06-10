package tsdb

// Ported from InfluxDB, modified.

import (
	"encoding/binary"
	"fmt"

	"github.com/mababaNiubi/variant"

	"github.com/golang/snappy"
)

// StringEncoder : varint-length-prefixed strings, Snappy-compressed.
//
// Encoding (before compression):
//
//	For each string:
//	  [uvarint len] [string bytes]
//	All prefixed strings are concatenated into a single byte slice.
//
// Compression:
//
//	The concatenated slice is compressed with Snappy.
//
// Binary layout (final):
//
//	[0]    marker: stringCompressedSnappy (1 byte)
//	[1:]   Snappy-compressed data:
//	         after decompression: [uvarint len1][str1][uvarint len2][str2]...
type StringEncoder struct {
	// The encoded bytes
	bytes []byte
}

// NewStringEncoder returns a new StringEncoder with an initial buffer ready to hold sz bytes.
func NewStringEncoder() *StringEncoder {
	return &StringEncoder{
		bytes: make([]byte, 0, 256),
	}
}

// Flush is no-op
func (e *StringEncoder) Flush() {}

// Reset sets the encoder back to its initial state.
func (e *StringEncoder) Reset() {
	e.bytes = e.bytes[:0]
}

// Write encodes s to the underlying buffer.
func (e *StringEncoder) Write(str variant.Variant) bool {
	s := str.AsString()
	var b [binary.MaxVarintLen64]byte
	i := binary.PutUvarint(b[:], uint64(len(s)))
	e.bytes = append(e.bytes, b[:i]...)

	// Append the string bytes
	e.bytes = append(e.bytes, s...)
	return true
}

// Bytes returns a copy of the underlying buffer.
func (e *StringEncoder) Bytes() ([]byte, error) {
	// Compress the currently appended bytes using snappy and prefix with
	// a 1 byte header for future extension
	data := snappy.Encode(nil, e.bytes)
	return append([]byte{stringCompressedSnappy}, data...), nil
}

// StringDecoder decodes a byte slice into strings.
type StringDecoder struct {
	b   []byte
	l   int
	i   int
	err error
}

// SetBytes initializes the decoder with bytes to read from.
// This must be called before calling any other method.
func (e *StringDecoder) SetBytes(b []byte) {
	// First byte stores the encoding type, only have snappy format
	// currently so ignore for now.
	var data []byte
	if len(b) > 0 {
		data, e.err = snappy.Decode(nil, b[1:])
	}

	e.b = data
	e.l = 0
	e.i = 0
	e.err = nil
}

// Next returns true if there are any values remaining to be decoded.
func (e *StringDecoder) Next() bool {
	if e.err != nil {
		return false
	}

	e.i += e.l
	return e.i < len(e.b)
}

// Read returns the next value from the decoder.
func (e *StringDecoder) Read() variant.Variant {
	// Read the length of the string
	length, n := binary.Uvarint(e.b[e.i:])
	if n <= 0 {
		e.err = fmt.Errorf("StringDecoder: invalid encoded string length")
		return emptyVariant
	}

	// The length of this string plus the length of the variable byte encoded length
	e.l = int(length) + n

	lower := e.i + n
	upper := lower + int(length)
	if upper < lower {
		e.err = fmt.Errorf("StringDecoder: length overflow")
		return emptyVariant
	}
	if upper > len(e.b) {
		e.err = fmt.Errorf("StringDecoder: not enough data to represent encoded string")
		return emptyVariant
	}

	return variant.NewString(string(e.b[lower:upper]))
}

// Error returns the last error encountered by the decoder.
func (e *StringDecoder) Error() error {
	return e.err
}
