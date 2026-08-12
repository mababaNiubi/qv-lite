package tsdb

// Ported from InfluxDB, modified.

import (
	"encoding/binary"
	"fmt"

	"github.com/mababaNiubi/variant"

	"github.com/golang/snappy"
)

// StringEncoder encodes strings adaptively: when a block has few distinct
// values, a dictionary encoding (stringCompressedDict) is emitted; otherwise
// it falls back to the snappy-compressed len-prefixed stream
// (stringCompressedSnappy). The decoder handles both.
type StringEncoder struct {
	// bytes accumulates the raw len-prefixed strings (snappy fallback path).
	bytes []byte
	// dict/order/idx accumulate distinct strings for the dictionary path.
	dict  map[string]int
	order []string
	idx   []int
}

// NewStringEncoder returns a new StringEncoder with an initial buffer ready to hold sz bytes.
func NewStringEncoder(batchSize ...int) *StringEncoder {
	return &StringEncoder{
		bytes: make([]byte, 0, encoderCap(batchSize...)*16),
		dict:  make(map[string]int, 32),
	}
}

// Flush is no-op
func (e *StringEncoder) Flush() {}

// Reset sets the encoder back to its initial state.
func (e *StringEncoder) Reset() {
	e.bytes = e.bytes[:0]
	e.order = e.order[:0]
	e.idx = e.idx[:0]
	clear(e.dict)
}

// Write encodes s to the underlying buffers (both the raw stream and the dict).
func (e *StringEncoder) Write(str variant.Variant) bool {
	s := str.AsString()
	var b [binary.MaxVarintLen64]byte
	i := binary.PutUvarint(b[:], uint64(len(s)))
	e.bytes = append(e.bytes, b[:i]...)
	e.bytes = append(e.bytes, s...)

	if idx, ok := e.dict[s]; ok {
		e.idx = append(e.idx, idx)
	} else {
		idx = len(e.order)
		e.dict[s] = idx
		e.order = append(e.order, s)
		e.idx = append(e.idx, idx)
	}
	return true
}

// Bytes returns the smaller of the dictionary or snappy encoding.
func (e *StringEncoder) Bytes() ([]byte, error) {
	snappyData := snappy.Encode(nil, e.bytes)
	if dictLen := e.dictEncodedLen(); dictLen < len(snappyData)+1 {
		return e.encodeDict(), nil
	}
	return append([]byte{stringCompressedSnappy}, snappyData...), nil
}

// dictEncodedLen returns the exact encoded size (including the marker byte) of
// the dictionary format: [marker][count uvarint][entries][index uvarints].
func (e *StringEncoder) dictEncodedLen() int {
	n := 1 + uvarintLen(uint64(len(e.order)))
	for _, s := range e.order {
		n += uvarintLen(uint64(len(s))) + len(s)
	}
	for _, idx := range e.idx {
		n += uvarintLen(uint64(idx))
	}
	return n
}

// uvarintLen returns the number of bytes binary.PutUvarint would emit for v.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// encodeDict emits the dictionary format:
//
//	[0]      marker: stringCompressedDict
//	[..]     count: uvarint — number of distinct strings
//	[..]     per entry: [len uvarint][string bytes]
//	[..]     per index: uvarint into the dictionary
func (e *StringEncoder) encodeDict() []byte {
	buf := make([]byte, 0, e.dictEncodedLen())
	var tmp [binary.MaxVarintLen64]byte
	buf = append(buf, stringCompressedDict)
	n := binary.PutUvarint(tmp[:], uint64(len(e.order)))
	buf = append(buf, tmp[:n]...)
	for _, s := range e.order {
		n = binary.PutUvarint(tmp[:], uint64(len(s)))
		buf = append(buf, tmp[:n]...)
		buf = append(buf, s...)
	}
	for _, idx := range e.idx {
		n = binary.PutUvarint(tmp[:], uint64(idx))
		buf = append(buf, tmp[:n]...)
	}
	return buf
}

// StringDecoder decodes either string encoding into an in-order string list.
type StringDecoder struct {
	list []string
	i    int
	err  error
}

// SetBytes initializes the decoder with bytes to read from.
func (e *StringDecoder) SetBytes(b []byte) {
	e.list = e.list[:0]
	e.i = 0
	e.err = nil
	if len(b) == 0 {
		return
	}
	switch b[0] {
	case stringCompressedSnappy:
		data, err := snappy.Decode(nil, b[1:])
		if err != nil {
			e.err = err
			return
		}
		e.list = parseLenPrefixed(data, e.list)
	case stringCompressedDict:
		var err error
		e.list, err = decodeDict(b[1:])
		if err != nil {
			e.err = err
		}
	default:
		e.err = fmt.Errorf("StringDecoder: invalid encoding type %d", b[0])
	}
}

// Next returns true if there are any values remaining to be decoded.
func (e *StringDecoder) Next() bool {
	if e.err != nil {
		return false
	}
	e.i++
	return e.i <= len(e.list)
}

// Read returns the next value from the decoder.
func (e *StringDecoder) Read() variant.Variant {
	if e.i <= 0 || e.i > len(e.list) {
		return emptyVariant
	}
	return variant.NewString(e.list[e.i-1])
}

// Error returns the last error encountered by the decoder.
func (e *StringDecoder) Error() error {
	return e.err
}

// parseLenPrefixed decodes [uvarint len][bytes]... into strings.
func parseLenPrefixed(b []byte, out []string) []string {
	pos := 0
	for pos < len(b) {
		l, n := binary.Uvarint(b[pos:])
		if n <= 0 || pos+n+int(l) > len(b) {
			break
		}
		pos += n
		out = append(out, string(b[pos:pos+int(l)]))
		pos += int(l)
	}
	return out
}

// decodeDict decodes [uvarint count][entry: len uvarint + bytes]...[index uvarint]...
func decodeDict(b []byte) ([]string, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, fmt.Errorf("StringDecoder: invalid dict count")
	}
	pos := n
	dict := make([]string, count)
	for i := uint64(0); i < count; i++ {
		l, n := binary.Uvarint(b[pos:])
		if n <= 0 || pos+n+int(l) > len(b) {
			return nil, fmt.Errorf("StringDecoder: truncated dict entry %d", i)
		}
		pos += n
		dict[i] = string(b[pos : pos+int(l)])
		pos += int(l)
	}
	var out []string
	for pos < len(b) {
		v, n := binary.Uvarint(b[pos:])
		if n <= 0 || v >= count {
			return nil, fmt.Errorf("StringDecoder: invalid dict index %d", v)
		}
		pos += n
		out = append(out, dict[v])
	}
	return out, nil
}
