package tsdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/mababaNiubi/variant"
)

// Binary layout for struct mode (flags & 0x01 == 0):
//
//	[0]   marker: adaptColumnCompressed (1 byte)
//	[1]   flags: 0x00 = struct (1 byte)
//	[2:4] column count N (uint16 LE)
//
//	Column headers (repeated N times):
//	  [0]   name length (uint8)
//	  [1:L] column name (UTF-8)
//
//	Column data blocks (repeated N times, same order):
//	  [0:8] encoded payload length (uint64 LE)
//	  [8:]  sub-encoder bytes (first byte = sub-type marker)
//
// Binary layout for non-struct mode (flags & 0x01 == 1):
//
//	[0]   marker: adaptColumnCompressed (1 byte)
//	[1]   flags: 0x01 = non-struct (1 byte)
//	[2:10] row count (uint64 LE)
//	[10:]  sub-encoder bytes (first byte = sub-type marker; may be empty)

// kv is a reusable key-value pair used during writeStruct.
type kv struct {
	key   string
	value variant.Variant
}

// AdaptColumnEncoder adaptively encodes variant values column-by-column.
// It automatically discovers columns from incoming Map variants and
// backfills missing rows. Non-Map values fall back to a nested encoder.
type AdaptColumnEncoder struct {
	isNotStruct    bool
	length         int
	floatPrecision uint8

	// Struct mode: parallel arrays indexed by column position.
	columnOrder    []string
	columnEncoders []Encoder
	columnTypes    []variant.Type

	// Reusable per-write state (reset each writeStruct call).
	writePairs  []kv
	seenColumns []bool

	// Non-struct mode
	vt      variant.Type
	encoder Encoder
}

// colIdx returns the index of a column name in columnOrder, or -1.
func (m *AdaptColumnEncoder) colIdx(name string) int {
	for i, n := range m.columnOrder {
		if n == name {
			return i
		}
	}
	return -1
}

func NewAdaptColumnEncoder(floatPrecision uint8) *AdaptColumnEncoder {
	return &AdaptColumnEncoder{
		columnOrder:    make([]string, 0, 4),
		columnEncoders: make([]Encoder, 0, 4),
		columnTypes:    make([]variant.Type, 0, 4),
		writePairs:     make([]kv, 0, 8),
		seenColumns:    make([]bool, 0, 4),
		floatPrecision: floatPrecision,
	}
}

// Write appends a value. Returns false when a column type changes —
// the caller (ssColumn) must restructure (glow), flush the current segment,
// and resubmit the rejected (timestamp, value) pair on the next Write.
func (m *AdaptColumnEncoder) Write(v variant.Variant) bool {
	if m.isNotStruct {
		return m.writeNonStruct(v)
	}
	return m.writeStruct(v)
}

// writeNonStruct delegates to a single nested encoder matching the variant type.
func (m *AdaptColumnEncoder) writeNonStruct(v variant.Variant) bool {
	vt := v.Type()
	if m.encoder == nil {
		if vt == variant.TypeMap {
			m.isNotStruct = false
			return m.writeStruct(v)
		}
		m.encoder = m.findColumnEncoder(vt)
		m.vt = vt
		m.encoder.Write(v)
		m.length++
		return true
	}
	if vt == variant.TypeMap {
		return false
	}
	if incompatibleType(m.vt, vt) {
		return false
	}
	if !m.encoder.Write(v) {
		return false
	}
	m.length++
	return true
}

// writeStruct uses a two-phase approach to ensure atomic row writes:
// phase 1 validates all column types, phase 2 writes all columns.
func (m *AdaptColumnEncoder) writeStruct(v variant.Variant) bool {
	if v.Type() != variant.TypeMap {
		if m.length == 0 {
			m.isNotStruct = true
			return m.writeNonStruct(v)
		}
		return false
	}

	// Collect key-value pairs into reusable slice.
	m.writePairs = m.writePairs[:0]
	v.Range(func(key string, value variant.Variant) bool {
		m.writePairs = append(m.writePairs, kv{key, value})
		return true
	})

	for _, p := range m.writePairs {
		cVt := p.value.Type()
		if idx := m.colIdx(p.key); idx >= 0 && incompatibleType(m.columnTypes[idx], cVt) {
			return false
		}
	}

	//write all columns.
	for _, p := range m.writePairs {
		cVt := p.value.Type()
		idx := m.colIdx(p.key)
		if idx >= 0 {
			if !m.columnEncoders[idx].Write(p.value) {
				return false
			}
		} else {
			// New column — add to schema.
			m.columnOrder = append(m.columnOrder, p.key)
			m.columnTypes = append(m.columnTypes, cVt)
			m.seenColumns = append(m.seenColumns, true)
			encoder := m.findColumnEncoder(cVt)
			m.columnEncoders = append(m.columnEncoders, encoder)
			// Backfill previous rows with empty.
			for range m.length {
				encoder.Write(emptyVariant)
			}
			encoder.Write(p.value)
		}
		if idx >= 0 {
			m.seenColumns[idx] = true
		}
	}

	// Fill missing columns with empty (skip when all columns present).
	for i := range m.columnOrder {
		if !m.seenColumns[i] {
			m.columnEncoders[i].Write(emptyVariant)
		} else {
			m.seenColumns[i] = false
		}
	}
	m.length++

	return true
}

// findColumnEncoder returns the best-fit encoder for a variant type.
// Maps produce a nested AdaptColumnEncoder for recursive struct support.
func (m *AdaptColumnEncoder) findColumnEncoder(vt variant.Type) Encoder {
	switch vt {
	case variant.TypeMap:
		return NewAdaptColumnEncoder(m.floatPrecision)
	case variant.TypeString:
		return NewStringEncoder()
	case variant.TypeUInt64, variant.TypeInt64:
		return NewIntegerEncoder()
	case variant.TypeFloat64:
		return NewFloatEncoder(m.floatPrecision)
	case variant.TypeBool:
		return NewBooleanEncoder()
	default:
		return NewJsonEncoder()
	}
}

func (m *AdaptColumnEncoder) Bytes() ([]byte, error) {
	if m.isNotStruct {
		return m.encodeNonStruct()
	}
	return m.encodeStruct()
}

func (m *AdaptColumnEncoder) encodeNonStruct() ([]byte, error) {
	if m.encoder == nil {
		return []byte{adaptColumnCompressed, 0x01}, nil
	}
	data, err := m.encoder.Bytes()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 10+len(data))
	b[0] = adaptColumnCompressed                              // [0] marker
	b[1] = 0x01                                               // [1] flags: non-struct
	binary.LittleEndian.PutUint64(b[2:10], uint64(len(data))) // [2:10] row count
	copy(b[10:], data)
	return b, nil
}

func (m *AdaptColumnEncoder) encodeStruct() ([]byte, error) {
	nCols := uint16(len(m.columnOrder))
	if nCols > 65025 {
		return nil, errors.New("AdaptColumnEncoder too many columns")
	}

	// 1. Collect the encoded data of each column and accumulate the total data length
	colData := make([][]byte, nCols)
	totalDataLen := 0
	for i, name := range m.columnOrder {
		encoder := m.columnEncoders[i]
		data, err := encoder.Bytes()
		if err != nil {
			return nil, fmt.Errorf("AdaptColumnEncoder column %q: %w", name, err)
		}
		colData[i] = data
		totalDataLen += len(data)
	}

	// 2. Calculate the total number of bytes required for the complete message Header: marker(1) + flags(1) + nCols(2) = 4
	totalLen := 4
	// Column name field: length of each name (1) + name content
	for _, name := range m.columnOrder {
		nameLen := len(name)
		if nameLen > 255 {
			return nil, errors.New("AdaptColumnEncoder invalid column name")
		}
		totalLen += 1 + nameLen
	}
	// Data block: length prefix of each column (8) + actual data
	totalLen += int(nCols)*8 + totalDataLen

	// 3. Allocate underlying byte slice in one go, using indexed writes (avoids append bounds checking overhead)
	b := make([]byte, totalLen)
	b[0] = byte(adaptColumnCompressed)
	b[1] = 0x00
	binary.LittleEndian.PutUint16(b[2:4], nCols)
	pos := 4

	for _, name := range m.columnOrder {
		b[pos] = byte(len(name))
		pos++
		copy(b[pos:], name)
		pos += len(name)
	}

	for _, data := range colData {
		binary.LittleEndian.PutUint64(b[pos:], uint64(len(data)))
		pos += 8
		copy(b[pos:], data)
		pos += len(data)
	}

	return b, nil
}

func (m *AdaptColumnEncoder) Reset() {
	m.isNotStruct = false
	m.length = 0
	for _, enc := range m.columnEncoders {
		enc.Reset()
	}
	m.columnOrder = m.columnOrder[:0]
	m.columnEncoders = m.columnEncoders[:0]
	m.columnTypes = m.columnTypes[:0]
	m.seenColumns = m.seenColumns[:0]
	m.vt = variant.TypeEmpty
	m.encoder = nil
}

// AdaptColumnDecoder reconstructs values from the self-describing binary
// format produced by AdaptColumnEncoder.Bytes(). It reads the column
// schema from the header and instantiates the correct sub-decoders.
type AdaptColumnDecoder struct {
	isNotStruct   bool
	columnDecoder map[string]Decoder
	columnOrder   []string
	decoder       Decoder
	err           error
}

// SetBytes parses the header and initializes sub-decoders.
func (d *AdaptColumnDecoder) SetBytes(b []byte) {
	if len(b) < 2 {
		d.err = fmt.Errorf("AdaptColumnDecoder: data too short (%d bytes)", len(b))
		return
	}
	if b[0] != byte(adaptColumnCompressed) {
		d.err = fmt.Errorf("AdaptColumnDecoder: invalid marker %d, expected %d", b[0], adaptColumnCompressed)
		return
	}

	d.isNotStruct = b[1]&0x01 != 0

	if d.isNotStruct {
		d.setBytesNonStruct(b)
	} else {
		d.setBytesStruct(b)
	}
}

// setBytesNonStruct: [marker 1B][flags 1B][row count 2B LE][sub-encoder bytes].
// Row count ensures correct termination regardless of sub-decoder behavior.
func (d *AdaptColumnDecoder) setBytesNonStruct(b []byte) {
	if len(b) < 10 {
		d.err = fmt.Errorf("AdaptColumnDecoder: non-struct data too short (%d bytes)", len(b))
		return
	}
	dataLength := binary.LittleEndian.Uint64(b[2:10])
	if len(b) > 10 {
		d.decoder = decoderForMarker(b[10])
		d.decoder.SetBytes(b[10 : 10+dataLength])
	}
}

// setBytesStruct parses column headers then data blocks.
// Column types are inferred from each data block's first byte.
func (d *AdaptColumnDecoder) setBytesStruct(b []byte) {
	if len(b) < 4 {
		d.err = fmt.Errorf("AdaptColumnDecoder: struct data too short (%d bytes)", len(b))
		return
	}
	nCols := binary.LittleEndian.Uint16(b[2:4]) // column count
	pos := 4
	d.columnOrder = make([]string, 0, nCols)

	for i := uint16(0); i < nCols; i++ {
		if pos >= len(b) {
			d.err = fmt.Errorf("AdaptColumnDecoder: truncated header at column %d", i)
			return
		}
		nameLen := int(b[pos])
		pos++
		if pos+nameLen > len(b) {
			d.err = fmt.Errorf("AdaptColumnDecoder: truncated name at column %d", i)
			return
		}
		name := string(b[pos : pos+nameLen])
		pos += nameLen
		d.columnOrder = append(d.columnOrder, name)
	}

	d.columnDecoder = make(map[string]Decoder, nCols)
	for _, name := range d.columnOrder {
		if pos+8 > len(b) {
			d.err = fmt.Errorf("AdaptColumnDecoder: truncated data length for column %q", name)
			return
		}
		dataLen := binary.LittleEndian.Uint64(b[pos : pos+8])
		pos += 8
		if pos+int(dataLen) > len(b) {
			d.err = fmt.Errorf("AdaptColumnDecoder: truncated data for column %q", name)
			return
		}

		var dec Decoder
		if dataLen > 0 {
			dec = decoderForMarker(b[pos])
			dec.SetBytes(b[pos : pos+int(dataLen)])
		} else {
			dec = &JsonDecoder{} // empty column, Next() returns false regardless
		}
		d.columnDecoder[name] = dec
		pos += int(dataLen)
	}
}

// decoderForMarker returns a decoder matching the sub-encoder's first byte.
// This mirrors AddSegment's switch on valueData[0].
func decoderForMarker(marker byte) Decoder {
	switch marker {
	case intUncompressed, intCompressedSimple, intCompressedRLE:
		return &IntegerDecoder{}
	case floatCompressedXDMI:
		return &FloatDecoder{}
	case stringCompressedSnappy:
		return &StringDecoder{}
	case booleanCompressedBitPacked, booleanCompressedRLETrue, booleanCompressedRLEFalse:
		return &BooleanDecoder{}
	case jsonCompressed:
		return &JsonDecoder{}
	//case columnCompressed:
	//	return NewColumnDecoder(nil)
	case adaptColumnCompressed:
		return &AdaptColumnDecoder{}
	default:
		return &JsonDecoder{}
	}
}

// Next advances all sub-decoders in lockstep. Returns false when the
// row count is exhausted, any column is exhausted, or an error occurred.
func (d *AdaptColumnDecoder) Next() bool {
	if d.err != nil {
		return false
	}
	if d.isNotStruct {
		if d.decoder == nil {
			return false
		}
		return d.decoder.Next()
	}
	if len(d.columnDecoder) == 0 {
		return false
	}
	for _, name := range d.columnOrder {
		if !d.columnDecoder[name].Next() {
			return false
		}
	}
	return true
}

// Read assembles the current row. For struct mode it builds a Map
// from each column decoder. For non-struct mode it delegates to the
// single nested decoder.
func (d *AdaptColumnDecoder) Read() variant.Variant {
	if d.isNotStruct {
		if d.decoder != nil {
			return d.decoder.Read()
		}
		return emptyVariant
	}
	v := variant.NewValueMap(make(map[string]variant.Variant))
	for _, name := range d.columnOrder {
		if dec, ok := d.columnDecoder[name]; ok {
			v.MapSet(name, dec.Read())
		}
	}
	return v
}

// ReadColumn reads a single column value by name without building the full map.
func (d *AdaptColumnDecoder) ReadColumn(name string) (variant.Variant, bool) {
	if d.isNotStruct {
		if d.decoder != nil {
			return d.decoder.Read(), true
		}
		return emptyVariant, false
	}
	dec, ok := d.columnDecoder[name]
	if !ok {
		return emptyVariant, false
	}
	return dec.Read(), true
}

// Error returns the first error encountered during decoding.
func (d *AdaptColumnDecoder) Error() error {
	if d.err != nil {
		return d.err
	}
	if d.isNotStruct {
		if d.decoder != nil {
			return d.decoder.Error()
		}
		return nil
	}
	for _, name := range d.columnOrder {
		if dec, ok := d.columnDecoder[name]; ok {
			if err := dec.Error(); err != nil {
				return fmt.Errorf("AdaptColumnDecoder column %q: %w", name, err)
			}
		}
	}
	return nil
}
