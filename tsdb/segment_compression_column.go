package tsdb

import (
	"encoding/binary"
	"fmt"

	"github.com/mababaNiubi/variant"
)

// ColumnEncoder Column encoding: fixed-schema structure, column-by-column.
//
// Each column is independently encoded by a sub-encoder matching its declared
// type (int, float, string, bool, json, nested structure, or unknown).
// The encoder writes rows in lockstep: for each row, every column encoder
// receives the corresponding value (or emptyVariant if the key is missing).
//
// Binary layout:
//
//	[0]      marker: columnCompressed (1 byte)
//	[1:9]    length_1: uint64 BE — byte length of column 1's encoded data
//	[9:17]   length_2: uint64 BE
//	...      (N × 8 bytes for N columns)
//	[..]     column_1_data — sub-encoder bytes (marker + payload)
//	[..]     column_2_data
//	...
//
// Decoder instantiates sub-decoders by reading each column's first byte
// (the sub-encoder marker) and dispatching to the corresponding Decoder type.
type ColumnEncoder struct {
	isNotStruct   bool
	columnEncoder []Encoder
	columnIndex   []string
}

func NewColumnEncoder(attribute []ColumnAttribute) *ColumnEncoder {
	// Create a default structure when none is provided.
	defaultAttribute := []ColumnAttribute{
		{
			Type: ColumnTypeUnknown,
		},
	}
	if len(attribute) != 0 {
		// Use the provided structure.
		defaultAttribute = attribute
	}
	me := &ColumnEncoder{
		columnIndex:   make([]string, len(defaultAttribute)),
		columnEncoder: make([]Encoder, len(defaultAttribute)),
		isNotStruct:   len(attribute) == 0,
	}
	for i, columnAttribute := range defaultAttribute {
		me.columnIndex[i] = columnAttribute.Name
		switch columnAttribute.Type {
		case ColumnTypeInt:
			me.columnEncoder[i] = NewIntegerEncoder()
		case ColumnTypeFloat:
			me.columnEncoder[i] = NewFloatEncoder(columnAttribute.FloatPrecision)
		case ColumnTypeString:
			me.columnEncoder[i] = NewStringEncoder()
		case ColumnTypeBool:
			me.columnEncoder[i] = NewBooleanEncoder()
		case ColumnTypeJson:
			me.columnEncoder[i] = NewJsonEncoder()
		case ColumnTypeStructure:
			me.columnEncoder[i] = NewColumnEncoder(columnAttribute.Structure)
		case ColumnTypeUnknown:
			me.columnEncoder[i] = NewUnknownEncoder(columnAttribute.FloatPrecision)
		default:
		}
	}
	return me
}

func (m *ColumnEncoder) Write(v variant.Variant) bool {
	for index, key := range m.columnIndex {
		// No structure: write value directly.
		if m.isNotStruct {
			if !m.columnEncoder[index].Write(v) {
				return false
			}
			continue
		}
		value, ok := v.MapGet(key)
		if !ok {
			// Previously written encoder data cannot be rolled back, so write an empty value
			// if the key is missing. Callers should validate structure before writing.
			m.columnEncoder[index].Write(emptyVariant)
			continue
		}
		if !m.columnEncoder[index].Write(value) {
			return false
		}
	}
	return true
}

func (m *ColumnEncoder) Bytes() ([]byte, error) {
	headerLen := 1 + len(m.columnEncoder)*8
	totalLen := headerLen
	subBytes := make([][]byte, len(m.columnEncoder))
	for i, encoder := range m.columnEncoder {
		b, err := encoder.Bytes()
		if err != nil {
			return nil, err
		}
		subBytes[i] = b
		totalLen += len(b)
	}
	buf := make([]byte, headerLen, totalLen)
	buf[0] = columnCompressed
	for i, b := range subBytes {
		binary.BigEndian.PutUint64(buf[1+i*8:], uint64(len(b)))
		buf = append(buf, b...)
	}
	return buf, nil
}

func (m *ColumnEncoder) Reset() {
	for i := range m.columnEncoder {
		m.columnEncoder[i].Reset()
	}
}

type ColumnDecoder struct {
	columnDecoder []Decoder
	columnIndex   map[string]int
	err           error
	attribute     []ColumnAttribute
}

func NewColumnDecoder(attribute []ColumnAttribute) *ColumnDecoder {
	md := &ColumnDecoder{
		columnIndex:   make(map[string]int),
		columnDecoder: make([]Decoder, len(attribute)),
		attribute:     attribute,
	}
	for i, columnAttribute := range attribute {
		md.columnIndex[columnAttribute.Name] = i
	}
	if len(attribute) == 0 {
		md.columnDecoder = make([]Decoder, 1)
	}
	return md
}

func (m *ColumnDecoder) SetBytes(compressedData []byte) {
	if len(compressedData) <= 0 {
		return
	}
	if compressedData[0] != columnCompressed {
		// Data may be corrupted (wrong compression type marker).
		return
	}
	startLength := 1 + len(m.columnDecoder)*8
	for i := range m.columnDecoder {
		length := int(binary.BigEndian.Uint64(compressedData[i*8+1 : (i+1)*8+1]))
		endLength := startLength + length
		if startLength >= len(compressedData) {
			m.err = fmt.Errorf("data may be corrupted: start length %v exceeds actual length %v", startLength, len(compressedData))
			return
		}
		switch compressedData[startLength] {
		case intUncompressed, intCompressedSimple, intCompressedRLE:
			m.columnDecoder[i] = &IntegerDecoder{}
		case jsonCompressed:
			m.columnDecoder[i] = &JsonDecoder{}
		case floatCompressedXDMI:
			m.columnDecoder[i] = &FloatDecoder{}
		case stringCompressedSnappy:
			m.columnDecoder[i] = &StringDecoder{}
		case booleanCompressedRLEFalse, booleanCompressedRLETrue, booleanCompressedBitPacked:
			m.columnDecoder[i] = &BooleanDecoder{}
		case columnCompressed:
			m.columnDecoder[i] = NewColumnDecoder(m.attribute[i].Structure)
		default:
			m.err = fmt.Errorf("unknown value compression type: %d", compressedData[startLength])
			return
		}
		if endLength > len(compressedData) {
			m.err = fmt.Errorf("data may be corrupted: end length %v exceeds actual length %v", endLength, len(compressedData))
			return
		}
		m.columnDecoder[i].SetBytes(compressedData[startLength:endLength])
		startLength = endLength
	}
}

func (m *ColumnDecoder) Next() bool {
	if len(m.columnDecoder) == 0 {
		return false
	}
	for i := range m.columnDecoder {
		if !m.columnDecoder[i].Next() {
			return false
		}
	}
	return true
}

func (m *ColumnDecoder) Read() variant.Variant {
	v := variant.NewValueMap(make(map[string]variant.Variant))
	if len(m.columnIndex) == 0 && len(m.columnDecoder) == 1 {
		return m.columnDecoder[0].Read()
	}
	for name, index := range m.columnIndex {
		v.MapSet(name, m.columnDecoder[index].Read())
	}
	return v
}

func (m *ColumnDecoder) Error() error {
	var errStr = ""
	if m.err != nil {
		errStr = m.err.Error()
	}
	for name, index := range m.columnIndex {
		if m.columnDecoder[index].Error() != nil {
			errStr += fmt.Sprintf("[%s]: %v ", name, m.columnDecoder[index].Error())
		}
	}
	if len(errStr) != 0 {
		return fmt.Errorf("ColumnDecoder.Error: %s ", errStr)
	}
	return nil
}
