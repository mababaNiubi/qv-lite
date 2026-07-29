package tsdb

import (
	"github.com/mababaNiubi/variant"
)

// UnknownEncoder defers encoder selection until the first value is written.
// Once a type is chosen, subsequent values with incompatible types cause Write
// to return false so the caller can flush and restructure.
type UnknownEncoder struct {
	floatPrecision uint8
	batchSize      int
	vt             variant.Type
	Encoder
}

func NewUnknownEncoder(floatPrecision uint8, batchSize ...int) *UnknownEncoder {
	return &UnknownEncoder{
		floatPrecision: floatPrecision,
		batchSize:      encoderCap(batchSize...),
	}
}

func (m *UnknownEncoder) Write(v variant.Variant) bool {
	if m.Encoder == nil {
		m.Encoder = m.adaptiveEncoder(v.Type())
		m.vt = v.Type()
		return m.Encoder.Write(v)
	}
	if incompatibleType(m.vt, v.Type()) {
		return false
	}
	return m.Encoder.Write(v)
}

func (m *UnknownEncoder) Bytes() ([]byte, error) {
	if m.Encoder != nil {
		return m.Encoder.Bytes()
	}
	return make([]byte, 0), nil
}

func (m *UnknownEncoder) Reset() {
	if m.Encoder != nil {
		m.Encoder.Reset()
		m.Encoder = nil
	}
}

func (m *UnknownEncoder) adaptiveEncoder(variantType variant.Type) Encoder {
	switch variantType {
	case variant.TypeFloat64:
		return NewFloatEncoder(m.floatPrecision, m.batchSize)
	case variant.TypeUInt64, variant.TypeInt64:
		return NewIntegerEncoder(m.batchSize)
	case variant.TypeString:
		return NewStringEncoder(m.batchSize)
	case variant.TypeBool:
		return NewBooleanEncoder(m.batchSize)
	default:
		return NewJsonEncoder()
	}
}

func incompatibleType(old variant.Type, new variant.Type) bool {
	if old == new {
		return false
	}
	if old == variant.TypeFloat64 && (new == variant.TypeUInt64 || new == variant.TypeInt64 || new == variant.TypeBool) {
		return false
	}
	return true
}
