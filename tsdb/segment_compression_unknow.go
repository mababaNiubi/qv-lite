package tsdb

import (
	"github.com/mababaNiubi/variant"
)

// UnknownEncoder adaptively selects a sub-encoder based on the type of the
// first value written. Once a type is chosen, subsequent values with incompatible
// types cause Write() to return false (signalling the caller to restructure/glow).
//
// Type compatibility (see incompatibleType):
//
//	Float64 accepts Int64, UInt64, and Bool (via AsFloat64 conversion).
//	All other type switches are rejected.
//
// Binary layout: delegates entirely to the chosen sub-encoder.
// No additional framing — the sub-encoder's Bytes() is returned as-is.
type UnknownEncoder struct {
	floatPrecision uint8
	vt             variant.Type
	Encoder
}

func NewUnknownEncoder(floatPrecision uint8) *UnknownEncoder {
	return &UnknownEncoder{
		floatPrecision: floatPrecision,
	}
}

func (m *UnknownEncoder) Write(v variant.Variant) bool {
	if m.Encoder == nil {
		m.Encoder = m.adaptiveEncoder(v.Type())
		m.vt = v.Type()
		return m.Encoder.Write(v)
	}
	if incompatibleType(m.vt, v.Type()) {
		// Type mismatch; this encoder's lifetime is over.
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
		return NewFloatEncoder(m.floatPrecision)
	case variant.TypeUInt64, variant.TypeInt64:
		return NewIntegerEncoder()
	case variant.TypeString:
		return NewStringEncoder()
	case variant.TypeBool:
		return NewBooleanEncoder()
	default:
		return NewJsonEncoder() //Each write requires comparing the data key structure, which leads to poor write performance, so ColumnEncoder is not used
	}
}

func incompatibleType(old variant.Type, new variant.Type) bool {
	if old == new {
		return false
	}
	// Float encoder also accepts int and bool values.
	if old == variant.TypeFloat64 && (new == variant.TypeUInt64 || new == variant.TypeInt64 || new == variant.TypeBool) {
		return false
	}
	return true
}
