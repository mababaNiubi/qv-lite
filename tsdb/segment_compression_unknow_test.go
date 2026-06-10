package tsdb

import (
	"github.com/mababaNiubi/variant"
	"testing"
)

func TestUnknownEncoder_Float64(t *testing.T) {
	enc := NewUnknownEncoder(0)
	if !enc.Write(variant.NewFloat64(3.14)) {
		t.Fatal("Write should return true")
	}
	if !enc.Write(variant.NewFloat64(2.71)) {
		t.Fatal("Write should return true")
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 {
		t.Error("expected non-empty bytes")
	}
}

func TestUnknownEncoder_Int64(t *testing.T) {
	enc := NewUnknownEncoder(0)
	enc.Write(variant.NewInt64(42))
	enc.Write(variant.NewInt64(100))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &IntegerDecoder{}
	dec.SetBytes(b)
	assertDecodeIntegers(t, dec, []int64{42, 100})
}

func TestUnknownEncoder_String(t *testing.T) {
	enc := NewUnknownEncoder(0)
	enc.Write(variant.NewString("hello"))
	enc.Write(variant.NewString("world"))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &StringDecoder{}
	dec.SetBytes(b)
	assertDecodeStrings(t, dec, []string{"hello", "world"})
}

func TestUnknownEncoder_Bool(t *testing.T) {
	enc := NewUnknownEncoder(0)
	enc.Write(variant.NewBool(true))
	enc.Write(variant.NewBool(false))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 {
		t.Error("expected non-empty bytes")
	}
}

func TestUnknownEncoder_TypeChange(t *testing.T) {
	enc := NewUnknownEncoder(0)
	enc.Write(variant.NewInt64(42))
	// Writing a string after int should signal incompatibility
	ok := enc.Write(variant.NewString("hello"))
	if ok {
		t.Error("expected Write to return false when type changes incompatibly")
	}
}

func TestUnknownEncoder_FloatToInt(t *testing.T) {
	enc := NewUnknownEncoder(0)
	if !enc.Write(variant.NewFloat64(3.14)) {
		t.Fatal("first write should succeed")
	}
	// Float encoder can accept int values
	if !enc.Write(variant.NewInt64(10)) {
		t.Error("float encoder should accept int values")
	}
}

func TestUnknownEncoder_FloatToBool(t *testing.T) {
	enc := NewUnknownEncoder(0)
	enc.Write(variant.NewFloat64(1.0))
	// Float encoder can accept bool values
	ok := enc.Write(variant.NewBool(true))
	if !ok {
		t.Error("float encoder should accept bool values")
	}
}

func TestUnknownEncoder_EmptyBytes(t *testing.T) {
	enc := NewUnknownEncoder(0)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 0 {
		t.Errorf("expected empty bytes, got %d bytes", len(b))
	}
}

func TestUnknownEncoder_Reset(t *testing.T) {
	enc := NewUnknownEncoder(0)
	enc.Write(variant.NewInt64(1))
	enc.Reset()
	enc.Write(variant.NewString("test"))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &StringDecoder{}
	dec.SetBytes(b)
	assertDecodeStrings(t, dec, []string{"test"})
}

func TestUnknownEncoder_JsonFallback(t *testing.T) {
	enc := NewUnknownEncoder(0)
	// Map type falls back to JsonEncoder
	enc.Write(variant.NewValueMap(map[string]variant.Variant{"a": variant.NewInt(1)}))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 {
		t.Error("expected non-empty bytes for JSON fallback")
	}
}

func TestIncompatibleType(t *testing.T) {
	if incompatibleType(variant.TypeInt64, variant.TypeInt64) {
		t.Error("same type should be compatible")
	}
	if incompatibleType(variant.TypeFloat64, variant.TypeInt64) {
		t.Error("float64 should be compatible with int64")
	}
	if incompatibleType(variant.TypeFloat64, variant.TypeBool) {
		t.Error("float64 should be compatible with bool")
	}
	if incompatibleType(variant.TypeFloat64, variant.TypeUInt64) {
		t.Error("float64 should be compatible with uint64")
	}
	if !incompatibleType(variant.TypeInt64, variant.TypeString) {
		t.Error("int64 should be incompatible with string")
	}
	if !incompatibleType(variant.TypeBool, variant.TypeFloat64) {
		t.Error("bool should be incompatible with float64")
	}
}
