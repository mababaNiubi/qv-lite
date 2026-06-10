package tsdb

import (
	"github.com/mababaNiubi/variant"
	"testing"
)

func TestBooleanEncoder_SingleTrue(t *testing.T) {
	enc := NewBooleanEncoder()
	enc.Write(variant.NewBool(true))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if b[0] != booleanCompressedRLETrue {
		t.Errorf("expected RLE true header, got %d", b[0])
	}

	dec := &BooleanDecoder{}
	dec.SetBytes(b)
	assertDecodeBooleans(t, dec, []bool{true})
}

func TestBooleanEncoder_SingleFalse(t *testing.T) {
	enc := NewBooleanEncoder()
	enc.Write(variant.NewBool(false))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if b[0] != booleanCompressedRLEFalse {
		t.Errorf("expected RLE false header, got %d", b[0])
	}

	dec := &BooleanDecoder{}
	dec.SetBytes(b)
	assertDecodeBooleans(t, dec, []bool{false})
}

func TestBooleanEncoder_AllTrue(t *testing.T) {
	enc := NewBooleanEncoder()
	for i := 0; i < 100; i++ {
		enc.Write(variant.NewBool(true))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &BooleanDecoder{}
	dec.SetBytes(b)
	expected := make([]bool, 100)
	for i := range expected {
		expected[i] = true
	}
	assertDecodeBooleans(t, dec, expected)
}

func TestBooleanEncoder_AllFalse(t *testing.T) {
	enc := NewBooleanEncoder()
	for i := 0; i < 100; i++ {
		enc.Write(variant.NewBool(false))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &BooleanDecoder{}
	dec.SetBytes(b)
	expected := make([]bool, 100)
	for i := range expected {
		expected[i] = false
	}
	assertDecodeBooleans(t, dec, expected)
}

func TestBooleanEncoder_Alternating(t *testing.T) {
	enc := NewBooleanEncoder()
	values := []bool{true, false, true, false, true, false, true, false}
	for _, v := range values {
		enc.Write(variant.NewBool(v))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &BooleanDecoder{}
	dec.SetBytes(b)
	assertDecodeBooleans(t, dec, values)
}

func TestBooleanEncoder_MixedPattern(t *testing.T) {
	enc := NewBooleanEncoder()
	// Pattern: 3 true, 2 false, 4 true, 1 false
	values := []bool{
		true, true, true,
		false, false,
		true, true, true, true,
		false,
	}
	for _, v := range values {
		enc.Write(variant.NewBool(v))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &BooleanDecoder{}
	dec.SetBytes(b)
	assertDecodeBooleans(t, dec, values)
}

func TestBooleanEncoder_Reset(t *testing.T) {
	enc := NewBooleanEncoder()
	enc.Write(variant.NewBool(true))
	enc.Reset()
	enc.Write(variant.NewBool(false))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if b[0] != booleanCompressedRLEFalse {
		t.Errorf("expected RLE false after reset, got %d", b[0])
	}
}

func TestBooleanEncoder_BitPackedBoundary(t *testing.T) {
	enc := NewBooleanEncoder()
	// Write 9 values that should trigger bit-packed mode (not all same)
	values := []bool{true, true, true, true, true, true, true, true, false}
	for _, v := range values {
		enc.Write(variant.NewBool(v))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &BooleanDecoder{}
	dec.SetBytes(b)
	assertDecodeBooleans(t, dec, values)
}

func TestBooleanDecoder_InvalidEncoding(t *testing.T) {
	dec := &BooleanDecoder{}
	dec.SetBytes([]byte{99}) // invalid encoding type
	if dec.Next() {
		t.Error("expected Next to return false for invalid encoding")
	}
	if dec.Error() == nil {
		t.Error("expected error for invalid encoding")
	}
}

func TestBooleanDecoder_EmptyBytes(t *testing.T) {
	dec := &BooleanDecoder{}
	dec.SetBytes([]byte{})
	if dec.Next() {
		t.Error("expected Next to return false for empty bytes")
	}
}

func TestBooleanEncoder_NonBoolValue(t *testing.T) {
	enc := NewBooleanEncoder()
	enc.Write(variant.NewString("not a bool"))
	if enc.err == nil {
		t.Error("expected error for non-bool value")
	}
}

func assertDecodeBooleans(t *testing.T, dec *BooleanDecoder, expected []bool) {
	t.Helper()
	var got []bool
	for dec.Next() {
		v := dec.Read()
		b, err := v.AsBool()
		if err != nil {
			t.Fatalf("unexpected error decoding bool: %v", err)
		}
		got = append(got, b)
	}
	if dec.Error() != nil {
		t.Fatalf("decoder error: %v", dec.Error())
	}
	if len(got) != len(expected) {
		t.Fatalf("decoded %d values, expected %d", len(got), len(expected))
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("at index %d: got %v, want %v", i, got[i], expected[i])
		}
	}
}
