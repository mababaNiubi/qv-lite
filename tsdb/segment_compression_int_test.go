package tsdb

import (
	"github.com/mababaNiubi/variant"
	"testing"
)

func TestIntegerEncoder_Simple(t *testing.T) {
	enc := NewIntegerEncoder()
	values := []int64{0, 1, 2, 3, 4, 5}
	for _, v := range values {
		enc.Write(variant.NewInt64(v))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &IntegerDecoder{}
	dec.SetBytes(b)
	assertDecodeIntegers(t, dec, values)
}

func TestIntegerEncoder_Decreasing(t *testing.T) {
	enc := NewIntegerEncoder()
	values := []int64{100, 90, 80, 70, 60}
	for _, v := range values {
		enc.Write(variant.NewInt64(v))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &IntegerDecoder{}
	dec.SetBytes(b)
	assertDecodeIntegers(t, dec, values)
}

func TestIntegerEncoder_NegativeValues(t *testing.T) {
	enc := NewIntegerEncoder()
	values := []int64{-5, -3, -1, 1, 3, 5}
	for _, v := range values {
		enc.Write(variant.NewInt64(v))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &IntegerDecoder{}
	dec.SetBytes(b)
	assertDecodeIntegers(t, dec, values)
}

func TestIntegerEncoder_ConstantValue(t *testing.T) {
	enc := NewIntegerEncoder()
	const n = 200
	for i := 0; i < n; i++ {
		enc.Write(variant.NewInt64(42))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &IntegerDecoder{}
	dec.SetBytes(b)
	expected := make([]int64, n)
	for i := range expected {
		expected[i] = 42
	}
	assertDecodeIntegers(t, dec, expected)
}

func TestIntegerEncoder_LargeValues(t *testing.T) {
	enc := NewIntegerEncoder()
	values := []int64{1 << 60, 1 << 59, 1 << 58}
	for _, v := range values {
		enc.Write(variant.NewInt64(v))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &IntegerDecoder{}
	dec.SetBytes(b)
	assertDecodeIntegers(t, dec, values)
}

func TestIntegerEncoder_SingleValue(t *testing.T) {
	enc := NewIntegerEncoder()
	enc.Write(variant.NewInt64(999))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &IntegerDecoder{}
	dec.SetBytes(b)
	assertDecodeIntegers(t, dec, []int64{999})
}

func TestIntegerEncoder_TwoValues(t *testing.T) {
	enc := NewIntegerEncoder()
	enc.Write(variant.NewInt64(10))
	enc.Write(variant.NewInt64(20))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &IntegerDecoder{}
	dec.SetBytes(b)
	assertDecodeIntegers(t, dec, []int64{10, 20})
}

func TestIntegerEncoder_Reset(t *testing.T) {
	enc := NewIntegerEncoder()
	enc.Write(variant.NewInt64(100))
	enc.Reset()
	enc.Write(variant.NewInt64(200))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &IntegerDecoder{}
	dec.SetBytes(b)
	assertDecodeIntegers(t, dec, []int64{200})
}

func TestIntegerDecoder_EmptyBytes(t *testing.T) {
	dec := &IntegerDecoder{}
	dec.SetBytes([]byte{})
	if dec.Next() {
		t.Error("expected Next to return false for empty bytes")
	}
}

func TestIntegerDecoder_InvalidEncoding(t *testing.T) {
	dec := &IntegerDecoder{}
	dec.SetBytes([]byte{99, 0, 0, 0, 0, 0, 0, 0, 0})
	if dec.Next() {
		t.Error("expected Next to return false for invalid encoding")
	}
}

func TestIntegerEncoder_NonIntValue(t *testing.T) {
	enc := NewIntegerEncoder()
	enc.Write(variant.NewString("not int"))
	b, err := enc.Bytes()
	if err == nil && b != nil {
		// error stored internally, next Bytes call returns it
		_, err2 := enc.Bytes()
		if err2 == nil {
			t.Error("expected error for non-int value")
		}
	}
}

func assertDecodeIntegers(t *testing.T, dec *IntegerDecoder, expected []int64) {
	t.Helper()
	var got []int64
	for dec.Next() {
		v := dec.Read()
		i, err := v.AsInt64()
		if err != nil {
			t.Fatalf("unexpected error decoding int: %v", err)
		}
		got = append(got, i)
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
