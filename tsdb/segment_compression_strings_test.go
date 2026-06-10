package tsdb

import (
	"github.com/mababaNiubi/variant"
	"testing"
)

func TestStringEncoder_Single(t *testing.T) {
	enc := NewStringEncoder()
	enc.Write(variant.NewString("hello"))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &StringDecoder{}
	dec.SetBytes(b)
	assertDecodeStrings(t, dec, []string{"hello"})
}

func TestStringEncoder_Multiple(t *testing.T) {
	enc := NewStringEncoder()
	values := []string{"hello", "world", "foo", "bar", "baz"}
	for _, v := range values {
		enc.Write(variant.NewString(v))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &StringDecoder{}
	dec.SetBytes(b)
	assertDecodeStrings(t, dec, values)
}

func TestStringEncoder_EmptyString(t *testing.T) {
	enc := NewStringEncoder()
	enc.Write(variant.NewString(""))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &StringDecoder{}
	dec.SetBytes(b)
	assertDecodeStrings(t, dec, []string{""})
}

func TestStringEncoder_Unicode(t *testing.T) {
	enc := NewStringEncoder()
	values := []string{"hello", "world", "🌍"}
	for _, v := range values {
		enc.Write(variant.NewString(v))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &StringDecoder{}
	dec.SetBytes(b)
	assertDecodeStrings(t, dec, values)
}

func TestStringEncoder_LargeString(t *testing.T) {
	enc := NewStringEncoder()
	large := make([]byte, 10000)
	for i := range large {
		large[i] = byte('a' + i%26)
	}
	enc.Write(variant.NewString(string(large)))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &StringDecoder{}
	dec.SetBytes(b)
	assertDecodeStrings(t, dec, []string{string(large)})
}

func TestStringEncoder_ManyStrings(t *testing.T) {
	enc := NewStringEncoder()
	values := make([]string, 100)
	for i := range values {
		values[i] = "value_" + string(rune('0'+i%10))
		enc.Write(variant.NewString(values[i]))
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &StringDecoder{}
	dec.SetBytes(b)
	assertDecodeStrings(t, dec, values)
}

func TestStringEncoder_Reset(t *testing.T) {
	enc := NewStringEncoder()
	enc.Write(variant.NewString("first"))
	enc.Reset()
	enc.Write(variant.NewString("second"))
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &StringDecoder{}
	dec.SetBytes(b)
	assertDecodeStrings(t, dec, []string{"second"})
}

func TestStringDecoder_EmptyBytes(t *testing.T) {
	dec := &StringDecoder{}
	dec.SetBytes([]byte{})
	if dec.Next() {
		t.Error("expected Next to return false for empty bytes")
	}
}

func assertDecodeStrings(t *testing.T, dec *StringDecoder, expected []string) {
	t.Helper()
	var got []string
	for dec.Next() {
		got = append(got, dec.Read().AsString())
	}
	if dec.Error() != nil {
		t.Fatalf("decoder error: %v", dec.Error())
	}
	if len(got) != len(expected) {
		t.Fatalf("decoded %d values, expected %d", len(got), len(expected))
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("at index %d: got %q, want %q", i, got[i], expected[i])
		}
	}
}
