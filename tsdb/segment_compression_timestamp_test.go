package tsdb

import (
	"testing"
)

func TestTimeEncoder_Simple(t *testing.T) {
	enc := NewTimeEncoder(10)
	timestamps := []int64{1000, 2000, 3000, 4000, 5000}
	for _, ts := range timestamps {
		enc.Write(ts)
	}
	if enc.Length() != 5 {
		t.Fatalf("expected length 5, got %d", enc.Length())
	}
	if enc.GetMinTime() != 1000 {
		t.Fatalf("expected min time 1000, got %d", enc.GetMinTime())
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &TimeDecoder{}
	dec.Init(b)
	assertDecodeTimestamps(t, dec, timestamps)
}

func TestTimeEncoder_RegularInterval(t *testing.T) {
	enc := NewTimeEncoder(100)
	// Timestamps every 10 seconds starting from a base time
	base := int64(1600000000)
	timestamps := make([]int64, 100)
	for i := range timestamps {
		timestamps[i] = base + int64(i)*10
		enc.Write(timestamps[i])
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &TimeDecoder{}
	dec.Init(b)
	assertDecodeTimestamps(t, dec, timestamps)
}

func TestTimeEncoder_SingleValue(t *testing.T) {
	enc := NewTimeEncoder(10)
	enc.Write(9999)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &TimeDecoder{}
	dec.Init(b)
	assertDecodeTimestamps(t, dec, []int64{9999})
}

func TestTimeEncoder_TwoValues(t *testing.T) {
	enc := NewTimeEncoder(10)
	enc.Write(1000)
	enc.Write(2000)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &TimeDecoder{}
	dec.Init(b)
	assertDecodeTimestamps(t, dec, []int64{1000, 2000})
}

func TestTimeEncoder_NonUniform(t *testing.T) {
	enc := NewTimeEncoder(10)
	timestamps := []int64{1000, 1500, 2222, 3555, 4999}
	for _, ts := range timestamps {
		enc.Write(ts)
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &TimeDecoder{}
	dec.Init(b)
	assertDecodeTimestamps(t, dec, timestamps)
}

func TestTimeEncoder_Reset(t *testing.T) {
	enc := NewTimeEncoder(10)
	enc.Write(1000)
	enc.Reset()
	enc.Write(2000)
	if enc.Length() != 1 {
		t.Errorf("expected length 1 after reset, got %d", enc.Length())
	}
	if enc.GetMinTime() != 2000 {
		t.Errorf("expected min time 2000, got %d", enc.GetMinTime())
	}
}

func TestTimeEncoder_LargeTimestamps(t *testing.T) {
	enc := NewTimeEncoder(10)
	base := int64(1700000000000000000) // nanoseconds
	timestamps := []int64{base, base + 1000000, base + 2000000}
	for _, ts := range timestamps {
		enc.Write(ts)
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dec := &TimeDecoder{}
	dec.Init(b)
	assertDecodeTimestamps(t, dec, timestamps)
}

func TestTimeDecoder_EmptyBytes(t *testing.T) {
	dec := &TimeDecoder{}
	dec.Init([]byte{})
	if dec.Next() {
		t.Error("expected Next to return false for empty bytes")
	}
}

func TestTimeDecoder_InvalidEncoding(t *testing.T) {
	dec := &TimeDecoder{}
	dec.Init([]byte{0xF0}) // invalid encoding type (15)
	if dec.Next() {
		t.Error("expected Next to return false for invalid encoding")
	}
	if dec.Error() == nil {
		t.Error("expected error for invalid encoding")
	}
}

func TestTimeDecoder_TruncatedData(t *testing.T) {
	// RLE encoding with less than 9 bytes
	dec := &TimeDecoder{}
	dec.Init([]byte{byte(timeCompressedRLE) << 4, 0, 0, 0}) // only 4 bytes
	if dec.Next() {
		t.Error("expected Next to return false for truncated RLE data")
	}
}

func TestTimeEncoder_MinTimeTracking(t *testing.T) {
	enc := NewTimeEncoder(10)
	enc.Write(5000)
	enc.Write(3000) // later time is less than first
	enc.Write(7000)
	if enc.GetMinTime() != 3000 {
		t.Errorf("expected min time 3000, got %d", enc.GetMinTime())
	}
}

func assertDecodeTimestamps(t *testing.T, dec *TimeDecoder, expected []int64) {
	t.Helper()
	var got []int64
	for dec.Next() {
		got = append(got, dec.Read())
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
