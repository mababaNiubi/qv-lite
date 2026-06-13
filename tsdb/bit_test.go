package tsdb

import (
	"io"
	"math"
	"testing"
)

// =============================================================================
// Round-trip tests: write with BitWriter, read with BitReader
// =============================================================================

func TestRoundTrip_WriteBits_ReadBits(t *testing.T) {
	tests := []struct {
		value uint64
		nbits int
	}{
		{1, 1}, {3, 2}, {7, 3}, {0xF, 4}, {0x1F, 5},
		{0x3F, 6}, {0x7F, 7}, {0xFF, 8}, {0x1FF, 9},
		{0x1234, 16}, {0x12345678, 32},
		{0, 7},
		{0x123456789ABCDEF0, 64},
		{0xDEADBEEFCAFEBABE, 64},
		{math.MaxUint64, 64},
		{(1 << 62) | (1 << 32) | 1, 63},
	}

	for _, tt := range tests {
		var bw BitWriter
		bw.Reset()
		err := bw.WriteBits(tt.value, tt.nbits)
		if err != nil {
			t.Fatalf("WriteBits(%d bits, 0x%X): %v", tt.nbits, tt.value, err)
		}
		bw.Flush(false)

		br := NewBitReader(bw.Bytes())
		got, err := br.ReadBits(uint(tt.nbits))
		if err != nil {
			t.Fatalf("ReadBits(%d bits) after WriteBits(0x%X): %v", tt.nbits, tt.value, err)
		}
		if got != tt.value {
			t.Fatalf("round-trip %d bits: got 0x%X, want 0x%X", tt.nbits, got, tt.value)
		}
	}
}

func TestRoundTrip_MixedSizes(t *testing.T) {
	cases := []struct {
		name   string
		values []uint64
		nbits  []uint
	}{
		{
			"varied widths",
			[]uint64{1, 3, 0xFF, 0x1234, 0x56789ABC, 7, 0},
			[]uint{1, 2, 8, 16, 32, 3, 5},
		},
		{
			"alternating",
			[]uint64{3, 0x55, 7, 0x1234, 0x0F, 0x12345678},
			[]uint{2, 8, 3, 16, 4, 32},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var bw BitWriter
			bw.Reset()
			for i, v := range c.values {
				if err := bw.WriteBits(v, int(c.nbits[i])); err != nil {
					t.Fatalf("write %d: %v", i, err)
				}
			}
			bw.Flush(false)

			br := NewBitReader(bw.Bytes())
			for i, want := range c.values {
				got, err := br.ReadBits(c.nbits[i])
				if err != nil {
					t.Fatalf("read %d: %v", i, err)
				}
				if got != want {
					t.Fatalf("read %d: got 0x%X, want 0x%X", i, got, want)
				}
			}
		})
	}
}

func TestRoundTrip_WriteBit_ReadBit(t *testing.T) {
	for _, n := range []int{1, 7, 8, 15, 16, 31, 32, 100, 127, 128, 1000} {
		var bw BitWriter
		bw.Reset()

		pattern := make([]bool, n)
		for i := range pattern {
			pattern[i] = i%7 == 0 || i%13 == 0
			bw.WriteBit(pattern[i])
		}
		bw.Flush(false)

		br := NewBitReader(bw.Bytes())
		for i, want := range pattern {
			got, err := br.ReadBit()
			if err != nil {
				t.Fatalf("n=%d, bit %d: %v", n, i, err)
			}
			if got != want {
				t.Fatalf("n=%d, bit %d: got %v, want %v", n, i, got, want)
			}
		}
	}
}

func TestRoundTrip_ManyBits_FastAndNormal(t *testing.T) {
	var bw BitWriter
	bw.Reset()
	for i := 0; i < 200; i++ {
		bw.WriteBit(i%3 == 0)
	}
	bw.Flush(false)

	br := NewBitReader(bw.Bytes())
	for i := 0; i < 200; i++ {
		want := i%3 == 0
		var got bool
		if br.CanReadBitFast() {
			got = br.ReadBitFast()
		} else {
			var err error
			got, err = br.ReadBit()
			if err != nil {
				t.Fatalf("bit %d: %v", i, err)
			}
		}
		if got != want {
			t.Fatalf("bit %d: got %v, want %v", i, got, want)
		}
	}
}

func TestRoundTrip_WriteByte_ReadBits(t *testing.T) {
	var bw BitWriter
	bw.Reset()

	bytes := []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x11}
	for _, b := range bytes {
		if err := bw.WriteByte(b); err != nil {
			t.Fatal(err)
		}
	}
	bw.Flush(false)

	br := NewBitReader(bw.Bytes())
	for i, want := range bytes {
		got, err := br.ReadBits(8)
		if err != nil {
			t.Fatalf("byte %d: %v", i, err)
		}
		if uint64(want) != got {
			t.Fatalf("byte %d: got 0x%X, want 0x%X", i, got, want)
		}
	}
}

func TestRoundTrip_MixedBitAndByte(t *testing.T) {
	var bw BitWriter
	bw.Reset()

	bw.WriteBit(true)
	bw.WriteBit(false)
	bw.WriteByte(0xAB)
	bw.WriteBit(true)
	bw.WriteByte(0xCD)
	bw.WriteBits(0x3, 2)
	bw.Flush(false)

	br := NewBitReader(bw.Bytes())

	if b, _ := br.ReadBit(); !b {
		t.Fatal("bit 0: want true")
	}
	if b, _ := br.ReadBit(); b {
		t.Fatal("bit 1: want false")
	}
	if v, _ := br.ReadBits(8); v != 0xAB {
		t.Fatalf("byte: got 0x%X, want 0xAB", v)
	}
	if b, _ := br.ReadBit(); !b {
		t.Fatal("bit after byte: want true")
	}
	if v, _ := br.ReadBits(8); v != 0xCD {
		t.Fatalf("byte: got 0x%X, want 0xCD", v)
	}
	if v, _ := br.ReadBits(2); v != 0x3 {
		t.Fatalf("2 bits: got 0x%X, want 0x3", v)
	}
}

func TestRoundTrip_64BitAligned(t *testing.T) {
	values := []uint64{
		0x0123456789ABCDEF,
		0xFEDCBA9876543210,
		0xAAAAAAAAAAAAAAAA,
	}
	var bw BitWriter
	bw.Reset()
	for _, v := range values {
		if err := bw.WriteBits(v, 64); err != nil {
			t.Fatal(err)
		}
	}
	bw.Flush(false)

	br := NewBitReader(bw.Bytes())
	for i, want := range values {
		got, err := br.ReadBits(64)
		if err != nil {
			t.Fatalf("value %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("value %d: got 0x%016X, want 0x%016X", i, got, want)
		}
	}
}

func TestRoundTrip_ByteGranular(t *testing.T) {
	for i := 0; i < 256; i++ {
		var bw BitWriter
		bw.Reset()
		for bit := 7; bit >= 0; bit-- {
			bw.WriteBit((i>>uint(bit))&1 == 1)
		}
		bw.Flush(false)

		br := NewBitReader(bw.Bytes())
		v, err := br.ReadBits(8)
		if err != nil {
			t.Fatalf("value 0x%X: %v", i, err)
		}
		if v != uint64(i) {
			t.Fatalf("value 0x%X: got 0x%X", i, v)
		}
	}
}

func TestRoundTrip_ByteBoundary(t *testing.T) {
	var bw BitWriter
	bw.Reset()
	for i := 0; i < 16; i++ {
		bw.WriteBit(i%2 == 0) // 1010...
	}
	bw.Flush(false)

	result := bw.Bytes()
	if len(result) != 2 {
		t.Fatalf("expected 2 bytes, got %d", len(result))
	}

	br := NewBitReader(result)
	v1, _ := br.ReadBits(8)
	v2, _ := br.ReadBits(8)
	if v1 != 0xAA || v2 != 0xAA {
		t.Fatalf("got 0x%X 0x%X, want 0xAA 0xAA", v1, v2)
	}
}

func TestRoundTrip_UnalignedWriteByte(t *testing.T) {
	var bw BitWriter
	bw.Reset()
	bw.WriteBit(true)
	bw.WriteBit(false)
	bw.WriteBit(true)  // 3 bits: 101
	bw.WriteByte(0xCD) // unaligned byte
	bw.Flush(false)

	br := NewBitReader(bw.Bytes())
	for _, want := range []bool{true, false, true} {
		got, err := br.ReadBit()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	v, err := br.ReadBits(8)
	if err != nil {
		t.Fatal(err)
	}
	if v != 0xCD {
		t.Fatalf("got 0x%X, want 0xCD", v)
	}
}

func TestRoundTrip_WriteBitAfterFlush(t *testing.T) {
	var bw BitWriter
	bw.Reset()
	bw.WriteBit(true)
	bw.Flush(false)
	bw.WriteBit(true)
	bw.Flush(false)

	br := NewBitReader(bw.Bytes())
	v1, _ := br.ReadBits(8)
	v2, _ := br.ReadBits(8)
	if v1 != 0x80 || v2 != 0x80 {
		t.Fatalf("got 0x%X 0x%X, want 0x80 0x80", v1, v2)
	}
}

func TestRoundTrip_LargeData(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	var bw BitWriter
	bw.Reset()
	for _, b := range data {
		bw.WriteByte(b)
	}
	bw.Flush(false)

	br := NewBitReader(bw.Bytes())
	for i, want := range data {
		got, err := br.ReadBits(8)
		if err != nil {
			t.Fatalf("byte %d: %v", i, err)
		}
		if uint64(want) != got {
			t.Fatalf("byte %d: got 0x%X, want 0x%X", i, got, want)
		}
	}
}

// =============================================================================
// BitReader edge cases
// =============================================================================

func TestBitReader_EOF_Simple(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		br := NewBitReader([]byte{})
		_, err := br.ReadBit()
		if err != io.EOF {
			t.Fatalf("expected EOF, got %v", err)
		}
	})

	t.Run("after all bits consumed", func(t *testing.T) {
		br := NewBitReader([]byte{0xFF})
		_, _ = br.ReadBits(8)
		_, err := br.ReadBits(1)
		if err != io.EOF {
			t.Fatalf("expected EOF, got %v", err)
		}
	})

	t.Run("incremental consume", func(t *testing.T) {
		br := NewBitReader([]byte{0x12, 0x34, 0x56}) // 24 bits
		for i := 0; i < 3; i++ {
			if _, err := br.ReadBits(8); err != nil {
				t.Fatalf("read %d: %v", i, err)
			}
		}
		_, err := br.ReadBits(1)
		if err != io.EOF {
			t.Fatalf("expected EOF, got %v", err)
		}
	})
}

func TestBitReader_EOF_Overflow(t *testing.T) {
	data := []byte{0x12, 0x34, 0x56} // 24 bits
	br := NewBitReader(data)

	_, err := br.ReadBits(16)
	if err != nil {
		t.Fatal(err)
	}
	// Only 8 bits remain, but request 16 — cross-buffer path should return EOF.
	_, err = br.ReadBits(16)
	if err != io.EOF {
		t.Fatalf("expected EOF for insufficient data, got %v", err)
	}
}

func TestBitReader_EOF_OverflowFromStart(t *testing.T) {
	data := []byte{0x12, 0x34, 0x56} // 24 bits
	br := NewBitReader(data)
	_, err := br.ReadBits(30) // > 24 available
	if err != io.EOF {
		t.Fatalf("expected EOF for insufficient data, got %v (value may be zero-padded)", err)
	}
}

func TestBitReader_CanReadBitFast(t *testing.T) {
	data := make([]byte, 16)
	for i := range data {
		data[i] = 0xFF
	}
	br := NewBitReader(data)

	if !br.CanReadBitFast() {
		t.Fatal("expected CanReadBitFast true after init")
	}

	// Read 62 fast bits: n goes from 64 down to 2
	for i := 0; i < 62; i++ {
		if !br.CanReadBitFast() {
			t.Fatalf("expected CanReadBitFast true at fast read %d", i)
		}
		br.ReadBitFast()
	}

	// n=2: still >1, so true
	if !br.CanReadBitFast() {
		t.Fatal("expected CanReadBitFast true with n=2")
	}
	br.ReadBitFast() // n=1

	// n=1: should be false
	if br.CanReadBitFast() {
		t.Fatal("expected CanReadBitFast false with n=1")
	}
}

func TestBitReader_Reset(t *testing.T) {
	br := NewBitReader([]byte{0xFF, 0x00})
	_, _ = br.ReadBits(8)

	br.Reset([]byte{0x80})
	v, err := br.ReadBit()
	if err != nil {
		t.Fatal(err)
	}
	if !v {
		t.Fatal("expected true")
	}
}

// =============================================================================
// BitWriter edge cases
// =============================================================================

func TestBitWriter_Flush(t *testing.T) {
	t.Run("flush true", func(t *testing.T) {
		var bw BitWriter
		bw.Reset()
		bw.WriteBit(true)
		bw.WriteBit(false)
		bw.WriteBit(true) // 101_____
		bw.Flush(true)

		br := NewBitReader(bw.Bytes())
		v, _ := br.ReadBits(8)
		if v != 0xBF { // 10111111
			t.Fatalf("got 0x%X, want 0xBF", v)
		}
	})

	t.Run("flush false", func(t *testing.T) {
		var bw BitWriter
		bw.Reset()
		bw.WriteBit(true)
		bw.WriteBit(false)
		bw.WriteBit(true) // 101_____
		bw.Flush(false)

		br := NewBitReader(bw.Bytes())
		v, _ := br.ReadBits(8)
		if v != 0xA0 { // 10100000
			t.Fatalf("got 0x%X, want 0xA0", v)
		}
	})

	t.Run("flush idempotent on full byte", func(t *testing.T) {
		var bw BitWriter
		bw.Reset()
		for i := 0; i < 8; i++ {
			bw.WriteBit(true)
		}
		bw.Flush(false)
		bw.Flush(false)

		result := bw.Bytes()
		if len(result) != 1 {
			t.Fatalf("expected 1 byte, got %d", len(result))
		}
	})
}

func TestBitWriter_Reset(t *testing.T) {
	var bw BitWriter
	for i := 0; i < 3; i++ {
		bw.Reset()
		bw.WriteBit(true)
		bw.WriteBit(false)
		bw.Flush(false)

		br := NewBitReader(bw.Bytes())
		v, _ := br.ReadBits(8)
		if v != 0x80 {
			t.Fatalf("iteration %d: got 0x%X, want 0x80", i, v)
		}
	}
}

func TestBitWriter_NewBitWriter(t *testing.T) {
	bw := NewBitWriter()

	// NewBitWriter should be immediately usable (no Reset needed)
	bw.WriteBit(true)
	bw.WriteBit(true)
	bw.Flush(false)

	br := NewBitReader(bw.Bytes())
	v, _ := br.ReadBits(8)
	if v != 0xC0 {
		t.Fatalf("got 0x%X, want 0xC0", v)
	}
}
