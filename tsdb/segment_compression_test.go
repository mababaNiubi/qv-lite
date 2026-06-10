package tsdb

import (
	"testing"
)

func TestZigZagEncode(t *testing.T) {
	tests := []struct {
		input int64
		want  uint64
	}{
		{0, 0},
		{-1, 1},
		{1, 2},
		{-2, 3},
		{2, 4},
		{-3, 5},
		{3, 6},
	}
	for _, tt := range tests {
		got := ZigZagEncode(tt.input)
		if got != tt.want {
			t.Errorf("ZigZagEncode(%d) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestZigZagDecode(t *testing.T) {
	tests := []struct {
		input uint64
		want  int64
	}{
		{0, 0},
		{1, -1},
		{2, 1},
		{3, -2},
		{4, 2},
	}
	for _, tt := range tests {
		got := ZigZagDecode(tt.input)
		if got != tt.want {
			t.Errorf("ZigZagDecode(%d) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestZigZagRoundTrip(t *testing.T) {
	values := []int64{0, 1, -1, 100, -100, 1<<62 - 1, -(1 << 62)}
	for _, v := range values {
		decoded := ZigZagDecode(ZigZagEncode(v))
		if decoded != v {
			t.Errorf("ZigZag round-trip: %d -> %d", v, decoded)
		}
	}
}
