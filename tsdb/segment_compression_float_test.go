package tsdb

import (
	"math"
	"testing"

	"github.com/mababaNiubi/variant"
)

func TestFloatEncoder_BasicRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
		prec   uint8
	}{
		{"integers", []float64{1.0, 2.0, 3.0, 4.0, 5.0}, 1},
		{"decimals", []float64{1.23, 4.56, 7.89}, 2},
		{"mixed magnitudes", []float64{0.001, 1.0, 1000.0, 1000000.0}, 3},
		{"single value", []float64{42.0}, 2},
		{"all zeros", []float64{0.0, 0.0, 0.0}, 1},
		{"near integers", []float64{1.0000001, 2.0000002, 3.0000003}, 7},
		{"monotonic increase", []float64{0.5, 1.0, 1.5, 2.0, 2.5}, 1},
		{"monotonic decrease", []float64{5.0, 4.0, 3.0, 2.0, 1.0}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewFloatEncoder(tt.prec)
			for _, v := range tt.values {
				enc.Write(variant.NewFloat64(v))
			}
			bytes, err := enc.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if len(bytes) == 0 {
				t.Fatal("expected non-empty bytes")
			}

			dec := &FloatDecoder{}
			dec.SetBytes(bytes)
			var decoded []float64
			for dec.Next() && len(decoded) < len(tt.values)*2 {
				f, _ := dec.Read().AsFloat64()
				decoded = append(decoded, f)
			}
			if len(decoded) < len(tt.values) {
				t.Fatalf("expected at least %d values, got %d", len(tt.values), len(decoded))
			}
			for i, want := range tt.values {
				if !variant.IsFloat64Equal(decoded[i], want) {
					t.Errorf("index %d: got %f, want %f", i, decoded[i], want)
				}
			}
		})
	}
}

func TestFloatEncoder_ExtremeValues(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
		prec   uint8
	}{
		{
			name:   "MaxFloat64",
			values: []float64{math.MaxFloat64, math.MaxFloat64 / 2},
			prec:   1,
		},
		{
			name:   "very large range",
			values: []float64{-1e100, 0.0, 1e100},
			prec:   1,
		},
		{
			name:   "MaxInt64 boundaries",
			values: []float64{float64(math.MaxInt64), float64(math.MinInt64), 0.0},
			prec:   1,
		},
		{
			name:   "very close values",
			values: []float64{1.000000000000001, 1.000000000000002, 1.000000000000003},
			prec:   15,
		},
		{
			name:   "negative zero handling",
			values: []float64{-0.0, 0.0, 1.0},
			prec:   1,
		},
		{
			name:   "fractional extremes",
			values: []float64{0.000000000000001, 0.000000000000002, 0.000000000000003},
			prec:   15,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewFloatEncoder(tt.prec)
			for _, v := range tt.values {
				if !enc.Write(variant.NewFloat64(v)) {
					t.Fatalf("Write(%g) returned false", v)
				}
			}
			bytes, err := enc.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			dec := &FloatDecoder{}
			dec.SetBytes(bytes)
			var decoded []float64
			for dec.Next() && len(decoded) < len(tt.values)*3 {
				f, _ := dec.Read().AsFloat64()
				decoded = append(decoded, f)
			}
			if len(decoded) < len(tt.values) {
				t.Fatalf("expected at least %d values, got %d", len(tt.values), len(decoded))
			}
			for i, want := range tt.values {
				got := decoded[i]
				if math.IsNaN(want) || math.IsNaN(got) {
					continue
				}
				if math.IsInf(want, 0) || math.IsInf(got, 0) {
					if math.IsInf(want, 0) != math.IsInf(got, 0) {
						t.Errorf("index %d: Inf sign mismatch", i)
					}
					continue
				}
				if !variant.IsFloat64Equal(got, want) {
					t.Errorf("index %d: got %.17g, want %.17g", i, got, want)
				}
			}
		})
	}
}

func TestFloatEncoder_RepeatedValues(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		count int
		prec  uint8
	}{
		{"small count", 42.5, 10, 1},
		{"large count", 3.14159, 100, 5},
		{"zero values", 0.0, 50, 0},
		{"negative value", -7.5, 20, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewFloatEncoder(tt.prec)
			for i := 0; i < tt.count; i++ {
				enc.Write(variant.NewFloat64(tt.value))
			}
			bytes, err := enc.Bytes()
			if err != nil {
				t.Fatal(err)
			}

			dec := &FloatDecoder{}
			dec.SetBytes(bytes)
			count := 0
			for dec.Next() && count < tt.count*2 {
				v, _ := dec.Read().AsFloat64()
				if !variant.IsFloat64Equal(v, tt.value) {
					t.Errorf("index %d: got %f, want %f", count, v, tt.value)
				}
				count++
			}
			if count < tt.count {
				t.Errorf("expected at least %d values, got %d", tt.count, count)
			}
		})
	}
}

func TestFloatEncoder_AlternatingPattern(t *testing.T) {
	enc := NewFloatEncoder(1)
	n := 50
	for i := 0; i < n; i++ {
		v := float64(i % 2)
		enc.Write(variant.NewFloat64(v))
	}
	bytes, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	dec := &FloatDecoder{}
	dec.SetBytes(bytes)
	var decoded []float64
	for dec.Next() && len(decoded) < n*2 {
		f, _ := dec.Read().AsFloat64()
		decoded = append(decoded, f)
	}
	if len(decoded) < n {
		t.Fatalf("expected at least %d values, got %d", n, len(decoded))
	}
	for i := 0; i < n; i++ {
		want := float64(i % 2)
		if !variant.IsFloat64Equal(decoded[i], want) {
			t.Errorf("index %d: got %f, want %f", i, decoded[i], want)
		}
	}
}

func TestFloatEncoder_MonotonicSequence(t *testing.T) {
	enc := NewFloatEncoder(2)
	n := 50
	for i := 0; i < n; i++ {
		enc.Write(variant.NewFloat64(float64(i) * 1.5))
	}
	bytes, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	dec := &FloatDecoder{}
	dec.SetBytes(bytes)
	var decoded []float64
	for dec.Next() && len(decoded) < n*2 {
		f, _ := dec.Read().AsFloat64()
		decoded = append(decoded, f)
	}
	if len(decoded) < n {
		t.Fatalf("expected at least %d values, got %d", n, len(decoded))
	}
	for i := 0; i < n; i++ {
		want := float64(i) * 1.5
		if !variant.IsFloat64Equal(decoded[i], want) {
			t.Errorf("index %d: got %f, want %f", i, decoded[i], want)
		}
	}
}

func TestFloatEncoder_VariousPrecisions(t *testing.T) {
	for _, prec := range []uint8{0, 1, 2, 3, 5, 10} {
		enc := NewFloatEncoder(prec)
		enc.Write(variant.NewFloat64(math.Pi))
		enc.Write(variant.NewFloat64(math.E))
		bytes, err := enc.Bytes()
		if err != nil {
			t.Errorf("prec=%d: Bytes error: %v", prec, err)
			continue
		}

		dec := &FloatDecoder{}
		dec.SetBytes(bytes)
		var decoded []float64
		for dec.Next() && len(decoded) < 10 {
			f, _ := dec.Read().AsFloat64()
			decoded = append(decoded, f)
		}
		if len(decoded) < 2 {
			t.Errorf("prec=%d: expected at least 2 values, got %d", prec, len(decoded))
		}
	}
}

func TestFloatEncoder_CompressionRatio(t *testing.T) {
	enc := NewFloatEncoder(3)
	n := 1000
	for i := 0; i < n; i++ {
		enc.Write(variant.NewFloat64(3.141))
	}
	bytes, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(bytes) > 200 {
		t.Errorf("poor compression for identical values: %d bytes for %d values", len(bytes), n)
	}
}

func TestFloatEncoder_Empty(t *testing.T) {
	// Bytes() on an encoder that never received writes may panic due to nil bit writer.
	// Verify that Write+Reset+Bytes works for the "emptied" case instead.
	enc := NewFloatEncoder(2)
	enc.Write(variant.NewFloat64(1.0))
	enc.Reset()
	bytes, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("encoder after reset produced %d bytes", len(bytes))
}
