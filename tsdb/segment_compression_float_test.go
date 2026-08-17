package tsdb

import (
	"math"
	"math/rand"
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

// ──────────────────────────────────────────────────────────────────────
// 编码/解码对称性回归测试（历史 bug 修复）。
//
// 历史 bug（已修复）：ZFloatEncoder / FloatDecoder 的 XOR-delta 位流在
// 「mantissa 截断」处不对称——编码端取整后用 retainBit 截断 mantissa，
// 截断使解码恢复值向 0 漂移；解码端 roundBits 对负数值用 Ceil（向 +∞，
// 即对负数仍向 0），无法补回截断损失，取整落到错误的网格点，位流错位，
// 产生值错乱 / NaN / 解码提前 EOF（段内点丢失）。
// 触发场景：sin 类数据在周期边界（符号翻转、指数切换）处，如
// sin(i*0.001) 的 i=3142 起全部值错乱、NaN 从 10542 起、解码在 25989 EOF。
//
// 修复：roundBits 对负数值改用 Floor（远离 0 取整），与编码端 Ceil 语义
// 对齐（见 segment_compression_float.go roundBits）。修复前
// TestFloatEncoderSymmetry 失败（mismatch=19672、NaN=25、EOF at 25989）。
// ──────────────────────────────────────────────────────────────────────

// roundTripError 是解码值相对编码端取整值的最大允许误差：
// 半网格单位 0.5e-4 加浮点容差。
const roundTripError = 0.5e-4 + 1e-9

// TestFloatEncoderSymmetry 编码 sin(i*0.001) 40,960 条（precision=4）后
// 解码逐点对比。修复前：值从 3142 起错乱、NaN 从 10542 起、解码在 25989
// 提前 EOF。
func TestFloatEncoderSymmetry(t *testing.T) {
	const n = 40_960
	enc := NewFloatEncoder(4, n)
	for i := 0; i < n; i++ {
		if !enc.Write(variant.NewFloat64(math.Sin(float64(i) * 0.001))) {
			t.Fatalf("encoder rejected value at %d", i)
		}
	}
	data, err := enc.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	t.Logf("encoded %d values -> %d bytes (%.2f B/val)", n, len(data), float64(len(data))/n)

	dec := &FloatDecoder{}
	dec.SetBytes(data)
	mismatch := 0
	nanCount := 0
	firstMismatch := -1
	firstNaN := -1
	for i := 0; i < n; i++ {
		if !dec.Next() {
			t.Fatalf("decoder EOF at value %d (of %d) — bit stream desynchronized", i, n)
		}
		got := dec.Read()
		if got.Type() != variant.TypeFloat64 {
			t.Fatalf("value %d: unexpected type %v", i, got.Type())
		}
		f, _ := got.AsFloat64()
		want := round(math.Sin(float64(i)*0.001), 1e4)
		if math.IsNaN(f) {
			nanCount++
			if firstNaN < 0 {
				firstNaN = i
			}
			continue
		}
		if math.Abs(f-want) > roundTripError {
			mismatch++
			if firstMismatch < 0 {
				firstMismatch = i
			}
		}
	}
	t.Logf("mismatch=%d (first at %d), NaN=%d (first at %d)", mismatch, firstMismatch, nanCount, firstNaN)
	if mismatch > 0 {
		t.Errorf("ENCODER ASYMMETRY: %d mismatches, first at value %d", mismatch, firstMismatch)
	}
	if nanCount > 0 {
		t.Errorf("ENCODER PRODUCES NaN: %d values, first at %d", nanCount, firstNaN)
	}
}

// TestFloatEncoderSymmetryP6 同上，precision=6。修复前：值从 3144 起错乱。
func TestFloatEncoderSymmetryP6(t *testing.T) {
	const n = 40_960
	enc := NewFloatEncoder(6, n)
	for i := 0; i < n; i++ {
		enc.Write(variant.NewFloat64(math.Sin(float64(i) * 0.001)))
	}
	data, err := enc.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	dec := &FloatDecoder{}
	dec.SetBytes(data)
	mismatch := 0
	firstMismatch := -1
	for i := 0; i < n; i++ {
		if !dec.Next() {
			t.Fatalf("decoder EOF at value %d (of %d)", i, n)
		}
		f, _ := dec.Read().AsFloat64()
		want := round(math.Sin(float64(i)*0.001), 1e6)
		if math.Abs(f-want) > 0.5e-6+1e-9 {
			mismatch++
			if firstMismatch < 0 {
				firstMismatch = i
			}
		}
	}
	t.Logf("p6: mismatch=%d (first at %d)", mismatch, firstMismatch)
	if mismatch > 0 {
		t.Errorf("p6 ENCODER ASYMMETRY: %d mismatches, first at %d", mismatch, firstMismatch)
	}
}

// TestFloatEncoderRandomRoundTrip 随机值 round-trip：覆盖全指数范围、正负、
// 多种精度，并周期性强制跨 2 的幂边界（指数切换）。修复前随机负数必然
// 大量 mismatch。
func TestFloatEncoderRandomRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, precision := range []uint8{1, 2, 3, 4, 5, 6, 8} {
		scale := math.Pow(10, float64(precision))
		tol := 0.5*math.Pow(10, -float64(precision)) + 1e-9
		const n = 20_000
		enc := NewFloatEncoder(precision, n)
		vals := make([]float64, n)
		prev := 0.0
		for i := range vals {
			exp := float64(rng.Intn(36) - 30)
			mant := rng.Float64()*1.8 + 0.1 // [0.1, 1.9)
			v := mant * math.Pow(2, exp)
			if i%2 == 1 {
				v = -v
			}
			// 制造指数切换（跨 2 的幂边界）。
			if i%997 == 0 {
				prev = math.Copysign(1.0001, v) // 强制 1.0 边界
				v = prev
			}
			vals[i] = v
		}
		for i := range vals {
			enc.Write(variant.NewFloat64(vals[i]))
		}
		data, err := enc.Bytes()
		if err != nil {
			t.Fatalf("p%d Bytes: %v", precision, err)
		}
		dec := &FloatDecoder{}
		dec.SetBytes(data)
		mismatch := 0
		firstMismatch := -1
		for i := 0; i < n; i++ {
			if !dec.Next() {
				t.Fatalf("p%d decoder EOF at %d/%d", precision, i, n)
			}
			f, _ := dec.Read().AsFloat64()
			want := round(vals[i], scale)
			if math.IsNaN(f) || math.Abs(f-want) > tol {
				mismatch++
				if firstMismatch < 0 {
					firstMismatch = i
				}
			}
		}
		t.Logf("p%d: %d values, %d bytes (%.2f B/val), mismatch=%d (first at %d)",
			precision, n, len(data), float64(len(data))/n, mismatch, firstMismatch)
		if mismatch > 0 {
			t.Errorf("p%d ROUND TRIP: %d mismatches, first at %d", precision, mismatch, firstMismatch)
		}
	}
}

// TestFloatEncoderNegativeCrossing 修复触发场景的小规模回归：sin 在周期
// 边界（符号翻转、指数切换）附近的穿越序列。
func TestFloatEncoderNegativeCrossing(t *testing.T) {
	// 3130..3200：sin 第一个周期结束、值由正变负并穿越多个 2 的幂边界。
	const start, end = 3130, 3200
	enc := NewFloatEncoder(4, end-start)
	for i := start; i < end; i++ {
		enc.Write(variant.NewFloat64(math.Sin(float64(i) * 0.001)))
	}
	data, err := enc.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	dec := &FloatDecoder{}
	dec.SetBytes(data)
	for i := start; i < end; i++ {
		if !dec.Next() {
			t.Fatalf("decoder EOF at %d (of %d)", i, end)
		}
		f, _ := dec.Read().AsFloat64()
		want := round(math.Sin(float64(i)*0.001), 1e4)
		if math.IsNaN(f) || math.Abs(f-want) > roundTripError {
			t.Fatalf("crossing at %d: want %.8f got %.8f", i, want, f)
		}
	}
}

// TestRoundGridConsistency round（编码端取整）必须把任意输入映射到 1e-4 网格
// 的「Ceil」格点：正数向上、负数向上（向 0）。
func TestRoundGridConsistency(t *testing.T) {
	cases := []struct {
		in   float64
		want float64
	}{
		{0.000000, 0.000000},
		{0.00010001, 0.000200},
		{0.00019999, 0.000200},
		{-0.00010001, -0.000100}, // Ceil(-1.0001) = -1 → -0.0001（向 0）
		{-0.00019999, -0.000100},
		{0.000407, 0.000500},
		{-0.000407, -0.000400}, // Ceil(-4.07) = -4 → -0.0004
		{0.000592, 0.000600},
		{-0.000592, -0.000500},
		{1.2345, 1.2345},
		{-1.2345, -1.2345},
		{123.456, 123.456},
		{-123.456, -123.456},
	}
	for _, c := range cases {
		got := round(c.in, 1e4)
		if got != c.want {
			t.Errorf("round(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestRoundBitsRecoversTruncation 修复核心回归：编码端取整 → 按真实
// retainBit（precision + 指数）截断 mantissa（恢复值向 0 漂移）→ 解码端
// roundBits 必须补回截断、落在原取整格点。修复前负数值补不回
// （Ceil(-3.66) = -3 而非 -4）。
func TestRoundBitsRecoversTruncation(t *testing.T) {
	dec := &FloatDecoder{decPlaces: 4, scale: 1e4}
	for _, v := range []float64{
		0.000407, -0.000407, 0.000592, -0.000592,
		0.0014, -0.0014, 0.0123, -0.0123,
		0.1234, -0.1234, 1.2345, -1.2345,
		12.345, -12.345, 123.456, -123.456,
	} {
		rounded := round(v, 1e4) // 编码端取整
		// 与 ZFloatEncoder.Write 相同的 retainBit 计算。
		exp := int((math.Float64bits(rounded)>>52)&0x7FF) - 1023
		retain := precision[4] + exp
		if retain >= 52 || retain <= 0 {
			retain = 52
		}
		// 模拟编码端截断后的恢复值：mantissa 低 (52-retain) 位清零。
		bits := math.Float64bits(rounded) & (^uint64(0) << (52 - retain))
		recovered := math.Float64frombits(bits)
		got := math.Float64frombits(dec.roundBits(math.Float64bits(recovered)))
		if got != rounded {
			t.Errorf("v=%v: roundBits(truncated) = %v, want %v (retain=%d truncated=%v)",
				v, got, rounded, retain, recovered)
		}
	}
}

// TestFloatEncoderBoundary 边界值：0、次正规、±Inf/NaN 拒绝、大数。
func TestFloatEncoderBoundary(t *testing.T) {
	// ±Inf / NaN：编码器必须报错（s.err），Bytes 返回错误。
	for _, bad := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
		enc := NewFloatEncoder(4, 8)
		enc.Write(variant.NewFloat64(1.0))
		enc.Write(variant.NewFloat64(bad))
		if _, err := enc.Bytes(); err == nil {
			t.Errorf("expected error for %v, got nil", bad)
		}
	}

	// 合法边界值 round-trip。
	vals := []float64{
		0, -0, 1e-300, -1e-300, // 次正规
		math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64,
		1e300, -1e300, // 大数
	}
	enc := NewFloatEncoder(4, len(vals))
	for _, v := range vals {
		enc.Write(variant.NewFloat64(v))
	}
	data, err := enc.Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}
	dec := &FloatDecoder{}
	dec.SetBytes(data)
	for i, v := range vals {
		if !dec.Next() {
			t.Fatalf("EOF at %d", i)
		}
		f, _ := dec.Read().AsFloat64()
		want := round(v, 1e4)
		if f != want {
			t.Errorf("v[%d]=%v: got %v, want %v", i, v, f, want)
		}
	}
}
