package server

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"

	"github.com/mababaNiubi/variant"
)

// TestValueToVariantNumberFastPath 验证数字快路径（jsonNumberToken +
// jsonNumberToVariant）与通用 JSON 解码路径的语义完全一致：
//   - 整数 → int64（溢出 → uint64 → float64）
//   - 含小数点/指数 → float64
//   - 非法 JSON number（前导零 / 缺小数位 / 正号）→ 报错
//
// 以及非数字类型走慢路径不受影响。
func TestValueToVariantNumberFastPath(t *testing.T) {
	cases := []struct {
		raw  string
		vt   string
		want any // int64 / uint64 / float64 / string / bool / nil(empty)
		err  bool
	}{
		{"123", "", int64(123), false},
		{"-5", "", int64(-5), false},
		{"0", "", int64(0), false},
		{"-0", "", int64(0), false},
		{"9007199254740993", "", int64(9007199254740993), false},
		{"18446744073709551615", "", uint64(18446744073709551615), false},
		{"18446744073709551616", "", 1.8446744073709552e19, false},
		{"36.5", "", 36.5, false},
		{"1e3", "", 1000.0, false},
		{"1E3", "", 1000.0, false},
		{"1.5e-3", "", 0.0015, false},
		{"-2.5E2", "", -250.0, false},
		{"0.5", "", 0.5, false},
		// 非规范字面量：快路径拒绝，回退通用 JSON 解码——encoding/json 对其
		// 宽容处理（"01"→0、"1.2.3"→1.2、"1_000"→1），必须保持一致。
		{"01", "", int64(0), false},
		{"00", "", int64(0), false},
		{"1.2.3", "", 1.2, false},
		{"1_000", "", int64(1), false},
		// 非法 JSON：慢路径同样报错。
		{"1.", "", nil, true},
		{"+1", "", nil, true},
		{"1e", "", nil, true},
		{"-", "", nil, true},
		// 显式 valueType 路径不受快路径影响。
		{"123", "int", int64(123), false},
		{"9007199254740993", "int", int64(9007199254740993), false},
		{"18446744073709551615", "uint", uint64(18446744073709551615), false},
		{"36.5", "float", 36.5, false},
		// 非数字类型走通用解码路径。
		{"true", "", true, false},
		{"false", "", false, false},
		{"null", "", nil, false},
		{`"hello"`, "", "hello", false},
	}
	for _, c := range cases {
		v, err := ValueToVariant(json.RawMessage(c.raw), c.vt)
		if c.err {
			if err == nil {
				t.Fatalf("ValueToVariant(%q, %q): want error, got %v", c.raw, c.vt, v)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ValueToVariant(%q, %q): %v", c.raw, c.vt, err)
		}
		switch want := c.want.(type) {
		case int64:
			got, e := v.AsInt64()
			if e != nil || got != want {
				t.Fatalf("ValueToVariant(%q): got %v (%v), want int64 %d", c.raw, got, e, want)
			}
		case uint64:
			got, e := v.AsUInt64()
			if e != nil || got != want {
				t.Fatalf("ValueToVariant(%q): got %v (%v), want uint64 %d", c.raw, got, e, want)
			}
		case float64:
			got, e := v.AsFloat64()
			if e != nil || math.Abs(got-want) > 1e-9 {
				t.Fatalf("ValueToVariant(%q): got %v (%v), want float %v", c.raw, got, e, want)
			}
		case string:
			if v.AsString() != want {
				t.Fatalf("ValueToVariant(%q): got %q, want %q", c.raw, v.AsString(), want)
			}
		case bool:
			got, _ := v.AsBool()
			if got != want {
				t.Fatalf("ValueToVariant(%q): got %v, want %v", c.raw, got, want)
			}
		case nil:
			if !v.IsEmpty() {
				t.Fatalf("ValueToVariant(%q): want empty, got %v", c.raw, v)
			}
		}
	}
}

// TestNativeToVariantNumberFastPath 验证二进制批量 JSON 值路径同样命中快路径。
func TestNativeToVariantNumberFastPath(t *testing.T) {
	v, err := nativeToVariant(json.RawMessage("42"))
	if err != nil {
		t.Fatalf("nativeToVariant(42): %v", err)
	}
	if i, _ := v.AsInt64(); i != 42 {
		t.Fatalf("nativeToVariant(42): got %v, want 42", i)
	}

	v, err = nativeToVariant(json.RawMessage("2.5"))
	if err != nil {
		t.Fatalf("nativeToVariant(2.5): %v", err)
	}
	if f, _ := v.AsFloat64(); f != 2.5 {
		t.Fatalf("nativeToVariant(2.5): got %v, want 2.5", f)
	}
}

// TestJsonNumberToken 直接验证 JSON number 语法判定。
func TestJsonNumberToken(t *testing.T) {
	valid := []string{"0", "-0", "1", "-1", "123", "-123", "0.5", "-0.5", "1e3", "1E3", "1e+3", "1e-3", "1.5e-3", "123456789012345678901234567890"}
	for _, s := range valid {
		if !jsonNumberToken([]byte(s)) {
			t.Fatalf("jsonNumberToken(%q) = false, want true", s)
		}
	}
	invalid := []string{"", "-", "+1", "01", "00", "1.", ".5", "1e", "1e+", "1e-", "1.2.3", "1_000", "abc", " 1", "1 "}
	for _, s := range invalid {
		if jsonNumberToken([]byte(s)) {
			t.Fatalf("jsonNumberToken(%q) = true, want false", s)
		}
	}
}

// TestValueToVariantFloatFloat 验证 valueType=float 与快路径不冲突（防御）。
func TestValueToVariantFloatFloat(t *testing.T) {
	v, err := ValueToVariant(json.RawMessage("1e3"), "float")
	if err != nil {
		t.Fatalf("ValueToVariant: %v", err)
	}
	f, _ := v.AsFloat64()
	if f != 1000 {
		t.Fatalf("got %v, want 1000", f)
	}
	if v.Type() != variant.TypeFloat64 {
		t.Fatalf("type = %v, want float64", v.Type())
	}
}

// BenchmarkValueToVariantNumber 对比数字值的快路径（直解析）与旧通用路径
// （JSON 解码器 + interface{} 树 + json.Number）的耗时与分配。
func BenchmarkValueToVariantNumber(b *testing.B) {
	raw := json.RawMessage("12345.678")
	b.Run("fastpath", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			trimmed := bytes.TrimSpace(raw)
			if !jsonNumberToken(trimmed) {
				b.Fatal("token mismatch")
			}
			if _, err := jsonNumberToVariant(trimmed); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decoder", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var rawAny any
			d := json.NewDecoder(bytes.NewReader(raw))
			d.UseNumber()
			if err := d.Decode(&rawAny); err != nil {
				b.Fatal(err)
			}
			if _, err := jsonToVariant(rawAny); err != nil {
				b.Fatal(err)
			}
		}
	})
}
