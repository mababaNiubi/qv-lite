package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/mababaNiubi/variant"
)

// 值编码（跨语言友好，仅原生 JSON）：
//
// 请求侧 value 字段直接用原生 JSON 值，类型由 JSON 类型推断：
//
//	36.5 / 1e3      → float64（含小数点/指数）
//	123             → int64（JSON 整数，|v| ≤ 2^53 精确）
//	"abc"           → string
//	true / false    → bool
//	{...} / [...]   → json 结构（map / list）
//	null            → empty
//
// int64 超过 2^53 时 JSON number 会丢精度——此时 value 传字符串数字，
// 并配合可选字段 valueType 声明类型：
//
//	{"table":"sensor","tag":"c","timestamp":...,"value":"9007199254740993","valueType":"int"}
//	{"table":"sensor","tag":"u","timestamp":...,"value":"18446744073709551615","valueType":"uint"}
//
// valueType 仅支持 "int" / "uint"（value 必须是字符串数字）；其余类型由
// JSON 原生类型推断，无需声明。
//
// 响应侧 points[].value 输出原生 JSON 值；仅 int/uint 携带 vtype 字段，
// 且超出 2^53 时 value 输出为字符串（保精度）：
//
//	{"timestamp":1700000000000,"value":36.5}
//	{"timestamp":1700000000000,"value":123,"vtype":"int"}
//	{"timestamp":1700000000000,"value":"9007199254740993","vtype":"int"}

const (
	vtypeInt    = "int"
	vtypeUint   = "uint"
	vtypeFloat  = "float"
	vtypeString = "string"
	vtypeBool   = "bool"
	vtypeJSON   = "json"
)

// maxSafeInt 是 JSON number 可精确表示的整数上限（2^53）。
const maxSafeInt = int64(1) << 53

// ValueToVariant 把原生 JSON value（以及可选 valueType 显式类型）转换为
// variant.Variant。
//
// valueType 为空：按 JSON 类型推断（数字→int/float、字符串→string、
// bool→bool、对象/数组→json、null→empty）。
//
// valueType 非空：显式声明类型——"int"/"uint" 时 value 为字符串或数字；
// "float"/"string"/"bool" 时 value 为对应 JSON 原生值；"json" 时 value
// 原样作为结构体。
func ValueToVariant(raw json.RawMessage, valueType string) (variant.Variant, error) {
	trimmed := bytes.TrimSpace(raw)
	switch valueType {
	case "":
		if len(trimmed) == 0 {
			return variant.NewEmpty(), nil
		}
		var rawAny any
		d := json.NewDecoder(bytes.NewReader(trimmed))
		d.UseNumber()
		if err := d.Decode(&rawAny); err != nil {
			return variant.NewEmpty(), fmt.Errorf("invalid value: %w", err)
		}
		return jsonToVariant(rawAny)
	case vtypeInt:
		s, err := jsonNumberString(trimmed)
		if err != nil {
			return variant.NewEmpty(), fmt.Errorf("valueType=int requires a number: %w", err)
		}
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return variant.NewEmpty(), fmt.Errorf("invalid int value %q: %w", s, err)
		}
		return variant.NewInt64(i), nil
	case vtypeUint:
		s, err := jsonNumberString(trimmed)
		if err != nil {
			return variant.NewEmpty(), fmt.Errorf("valueType=uint requires a number: %w", err)
		}
		u, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return variant.NewEmpty(), fmt.Errorf("invalid uint value %q: %w", s, err)
		}
		return variant.NewUInt64(u), nil
	case vtypeFloat:
		s, err := jsonNumberString(trimmed)
		if err != nil {
			return variant.NewEmpty(), fmt.Errorf("valueType=float requires a number: %w", err)
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return variant.NewEmpty(), fmt.Errorf("invalid float value %q: %w", s, err)
		}
		return variant.NewFloat64(f), nil
	case vtypeString:
		s, err := jsonString(trimmed)
		if err != nil {
			return variant.NewEmpty(), fmt.Errorf("valueType=string: %w", err)
		}
		return variant.NewString(s), nil
	case vtypeBool:
		var b bool
		if err := json.Unmarshal(trimmed, &b); err != nil {
			return variant.NewEmpty(), fmt.Errorf("valueType=bool: %w", err)
		}
		return variant.NewBool(b), nil
	case vtypeJSON:
		return nativeToVariant(trimmed)
	default:
		return variant.NewEmpty(), fmt.Errorf("unknown valueType %q (supported: int, uint, float, string, bool, json)", valueType)
	}
}

// nativeToVariant 按 JSON 类型推断 value。
func nativeToVariant(raw json.RawMessage) (variant.Variant, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return variant.NewEmpty(), nil
	}
	var rawAny any
	d := json.NewDecoder(bytes.NewReader(trimmed))
	d.UseNumber()
	if err := d.Decode(&rawAny); err != nil {
		return variant.NewEmpty(), fmt.Errorf("invalid value: %w", err)
	}
	return jsonToVariant(rawAny)
}

// VariantToRawJSON 把查询结果值编码为原生 JSON 值。
// 返回 (原生 JSON, vtype)。vtype 仅在 int/uint 时非空（客户端据此处理
// 超精度字符串）；其余类型由 JSON 类型自表达，vtype 为空。
func VariantToRawJSON(v variant.Variant) (json.RawMessage, string, error) {
	switch v.Type() {
	case variant.TypeInt64:
		i, _ := v.AsInt64()
		if i >= -maxSafeInt && i <= maxSafeInt {
			return json.RawMessage(strconv.FormatInt(i, 10)), vtypeInt, nil
		}
		// 超出 2^53：输出字符串保精度。
		raw, err := json.Marshal(strconv.FormatInt(i, 10))
		return raw, vtypeInt, err
	case variant.TypeUInt64:
		u, _ := v.AsUInt64()
		if u <= uint64(maxSafeInt) {
			return json.RawMessage(strconv.FormatUint(u, 10)), vtypeUint, nil
		}
		raw, err := json.Marshal(strconv.FormatUint(u, 10))
		return raw, vtypeUint, err
	case variant.TypeFloat64:
		f, _ := v.AsFloat64()
		raw, err := json.Marshal(f)
		return raw, "", err
	case variant.TypeString:
		raw, err := json.Marshal(v.AsString())
		return raw, "", err
	case variant.TypeBool:
		b, _ := v.AsBool()
		raw, err := json.Marshal(b)
		return raw, "", err
	case variant.TypeMap, variant.TypeList:
		raw, err := json.Marshal(v.AsInterface())
		return raw, "", err
	case variant.TypeEmpty:
		return json.RawMessage("null"), "", nil
	default:
		raw, err := json.Marshal(v.AsInterface())
		return raw, "", err
	}
}

// jsonToVariant 把 JSON 解码值（UseNumber）转换为 variant.Variant。
// 数字推断规则：无小数点/指数 → int64（超出 int64 → uint64 → float64）；
// 否则 float64。
func jsonToVariant(raw any) (variant.Variant, error) {
	switch t := raw.(type) {
	case nil:
		return variant.NewEmpty(), nil
	case bool:
		return variant.NewBool(t), nil
	case string:
		return variant.NewString(t), nil
	case json.Number:
		s := t.String()
		if !stringsHasAnyNumber(s) {
			if i, err := t.Int64(); err == nil {
				return variant.NewInt64(i), nil
			}
			if u, err := strconv.ParseUint(s, 10, 64); err == nil {
				return variant.NewUInt64(u), nil
			}
		}
		f, err := t.Float64()
		if err != nil {
			return variant.NewEmpty(), fmt.Errorf("bad number %q", s)
		}
		return variant.NewFloat64(f), nil
	case int64:
		return variant.NewInt64(t), nil
	case int:
		return variant.NewInt64(int64(t)), nil
	case float64:
		return variant.NewFloat64(t), nil
	case []any:
		items := make([]variant.Variant, 0, len(t))
		for _, it := range t {
			vv, err := jsonToVariant(it)
			if err != nil {
				return variant.NewEmpty(), err
			}
			items = append(items, vv)
		}
		return variant.New(items), nil
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, it := range t {
			vv, err := jsonToVariant(it)
			if err != nil {
				return variant.NewEmpty(), err
			}
			m[k] = vv
		}
		return variant.New(m), nil
	default:
		return variant.NewEmpty(), fmt.Errorf("unsupported json value %T", raw)
	}
}

// jsonString 提取 JSON 字符串值（valueType 显式类型时 value 必须是字符串）。
func jsonString(raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("expected a string, got %s", raw)
	}
	return s, nil
}

// jsonNumberString 从 JSON 值提取数字文本：接受裸数字（123）或字符串（"123"）。
func jsonNumberString(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && trimmed[0] == '"' {
		return jsonString(trimmed)
	}
	return string(trimmed), nil
}

// stringsHasAnyNumber 判断数字字面量是否含小数点/指数（含则为 float）。
func stringsHasAnyNumber(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.', 'e', 'E':
			return true
		}
	}
	return false
}
