package server

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/mababaNiubi/variant"
)

// Line protocol 写入通道（InfluxDB Line Protocol 兼容子集）。
//
// 每行一个数据点，格式：
//
//	<measurement>[,<tag1>=<v1>[,<tag2>=<v2>...]] <field1>=<v1>[,<field2>=<v2>...] [<timestamp>]
//
// 映射规则：
//   - measurement → 表名（空 = default 表；`,` 与空格需转义 `\,` `\ `）；
//   - tag set → qv-lite 的 tag 标识：若含 `tag=<v>` 键则直接用其值，
//     否则用整个 tag set 的 "k=v,k2=v2" 字符串（保留全部维度）；
//   - field set：单个 field → 值直接作为该点值；多个 field → 打包为
//     结构体（map）值；
//   - timestamp：可选末尾整数，单位任意（文档约定为纳秒，与 Influx 一致）；
//     缺省用服务器当前纳秒时间。
//
// 值字面量类型推断（与 Influx 一致）：
//
//	"string"   → string（支持 \" \\ \n \t 转义）
//	true/false → bool
//	123i       → int64
//	123u       → uint64（扩展）
//	1.5 / 1e3  → float64
//
// 任何一行的解析错误都会导致整批请求返回 400（含行号）。

const (
	linePrecisionNS = "ns" // 文档默认单位；引擎不强制单位，原样存储
)

// linePoint 是解析出的一行数据（含表名，供按表分组批量写）。
type linePoint struct {
	Table     string
	Tag       string
	Timestamp int64
	Value     variant.Variant
}

// parseLine 解析单行 line protocol。
func parseLine(line []byte, nowNS int64) (linePoint, error) {
	pos := 0

	// 1) measurement（表名）与 tag set（到第一个空格为止）。
	table, tags, err := parseSeries(line, &pos)
	if err != nil {
		return linePoint{}, err
	}

	// 2) field set（到下一个空格或行尾）。
	fields, err := parseFields(line, &pos)
	if err != nil {
		return linePoint{}, err
	}

	// 3) 可选 timestamp（行尾剩余部分）。
	ts := nowNS
	if rest := bytes.TrimSpace(line[pos:]); len(rest) > 0 {
		ts, err = strconv.ParseInt(string(rest), 10, 64)
		if err != nil {
			return linePoint{}, fmt.Errorf("invalid timestamp %q", rest)
		}
	}

	tag := tagFromSet(tags)
	value, err := fieldsToValue(fields)
	if err != nil {
		return linePoint{}, err
	}
	return linePoint{Table: table, Tag: tag, Timestamp: ts, Value: value}, nil
}

// parseSeries 解析 measurement 与 tag set。返回表名与 tag 对列表。
func parseSeries(line []byte, pos *int) (string, []tagPair, error) {
	// measurement：到 ','（tag 开始）或 ' '（无 tag）为止。
	var table []byte
	for *pos < len(line) && line[*pos] != ',' && line[*pos] != ' ' {
		b, ok := unescapeByte(line, pos)
		if !ok {
			return "", nil, errors.New("invalid escape in measurement")
		}
		table = append(table, b)
		*pos++
	}
	if len(table) == 0 {
		return "", nil, errors.New("empty measurement")
	}

	var tags []tagPair
	if *pos < len(line) && line[*pos] == ',' {
		for *pos < len(line) && line[*pos] != ' ' {
			*pos++ // 跳过 ','
			key, err := readToken(line, pos, '=', ',', ' ')
			if err != nil {
				return "", nil, err
			}
			if *pos >= len(line) || line[*pos] != '=' {
				return "", nil, fmt.Errorf("malformed tag %q", key)
			}
			*pos++ // 跳过 '='
			val, err := readToken(line, pos, ',', ' ')
			if err != nil {
				return "", nil, err
			}
			tags = append(tags, tagPair{key: string(key), value: string(val)})
		}
	}
	// 跳过 measurement/tags 与 fields 之间的空格。
	for *pos < len(line) && line[*pos] == ' ' {
		*pos++
	}
	return string(table), tags, nil
}

type tagPair struct {
	key   string
	value string
}

// tagFromSet 把 tag set 映射为 qv-lite 的 tag 标识。
func tagFromSet(tags []tagPair) string {
	if len(tags) == 0 {
		return ""
	}
	for _, t := range tags {
		if t.key == "tag" {
			return t.value // 约定：tag=<v> 直接作为标识
		}
	}
	// 否则保留全部维度：k=v,k2=v2
	buf := make([]byte, 0, 32)
	for i, t := range tags {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, t.key...)
		buf = append(buf, '=')
		buf = append(buf, t.value...)
	}
	return string(buf)
}

// parseFields 解析 field set（k=v 对）。返回解析后的字段名与字面量。
func parseFields(line []byte, pos *int) ([]fieldPair, error) {
	var fields []fieldPair
	for {
		if *pos >= len(line) {
			return nil, errors.New("missing field set")
		}
		key, err := readToken(line, pos, '=', ' ')
		if err != nil {
			return nil, err
		}
		if *pos >= len(line) || line[*pos] != '=' {
			return nil, fmt.Errorf("malformed field %q", key)
		}
		*pos++ // 跳过 '='
		val, err := parseFieldValue(line, pos)
		if err != nil {
			return nil, err
		}
		fields = append(fields, fieldPair{key: string(key), value: val})
		// 逗号分隔继续；空格后是 timestamp。
		if *pos < len(line) && line[*pos] == ',' {
			*pos++
			continue
		}
		break
	}
	if len(fields) == 0 {
		return nil, errors.New("missing field set")
	}
	return fields, nil
}

type fieldPair struct {
	key   string
	value variant.Variant
}

// parseFieldValue 解析单个字段值字面量（引号字符串 / 数字 / bool）。
func parseFieldValue(line []byte, pos *int) (variant.Variant, error) {
	if *pos >= len(line) {
		return variant.NewEmpty(), errors.New("missing field value")
	}
	switch line[*pos] {
	case '"':
		// 引号字符串，支持 \" \\ \n \t \r 转义。
		*pos++
		var buf []byte
		for *pos < len(line) {
			c := line[*pos]
			if c == '"' {
				*pos++
				return variant.NewString(string(buf)), nil
			}
			if c == '\\' {
				if *pos+1 >= len(line) {
					return variant.NewEmpty(), errors.New("invalid string escape")
				}
				*pos++
				switch line[*pos] {
				case '"', '\\':
					buf = append(buf, line[*pos])
				case 'n':
					buf = append(buf, '\n')
				case 't':
					buf = append(buf, '\t')
				case 'r':
					buf = append(buf, '\r')
				default:
					return variant.NewEmpty(), fmt.Errorf("invalid string escape \\%c", line[*pos])
				}
				*pos++
				continue
			}
			buf = append(buf, c)
			*pos++
		}
		return variant.NewEmpty(), errors.New("unterminated string")
	default:
		// 数字或 bool：读到 ',' 或 ' ' 或行尾。
		start := *pos
		for *pos < len(line) && line[*pos] != ',' && line[*pos] != ' ' {
			*pos++
		}
		tok := string(line[start:*pos])
		switch tok {
		case "true":
			return variant.NewBool(true), nil
		case "false":
			return variant.NewBool(false), nil
		}
		// 整数后缀 i / u。
		if n := len(tok); n > 1 {
			switch tok[n-1] {
			case 'i':
				if v, err := strconv.ParseInt(tok[:n-1], 10, 64); err == nil {
					return variant.NewInt64(v), nil
				}
			case 'u':
				if v, err := strconv.ParseUint(tok[:n-1], 10, 64); err == nil {
					return variant.NewUInt64(v), nil
				}
			}
		}
		if v, err := strconv.ParseFloat(tok, 64); err == nil {
			return variant.NewFloat64(v), nil
		}
		return variant.NewEmpty(), fmt.Errorf("invalid field value %q", tok)
	}
}

// fieldsToValue 单 field 直接用值；多 field 打包为结构体。
func fieldsToValue(fields []fieldPair) (variant.Variant, error) {
	if len(fields) == 1 {
		return fields[0].value, nil
	}
	m := make(map[string]interface{}, len(fields))
	for _, f := range fields {
		m[f.key] = f.value
	}
	return variant.New(m), nil
}

// readToken 读取转义感知的 token（到任意分隔符为止）。
func readToken(line []byte, pos *int, seps ...byte) ([]byte, error) {
	var buf []byte
	isSep := func(c byte) bool {
		for _, s := range seps {
			if c == s {
				return true
			}
		}
		return false
	}
	for *pos < len(line) {
		c := line[*pos]
		if c == '\\' {
			b, ok := unescapeByte(line, pos)
			if !ok {
				return nil, errors.New("invalid escape")
			}
			buf = append(buf, b)
			*pos++
			continue
		}
		if isSep(c) {
			break
		}
		buf = append(buf, c)
		*pos++
	}
	if len(buf) == 0 {
		return nil, errors.New("empty token")
	}
	return buf, nil
}

// unescapeByte 处理当前位置的转义字符（若当前是 '\'）。
func unescapeByte(line []byte, pos *int) (byte, bool) {
	if line[*pos] != '\\' {
		return line[*pos], true
	}
	if *pos+1 >= len(line) {
		return 0, false
	}
	switch line[*pos+1] {
	case ',', '=', ' ', '\\':
		*pos++
		return line[*pos], true
	default:
		return 0, false
	}
}
