package server

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"unsafe"

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
//
// 性能：解析为 span 化（只记录字节区间，不拷贝），token 扫描零分配；
// 表名/tag 以原始字节区间返回，由调用方 internBytes 零分配驻留（重复值不
// 新分配）；仅含转义或字符串值时才分配。数字与时间戳用手工逐字节解析
// （零分配），浮点用零拷贝 string 视图交给 strconv。

const (
	linePrecisionNS = "ns" // 文档默认单位；引擎不强制单位，原样存储
)

// linePoint 是解析出的一行数据。Table/Tag 是指向 line 的只读字节区间
// （不拷贝；scanner 缓冲随后会被覆写），调用方必须立即 intern 成规范串。
type linePoint struct {
	Table     tokenSpan
	TableEsc  bool
	Tag       tokenSpan
	TagEsc    bool
	Timestamp int64
	Value     variant.Variant
}

// tokenSpan 是 line 中的一段子区间 [start,end)。
type tokenSpan struct {
	start, end int
}

// spanString 返回区间文本；esc 为 true 时解码转义（扫描阶段已校验转义合法）。
func spanString(line []byte, s tokenSpan, esc bool) string {
	if !esc {
		return string(line[s.start:s.end])
	}
	buf := make([]byte, 0, s.end-s.start)
	for i := s.start; i < s.end; i++ {
		if line[i] == '\\' {
			i++
			buf = append(buf, line[i])
			continue
		}
		buf = append(buf, line[i])
	}
	return string(buf)
}

// spanEq 零分配比较区间与字符串是否相等（esc 时先解码）。
func spanEq(line []byte, s tokenSpan, esc bool, want string) bool {
	if esc {
		return spanString(line, s, true) == want
	}
	if s.end-s.start != len(want) {
		return false
	}
	for i := 0; i < len(want); i++ {
		if line[s.start+i] != want[i] {
			return false
		}
	}
	return true
}

// scanToken 从 *pos 扫描到任意分隔符（seps）或行尾，返回 token 区间（零分配）。
// 遇转义时校验转义合法性并置 *esc=true（区间仍为原始文本，由 spanString
// 解码）。返回后 *pos 指向分隔符（未消费）或行尾。
func scanToken(line []byte, pos *int, esc *bool, seps ...byte) (tokenSpan, error) {
	start := *pos
	hasEsc := false
	for *pos < len(line) {
		c := line[*pos]
		if c == '\\' {
			if *pos+1 >= len(line) {
				return tokenSpan{}, errors.New("invalid escape")
			}
			n := line[*pos+1]
			switch n {
			case ',', '=', ' ', '\\':
				hasEsc = true
				*pos += 2
				continue
			default:
				return tokenSpan{}, errors.New("invalid escape")
			}
		}
		isSep := false
		for _, s := range seps {
			if c == s {
				isSep = true
				break
			}
		}
		if isSep {
			break
		}
		*pos++
	}
	if *pos == start {
		return tokenSpan{}, errors.New("empty token")
	}
	*esc = hasEsc
	return tokenSpan{start: start, end: *pos}, nil
}

// parseLine 解析单行 line protocol。tagScratch/fieldScratch 为调用方持有的
// 复用缓冲（请求级）：parseSeries/parseFields 直接 append 进复用切片，
// 避免每行从 nil 增长临时切片。
func parseLine(line []byte, nowNS int64, tagScratch *[]tagPair, fieldScratch *[]fieldPair) (linePoint, error) {
	*tagScratch = (*tagScratch)[:0]
	*fieldScratch = (*fieldScratch)[:0]
	pos := 0

	// 1) measurement（表名）与 tag set（到第一个空格为止）。
	tspan, tableEsc, tags, err := parseSeries(line, &pos, tagScratch)
	if err != nil {
		return linePoint{}, err
	}
	tagSpan, tagEsc := tagFromSet(line, tags)

	// 2) field set（到下一个空格或行尾）。
	fields, err := parseFields(line, &pos, fieldScratch)
	if err != nil {
		return linePoint{}, err
	}

	// 3) 可选 timestamp（行尾剩余部分）。
	ts := nowNS
	if rest := bytes.TrimSpace(line[pos:]); len(rest) > 0 {
		var ok bool
		if ts, ok = parseSpanInt64(rest, 0, len(rest)); !ok {
			// 回退 strconv（保持与旧实现一致的解析范围与错误信息）。
			s := unsafe.String(unsafe.SliceData(rest), len(rest))
			var perr error
			ts, perr = strconv.ParseInt(s, 10, 64)
			if perr != nil {
				return linePoint{}, fmt.Errorf("invalid timestamp %q", s)
			}
		}
	}

	value, err := fieldsToValue(line, fields)
	if err != nil {
		return linePoint{}, err
	}
	return linePoint{
		Table:     tspan,
		TableEsc:  tableEsc,
		Tag:       tagSpan,
		TagEsc:    tagEsc,
		Timestamp: ts,
		Value:     value,
	}, nil
}

type tagPair struct {
	key, value tokenSpan
	keyEsc     bool
	valEsc     bool
}

// parseSeries 解析 measurement 与 tag set。返回表名（measurement）字节区间、
// 是否含转义，以及 tag 区间列表。tags 为复用缓冲：追加到 *tags，避免每行
// 分配切片。
func parseSeries(line []byte, pos *int, tags *[]tagPair) (tokenSpan, bool, []tagPair, error) {
	// measurement：到 ','（tag 开始）或 ' '（无 tag）为止。
	var tableEsc bool
	tspan, err := scanToken(line, pos, &tableEsc, ',', ' ')
	if err != nil {
		return tokenSpan{}, false, nil, errors.New("invalid escape in measurement")
	}

	if *pos < len(line) && line[*pos] == ',' {
		for *pos < len(line) && line[*pos] != ' ' {
			*pos++ // 跳过 ','
			var keyEsc bool
			k, err := scanToken(line, pos, &keyEsc, '=', ',', ' ')
			if err != nil {
				return tokenSpan{}, false, nil, err
			}
			if *pos >= len(line) || line[*pos] != '=' {
				return tokenSpan{}, false, nil, fmt.Errorf("malformed tag %q", spanString(line, k, keyEsc))
			}
			*pos++ // 跳过 '='
			var valEsc bool
			v, err := scanToken(line, pos, &valEsc, ',', ' ')
			if err != nil {
				return tokenSpan{}, false, nil, err
			}
			*tags = append(*tags, tagPair{key: k, keyEsc: keyEsc, value: v, valEsc: valEsc})
		}
	}
	// 跳过 measurement/tags 与 fields 之间的空格。
	for *pos < len(line) && line[*pos] == ' ' {
		*pos++
	}
	return tspan, tableEsc, *tags, nil
}

// tagFromSet 把 tag set 映射为 qv-lite 的 tag 标识（字节区间 + 转义标记）。
// 约定：含 tag=<v> 键时直接用其值作为标识，否则保留整个 tag 段文本。
func tagFromSet(line []byte, tags []tagPair) (tokenSpan, bool) {
	if len(tags) == 0 {
		return tokenSpan{}, false
	}
	for _, t := range tags {
		if spanEq(line, t.key, t.keyEsc, "tag") {
			return t.value, t.valEsc
		}
	}
	esc := false
	for _, t := range tags {
		if t.keyEsc || t.valEsc {
			esc = true
			break
		}
	}
	return tokenSpan{start: tags[0].key.start, end: tags[len(tags)-1].value.end}, esc
}

type fieldPair struct {
	key    tokenSpan
	keyEsc bool
	value  variant.Variant
}

// parseFields 解析 field set（k=v 对）。返回解析后的字段名区间与字面量。
// fields 为复用缓冲：追加到 *fields，避免每行分配切片。
func parseFields(line []byte, pos *int, fields *[]fieldPair) ([]fieldPair, error) {
	for {
		if *pos >= len(line) {
			return nil, errors.New("missing field set")
		}
		var keyEsc bool
		k, err := scanToken(line, pos, &keyEsc, '=', ' ')
		if err != nil {
			return nil, err
		}
		if *pos >= len(line) || line[*pos] != '=' {
			return nil, fmt.Errorf("malformed field %q", spanString(line, k, keyEsc))
		}
		*pos++ // 跳过 '='
		val, err := parseFieldValue(line, pos)
		if err != nil {
			return nil, err
		}
		*fields = append(*fields, fieldPair{key: k, keyEsc: keyEsc, value: val})
		// 逗号分隔继续；空格后是 timestamp。
		if *pos < len(line) && line[*pos] == ',' {
			*pos++
			continue
		}
		break
	}
	if len(*fields) == 0 {
		return nil, errors.New("missing field set")
	}
	return *fields, nil
}

// parseFieldValue 解析单个字段值字面量（引号字符串 / 数字 / bool）。
func parseFieldValue(line []byte, pos *int) (variant.Variant, error) {
	if *pos >= len(line) {
		return variant.NewEmpty(), errors.New("missing field value")
	}
	if line[*pos] == '"' {
		return parseQuotedString(line, pos)
	}
	// 数字或 bool：读到 ',' 或 ' ' 或行尾。
	var esc bool
	tok, err := scanToken(line, pos, &esc, ',', ' ')
	if err != nil {
		return variant.NewEmpty(), err
	}
	if spanEq(line, tok, esc, "true") {
		return variant.NewBool(true), nil
	}
	if spanEq(line, tok, esc, "false") {
		return variant.NewBool(false), nil
	}
	// 整数后缀 i / u（裸数字按 float 处理，与 Influx 一致）。
	if n := tok.end - tok.start; n > 1 {
		switch line[tok.end-1] {
		case 'i':
			if v, ok := parseSpanInt64(line, tok.start, tok.end-1); ok {
				return variant.NewInt64(v), nil
			}
		case 'u':
			if v, ok := parseSpanUint64(line, tok.start, tok.end-1); ok {
				return variant.NewUInt64(v), nil
			}
		}
	}
	s := unsafe.String(unsafe.SliceData(line[tok.start:tok.end]), tok.end-tok.start)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return variant.NewFloat64(f), nil
	}
	return variant.NewEmpty(), fmt.Errorf("invalid field value %q", s)
}

// parseQuotedString 解析引号字符串，支持 \" \\ \n \t \r 转义。
// 无转义时零拷贝切片 + 一次 string 转换；有转义才解码。
func parseQuotedString(line []byte, pos *int) (variant.Variant, error) {
	*pos++ // 跳过 '"'
	start := *pos
	hasEsc := false
	for *pos < len(line) {
		c := line[*pos]
		if c == '"' {
			break
		}
		if c == '\\' {
			if *pos+1 >= len(line) {
				return variant.NewEmpty(), errors.New("invalid string escape")
			}
			switch line[*pos+1] {
			case '"', '\\', 'n', 't', 'r':
				hasEsc = true
				*pos += 2
				continue
			default:
				return variant.NewEmpty(), fmt.Errorf("invalid string escape \\%c", line[*pos+1])
			}
		}
		*pos++
	}
	if *pos >= len(line) {
		return variant.NewEmpty(), errors.New("unterminated string")
	}
	end := *pos
	*pos++ // 跳过 '"'

	if !hasEsc {
		return variant.NewString(string(line[start:end])), nil
	}
	buf := make([]byte, 0, end-start)
	for i := start; i < end; i++ {
		c := line[i]
		if c == '\\' {
			i++
			switch line[i] {
			case 'n':
				buf = append(buf, '\n')
			case 't':
				buf = append(buf, '\t')
			case 'r':
				buf = append(buf, '\r')
			default:
				buf = append(buf, line[i])
			}
			continue
		}
		buf = append(buf, c)
	}
	return variant.NewString(string(buf)), nil
}

// fieldsToValue 单 field 直接用值；多 field 打包为结构体。
func fieldsToValue(line []byte, fields []fieldPair) (variant.Variant, error) {
	if len(fields) == 1 {
		return fields[0].value, nil
	}
	m := make(map[string]interface{}, len(fields))
	for _, f := range fields {
		m[spanString(line, f.key, f.keyEsc)] = f.value
	}
	return variant.New(m), nil
}
