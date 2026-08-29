package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

// handleBatchJSON 是 /api/v1/batch 的 JSON 通道：手写流式解析器，替代
// encoding/json 的 token 遍历 + 每点反射解码（pprof 中 JSON 通道 ~60% CPU
// 的来源），并保持既有语义：
//
//   - 流式：请求体逐块读入复用缓冲，不整体驻留内存（内存恒定，与二进制
//     通道一致），边读边攒批入库；
//   - 两段式：先把 points 数组的每个元素整体拷贝进请求级 scratch（跨块
//     安全），再在内存中按字段解析——tag/时间戳/值全程零分配快路径
//     （tag 走 internBytes 驻留、数字走 jsonNumberToVariant、整数走
//     parseSpanInt64）；
//   - 未知字段与未知顶层键跳过但不报错（与旧行为一致）；
//   - 值类型语义与 valueType 完全复用 ValueToVariant。
//
// 行为差异（有意收紧的边界）：points 元素必须为对象、points 必须是数组、
// 时间戳必须是合法 JSON 整数——旧反射路径对这几类畸形输入会静默吞掉或
// 产生费解错误，现在统一返回 400。

// jsonBatchParser 持有流式解析状态。
type jsonBatchParser struct {
	src  *blockReader // 复用二进制通道的块读缓冲（跨块 refill 安全）
	elem []byte       // 当前值原始字节的请求级 scratch（供 span 解析，跨块安全）
	key  []byte       // 当前键解码字节的请求级 scratch
}

func newJSONBatchParser(br *bufio.Reader) *jsonBatchParser {
	return &jsonBatchParser{
		src:  newBlockReader(br),
		elem: make([]byte, 0, 256), // 平均点大小，摊薄 scratch 扩容
		key:  make([]byte, 0, 16),
	}
}

// peek 返回当前字节但不消费。流式：缓冲耗尽时 refill。
func (p *jsonBatchParser) peek() (byte, error) {
	if p.src.pos >= p.src.end {
		if err := p.src.refill(1); err != nil {
			return 0, err
		}
	}
	return p.src.buf[p.src.pos], nil
}

// consume 消费一个字节（调用方须先 peek 确认有字节）。
func (p *jsonBatchParser) consume() {
	p.src.pos++
}

// skipSpace 跳过空白（不消费其他字符）。
func (p *jsonBatchParser) skipSpace() error {
	for {
		b, err := p.peek()
		if err != nil {
			return err
		}
		if b != ' ' && b != '\t' && b != '\n' && b != '\r' {
			return nil
		}
		p.consume()
	}
}

// expectByte 消费并校验一个字节。
func (p *jsonBatchParser) expectByte(want byte) error {
	b, err := p.peek()
	if err != nil {
		return err
	}
	if b != want {
		return fmt.Errorf("expected %q, found %q", want, b)
	}
	p.consume()
	return nil
}

// copyValue 把下一个 JSON 值（对象/数组/字符串/数字/字面量）的原始字节
// 追加到 dst 并返回。字符串/嵌套感知：引号内的 { } [ ] 不参与配平，
// 反斜杠转义跳过下一字节；深度 0 的分隔符（, } ]）不消费，留给调用方。
// 结果在 dst 里（请求级 scratch），流式 refill 不使其失效。
//
// 热循环直接索引读缓冲（不进 peek/consume 函数），仅在窗口耗尽时 refill，
// 逐字节成本约为一次分支+追加。
func (p *jsonBatchParser) copyValue(dst []byte) ([]byte, error) {
	depth := 0
	inString := false
	esc := false
	started := false
	r := p.src
	for {
		if r.pos >= r.end {
			if err := r.refill(1); err != nil {
				return nil, err
			}
		}
		for r.pos < r.end {
			b := r.buf[r.pos]
			if !started {
				if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
					r.pos++
					continue
				}
				started = true
			}
			if depth == 0 && !inString && (b == ',' || b == '}' || b == ']') {
				return dst, nil // 标量结束：分隔符留给调用方
			}
			r.pos++
			dst = append(dst, b)
			if inString {
				if esc {
					esc = false
				} else if b == '\\' {
					esc = true
				} else if b == '"' {
					inString = false
					if depth == 0 {
						return dst, nil
					}
				}
				continue
			}
			switch b {
			case '"':
				inString = true
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return dst, nil
				}
			}
		}
	}
}

// readKey 读取下一个对象键（JSON 字符串）并解码到 p.key。
func (p *jsonBatchParser) readKey() ([]byte, error) {
	if err := p.skipSpace(); err != nil {
		return nil, err
	}
	b, err := p.peek()
	if err != nil {
		return nil, err
	}
	if b != '"' {
		return nil, errors.New("expected string key")
	}
	raw, err := p.copyValue(p.elem[:0])
	if err != nil {
		return nil, err
	}
	return jsonStringValue(raw, p.key)
}

// bytesEqString 零分配比较字节切片与字符串。
func bytesEqString(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		if b[i] != s[i] {
			return false
		}
	}
	return true
}

// jsonStringValue 解析 JSON 字符串字面量（含引号）为解码后的内容。
// 无转义时零拷贝返回引号内切片（同时校验控制字符）；含转义时经
// encoding/json 解码（与旧反射路径语义逐字节一致）并复用 buf。
func jsonStringValue(raw []byte, buf []byte) ([]byte, error) {
	n := len(raw)
	if n < 2 || raw[0] != '"' || raw[n-1] != '"' {
		return nil, errors.New("expected a JSON string")
	}
	inner := raw[1 : n-1]
	for _, c := range inner {
		if c == '\\' {
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				return nil, err
			}
			return append(buf[:0], s...), nil
		}
		if c < 0x20 {
			return nil, errors.New("invalid control character in JSON string")
		}
	}
	return inner, nil
}

// skipSpaceIn 跳过内存缓冲中的空白（applyPoint 内部使用）。
func skipSpaceIn(b []byte, i *int) {
	for *i < len(b) {
		c := b[*i]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			return
		}
		*i++
	}
}

// scanStringIn 从 b[*i] == '"' 扫描完整字符串字面量，返回其区间（含引号）
// 与是否含转义，*i 推进到闭引号之后。
func scanStringIn(b []byte, i *int) (tokenSpan, bool, error) {
	start := *i
	esc := false
	j := start + 1
	for j < len(b) {
		c := b[j]
		if c == '\\' {
			esc = true
			j += 2
			continue
		}
		if c == '"' {
			*i = j + 1
			return tokenSpan{start: start, end: j + 1}, esc, nil
		}
		if c < 0x20 {
			return tokenSpan{}, false, errors.New("invalid control character in JSON string")
		}
		j++
	}
	return tokenSpan{}, false, errors.New("unterminated string")
}

// scanValueIn 扫描 b 中从 *i 开始的完整 JSON 值（字符串/数字/字面量/
// 对象/数组），*i 推进到值末尾。跳过未知字段值时使用。
func scanValueIn(b []byte, i *int) error {
	if *i >= len(b) {
		return errors.New("unexpected end of point")
	}
	start := *i
	switch c := b[start]; {
	case c == '"':
		sp, _, err := scanStringIn(b, i)
		if err != nil {
			return err
		}
		*i = sp.end
		return nil
	case c == '{' || c == '[':
		depth := 1
		j := start + 1
		inString := false
		esc := false
		for j < len(b) {
			cc := b[j]
			if inString {
				if esc {
					esc = false
				} else if cc == '\\' {
					esc = true
				} else if cc == '"' {
					inString = false
				}
			} else {
				switch cc {
				case '"':
					inString = true
				case '{', '[':
					depth++
				case '}', ']':
					depth--
					if depth == 0 {
						*i = j + 1
						return nil
					}
				}
			}
			j++
		}
		return errors.New("unterminated object or array")
	case c == 't':
		if *i+4 <= len(b) && bytesEqString(b[start:start+4], "true") {
			*i = start + 4
			return nil
		}
	case c == 'f':
		if *i+5 <= len(b) && bytesEqString(b[start:start+5], "false") {
			*i = start + 5
			return nil
		}
	case c == 'n':
		if *i+4 <= len(b) && bytesEqString(b[start:start+4], "null") {
			*i = start + 4
			return nil
		}
	case c == '-' || (c >= '0' && c <= '9'):
		j := start + 1
		for j < len(b) {
			cc := b[j]
			if cc == ' ' || cc == '\t' || cc == '\n' || cc == '\r' || cc == ',' || cc == '}' || cc == ']' {
				break
			}
			j++
		}
		*i = j
		return nil
	}
	return fmt.Errorf("invalid value at byte %d", start)
}

// applyPoint 解析 points 数组的一个元素（copyValue 得到的对象原始字节），
// 填充 out 并追加到 g。tag/valueType 字符串解码复用 ValueToVariant 的
// 语义；数字与时间戳走零分配手工解析。
func (p *jsonBatchParser) applyPoint(elem []byte, g *StreamIngestor, table string, out *tsdb.TagPoint) error {
	i := 0
	skipSpaceIn(elem, &i)
	if i >= len(elem) || elem[i] != '{' {
		return errors.New("expected object")
	}
	i++
	out.Tag = ""
	out.Timestamp = 0
	var (
		ts    int64
		valS  = -1
		valE  = -1
		vtype string
	)
	for {
		skipSpaceIn(elem, &i)
		if i >= len(elem) {
			return errors.New("unexpected end of point")
		}
		if elem[i] == '}' {
			i++
			break
		}
		if elem[i] != '"' {
			return errors.New("expected string key")
		}
		keyRaw, keyEsc, err := scanStringIn(elem, &i)
		if err != nil {
			return err
		}
		var keyBytes []byte
		if keyEsc {
			keyBytes, err = jsonStringValue(elem[keyRaw.start:keyRaw.end], p.key)
			if err != nil {
				return err
			}
		} else {
			// 无转义：scanStringIn 已校验，直接取引号内区间，免二次扫描。
			keyBytes = elem[keyRaw.start+1 : keyRaw.end-1]
		}
		skipSpaceIn(elem, &i)
		if i >= len(elem) || elem[i] != ':' {
			return errors.New("expected ':'")
		}
		i++
		skipSpaceIn(elem, &i)
		if i >= len(elem) {
			return errors.New("unexpected end of point")
		}
		switch {
		case bytesEqString(keyBytes, "tag"):
			if elem[i] != '"' {
				return errors.New("tag must be a string")
			}
			tagRaw, tagEsc, err := scanStringIn(elem, &i)
			if err != nil {
				return err
			}
			if tagEsc {
				decoded, err := jsonStringValue(elem[tagRaw.start:tagRaw.end], p.key)
				if err != nil {
					return err
				}
				out.Tag = g.intern(string(decoded))
			} else {
				// 无转义：零拷贝区间（指向 elem，稳定）→ internBytes 驻留。
				out.Tag = g.internBytes(elem[tagRaw.start+1 : tagRaw.end-1])
			}
		case bytesEqString(keyBytes, "timestamp"):
			start := i
			if err := scanValueIn(elem, &i); err != nil {
				return err
			}
			span := elem[start:i]
			if len(span) == 0 || span[0] == '+' {
				return errors.New("invalid timestamp")
			}
			var ok bool
			if ts, ok = parseSpanInt64(span, 0, len(span)); !ok {
				return fmt.Errorf("invalid timestamp %q", span)
			}
		case bytesEqString(keyBytes, "value"):
			valS = i
			if err := scanValueIn(elem, &i); err != nil {
				return err
			}
			valE = i
		case bytesEqString(keyBytes, "valueType"):
			if elem[i] != '"' {
				return errors.New("valueType must be a string")
			}
			vtRaw, vtEsc, err := scanStringIn(elem, &i)
			if err != nil {
				return err
			}
			var vt []byte
			if vtEsc {
				vt, err = jsonStringValue(elem[vtRaw.start:vtRaw.end], p.key)
				if err != nil {
					return err
				}
			} else {
				vt = elem[vtRaw.start+1 : vtRaw.end-1]
			}
			vtype = string(vt)
		default:
			// 未知字段：跳过值（结构与字符串感知，仍校验基础语法）。
			if err := scanValueIn(elem, &i); err != nil {
				return err
			}
		}
		// 字段间分隔符：, 继续；} 结束。
		skipSpaceIn(elem, &i)
		if i >= len(elem) {
			return errors.New("unexpected end of point")
		}
		if elem[i] == ',' {
			i++
			continue
		}
		if elem[i] == '}' {
			i++
			break
		}
		return errors.New("expected ',' or '}'")
	}
	// valueType 语义完全复用 ValueToVariant：缺 value 时旧路径报
	// "valueType=int requires a number"，此处保持一致（传空 raw）。
	var raw []byte
	if valS >= 0 {
		raw = elem[valS:valE]
	}
	v, err := ValueToVariant(raw, vtype)
	if err != nil {
		return err
	}
	out.Value = v
	out.Timestamp = ts
	if err := g.Add(table, *out); err != nil {
		return err
	}
	return nil
}

// readPoints 处理 "points" 键：解析数组并逐点入库。queryTable 为查询参数
// 指定的表（优先），bodyTable 为请求体内 "table" 键的值。
func (p *jsonBatchParser) readPoints(g *StreamIngestor, queryTable, bodyTable string) error {
	if err := p.expectByte('['); err != nil {
		return err
	}
	eff := queryTable
	if eff == "" {
		eff = bodyTable
	}
	eff = g.intern(eff) // 请求级驻留一次
	var pt tsdb.TagPoint
	for {
		if err := p.skipSpace(); err != nil {
			return err
		}
		b, err := p.peek()
		if err != nil {
			return err
		}
		if b == ']' {
			p.consume()
			return nil
		}
		if b == ',' {
			p.consume()
			continue
		}
		elem, err := p.copyValue(p.elem[:0])
		if err != nil {
			return err
		}
		if err := p.applyPoint(elem, g, eff, &pt); err != nil {
			return fmt.Errorf("bad point: %v", err)
		}
	}
}

// handleBatchJSON 处理 /api/v1/batch 的 JSON 请求体。
func (s *Server) handleBatchJSON(w http.ResponseWriter, r *http.Request) {
	p := newJSONBatchParser(bufio.NewReader(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)))
	queryTable := r.URL.Query().Get("table")
	var (
		bodyTable string
		g         *StreamIngestor
	)
	if err := p.skipSpace(); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := p.expectByte('{'); err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("bad request: expected object"))
		return
	}
	for {
		if err := p.skipSpace(); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		b, err := p.peek()
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		if b == '}' {
			p.consume()
			break
		}
		key, err := p.readKey()
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
			return
		}
		if err := p.skipSpace(); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		if err := p.expectByte(':'); err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
			return
		}
		if err := p.skipSpace(); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		switch {
		case bytesEqString(key, "table"):
			raw, err := p.copyValue(p.elem[:0])
			if err != nil {
				writeErr(w, http.StatusBadRequest, err)
				return
			}
			content, err := jsonStringValue(raw, p.key)
			if err != nil {
				writeErr(w, http.StatusBadRequest, err)
				return
			}
			bodyTable = string(content)
		case bytesEqString(key, "points"):
			if g == nil {
				g = s.newStreamIngestor()
			}
			if err := p.readPoints(g, queryTable, bodyTable); err != nil {
				writeErr(w, http.StatusBadRequest, fmt.Errorf("bad request: %v", err))
				return
			}
		default:
			// 未知顶层键：跳过值。
			if _, err := p.copyValue(p.elem[:0]); err != nil {
				writeErr(w, http.StatusBadRequest, err)
				return
			}
		}
		// 键后分隔符：, 继续，} 结束。
		if err := p.skipSpace(); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		sep, err := p.peek()
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		if sep == ',' {
			p.consume()
			continue
		}
		if sep == '}' {
			p.consume()
			break
		}
		writeErr(w, http.StatusBadRequest, fmt.Errorf("bad request: expected ',' or '}', found %q", sep))
		return
	}
	if g == nil {
		writeJSON(w, http.StatusOK, map[string]any{"written": 0})
		return
	}
	written, err := g.Finish()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"written": written})
}
