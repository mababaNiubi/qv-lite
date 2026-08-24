package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

type writeRequest struct {
	Table     string          `json:"table"`
	Tag       string          `json:"tag"`
	Timestamp int64           `json:"timestamp"`
	Value     json.RawMessage `json:"value"`               // 原生 JSON 值
	ValueType string          `json:"valueType,omitempty"` // "int"/"uint"，value 为字符串数字
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	var req writeRequest
	if err := decodeBody(r, &req); err != nil {
		badRequest(w, err)
		return
	}
	if req.Tag == "" {
		writeErr(w, http.StatusBadRequest, errors.New("tag is required"))
		return
	}
	v, err := ValueToVariant(req.Value, req.ValueType)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !s.flushOr500(w) {
		return
	}
	written, err := s.db.Write(req.Table, req.Tag, req.Timestamp, v)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"written": written})
}

type pointRequest struct {
	Tag       string          `json:"tag"`
	Timestamp int64           `json:"timestamp"`
	Value     json.RawMessage `json:"value"`
	ValueType string          `json:"valueType,omitempty"`
}

// handleBatch 处理 /api/v1/batch：二进制高吞吐路径，或 JSON 流式数组。
func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); len(ct) >= len(BatchContentType) && ct[:len(BatchContentType)] == BatchContentType {
		s.handleBatchBinary(w, r)
		return
	}
	// JSON 流式处理：手动遍历顶层对象，边解码 points 边分批入库，不把整个
	// 请求反序列化进内存。
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	tok, err := dec.Token()
	if err != nil {
		badRequest(w, err)
		return
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		writeErr(w, http.StatusBadRequest, errors.New("bad request: expected object"))
		return
	}
	var (
		table string
		g     *StreamIngestor
	)
	applyTable := func(name string) string {
		if p := r.URL.Query().Get("table"); p != "" {
			return p // 查询参数优先
		}
		return name
	}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		key, _ := keyTok.(string)
		switch key {
		case "table":
			if err := dec.Decode(&table); err != nil {
				writeErr(w, http.StatusBadRequest, err)
				return
			}
		case "points":
			table = applyTable(table)
			if _, err := dec.Token(); err != nil { // '['
				writeErr(w, http.StatusBadRequest, err)
				return
			}
			g = s.newStreamIngestor()
			table = g.intern(table)
			var p pointRequest
			for dec.More() {
				p = pointRequest{}
				if err := dec.Decode(&p); err != nil {
					writeErr(w, http.StatusBadRequest, fmt.Errorf("bad point: %w", err))
					return
				}
				v, err := ValueToVariant(p.Value, p.ValueType)
				if err != nil {
					writeErr(w, http.StatusBadRequest, fmt.Errorf("point %q: %w", p.Tag, err))
					return
				}
				p.Tag = g.intern(p.Tag)
				if err := g.Add(table, tsdb.TagPoint{Tag: p.Tag, Timestamp: p.Timestamp, Value: v}); err != nil {
					writeErr(w, http.StatusInternalServerError, err)
					return
				}
			}
			_, _ = dec.Token() // ']'
		default:
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil { // 跳过未知字段
				writeErr(w, http.StatusBadRequest, err)
				return
			}
		}
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

// handleWriteLine 提供 InfluxDB Line Protocol 兼容的文本写入通道。流式处理：
// 边读边逐行解析，攒满一批立即入库，无需等待整个 body。
func (s *Server) handleWriteLine(w http.ResponseWriter, r *http.Request) {
	nowNS := time.Now().UnixNano()
	g := s.newStreamIngestor()
	scanner := bufio.NewScanner(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 支持最长 1MB 单行
	var tagScratch []tagPair
	var fieldScratch []fieldPair
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		p, err := parseLine(line, nowNS, &tagScratch, &fieldScratch)
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("line %d: %w", lineNo, err))
			return
		}
		// 解析只返回字节区间；立即驻留（scanner 缓冲随后会被覆写）。
		if err := g.Add(g.internSpan(line, p.Table, p.TableEsc), tsdb.TagPoint{
			Tag:       g.internSpan(line, p.Tag, p.TagEsc),
			Timestamp: p.Timestamp,
			Value:     p.Value,
		}); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
	}
	if err := scanner.Err(); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("read body: %w", err))
		return
	}
	written, err := g.Finish()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"written": written})
}

// ingest 统一入库入口，消费调用方的 points 缓冲（所有权移交）：流水线模式
// Submit 交给后台写入器零拷贝合并；直写模式 WriteBatch 同步完成后缓冲立即
// clear 归还 pointBatchPool。
func (s *Server) ingest(table string, points []tsdb.TagPoint) (int, error) {
	if s.writer != nil {
		return s.writer.Submit(table, points), nil
	}
	n, err := s.db.WriteBatch(table, points)
	clear(points)
	pointBatchPool.Put(points[:0])
	return n, err
}

// flushWrites 保证流水线缓冲已入库（读取/顺序敏感操作前调用）。
func (s *Server) flushWrites() error {
	if s.writer != nil {
		return s.writer.Flush()
	}
	return nil
}

// flushOr500 先 flushWrites，失败则写 500 并返回 false。
func (s *Server) flushOr500(w http.ResponseWriter) bool {
	if err := s.flushWrites(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return false
	}
	return true
}
