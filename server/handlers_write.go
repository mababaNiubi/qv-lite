package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// handleBatch 处理 /api/v1/batch：二进制高吞吐路径，或 JSON 流式数组。
func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); len(ct) >= len(BatchContentType) && ct[:len(BatchContentType)] == BatchContentType {
		s.handleBatchBinary(w, r)
		return
	}
	// JSON 通道：手写流式解析（见 json_batch.go），边解析边分批入库，不把
	// 整个请求反序列化进内存。
	s.handleBatchJSON(w, r)
}

// handleWriteLine 提供 InfluxDB Line Protocol 兼容的文本写入通道。流式处理：
// 边读边逐行解析，攒满一批立即入库，无需等待整个 body。
func (s *Server) handleWriteLine(w http.ResponseWriter, r *http.Request) {
	nowNS := time.Now().UnixNano()
	g := s.newStreamIngestor()
	// 预分配整批缓冲：Line 流通常是单表连续流，避免 pending 切片从
	// 1024 起步的 1.25x 扩容链（扩容总拷贝 ≈ 最终容量 5 倍，占该路径
	// 分配 ~67%）。
	g.firstHint = streamBatchSize
	br := bufio.NewReaderSize(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes), 64*1024)
	var tagScratch []tagPair
	var fieldScratch []fieldPair
	lineNo := 0
	for {
		line, err := br.ReadSlice('\n')
		if len(line) > 0 {
			lineNo++
			line = bytes.TrimSpace(line)
			if len(line) > 0 && line[0] != '#' {
				p, perr := parseLine(line, nowNS, &tagScratch, &fieldScratch)
				if perr != nil {
					writeErr(w, http.StatusBadRequest, fmt.Errorf("line %d: %w", lineNo, perr))
					return
				}
				// 解析只返回字节区间；立即驻留（读缓冲随后会被覆写）。
				if aerr := g.Add(g.internSpan(line, p.Table, p.TableEsc), tsdb.TagPoint{
					Tag:       g.internSpan(line, p.Tag, p.TagEsc),
					Timestamp: p.Timestamp,
					Value:     p.Value,
				}); aerr != nil {
					writeErr(w, http.StatusInternalServerError, aerr)
					return
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			if err == bufio.ErrBufferFull {
				// 单行超过读缓冲：取剩余部分拼接（行长由 MaxBodyBytes 兜底）。
				rest, rerr := br.ReadString('\n')
				joined := make([]byte, 0, len(line)+len(rest))
				joined = append(joined, line...)
				joined = append(joined, rest...)
				lineNo++
				joined = bytes.TrimSpace(joined)
				if len(joined) > 0 && joined[0] != '#' {
					p, perr := parseLine(joined, nowNS, &tagScratch, &fieldScratch)
					if perr != nil {
						writeErr(w, http.StatusBadRequest, fmt.Errorf("line %d: %w", lineNo, perr))
						return
					}
					if aerr := g.Add(g.internSpan(joined, p.Table, p.TableEsc), tsdb.TagPoint{
						Tag:       g.internSpan(joined, p.Tag, p.TagEsc),
						Timestamp: p.Timestamp,
						Value:     p.Value,
					}); aerr != nil {
						writeErr(w, http.StatusInternalServerError, aerr)
						return
					}
				}
				if rerr == io.EOF {
					break
				}
				continue
			}
			writeErr(w, http.StatusBadRequest, fmt.Errorf("read body: %w", err))
			return
		}
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
