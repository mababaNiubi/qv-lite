package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

// Server wraps an embedded tsdb engine and exposes it over a JSON HTTP API.
type Server struct {
	cfg    *Config
	db     *tsdb.DB
	http   *http.Server
	mux    *http.ServeMux
	start  time.Time
	logger Logger
	writer *PipelinedWriter // 非 nil 时开启 decode→入库流水线
}

// Logger is the minimal logging interface used by the server. It defaults to
// a simple stderr logger when nil.
type Logger interface {
	Printf(format string, v ...any)
}

type stdLogger struct{}

func (stdLogger) Printf(format string, v ...any) {
	fmt.Fprintf(os.Stderr, "[tsdb-server] "+format+"\n", v...)
}

// New builds a Server from a config and opens the underlying tsdb database.
func New(cfg *Config, logger Logger) (*Server, error) {
	cfg.ApplyDefaults()
	if logger == nil {
		logger = stdLogger{}
	}
	db, err := tsdb.Open(cfg.DB, context.Background())
	if err != nil {
		return nil, fmt.Errorf("open tsdb at %s: %w", cfg.DB.Path, err)
	}
	s := &Server{
		cfg:    cfg,
		db:     db,
		mux:    http.NewServeMux(),
		start:  time.Now(),
		logger: logger,
	}
	// 可选：decode→入库流水线（编解码与入库并行，合并小批提高吞吐）。
	if cfg.WriteBufferMs > 0 {
		s.writer = NewPipelinedWriter(db, int(cfg.WriteBufferMs), cfg.WriteBatchSize)
	}
	s.routes()
	// Setup a production-ish http.Server with sane timeouts.
	s.http = &http.Server{
		Addr:         cfg.Listen,
		Handler:      s.Handler(),
		ReadTimeout:  mustDuration(cfg.ReadTimeout, 30*time.Second),
		WriteTimeout: mustDuration(cfg.WriteTimeout, 60*time.Second),
	}
	return s, nil
}

// Handler returns the root http.Handler (useful for embedding in tests or
// mounting under another router). It applies auth (when configured) and panic
// recovery.
func (s *Server) Handler() http.Handler {
	return s.withAuth(s.withRecovery(s.mux))
}

// DB exposes the underlying engine for advanced use.
func (s *Server) DB() *tsdb.DB { return s.db }

func (s *Server) routes() {
	if s.cfg.EnablePprof {
		registerPprof(s.mux)
	}
	s.mux.HandleFunc("GET /api/v1/health", s.handleHealth)
	s.mux.HandleFunc("POST /api/v1/tables", s.handleCreateTable)
	s.mux.HandleFunc("GET /api/v1/tables", s.handleListTables)
	s.mux.HandleFunc("POST /api/v1/write", s.handleWrite)
	s.mux.HandleFunc("POST /api/v1/write/line", s.handleWriteLine)
	s.mux.HandleFunc("POST /api/v1/batch", s.handleBatch)
	s.mux.HandleFunc("POST /api/v1/query", s.handleQuery)
	s.mux.HandleFunc("POST /api/v1/query/latest", s.handleQueryLatest)
}

// ListenAndServe starts the HTTP listener. It blocks until the context is
// cancelled or the server fails. On ctx cancellation it performs graceful
// shutdown, waiting for in-flight requests and then closing the engine.
func (s *Server) ListenAndServe(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.logger.Printf("listening on %s, data path %s", s.cfg.Listen, s.cfg.DB.Path)
		if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		return s.Shutdown(context.Background())
	case err := <-errCh:
		return err
	}
}

// Shutdown gracefully stops accepting new connections, gives in-flight
// requests up to timeout to finish, then closes the engine.
func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.http.Shutdown(ctx); err != nil {
		s.logger.Printf("http shutdown: %v", err)
	}
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			s.logger.Printf("pipeline writer close: %v", err)
		}
	}
	return s.db.Close()
}

// RunListenAndServeSignal is a convenience that wires SIGINT/SIGTERM to a
// graceful shutdown. Call it from main.
func (s *Server) RunListenAndServeSignal() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	err := s.ListenAndServe(ctx)
	if ctx.Err() != nil {
		return nil // clean shutdown via signal
	}
	return err
}

// ---------------------------------------------------------------- handlers

type apiError struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, apiError{Error: err.Error()})
}

func decodeBody(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	return dec.Decode(dst)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"uptime":  time.Since(s.start).String(),
		"tables":  s.db.TableNames(),
		"version": "1",
	})
}

func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	infos := s.db.TableInfos()
	type tv struct {
		Name           string                 `json:"name"`
		Desc           string                 `json:"desc"`
		Type           tsdb.ColumnType        `json:"type"`
		FloatPrecision uint8                  `json:"float_precision"`
		Structure      []tsdb.ColumnAttribute `json:"structure,omitempty"`
	}
	out := make([]tv, 0, len(infos))
	for _, info := range infos {
		out = append(out, tv{
			Name:           info.Name,
			Desc:           info.Desc,
			Type:           info.Type,
			FloatPrecision: info.FloatPrecision,
			Structure:      info.Structure,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"tables": out})
}

type createTableRequest struct {
	Name           string       `json:"name"`
	Desc           string       `json:"desc"`
	FloatPrecision uint8        `json:"float_precision"`
	Columns        []columnSpec `json:"columns,omitempty"`
}

type columnSpec struct {
	Name string `json:"name"`
	Desc string `json:"desc"`
	Type uint8  `json:"type"`
}

func (s *Server) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	var req createTableRequest
	if err := decodeBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("bad request: %w", err))
		return
	}
	if req.Name == "" {
		writeErr(w, http.StatusBadRequest, errors.New("table name is required"))
		return
	}
	// 保证写入缓冲先落库，避免建表与写入交错。
	if err := s.flushWrites(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	attr := tsdb.ColumnAttribute{
		Name:           req.Name,
		Desc:           req.Desc,
		FloatPrecision: req.FloatPrecision,
	}
	if lp := attr.FloatPrecision; lp == 0 {
		attr.FloatPrecision = 4
	}
	for _, c := range req.Columns {
		attr.Structure = append(attr.Structure, tsdb.ColumnAttribute{
			Name: c.Name, Desc: c.Desc, Type: tsdb.ColumnType(c.Type),
		})
	}
	if err := s.db.CreateTable(tsdb.TableInfo{ColumnAttribute: attr}); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, tsdb.ErrorTableExists) {
			status = http.StatusConflict
		}
		writeErr(w, status, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"created": true, "name": req.Name})
}

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
		writeErr(w, http.StatusBadRequest, fmt.Errorf("bad request: %w", err))
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
	// 保证此前流水线数据先入库（顺序写）。
	if err := s.flushWrites(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	written, err := s.db.Write(req.Table, req.Tag, req.Timestamp, v)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, tsdb.ErrorTableNotExists) {
			status = http.StatusNotFound
		} else if errors.Is(err, tsdb.ErrorTimeOut) || errors.Is(err, tsdb.ErrorValueIsEmpty) {
			status = http.StatusBadRequest
		}
		writeErr(w, status, err)
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

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	// High-throughput binary path (client.WriteBatch uses this by default).
	if ct := r.Header.Get("Content-Type"); len(ct) >= len(BatchContentType) && ct[:len(BatchContentType)] == BatchContentType {
		s.handleBatchBinary(w, r)
		return
	}
	// JSON 数组流式处理：手动遍历顶层对象，边解码 points 边分批入库，
	// 不把整个请求反序列化进内存。
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	tok, err := dec.Token()
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("bad request: %w", err))
		return
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		writeErr(w, http.StatusBadRequest, errors.New("bad request: expected object"))
		return
	}
	var (
		table    string
		urlTable = r.URL.Query().Get("table") // 查询参数优先，避免依赖 body 字段顺序
		g        *StreamIngestor
	)
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
			if urlTable != "" {
				table = urlTable
			}
		case "points":
			if urlTable != "" {
				table = urlTable
			}
			if _, err := dec.Token(); err != nil { // '['
				writeErr(w, http.StatusBadRequest, err)
				return
			}
			g = s.newStreamIngestor()
			for dec.More() {
				var p pointRequest
				if err := dec.Decode(&p); err != nil {
					writeErr(w, http.StatusBadRequest, fmt.Errorf("bad point: %w", err))
					return
				}
				v, err := ValueToVariant(p.Value, p.ValueType)
				if err != nil {
					writeErr(w, http.StatusBadRequest, fmt.Errorf("point %q: %w", p.Tag, err))
					return
				}
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

// handleWriteLine 提供 InfluxDB Line Protocol 兼容的文本写入通道，
// 跨语言零依赖（任何语言拼字符串即可）。每行一个数据点。
// 流式处理：边读边逐行解析，攒满一批立即入库，无需等待整个 body。
func (s *Server) handleWriteLine(w http.ResponseWriter, r *http.Request) {
	nowNS := time.Now().UnixNano()
	g := s.newStreamIngestor()
	scanner := bufio.NewScanner(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 支持最长 1MB 单行
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		p, err := parseLine(line, nowNS)
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("line %d: %w", lineNo, err))
			return
		}
		if err := g.Add(p.Table, tsdb.TagPoint{Tag: p.Tag, Timestamp: p.Timestamp, Value: p.Value}); err != nil {
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

// ingest 统一入库入口：未启用流水线时直接调用引擎；启用时 Submit 进缓冲
// 由后台合并入库（返回入队点数）。
func (s *Server) ingest(table string, points []tsdb.TagPoint) (int, error) {
	if s.writer != nil {
		return s.writer.Submit(table, points), nil
	}
	return s.db.WriteBatch(table, points)
}

// flushWrites 在读取/顺序敏感操作前调用，保证流水线缓冲的数据已入库。
func (s *Server) flushWrites() error {
	if s.writer != nil {
		return s.writer.Flush()
	}
	return nil
}

type queryRequest struct {
	Table          string          `json:"table"`
	Tag            string          `json:"tag"`
	StartTime      int64           `json:"start"`
	EndTime        int64           `json:"end"`
	WindowSize     int64           `json:"window,omitempty"`      // <=0 returns raw points
	Polymerization uint8           `json:"aggregation,omitempty"` // 0 avg,1 min,2 max
	Condition      json.RawMessage `json:"condition,omitempty"`
}

type pointResult struct {
	Timestamp int64           `json:"timestamp"`
	Value     json.RawMessage `json:"value"`           // 原生 JSON 值
	VType     string          `json:"vtype,omitempty"` // 仅 int/uint 时输出（超 2^53 时 value 为字符串）
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := decodeBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("bad request: %w", err))
		return
	}
	cond, err := decodeCondition(req.Condition)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	// 保证之前写入的数据已入库（读一致性）。
	if err := s.flushWrites(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	raw, err := s.db.Query(req.Table, req.Tag, req.StartTime, req.EndTime, req.WindowSize, req.Polymerization, cond)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]pointResult, 0, len(raw))
	for _, p := range raw {
		val, vtype, e := VariantToRawJSON(p.V)
		if e != nil {
			writeErr(w, http.StatusInternalServerError, e)
			return
		}
		out = append(out, pointResult{Timestamp: p.Tms, Value: val, VType: vtype})
	}
	writeJSON(w, http.StatusOK, map[string]any{"points": out, "count": len(out)})
}

func (s *Server) handleQueryLatest(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Table string `json:"table"`
		Tag   string `json:"tag"`
	}
	if err := decodeBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("bad request: %w", err))
		return
	}
	if err := s.flushWrites(); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	p, err := s.db.QueryLatest(req.Table, req.Tag)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, tsdb.ErrorTableNotExists) {
			status = http.StatusNotFound
		} else if errors.Is(err, tsdb.ErrorTagNotFound) || errors.Is(err, tsdb.ErrorNoDataForTag) {
			status = http.StatusNotFound
		}
		writeErr(w, status, err)
		return
	}
	if p == nil {
		writeErr(w, http.StatusNotFound, tsdb.ErrorTagNotFound)
		return
	}
	val, vtype, err := VariantToRawJSON(p.V)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, pointResult{Timestamp: p.Tms, Value: val, VType: vtype})
}
