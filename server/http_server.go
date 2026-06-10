package server

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/mababaNiubi/variant"
	"io"
	"net"
	"net/http"
	"os"
	"qvdb/tsdb"
	"strings"
)

type HttpServer struct {
	executor *Executor
	listener net.Listener
	server   *http.Server
	db       *tsdb.DB
}

func NewHttpServer(db *tsdb.DB, addr string) (*HttpServer, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	executor := NewExecutor(db)
	hs := &HttpServer{
		executor: executor,
		listener: listener,
		db:       db,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/write/batch", hs.handleWriteBatch)
	mux.HandleFunc("/query", hs.handleQuery)
	mux.HandleFunc("/health", hs.handleHealth)
	mux.HandleFunc("/create-table", hs.handleCreateTable)
	hs.server = &http.Server{Handler: mux}
	return hs, nil
}

func (hs *HttpServer) Addr() string {
	return hs.listener.Addr().String()
}

func (hs *HttpServer) Start() error {
	fmt.Fprintf(os.Stderr, "HTTP server listening on %s\n", hs.listener.Addr())
	return hs.server.Serve(hs.listener)
}

func (hs *HttpServer) Close() error {
	return hs.server.Close()
}

func (hs *HttpServer) StartAsync() {
	go func() {
		if err := hs.Start(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "HTTP server error: %v\n", err)
		}
	}()
}

type batchRowRequest struct {
	Tag       string      `json:"tag"`
	Timestamp int64       `json:"timestamp"`
	Value     interface{} `json:"value"`
}

type batchWriteRequest struct {
	Table string            `json:"table"`
	Rows  []batchRowRequest `json:"rows"`
}

func (hs *HttpServer) handleWriteBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, &Result{Success: false, Message: "method not allowed"})
		return
	}
	body, err := decompressBody(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, &Result{Success: false, Message: err.Error()})
		return
	}
	var req batchWriteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, &Result{Success: false, Message: "invalid JSON: " + err.Error()})
		return
	}
	if len(req.Rows) == 0 {
		writeJSON(w, http.StatusOK, &Result{Success: true, Message: "inserted 0"})
		return
	}
	inserted := 0
	for i := range req.Rows {
		_, err := hs.db.Write(req.Table, req.Rows[i].Tag, req.Rows[i].Timestamp, variant.New(req.Rows[i].Value))
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, &Result{Success: false, Message: err.Error()})
			return
		}
		inserted++
	}
	writeJSON(w, http.StatusOK, &Result{Success: true, Message: fmt.Sprintf("inserted %d", inserted)})
	return
}

func decompressBody(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read body: %w", err)
	}
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		reader, err := gzip.NewReader(strings.NewReader(string(body)))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress gzip body: %w", err)
		}
		defer reader.Close()
		decompressed, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read decompressed body: %w", err)
		}
		return decompressed, nil
	}
	return body, nil
}

type conditionRequest struct {
	Column   string      `json:"column"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

type queryRequest struct {
	Table          string            `json:"table"`
	Tag            string            `json:"tag"`
	StartTime      int64             `json:"startTime"`
	EndTime        int64             `json:"endTime"`
	Limit          int64             `json:"limit"`
	Polymerization string            `json:"polymerization"`
	Condition      *conditionRequest `json:"condition,omitempty"`
}

func (hs *HttpServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, &Result{Success: false, Message: "method not allowed"})
		return
	}
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, &Result{Success: false, Message: "invalid JSON: " + err.Error()})
		return
	}
	stmt := &SelectStmt{
		Table:          req.Table,
		Tag:            req.Tag,
		StartTime:      req.StartTime,
		EndTime:        req.EndTime,
		Limit:          req.Limit,
		Polymerization: req.Polymerization,
	}
	if req.Condition != nil {
		stmt.Having = tsdb.Condition{
			ColumnAttributeName: mapColumnName(req.Condition.Column),
			Type:                tsdb.ConditionOperator(req.Condition.Operator),
			Value:               variant.New(req.Condition.Value),
		}
	}
	result, err := hs.executor.Execute(stmt)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, &Result{Success: false, Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type createTableRequest struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Precision uint8  `json:"precision"`
}

func parseColumnType(s string) (tsdb.ColumnType, error) {
	switch s {
	case "int":
		return tsdb.ColumnTypeInt, nil
	case "float":
		return tsdb.ColumnTypeFloat, nil
	case "string":
		return tsdb.ColumnTypeString, nil
	case "bool":
		return tsdb.ColumnTypeBool, nil
	case "json":
		return tsdb.ColumnTypeJson, nil
	case "structure":
		return tsdb.ColumnTypeStructure, nil
	case "", "unknown":
		return tsdb.ColumnTypeUnknown, nil
	default:
		return 0, fmt.Errorf("unknown column type: %s", s)
	}
}

func (hs *HttpServer) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, &Result{Success: false, Message: "method not allowed"})
		return
	}
	var req createTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, &Result{Success: false, Message: "invalid JSON: " + err.Error()})
		return
	}
	if req.Name == "" {
		writeJSON(w, http.StatusBadRequest, &Result{Success: false, Message: "table name is required"})
		return
	}
	colType, err := parseColumnType(req.Type)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, &Result{Success: false, Message: err.Error()})
		return
	}
	stmt := &CreateTableStmt{
		Name:      req.Name,
		Type:      colType,
		Precision: req.Precision,
	}
	result, err := hs.executor.Execute(stmt)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, &Result{Success: false, Message: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (hs *HttpServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, &Result{Success: true, Message: "ok"})
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
