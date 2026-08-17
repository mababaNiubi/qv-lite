// Package client provides a lightweight Go client for the qv-lite tsdb
// server's HTTP API. It is safe for concurrent use: connections are pooled
// (http.Client) and calls are synchronous.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Value is a write value: an explicit type plus its native JSON encoding.
//
// Type is empty when the type is inferred from the JSON value on the server
// (Native). The typed constructors (Int/Uint/Float/String/Bool/JSON) set Type
// explicitly so int64 vs float64 stays exact on the wire regardless of
// magnitude — Go's json.Marshal writes int64 digits verbatim, so even values
// beyond 2^53 survive (the server decodes via json.Number).
type Value struct {
	Type string          // "" | "int" | "uint" | "float" | "string" | "bool" | "json"
	Raw  json.RawMessage // pre-encoded native JSON payload
}

// Int builds an exact int64 value.
func Int(v int64) Value {
	b, _ := json.Marshal(v)
	return Value{Type: "int", Raw: b}
}

// Uint builds an exact uint64 value.
func Uint(v uint64) Value {
	b, _ := json.Marshal(v)
	return Value{Type: "uint", Raw: b}
}

// Float builds a float value. Typed explicitly so 36.0 is not inferred as int.
func Float(v float64) Value {
	b, _ := json.Marshal(v)
	return Value{Type: "float", Raw: b}
}

// String builds a string value.
func String(v string) Value {
	b, _ := json.Marshal(v)
	return Value{Type: "string", Raw: b}
}

// Bool builds a bool value.
func Bool(v bool) Value {
	b, _ := json.Marshal(v)
	return Value{Type: "bool", Raw: b}
}

// JSON builds a structured value (map/list). v must be JSON-serializable.
func JSON(v any) Value {
	b, _ := json.Marshal(v)
	return Value{Type: "json", Raw: b}
}

// Native wraps a raw Go value without an explicit type; the server infers the
// type from the JSON value (integer literals → int64, decimals → float64).
func Native(v any) Value {
	b, _ := json.Marshal(v)
	return Value{Raw: b}
}

// Point is a single timestamp/value pair returned by queries.
//
// Value holds the native JSON value. VType is non-empty only for int/uint:
// those values are emitted as JSON numbers when within ±2^53, otherwise as
// strings (to preserve precision) and must be decoded via AsInt64/AsUint64.
type Point struct {
	Timestamp int64
	Value     json.RawMessage
	VType     string // "int" | "uint" | "" (float/string/bool/json/empty)
}

// AsFloat64 decodes a float point value.
func (p *Point) AsFloat64() (float64, bool) {
	if p.VType != "" {
		return 0, false
	}
	var f float64
	if err := json.Unmarshal(p.Value, &f); err != nil {
		return 0, false
	}
	return f, true
}

// AsInt64 decodes an int/uint point value, accepting both native JSON numbers
// and precision-preserving strings (|v| > 2^53).
func (p *Point) AsInt64() (int64, bool) {
	if p.VType != "int" && p.VType != "uint" {
		return 0, false
	}
	raw := strings.TrimSpace(string(p.Value))
	if len(raw) > 1 && raw[0] == '"' {
		var s string
		if err := json.Unmarshal(p.Value, &s); err != nil {
			return 0, false
		}
		raw = s
	}
	if p.VType == "uint" {
		u, err := strconv.ParseUint(raw, 10, 64)
		return int64(u), err == nil
	}
	i, err := strconv.ParseInt(raw, 10, 64)
	return i, err == nil
}

// AsString decodes a string point value.
func (p *Point) AsString() (string, bool) {
	if p.VType != "" {
		return "", false
	}
	var s string
	if err := json.Unmarshal(p.Value, &s); err != nil {
		return "", false
	}
	return s, true
}

// AsBool decodes a bool point value.
func (p *Point) AsBool() (bool, bool) {
	if p.VType != "" {
		return false, false
	}
	var b bool
	if err := json.Unmarshal(p.Value, &b); err != nil {
		return false, false
	}
	return b, true
}

// TagPoint is a single point in a batch write request.
//
// Value uses the typed form (Int/Uint/Float/String/Bool/JSON/Native), which
// keeps integer precision exact regardless of magnitude.
type TagPoint struct {
	Tag       string
	Timestamp int64
	Value     Value
}

// MarshalJSON 输出 {"tag","timestamp","value","valueType"}，value 为原生
// JSON 值，valueType 仅显式类型时出现。
func (p TagPoint) MarshalJSON() ([]byte, error) {
	type wire struct {
		Tag       string          `json:"tag"`
		Timestamp int64           `json:"timestamp"`
		Value     json.RawMessage `json:"value"`
		ValueType string          `json:"valueType,omitempty"`
	}
	return json.Marshal(wire{Tag: p.Tag, Timestamp: p.Timestamp, Value: p.Value.Raw, ValueType: p.Value.Type})
}

// TableInfo describes a table's metadata as reported by the server.
type TableInfo struct {
	Name           string            `json:"name"`
	Desc           string            `json:"desc"`
	Type           uint8             `json:"type"`
	FloatPrecision uint8             `json:"float_precision"`
	Structure      []ColumnAttribute `json:"structure,omitempty"`
}

// ColumnAttribute describes one structured column of a table.
type ColumnAttribute struct {
	Name string `json:"name"`
	Desc string `json:"desc"`
	Type uint8  `json:"type"`
}

// Client talks to a tsdb-server instance over HTTP.
type Client struct {
	base  string
	http  *http.Client
	token string // optional X-Auth-Token header
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient overrides the underlying *http.Client (e.g. to set timeouts,
// a custom transport, or TLS config).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.http = hc }
}

// WithToken sets a static X-Auth-Token header on every request.
func WithToken(token string) Option {
	return func(c *Client) { c.token = token }
}

// New creates a Client for the given server base URL (e.g. "http://127.0.0.1:8686").
func New(baseURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base url %q: %w", baseURL, err)
	}
	if u.Scheme == "" {
		u.Scheme = "http"
	}
	c := &Client{
		base: u.String(),
		http: &http.Client{Timeout: 60 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// do performs a single HTTP call and decodes a 2xx response into out.
func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}
	return c.doRaw(ctx, method, path, "application/json", buf.Bytes(), out)
}

// doRaw performs a single HTTP call with a pre-encoded body.
func (c *Client) doRaw(ctx context.Context, method, path, contentType string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("X-Auth-Token", c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("do request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		return &StatusError{StatusCode: resp.StatusCode, Message: e.Error}
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

// StatusError is returned for non-2xx responses from the server.
type StatusError struct {
	StatusCode int
	Message    string
}

func (e *StatusError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("tsdb server: HTTP %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("tsdb server: HTTP %d", e.StatusCode)
}

// ---------------------------------------------------------------- methods

// Health performs a lightweight health check.
func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/api/v1/health", nil, nil)
}

// ListTables returns the metadata of all tables.
func (c *Client) ListTables(ctx context.Context) ([]TableInfo, error) {
	// The server returns {"tables":[...]}
	var resp struct {
		Tables json.RawMessage `json:"tables"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/v1/tables", nil, &resp); err != nil {
		return nil, err
	}
	var tables []TableInfo
	if err := json.Unmarshal(resp.Tables, &tables); err != nil {
		return nil, fmt.Errorf("decode tables: %w", err)
	}
	return tables, nil
}

// CreateTable creates a new table.
func (c *Client) CreateTable(ctx context.Context, name string, cols []ColumnAttribute, floatPrecision uint8) error {
	body := map[string]any{
		"name": name, "float_precision": floatPrecision,
	}
	if len(cols) > 0 {
		body["columns"] = cols
	}
	var out map[string]any
	return c.do(ctx, http.MethodPost, "/api/v1/tables", body, &out)
}

// Write writes a single point. Returns true if the engine accepted it.
func (c *Client) Write(ctx context.Context, table, tag string, timestamp int64, value Value) (bool, error) {
	body := map[string]any{
		"table": table, "tag": tag, "timestamp": timestamp,
		"value": value.Raw, "valueType": value.Type,
	}
	var out struct {
		Written bool `json:"written"`
	}
	if err := c.do(ctx, http.MethodPost, "/api/v1/write", body, &out); err != nil {
		return false, err
	}
	return out.Written, nil
}

// WriteBatch writes many points to one table in a single request. Returns the
// number of points actually accepted.
//
// It uses the compact binary protocol ("application/x-tsdb-batch") by default
// for the best throughput. If the batch mixes value types, or the server does
// not support the binary format, it transparently falls back to JSON.
func (c *Client) WriteBatch(ctx context.Context, table string, points []TagPoint) (int, error) {
	if len(points) == 0 {
		return 0, nil
	}
	body, ok, err := encodeBatchBinary(table, points)
	if err != nil {
		return 0, err
	}
	if ok {
		var out struct {
			Written int `json:"written"`
		}
		err := c.doRaw(ctx, http.MethodPost, "/api/v1/batch", batchContentType, body, &out)
		if err == nil {
			return out.Written, nil
		}
		// Fall back to JSON only when the server rejected the binary format.
		var se *StatusError
		if !errors.As(err, &se) || (se.StatusCode != http.StatusUnsupportedMediaType && se.StatusCode != http.StatusBadRequest) {
			return 0, err
		}
	}
	return c.writeBatchJSON(ctx, table, points)
}

// WriteBatchJSON writes a batch using the JSON protocol. It exists for
// mixed-type batches and servers that do not support the binary protocol;
// WriteBatch normally auto-selects.
func (c *Client) WriteBatchJSON(ctx context.Context, table string, points []TagPoint) (int, error) {
	return c.writeBatchJSON(ctx, table, points)
}

// writeBatchJSON is the JSON batch write path. 用 struct 保证 "table" 先于
// "points" 序列化（服务器流式解析依赖 table 在前）。
func (c *Client) writeBatchJSON(ctx context.Context, table string, points []TagPoint) (int, error) {
	var body struct {
		Table  string     `json:"table"`
		Points []TagPoint `json:"points"`
	}
	body.Table = table
	body.Points = points
	var out struct {
		Written int `json:"written"`
	}
	if err := c.do(ctx, http.MethodPost, "/api/v1/batch", body, &out); err != nil {
		return 0, err
	}
	return out.Written, nil
}

// Query fetches points. If window <= 0 raw points are returned; otherwise points
// are aggregated using the given fusion mode (0 avg, 1 min, 2 max).
func (c *Client) Query(ctx context.Context, table, tag string, start, end int64, window int64, fusion uint8, cond any) ([]Point, error) {
	body := map[string]any{
		"table": table, "tag": tag, "start": start, "end": end,
		"window": window, "aggregation": fusion,
	}
	if cond != nil {
		body["condition"] = cond
	}
	var resp struct {
		Points []struct {
			Timestamp int64           `json:"timestamp"`
			Value     json.RawMessage `json:"value"`
			VType     string          `json:"vtype"`
		} `json:"points"`
	}
	if err := c.do(ctx, http.MethodPost, "/api/v1/query", body, &resp); err != nil {
		return nil, err
	}
	out := make([]Point, 0, len(resp.Points))
	for _, p := range resp.Points {
		out = append(out, Point{Timestamp: p.Timestamp, Value: p.Value, VType: p.VType})
	}
	return out, nil
}

// QueryLatest returns the most recent point for a tag.
func (c *Client) QueryLatest(ctx context.Context, table, tag string) (*Point, error) {
	body := map[string]any{"table": table, "tag": tag}
	var out struct {
		Timestamp int64           `json:"timestamp"`
		Value     json.RawMessage `json:"value"`
		VType     string          `json:"vtype"`
	}
	if err := c.do(ctx, http.MethodPost, "/api/v1/query/latest", body, &out); err != nil {
		return nil, err
	}
	return &Point{Timestamp: out.Timestamp, Value: out.Value, VType: out.VType}, nil
}

// WriteLine writes data in InfluxDB Line Protocol compatible format
// (server endpoint POST /api/v1/write/line). Typically one point per line:
//
//	sensor,tag=cpu value=36.5 1700000000000000000
//	sensor,tag=cpu count=42i 1700000000000000001
//
// Returns the number of points accepted.
func (c *Client) WriteLine(ctx context.Context, lines string) (int, error) {
	var out struct {
		Written int `json:"written"`
	}
	if err := c.doRaw(ctx, http.MethodPost, "/api/v1/write/line", "text/plain", []byte(lines), &out); err != nil {
		return 0, err
	}
	return out.Written, nil
}
