package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func testServer(t *testing.T, cfg *Config) *Server {
	t.Helper()
	if cfg == nil {
		cfg = &Config{}
	}
	cfg.ApplyDefaults()
	cfg.DB.Path = filepath.Join(t.TempDir(), "data")
	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	return srv
}

func postJSON(t *testing.T, h http.Handler, path string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.UseNumber() // keep int64 exact instead of coercing to float64
	if err := dec.Decode(&out); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return rec, out
}

func getJSON(t *testing.T, h http.Handler, path string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return rec, out
}

func TestHealth(t *testing.T) {
	srv := testServer(t, nil)
	rec, out := getJSON(t, srv.Handler(), "/api/v1/health")
	if rec.Code != http.StatusOK {
		t.Fatalf("health code %d", rec.Code)
	}
	if out["status"] != "ok" {
		t.Fatalf("health status %v", out["status"])
	}
}

func TestWriteQueryRoundTrip(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	base := time.Now().UnixMilli() - 60_000

	// create table
	rec, out := postJSON(t, h, "/api/v1/tables", map[string]any{
		"name": "sensor", "float_precision": 2,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create table: %d %v", rec.Code, out)
	}

	// write one int point（int64 超 2^53，用 valueType 声明 + 字符串保精度）
	rec, out = postJSON(t, h, "/api/v1/write", map[string]any{
		"table": "sensor", "tag": "counter", "timestamp": base,
		"value": "9007199254740993", "valueType": "int",
	})
	if rec.Code != http.StatusOK || out["written"] != true {
		t.Fatalf("write: %d %v", rec.Code, out)
	}

	// write one native string value（原生 JSON 值推断）
	rec, out = postJSON(t, h, "/api/v1/write", map[string]any{
		"table": "sensor", "tag": "status", "timestamp": base,
		"value": "ok",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("write string: %d %v", rec.Code, out)
	}

	// batch write floats（原生 JSON 数字：带小数点推断为 float，整数推断为 int64）
	pts := make([]map[string]any, 3)
	for i := range pts {
		pts[i] = map[string]any{
			"tag": "cpu", "timestamp": base + int64(i)*1000,
			"value": 36.5 + float64(i), // 36.5 / 37.5 / 38.5
		}
	}
	rec, out = postJSON(t, h, "/api/v1/batch?table=sensor", map[string]any{"points": pts})
	if rec.Code != http.StatusOK || out["written"] != json.Number("3") {
		t.Fatalf("batch: %d %v", rec.Code, out)
	}

	// query raw
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "sensor", "tag": "cpu",
		"start": base - 1, "end": base + 10_000, "window": 0, "aggregation": 0,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("query: %d %v", rec.Code, out)
	}
	points := out["points"].([]any)
	if len(points) != 3 {
		t.Fatalf("query points = %d, want 3", len(points))
	}
	// 原生 JSON 值：float 输出为 number，无 vtype。
	pFirst := points[0].(map[string]any)
	if pFirst["value"] != json.Number("36.5") {
		t.Fatalf("float native value = %v", pFirst["value"])
	}
	if _, has := pFirst["vtype"]; has {
		t.Fatalf("float should not carry vtype: %v", pFirst)
	}

	// verify the int round-tripped exactly: 超 2^53 → value 为字符串 + vtype
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "sensor", "tag": "counter",
		"start": base - 1, "end": base + 1, "window": 0, "aggregation": 0,
	})
	p0 := out["points"].([]any)[0].(map[string]any)
	if p0["value"] != "9007199254740993" || p0["vtype"] != "int" {
		t.Fatalf("int roundtrip got %v", p0)
	}

	// string 值原生输出
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "sensor", "tag": "status",
		"start": base - 1, "end": base + 1, "window": 0, "aggregation": 0,
	})
	ps := out["points"].([]any)[0].(map[string]any)
	if ps["value"] != "ok" {
		t.Fatalf("string native value = %v", ps["value"])
	}

	// query latest
	rec, out = postJSON(t, h, "/api/v1/query/latest", map[string]any{"table": "sensor", "tag": "cpu"})
	if rec.Code != http.StatusOK {
		t.Fatalf("latest: %d %v", rec.Code, out)
	}
	if out["timestamp"] != json.Number(strconv.FormatInt(base+2000, 10)) {
		t.Fatalf("latest ts = %v", out["timestamp"])
	}
}

func TestQueryAggregationAndCondition(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	base := time.Now().UnixMilli() - 60_000

	_, _ = postJSON(t, h, "/api/v1/tables", map[string]any{"name": "t"})
	pts := make([]map[string]any, 6)
	for i := range pts {
		pts[i] = map[string]any{
			"tag": "cpu", "timestamp": base + int64(i)*1000,
			"value": float64(36 + i), // 原生 JSON 数字
		}
	}
	postJSON(t, h, "/api/v1/batch?table=t", map[string]any{"points": pts})

	// condition: value > 38（原生 JSON 数字）
	rec, out := postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "t", "tag": "cpu", "start": base - 1, "end": base + 10_000,
		"window": 0, "aggregation": 0,
		"condition": map[string]any{"column": "", "op": ">", "value": 38},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("cond query: %d %v", rec.Code, out)
	}
	if n := len(out["points"].([]any)); n != 3 {
		t.Fatalf("cond points = %d, want 3", n)
	}
}

// TestLineProtocolWrite 验证 InfluxDB Line Protocol 兼容写入通道：
// 字面量类型推断、tag= 约定、多表分组、多 field 打包结构。
func TestLineProtocolWrite(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()

	_, _ = postJSON(t, h, "/api/v1/tables", map[string]any{"name": "sensor"})
	_, _ = postJSON(t, h, "/api/v1/tables", map[string]any{"name": "other"})

	body := "sensor,tag=cpu value=36.5 1700000000000\n" +
		"sensor,tag=cpu value=42i 1700000001000\n" +
		"sensor,tag=cpu value=true 1700000002000\n" +
		`sensor,tag=cpu value="hello" 1700000003000` + "\n" +
		"# comment line\n" +
		"\n" +
		"other,tag=meter a=1.5,b=2i 1700000004000\n"
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write/line", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("line write: %d %s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.UseNumber()
	_ = dec.Decode(&out)
	if out["written"] != json.Number("5") {
		t.Fatalf("line written = %v", out["written"])
	}

	// 查询验证类型推断与 tag= 约定。
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "sensor", "tag": "cpu",
		"start": 1700000000000, "end": 1700000004000, "window": 0, "aggregation": 0,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("query: %d %v", rec.Code, out)
	}
	points := out["points"].([]any)
	if len(points) != 4 {
		t.Fatalf("points = %d, want 4", len(points))
	}
	checks := []struct {
		value any
		vtype string
	}{
		{json.Number("36.5"), ""},  // float 原生数字，无 vtype
		{json.Number("42"), "int"}, // 42i → int
		{true, ""},                 // bool
		{"hello", ""},              // string
	}
	for i, c := range checks {
		p := points[i].(map[string]any)
		if p["value"] != c.value {
			t.Fatalf("point %d value = %v, want %v", i, p["value"], c.value)
		}
		gotVType, has := p["vtype"]
		if c.vtype == "" {
			if has {
				t.Fatalf("point %d should not carry vtype, got %v", i, p)
			}
		} else if !has || gotVType != c.vtype {
			t.Fatalf("point %d vtype = %v, want %q", i, gotVType, c.vtype)
		}
	}

	// 多 field 打包结构 + 无 tag= 键时 tag 用整个 tag set。
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "other", "tag": "meter",
		"start": 1700000004000, "end": 1700000005000, "window": 0, "aggregation": 0,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("query other: %d %v", rec.Code, out)
	}
	po := out["points"].([]any)[0].(map[string]any)
	val := po["value"].(map[string]any)
	if val["a"] != json.Number("1.5") || val["b"] != json.Number("2") {
		t.Fatalf("multi-field struct = %v", val)
	}
}

// TestLineProtocolErrors 验证 line protocol 解析错误返回 400 与行号。
func TestLineProtocolErrors(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/write/line",
		strings.NewReader("sensor,tag=cpu value=36.5 1700000000000\nbad line here\n"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad line code = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "line 2") {
		t.Fatalf("error should reference line 2: %s", rec.Body.String())
	}
}

func TestAuthRequired(t *testing.T) {
	srv := testServer(t, &Config{Token: "secret"})
	h := srv.Handler()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no-token health code = %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	req.Header.Set("X-Auth-Token", "secret")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("with-token health code = %d, want 200", rec.Code)
	}
}

func TestBinaryBatchRoundTrip(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	base := time.Now().UnixMilli() - 60_000

	_, _ = postJSON(t, h, "/api/v1/tables", map[string]any{"name": "bb"})

	// Build a binary batch: 3 float points.
	table := "bb"
	count := 3
	var buf bytes.Buffer
	buf.Write([]byte{0x54, 0x53, 1, batchValueFloat})
	var tlen [2]byte
	binary.BigEndian.PutUint16(tlen[:], uint16(len(table)))
	buf.Write(tlen[:])
	buf.WriteString(table)
	var cnt [4]byte
	binary.BigEndian.PutUint32(cnt[:], uint32(count))
	buf.Write(cnt[:])
	for i := 0; i < count; i++ {
		tag := "cpu"
		binary.BigEndian.PutUint16(tlen[:], uint16(len(tag)))
		buf.Write(tlen[:])
		buf.WriteString(tag)
		var ts [8]byte
		binary.BigEndian.PutUint64(ts[:], uint64(base+int64(i)*1000))
		buf.Write(ts[:])
		var val [8]byte
		binary.BigEndian.PutUint64(val[:], math.Float64bits(36.0+float64(i)))
		buf.Write(val[:])
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/batch", &buf)
	req.Header.Set("Content-Type", BatchContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("binary batch: %d %s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.UseNumber()
	_ = dec.Decode(&out)
	if out["written"] != json.Number("3") {
		t.Fatalf("binary written = %v", out["written"])
	}

	// Verify values round-tripped exactly.
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "bb", "tag": "cpu", "start": base - 1, "end": base + 10_000,
		"window": 0, "aggregation": 0,
	})
	points := out["points"].([]any)
	if len(points) != 3 {
		t.Fatalf("points = %d, want 3", len(points))
	}
	first := points[0].(map[string]any)
	if first["value"] != json.Number("36") {
		t.Fatalf("first value = %v", first["value"])
	}
	last := points[2].(map[string]any)
	if last["value"] != json.Number("38") {
		t.Fatalf("last value = %v", last["value"])
	}
}

func TestBinaryBatchRejectsBadMagic(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/batch", bytes.NewReader([]byte{1, 2, 3, 4}))
	req.Header.Set("Content-Type", BatchContentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad magic code = %d, want 400", rec.Code)
	}
}

func TestConfigValidateAndDefaults(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ApplyDefaults()
	if cfg.Listen != ":8686" {
		t.Fatalf("listen default = %q", cfg.Listen)
	}
	if !cfg.DB.AsyncFlush {
		t.Fatal("async flush should default true")
	}
	if cfg.DB.SecondaryCompressionName != "zstd" {
		t.Fatalf("compression default = %q", cfg.DB.SecondaryCompressionName)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	// data dir should be absolute after validate
	if !filepath.IsAbs(cfg.DB.Path) {
		t.Fatalf("db path not absolute: %q", cfg.DB.Path)
	}
	_ = os.RemoveAll(cfg.DB.Path)
}
