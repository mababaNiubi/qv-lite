package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// chunkReader 每次 Read 只吐 maxN 字节，用于逼出流式解析器的跨块 refill 路径。
type chunkReader struct {
	data []byte
	i    int
	maxN int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.i >= len(c.data) {
		return 0, io.EOF
	}
	n := min(len(p), c.maxN, len(c.data)-c.i)
	copy(p, c.data[c.i:c.i+n])
	c.i += n
	return n, nil
}

// postBatch 发送 JSON batch 请求体。
func postBatch(t *testing.T, h http.Handler, path string, body io.Reader) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.UseNumber()
	_ = dec.Decode(&out)
	return rec, out
}

// TestJSONBatchValueTypes 验证 JSON batch 的完整值类型推断与 valueType 覆盖：
// float/int/uint（超 2^53 用字符串 + valueType）、string、bool、数组、对象。
func TestJSONBatchValueTypes(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	postJSON(t, h, "/api/v1/tables", map[string]any{"name": "t"})
	base := time.Now().UnixMilli() - 60_000

	body := fmt.Sprintf(`{"table":"t","points":[
		{"tag":"f","timestamp":%d,"value":36.5},
		{"tag":"i","timestamp":%d,"value":42},
		{"tag":"big","timestamp":%d,"value":"9007199254740993","valueType":"int"},
		{"tag":"ubig","timestamp":%d,"value":"18446744073709551615","valueType":"uint"},
		{"tag":"s","timestamp":%d,"value":"hello"},
		{"tag":"b","timestamp":%d,"value":true},
		{"tag":"l","timestamp":%d,"value":[1,2,3]},
		{"tag":"o","timestamp":%d,"value":{"x":1,"y":"z"}}
	]}`, base, base+1, base+2, base+3, base+4, base+5, base+6, base+7)
	rec, out := postBatch(t, h, "/api/v1/batch", strings.NewReader(body))
	if rec.Code != http.StatusOK {
		t.Fatalf("batch: %d %s", rec.Code, rec.Body.String())
	}
	if out["written"] != json.Number("8") {
		t.Fatalf("written = %v, want 8", out["written"])
	}

	// 逐 tag 查询验证值类型。
	checks := []struct {
		tag   string
		value any
		vtype string
	}{
		{"f", json.Number("36.5"), ""},
		{"i", json.Number("42"), "int"},
		{"big", "9007199254740993", "int"},
		{"ubig", "18446744073709551615", "uint"},
		{"s", "hello", ""},
		{"b", true, ""},
	}
	for _, c := range checks {
		rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
			"table": "t", "tag": c.tag, "start": base - 1, "end": base + 10_000,
			"window": 0, "aggregation": 0,
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("query %s: %d %s", c.tag, rec.Code, rec.Body.String())
		}
		p := out["points"].([]any)[0].(map[string]any)
		if p["value"] != c.value {
			t.Fatalf("tag %s value = %v, want %v", c.tag, p["value"], c.value)
		}
		got, has := p["vtype"]
		if c.vtype == "" {
			if has {
				t.Fatalf("tag %s should not carry vtype, got %v", c.tag, p)
			}
		} else if !has || got != c.vtype {
			t.Fatalf("tag %s vtype = %v, want %q", c.tag, got, c.vtype)
		}
	}
	// 数组 / 对象值。
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "t", "tag": "l", "start": base - 1, "end": base + 10_000,
		"window": 0, "aggregation": 0,
	})
	lv := out["points"].([]any)[0].(map[string]any)["value"].([]any)
	if len(lv) != 3 || lv[1] != json.Number("2") {
		t.Fatalf("list value = %v", lv)
	}
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "t", "tag": "o", "start": base - 1, "end": base + 10_000,
		"window": 0, "aggregation": 0,
	})
	ov := out["points"].([]any)[0].(map[string]any)["value"].(map[string]any)
	if ov["y"] != "z" {
		t.Fatalf("object value = %v", ov)
	}
}

// TestJSONBatchChunked 用逐字节 / 小块读取器发送同一请求体，验证流式
// 解析器跨块 refill 路径与整块结果一致。
func TestJSONBatchChunked(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	postJSON(t, h, "/api/v1/tables", map[string]any{"name": "t"})
	base := time.Now().UnixMilli() - 60_000
	body := fmt.Sprintf(`{"points":[{"tag":"cpu","timestamp":%d,"value":1.5},{"tag":"cpu","timestamp":%d,"value":"s"},{"tag":"mem","timestamp":%d,"value":3}]}`, base, base+1, base+2)

	// 整块（对照）
	rec, out := postBatch(t, h, "/api/v1/batch?table=t", strings.NewReader(body))
	if rec.Code != http.StatusOK || out["written"] != json.Number("3") {
		t.Fatalf("whole: %d %v", rec.Code, out)
	}
	for _, n := range []int{1, 3, 7, 13} {
		rec, out = postBatch(t, h, "/api/v1/batch?table=t", &chunkReader{data: []byte(body), maxN: n})
		if rec.Code != http.StatusOK || out["written"] != json.Number("3") {
			t.Fatalf("chunk %d: %d %v", n, rec.Code, out)
		}
	}
	// 验证值跨块后仍正确（整块 + 4 种分块共 5 次写入，各 2 个 cpu 点）。
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "t", "tag": "cpu", "start": base - 1, "end": base + 10_000,
		"window": 0, "aggregation": 0,
	})
	pts := out["points"].([]any)
	if len(pts) != 10 {
		t.Fatalf("cpu points = %d, want 10", len(pts))
	}
	if pts[0].(map[string]any)["value"] != json.Number("1.5") {
		t.Fatalf("float value = %v", pts[0])
	}
	if pts[5].(map[string]any)["value"] != "s" {
		t.Fatalf("string value = %v", pts[5])
	}
}

// TestJSONBatchEscapes 验证转义标签/值/键与 \uXXXX 的处理。
func TestJSONBatchEscapes(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	postJSON(t, h, "/api/v1/tables", map[string]any{"name": "t"})
	base := time.Now().UnixMilli() - 60_000
	// tag 含转义引号与 \u 转义；值含 \n 转义。
	body := fmt.Sprintf(`{"points":[
		{"tag":"a\"b","timestamp":%d,"value":"x\ny"},
		{"tag":"\u0041","timestamp":%d,"value":1}
	]}`, base, base+1)
	rec, out := postBatch(t, h, "/api/v1/batch?table=t", strings.NewReader(body))
	if rec.Code != http.StatusOK || out["written"] != json.Number("2") {
		t.Fatalf("batch: %d %v", rec.Code, out)
	}
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "t", "tag": `a"b`, "start": base - 1, "end": base + 10_000,
		"window": 0, "aggregation": 0,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("query escaped tag: %d %s", rec.Code, rec.Body.String())
	}
	v := out["points"].([]any)[0].(map[string]any)["value"]
	if v != "x\ny" {
		t.Fatalf("escaped value = %v", v)
	}
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "t", "tag": "A", "start": base - 1, "end": base + 10_000,
		"window": 0, "aggregation": 0,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("query unicode tag: %d %s", rec.Code, rec.Body.String())
	}
}

// TestJSONBatchTablePrecedence 验证 ?table= 查询参数优先于 body 内 table 键。
func TestJSONBatchTablePrecedence(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	_, _ = postJSON(t, h, "/api/v1/tables", map[string]any{"name": "body"})
	_, _ = postJSON(t, h, "/api/v1/tables", map[string]any{"name": "query"})
	base := time.Now().UnixMilli() - 60_000

	// 无查询参数 → body table。
	body := fmt.Sprintf(`{"table":"body","points":[{"tag":"cpu","timestamp":%d,"value":1}]}`, base)
	rec, out := postBatch(t, h, "/api/v1/batch", strings.NewReader(body))
	if rec.Code != http.StatusOK || out["written"] != json.Number("1") {
		t.Fatalf("body table: %d %v", rec.Code, out)
	}
	// 查询参数 → 覆盖 body table。
	rec, out = postBatch(t, h, "/api/v1/batch?table=query", strings.NewReader(body))
	if rec.Code != http.StatusOK || out["written"] != json.Number("1") {
		t.Fatalf("query table: %d %v", rec.Code, out)
	}
	// table 键在 points 之后（仍生效）。
	body = fmt.Sprintf(`{"points":[{"tag":"cpu","timestamp":%d,"value":1}],"table":"body"}`, base)
	rec, out = postBatch(t, h, "/api/v1/batch", strings.NewReader(body))
	if rec.Code != http.StatusOK || out["written"] != json.Number("1") {
		t.Fatalf("table after points: %d %v", rec.Code, out)
	}
}

// TestJSONBatchUnknownFields 验证未知顶层键与未知字段被跳过。
func TestJSONBatchUnknownFields(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	_, _ = postJSON(t, h, "/api/v1/tables", map[string]any{"name": "t"})
	base := time.Now().UnixMilli() - 60_000
	body := fmt.Sprintf(`{"extra":{"deep":[1,2,{"x":null}]},"points":[{"tag":"cpu","timestamp":%d,"value":1,"note":{"k":["v"]},"extra2":"zz"}],"tail":true}`, base)
	rec, out := postBatch(t, h, "/api/v1/batch?table=t", strings.NewReader(body))
	if rec.Code != http.StatusOK || out["written"] != json.Number("1") {
		t.Fatalf("batch: %d %v", rec.Code, out)
	}
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "t", "tag": "cpu", "start": base - 1, "end": base + 10_000,
		"window": 0, "aggregation": 0,
	})
	if rec.Code != http.StatusOK || len(out["points"].([]any)) != 1 {
		t.Fatalf("query: %d %v", rec.Code, out)
	}
}

// TestJSONBatchEdgeCases 验证空/缺失 points 与边界语义。
func TestJSONBatchEdgeCases(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	postJSON(t, h, "/api/v1/tables", map[string]any{"name": "t"})

	// 空对象 → written 0。
	rec, out := postBatch(t, h, "/api/v1/batch", strings.NewReader(`{}`))
	if rec.Code != http.StatusOK || out["written"] != json.Number("0") {
		t.Fatalf("empty object: %d %v", rec.Code, out)
	}
	// 空 points 数组 → written 0。
	rec, out = postBatch(t, h, "/api/v1/batch", strings.NewReader(`{"points":[]}`))
	if rec.Code != http.StatusOK || out["written"] != json.Number("0") {
		t.Fatalf("empty points: %d %v", rec.Code, out)
	}
	// 缺省表名（无 table，无查询参数）→ 默认表。
	base := time.Now().UnixMilli() - 60_000
	rec, out = postBatch(t, h, "/api/v1/batch", strings.NewReader(fmt.Sprintf(`{"points":[{"tag":"d","timestamp":%d,"value":1}]}`, base)))
	if rec.Code != http.StatusOK || out["written"] != json.Number("1") {
		t.Fatalf("default table: %d %v", rec.Code, out)
	}
	// 缺 timestamp → 0（与旧行为一致，被引擎接受）。
	rec, out = postBatch(t, h, "/api/v1/batch?table=t", strings.NewReader(`{"points":[{"tag":"z","value":1}]}`))
	if rec.Code != http.StatusOK || out["written"] != json.Number("1") {
		t.Fatalf("missing timestamp: %d %v", rec.Code, out)
	}
}

// TestJSONBatchMalformed 验证畸形输入统一 400。
func TestJSONBatchMalformed(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	base := time.Now().UnixMilli() - 60_000

	cases := []string{
		`not json`,                // 非 JSON
		`[1,2]`,                   // 顶层非对象
		`{"points": {"tag":"t"}}`, // points 非数组
		`{"points": [42]}`,        // 元素非对象
		`{"points": [{"tag":"t","timestamp":"abc","value":1}]}`,                    // timestamp 非数字
		`{"points": [{"tag":"t","timestamp":1.5,"value":1}]}`,                      // timestamp 非整数
		`{"points": [{"tag":"t","timestamp":+1,"value":1}]}`,                       // 非 JSON 整数
		`{"points": [{"tag":"t","timestamp":1,"value":}]}`,                         // 缺 value
		`{"points": [{"tag":"t","timestamp":1,"value":tru}]}`,                      // 残缺字面量
		`{"points": [{"tag":"t","timestamp":1,"value":"\q"}]}`,                     // 非法转义
		`{"points": [{"tag":"t","timestamp":1,"value":1}`,                          // 截断
		fmt.Sprintf(`{"points":[{"tag":"t","timestamp":%d,"value":1,"x":2}`, base), // 截断对象
		`{"points": [{"tag":"t","timestamp":1,"value":1}],"tail":`,                 // 截断尾部
	}
	for _, body := range cases {
		rec, out := postBatch(t, h, "/api/v1/batch", strings.NewReader(body))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %q: code = %d, want 400 (out=%v)", body, rec.Code, out)
		}
	}
}
