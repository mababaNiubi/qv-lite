package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// TestQueryLimitAndOffset verifies the raw query path honors limit/offset and
// still returns the historical {"points":[...],"count":N} shape.
func TestQueryLimitAndOffset(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	base := time.Now().UnixMilli() - 60_000

	if rec, out := postJSON(t, h, "/api/v1/tables", map[string]any{"name": "t"}); rec.Code != http.StatusCreated {
		t.Fatalf("create table: %d %v", rec.Code, out)
	}
	pts := make([]map[string]any, 30)
	for i := range pts {
		pts[i] = map[string]any{"tag": "cpu", "timestamp": base + int64(i)*1000, "value": float64(i)}
	}
	if rec, out := postJSON(t, h, "/api/v1/batch?table=t", map[string]any{"points": pts}); rec.Code != http.StatusOK {
		t.Fatalf("batch: %d %v", rec.Code, out)
	}

	// limit only
	rec, out := postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "t", "tag": "cpu", "start": base - 1, "end": base + 100_000,
		"window": 0, "limit": 5,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("limit query: %d %v", rec.Code, out)
	}
	points := out["points"].([]any)
	if len(points) != 5 {
		t.Fatalf("limit=5: got %d points", len(points))
	}
	if out["count"] != json.Number("5") {
		t.Fatalf("count = %v, want 5", out["count"])
	}

	// offset + limit: points 10..14
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "t", "tag": "cpu", "start": base - 1, "end": base + 100_000,
		"window": 0, "limit": 5, "offset": 10,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("offset query: %d %v", rec.Code, out)
	}
	points = out["points"].([]any)
	if len(points) != 5 {
		t.Fatalf("offset query: got %d points", len(points))
	}
	if ts := points[0].(map[string]any)["timestamp"]; ts != json.Number(strconv.FormatInt(base+10_000, 10)) {
		t.Fatalf("first offset point ts = %v, want %d", ts, base+10_000)
	}

	// window query honors limit on aggregated output
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "t", "tag": "cpu", "start": base - 1, "end": base + 100_000,
		"window": 3_000, "limit": 2,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("window+limit query: %d %v", rec.Code, out)
	}
	if n := len(out["points"].([]any)); n != 2 {
		t.Fatalf("window limit=2: got %d points", n)
	}

	// unknown table: empty result, 200 (historical behavior)
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "missing", "tag": "cpu", "start": base - 1, "end": base + 100_000,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("missing table: %d %v", rec.Code, out)
	}
	if n := len(out["points"].([]any)); n != 0 {
		t.Fatalf("missing table points = %d, want 0", n)
	}
}

// TestQueryStreamedBodyValidJSON writes enough points to cross the flush
// threshold in writePointStream and verifies the streamed body is still a
// single valid JSON document.
func TestQueryStreamedBodyValidJSON(t *testing.T) {
	srv := testServer(t, nil)
	h := srv.Handler()
	base := time.Now().UnixMilli() - 60_000

	if rec, out := postJSON(t, h, "/api/v1/tables", map[string]any{"name": "s"}); rec.Code != http.StatusCreated {
		t.Fatalf("create table: %d %v", rec.Code, out)
	}
	pts := make([]map[string]any, 3000)
	for i := range pts {
		pts[i] = map[string]any{"tag": "cpu", "timestamp": base + int64(i)*1000, "value": float64(i)}
	}
	if rec, out := postJSON(t, h, "/api/v1/batch?table=s", map[string]any{"points": pts}); rec.Code != http.StatusOK {
		t.Fatalf("batch: %d %v", rec.Code, out)
	}

	rec, out := postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "s", "tag": "cpu", "start": base - 1, "end": base + 10_000_000,
		"window": 0,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("query: %d %v", rec.Code, out)
	}
	points := out["points"].([]any)
	if len(points) != 3000 {
		t.Fatalf("expected 3000 points, got %d", len(points))
	}
	if out["count"] != json.Number("3000") {
		t.Fatalf("count = %v, want 3000", out["count"])
	}
}
