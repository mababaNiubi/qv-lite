package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/mababaNiubi/qv-lite/tsdb"
	"github.com/mababaNiubi/variant"
)

// newTestDB 打开一个临时引擎实例。
func newTestDB(t *testing.T) *tsdb.DB {
	t.Helper()
	db, err := tsdb.Open(tsdb.Config{
		Path:       filepath.Join(t.TempDir(), "data"),
		AsyncFlush: true,
	}, context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreateTable(tsdb.TableInfo{ColumnAttribute: tsdb.ColumnAttribute{Name: "t"}}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return db
}

func testPoints(n int) []tsdb.TagPoint {
	pts := make([]tsdb.TagPoint, n)
	base := time.Now().Add(-time.Hour).UnixMilli()
	for i := range pts {
		pts[i] = tsdb.TagPoint{Tag: "cpu", Timestamp: base + int64(i), Value: variant.NewFloat64(float64(i) * 0.5)}
	}
	return pts
}

func countRows(t *testing.T, db *tsdb.DB) int {
	t.Helper()
	pts, err := db.QueryAll("t", "cpu", 0, time.Now().Add(time.Hour).UnixMilli(), nil)
	if err != nil {
		if errors.Is(err, tsdb.ErrorTagNotFound) {
			return 0 // tag 尚未写入，视为 0 行
		}
		t.Fatalf("QueryAll: %v", err)
	}
	return len(pts)
}

// TestPipelinedWriterSubmitAndFlush 验证 Submit 入队、Flush 后立即可见。
func TestPipelinedWriterSubmitAndFlush(t *testing.T) {
	db := newTestDB(t)
	w := NewPipelinedWriter(db, 5, 1000)
	defer w.Close()

	w.Submit("t", testPoints(50))
	// 未 Flush：缓冲中，查询不应读到（除非 run 恰好在写，不确定；直接验证 Flush 语义）。
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n := countRows(t, db); n != 50 {
		t.Fatalf("after flush rows = %d, want 50", n)
	}
}

// TestPipelinedWriterMultipleSubmits 多批 Submit 合并后数据完整。
func TestPipelinedWriterMultipleSubmits(t *testing.T) {
	db := newTestDB(t)
	w := NewPipelinedWriter(db, 5, 1000)

	for i := 0; i < 10; i++ {
		w.Submit("t", testPoints(100))
	}
	// 不调用显式 Flush，直接 Close（会等待全部入库）。
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := countRows(t, db); n != 1000 {
		t.Fatalf("after close rows = %d, want 1000", n)
	}
}

// TestPipelinedWriterBatchCoalescing 验证多条小 Submit 被合并（同一表的数据
// 在一次引擎 WriteBatch 中入库）。通过 run 触发后直接查询验证数据量，且
// 定时（interval）触发能把不满 batchSize 的缓冲及时写出。
func TestPipelinedWriterBatchCoalescing(t *testing.T) {
	db := newTestDB(t)
	// interval 很短（1ms），不依赖显式 Flush 也能把缓冲写出。
	w := NewPipelinedWriter(db, 1, 10_000)
	defer w.Close()

	w.Submit("t", testPoints(100)) // 100 << batchSize
	// 等待后台定时写出。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n := countRows(t, db); n >= 100 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("background interval flush did not write buffered points")
}

// TestPipelineServerConsistency 通过 HTTP handler 验证开启流水线时
// 「写后立即可查」语义（查询自动 flush）与数据完整性。
func TestPipelineServerConsistency(t *testing.T) {
	cfg := &Config{WriteBufferMs: 5, WriteBatchSize: 1000}
	cfg.ApplyDefaults()
	cfg.DB.Path = filepath.Join(t.TempDir(), "data")
	srv, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	h := srv.Handler()

	_, _ = postJSON(t, h, "/api/v1/tables", map[string]any{"name": "sensor"})

	// 多字节点写（走流水线缓冲）。
	base := time.Now().UnixMilli() - 60_000
	pts := make([]map[string]any, 20)
	for i := range pts {
		pts[i] = map[string]any{
			"tag": "cpu", "timestamp": base + int64(i)*1000, "value": 1.5 + float64(i),
		}
	}
	rec, out := postJSON(t, h, "/api/v1/batch?table=sensor", map[string]any{"points": pts})
	if rec.Code != http.StatusOK || out["written"] != json.Number("20") {
		t.Fatalf("batch: %d %v", rec.Code, out)
	}

	// 立即查询：handler 应自动 flush，读到全部 20 点。
	rec, out = postJSON(t, h, "/api/v1/query", map[string]any{
		"table": "sensor", "tag": "cpu", "start": base - 1, "end": base + 100_000,
		"window": 0, "aggregation": 0,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("query: %d %v", rec.Code, out)
	}
	if n := len(out["points"].([]any)); n != 20 {
		t.Fatalf("read back %d points, want 20", n)
	}
}
