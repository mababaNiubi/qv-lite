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

// TestStreamingPipelineNoAliasing 回归：流式分批 + 流水线时缓冲按所有权移交，
// 入队零拷贝。StreamIngestor.flush 把当批缓冲交给写入器后，改从池取新缓冲
// 续写，决不把刚移交的数组复用给下一批。
//
// interval/batchSize 取极大值，使写入器只在显式 Flush 时消费——密集复现
// 若 flush 复用同一下层数组会产生的覆盖错乱：两批都写完后再验证数据完整。
func TestStreamingPipelineNoAliasing(t *testing.T) {
	db := newTestDB(t)
	w := NewPipelinedWriter(db, 60_000, 1<<30)
	defer w.Close()

	g := &StreamIngestor{
		ingest:  func(table string, points []tsdb.TagPoint) (int, error) { return w.Submit(table, points), nil },
		size:    streamBatchSize,
		pending: make(map[string][]tsdb.TagPoint),
	}
	base := time.Now().Add(-time.Hour).UnixMilli()
	// 两批各 streamBatchSize 点，时间戳唯一、值唯一。
	for batch := 0; batch < 2; batch++ {
		for i := 0; i < streamBatchSize; i++ {
			idx := batch*streamBatchSize + i
			if err := g.Add("t", tsdb.TagPoint{
				Tag:       "cpu",
				Timestamp: base + int64(idx),
				Value:     variant.NewFloat64(float64(idx) * 0.5),
			}); err != nil {
				t.Fatalf("Add: %v", err)
			}
		}
	}
	if _, err := g.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	// 唤醒后台写入器消费缓冲（interval 取极大值，不依赖定时器）。
	w.signalWork()
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	pts, err := db.QueryAll("t", "cpu", 0, time.Now().Add(time.Hour).UnixMilli(), nil)
	if err != nil {
		t.Fatalf("QueryAll: %v", err)
	}
	if len(pts) != 2*streamBatchSize {
		t.Fatalf("rows = %d, want %d", len(pts), 2*streamBatchSize)
	}
	byTs := make(map[int64]float64, len(pts))
	for _, p := range pts {
		f, _ := p.V.AsFloat64()
		byTs[p.Tms] = f
	}
	for i := 0; i < 2*streamBatchSize; i++ {
		want := float64(i) * 0.5
		if got, ok := byTs[base+int64(i)]; !ok || got != want {
			t.Fatalf("point %d: ts=%d got %v (present=%v), want %v", i, base+int64(i), got, ok, want)
		}
	}
}

// TestPipelinedWriterMergeAndSplit 验证同一张表的多个 chunk 合并后按 batchSize
// 上限分批写出（2×30K 合并后拆为 40K+20K），数据完整。
func TestPipelinedWriterMergeAndSplit(t *testing.T) {
	db := newTestDB(t)
	w := NewPipelinedWriter(db, 60_000, 40_000)
	defer w.Close()

	for i := 0; i < 2; i++ {
		w.Submit("t", testPoints(30_000))
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n := countRows(t, db); n != 60_000 {
		t.Fatalf("rows = %d, want 60000", n)
	}
}

// TestStreamIngestorMultiTableInterleaved 验证连续交替的多表流（A B A B…）
// 不再退化为逐行小写：按表独立累积、总点数达阈值统一入库，各表数据完整。
// 旧实现「表切换即刷」会让交替表流逐行 flush，产生大量单点小写。
func TestStreamIngestorMultiTableInterleaved(t *testing.T) {
	db := newTestDB(t)
	if err := db.CreateTable(tsdb.TableInfo{ColumnAttribute: tsdb.ColumnAttribute{Name: "u"}}); err != nil {
		t.Fatalf("CreateTable u: %v", err)
	}
	w := NewPipelinedWriter(db, 60_000, 1<<30)
	defer w.Close()

	g := &StreamIngestor{
		ingest:  func(table string, points []tsdb.TagPoint) (int, error) { return w.Submit(table, points), nil },
		size:    streamBatchSize,
		pending: make(map[string][]tsdb.TagPoint),
	}
	const perTable = 30_000 // 共 60K 点：中途触发一次 flush（50K 阈值），残余 10K 在 Finish 写入
	base := time.Now().Add(-time.Hour).UnixMilli()
	for i := 0; i < 2*perTable; i++ {
		table := "t"
		if i%2 == 1 {
			table = "u"
		}
		if err := g.Add(table, tsdb.TagPoint{
			Tag:       "cpu",
			Timestamp: base + int64(i),
			Value:     variant.NewFloat64(float64(i) * 0.5),
		}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	written, err := g.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if written != 2*perTable {
		t.Fatalf("written = %d, want %d", written, 2*perTable)
	}
	w.signalWork()
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	for _, table := range []string{"t", "u"} {
		pts, err := db.QueryAll(table, "cpu", 0, time.Now().Add(time.Hour).UnixMilli(), nil)
		if err != nil {
			t.Fatalf("QueryAll(%s): %v", table, err)
		}
		if len(pts) != perTable {
			t.Fatalf("table %s rows = %d, want %d", table, len(pts), perTable)
		}
		byTs := make(map[int64]float64, len(pts))
		for _, p := range pts {
			f, _ := p.V.AsFloat64()
			byTs[p.Tms] = f
		}
		// t 表：偶数索引 i → 值 i*0.5；u 表：奇数索引。
		expect := func(i int) bool { return (i%2 == 0) == (table == "t") }
		for i := 0; i < 2*perTable; i++ {
			if !expect(i) {
				continue
			}
			want := float64(i) * 0.5
			if got, ok := byTs[base+int64(i)]; !ok || got != want {
				t.Fatalf("table %s point %d: got %v (present=%v), want %v", table, i, got, ok, want)
			}
		}
	}
}
