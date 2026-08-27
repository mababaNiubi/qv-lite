package tsdb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

func newBatcherTestDB(t *testing.T, ingest IngestConfig) (*DB, *ssTable) {
	t.Helper()
	db, err := Open(Config{
		Path:         tempDir(t),
		IngestConfig: ingest,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "batcher", Type: ColumnTypeInt},
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	table, ok := db.ssTables.Load("batcher")
	if !ok {
		_ = db.Close()
		t.Fatal("batcher table not found")
	}
	return db, table
}

func waitForTag(t *testing.T, table *ssTable, tag string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := table.Meta.Load(tag); ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("tag %q was not resolved by background batcher", tag)
}

func TestTableBatcherDefersTagResolutionUntilVisibilityBarrier(t *testing.T) {
	db, table := newBatcherTestDB(t, IngestConfig{
		Shards:          8,
		MaxBatchSize:    1 << 20,
		FlushIntervalMs: 60_000,
		QueueSize:       2,
	})
	defer db.Close()

	base := time.Now().UnixNano()
	if ok, err := db.Write("batcher", "raw-tag", base, variant.NewInt(7)); err != nil || !ok {
		t.Fatalf("write: ok=%v err=%v", ok, err)
	}
	if _, ok := table.Meta.Load("raw-tag"); ok {
		t.Fatal("tag was resolved on the caller write path")
	}

	points, err := db.QueryAll("batcher", "raw-tag", base-1, base+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 1 {
		t.Fatalf("visibility barrier returned %d points, want 1", len(points))
	}
	if _, ok := table.Meta.Load("raw-tag"); !ok {
		t.Fatal("tag was not resolved during background commit")
	}
}

func TestTableBatcherThresholdAndTimerTriggers(t *testing.T) {
	db, table := newBatcherTestDB(t, IngestConfig{
		Shards:          4,
		MaxBatchSize:    4,
		FlushIntervalMs: 20,
		QueueSize:       2,
	})
	defer db.Close()

	base := time.Now().UnixNano()
	points := make([]TagPoint, 4)
	for i := range points {
		points[i] = TagPoint{Tag: "threshold", Timestamp: base + int64(i), Value: variant.NewInt(i)}
	}
	if n, err := db.WriteBatch("batcher", points); err != nil || n != len(points) {
		t.Fatalf("threshold batch: n=%d err=%v", n, err)
	}
	waitForTag(t, table, "threshold")

	if ok, err := db.Write("batcher", "timer", base, variant.NewInt(1)); err != nil || !ok {
		t.Fatalf("timer write: ok=%v err=%v", ok, err)
	}
	waitForTag(t, table, "timer")
}

func TestTableBatcherClosePersistsActiveBatch(t *testing.T) {
	dir := tempDir(t)
	cfg := Config{
		Path: dir,
		IngestConfig: IngestConfig{
			Shards:          4,
			MaxBatchSize:    1 << 20,
			FlushIntervalMs: 60_000,
			QueueSize:       2,
		},
	}
	db, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "close", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UnixNano()
	for _, offset := range []int64{30, 10, 20} {
		if _, err := db.Write("close", "late", base+offset, variant.NewInt64(offset)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	points, err := reopened.QueryAll("close", "late", base, base+40, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 3 {
		t.Fatalf("after reopen got %d points, want 3", len(points))
	}
	for i, want := range []int64{10, 20, 30} {
		if points[i].Tms != base+want {
			t.Fatalf("point %d timestamp=%d want=%d", i, points[i].Tms, base+want)
		}
	}
}

func TestTableBatcherBackpressureWakesAllEligibleWriters(t *testing.T) {
	b := &tableBatcher{maxActive: 2}
	b.spaceCond = sync.NewCond(&b.waitMu)
	b.activePoints.Store(5)
	defer func() {
		b.closed.Store(true)
		b.notifyWaiters()
	}()

	const writers = 3
	results := make(chan error, writers)
	for range writers {
		go func() { results <- b.waitIfOverloaded(5) }()
	}

	deadline := time.Now().Add(2 * time.Second)
	for b.waiters.Load() != writers && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := b.waiters.Load(); got != writers {
		t.Fatalf("blocked writers=%d, want %d", got, writers)
	}

	b.activePoints.Add(-5)
	b.notifyWaiters()
	for remaining := writers; remaining > 0; remaining-- {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("backpressure wake timed out with %d writers remaining", remaining)
		}
	}
}
