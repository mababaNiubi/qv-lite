package tsdb

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

// openQueryTestDB opens a table with a tiny WAL so flushes happen during
// writes, producing data on both disk segments and the WAL.
func openQueryTestDB(t *testing.T, tableType ColumnType) (*DB, int64) {
	t.Helper()
	db, err := Open(Config{
		Path:           tempDir(t),
		WalConfig:      WalConfig{MaxFileSize: 4 * 1024, MaxFileNumber: 4},
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	info := TableInfo{ColumnAttribute: ColumnAttribute{Name: "it", Type: tableType}}
	if tableType == ColumnTypeUnknown {
		info = TableInfo{ColumnAttribute: ColumnAttribute{Name: "it", FloatPrecision: 2}}
	}
	if err := db.CreateTable(info); err != nil {
		t.Fatal(err)
	}
	return db, time.Now().UnixNano()
}

func writeRange(t *testing.T, db *DB, table, tag string, base, n int64) {
	t.Helper()
	for i := int64(0); i < n; i++ {
		if _, err := db.Write(table, tag, base+i, variant.NewInt64(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
}

func collectIter(t *testing.T, it PointIter) []Point {
	t.Helper()
	defer it.Close()
	var out []Point
	for {
		p, ok, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			return out
		}
		out = append(out, p)
	}
}

// TestQueryIter_LimitAndOffset verifies offset/limit apply after condition
// filtering and in time order.
func TestQueryIter_LimitAndOffset(t *testing.T) {
	db, base := openQueryTestDB(t, ColumnTypeInt)
	writeRange(t, db, "it", "cpu", base, 100)

	it, err := db.QueryIter(context.Background(), "it", "cpu", base-1, base+200, nil, &QueryOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	pts := collectIter(t, it)
	if len(pts) != 10 {
		t.Fatalf("limit=10: got %d points", len(pts))
	}
	if pts[0].Tms != base || pts[9].Tms != base+9 {
		t.Fatalf("limit window wrong: first=%d last=%d", pts[0].Tms, pts[9].Tms)
	}

	it, err = db.QueryIter(context.Background(), "it", "cpu", base-1, base+200, nil, &QueryOptions{Limit: 10, Offset: 90})
	if err != nil {
		t.Fatal(err)
	}
	pts = collectIter(t, it)
	if len(pts) != 10 {
		t.Fatalf("offset=90 limit=10: got %d points", len(pts))
	}
	if pts[0].Tms != base+90 || pts[9].Tms != base+99 {
		t.Fatalf("offset window wrong: first=%d last=%d", pts[0].Tms, pts[9].Tms)
	}

	// Offset beyond the result set yields nothing.
	it, err = db.QueryIter(context.Background(), "it", "cpu", base-1, base+200, nil, &QueryOptions{Limit: 10, Offset: 500})
	if err != nil {
		t.Fatal(err)
	}
	if pts = collectIter(t, it); len(pts) != 0 {
		t.Fatalf("offset beyond end: got %d points", len(pts))
	}
}

// TestQueryIter_ConditionAndLimit verifies the condition is applied before
// limit counting.
func TestQueryIter_ConditionAndLimit(t *testing.T) {
	db, base := openQueryTestDB(t, ColumnTypeInt)
	writeRange(t, db, "it", "cpu", base, 100)

	cond := Condition{ColumnAttributeName: "", Operator: OpGreaterThan, Value: variant.NewInt64(50)}
	it, err := db.QueryIter(context.Background(), "it", "cpu", base-1, base+200, cond, &QueryOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	pts := collectIter(t, it)
	if len(pts) != 10 {
		t.Fatalf("cond+limit: got %d points", len(pts))
	}
	if v, _ := pts[0].V.AsInt64(); v != 51 {
		t.Fatalf("first cond point value = %d, want 51", v)
	}
	if v, _ := pts[9].V.AsInt64(); v != 60 {
		t.Fatalf("last cond point value = %d, want 60", v)
	}
}

// TestQueryIter_SpansDiskAndWAL verifies a query whose data lives on both
// flushed segments and the active WAL returns every point exactly once, in
// time order.
func TestQueryIter_SpansDiskAndWAL(t *testing.T) {
	db, base := openQueryTestDB(t, ColumnTypeInt)
	writeRange(t, db, "it", "cpu", base, 500)    // forces many flushes → disk
	writeRange(t, db, "it", "cpu", base+500, 50) // stays in the WAL

	it, err := db.QueryIter(context.Background(), "it", "cpu", base-1, base+600, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	pts := collectIter(t, it)
	if len(pts) != 550 {
		t.Fatalf("expected 550 points, got %d", len(pts))
	}
	for i := 1; i < len(pts); i++ {
		if pts[i].Tms < pts[i-1].Tms {
			t.Fatalf("not sorted at %d: %d < %d", i, pts[i].Tms, pts[i-1].Tms)
		}
	}
	seen := make(map[int64]bool, len(pts))
	for _, p := range pts {
		if seen[p.Tms] {
			t.Fatalf("duplicate timestamp %d", p.Tms)
		}
		seen[p.Tms] = true
	}
}

// TestQueryIter_ContextCancel verifies cancellation stops iteration early and
// surfaces the context error.
func TestQueryIter_ContextCancel(t *testing.T) {
	db, base := openQueryTestDB(t, ColumnTypeInt)
	writeRange(t, db, "it", "cpu", base, 200)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	it, err := db.QueryIter(ctx, "it", "cpu", base-1, base+300, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	n := 0
	gotCancel := false
	for {
		_, ok, err := it.Next()
		if err != nil {
			if ctx.Err() != nil {
				gotCancel = true
			}
			break
		}
		if !ok {
			break
		}
		n++
		if n == 10 {
			cancel()
		}
	}
	if !gotCancel {
		t.Fatal("expected context cancellation error from Next")
	}
}

// TestQueryIter_CloseIdempotent verifies Close can be called repeatedly.
func TestQueryIter_CloseIdempotent(t *testing.T) {
	db, base := openQueryTestDB(t, ColumnTypeInt)
	writeRange(t, db, "it", "cpu", base, 10)
	it, err := db.QueryIter(context.Background(), "it", "cpu", base-1, base+20, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestQueryWindow_SpansFlushBoundary verifies window aggregation is correct
// when a window straddles the disk/WAL boundary (single-pass merge, not the
// old disk-then-WAL concatenation).
func TestQueryWindow_SpansFlushBoundary(t *testing.T) {
	db, base := openQueryTestDB(t, ColumnTypeInt)
	writeRange(t, db, "it", "cpu", base, 100) // forces flushes → disk

	// Window of 10 ns over 100 points → 10 windows; MaxFusion value = 9,19,...,99.
	pts, err := db.Query("it", "cpu", base-1, base+200, 10, MaxFusion, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(pts) != 10 {
		t.Fatalf("expected 10 windows, got %d", len(pts))
	}
	for i, p := range pts {
		want := int64(i*10 + 9)
		if v, _ := p.V.AsInt64(); v != want {
			t.Fatalf("window %d value = %d, want %d", i, v, want)
		}
	}
}

// TestQueryAll_LateDataSorted exercises the WAL needsSort fallback through the
// merged iterator: late points are sorted with the rest.
func TestQueryAll_LateDataSorted(t *testing.T) {
	db, base := openQueryTestDB(t, ColumnTypeInt)
	writeRange(t, db, "it", "cpu", base, 20) // disk + wal
	// Late writes: older than existing data.
	for _, off := range []int64{200, 100, 300, 150, 250} {
		if _, err := db.Write("it", "cpu", base+off, variant.NewInt64(off)); err != nil {
			t.Fatal(err)
		}
	}
	pts, err := db.QueryAll("it", "cpu", base-1, base+400, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(pts) != 25 {
		t.Fatalf("expected 25 points, got %d", len(pts))
	}
	for i := 1; i < len(pts); i++ {
		if pts[i].Tms < pts[i-1].Tms {
			t.Fatalf("late data not sorted at %d", i)
		}
	}
}

// TestQueryHighCardWAL_IndexCorrectness exercises the lazy WAL tag index: with
// a high-cardinality WAL, querying each tag must return exactly one point and
// repeated queries must not drift (this is the scenario that previously hung
// when the indexed position state was reset incorrectly). The last pass runs
// 8 concurrent readers over the same buffer to verify the index build is
// race-free and idempotent.
func TestQueryHighCardWAL_IndexCorrectness(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(Config{
		Path:           dir,
		WalConfig:      WalConfig{MaxFileSize: 256 * 1024 * 1024}, // no flush: all data stays in the WAL
		MaxStorageTime: 24 * 60 * 60 * 365,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.CreateTable(TableInfo{ColumnAttribute: ColumnAttribute{Name: "hc", Type: ColumnTypeInt}}); err != nil {
		t.Fatal(err)
	}

	const numTags = 50_000
	base := time.Now().UnixNano()
	batch := make([]TagPoint, 0, 4096)
	for i := 0; i < numTags; i++ {
		batch = append(batch, TagPoint{
			Tag:       "tag_" + strconv.Itoa(i),
			Timestamp: base + int64(i),
			Value:     variant.NewInt64(int64(i)),
		})
		if len(batch) == 4096 {
			if _, err := db.WriteBatch("hc", batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if _, err := db.WriteBatch("hc", batch); err != nil {
			t.Fatal(err)
		}
	}

	// Query every tag twice (second pass exercises the cached index).
	for pass := 0; pass < 2; pass++ {
		for i := 0; i < numTags; i += 997 { // stride keeps it fast but thorough
			pts, err := db.QueryAll("hc", "tag_"+strconv.Itoa(i), base-1, base+int64(numTags)+1, nil)
			if err != nil {
				t.Fatalf("pass %d tag %d: %v", pass, i, err)
			}
			if len(pts) != 1 {
				t.Fatalf("pass %d tag %d: expected 1 point, got %d", pass, i, len(pts))
			}
			if v, _ := pts[0].V.AsInt64(); v != int64(i) {
				t.Fatalf("pass %d tag %d: value %d, want %d", pass, i, v, i)
			}
		}
	}

	// Full range: every point exactly once, in order.
	seen := make(map[int64]bool, numTags)
	for i := 0; i < numTags; i++ {
		pts, err := db.QueryAll("hc", "tag_"+strconv.Itoa(i), base-1, base+int64(numTags)+1, nil)
		if err != nil {
			t.Fatal(err)
		}
		seen[pts[0].Tms] = true
	}
	if len(seen) != numTags {
		t.Fatalf("expected %d unique timestamps, got %d", numTags, len(seen))
	}

	// Concurrent readers of the same high-cardinality buffer: the lazy index
	// build must be race-free and idempotent.
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				tag := fmt.Sprintf("tag_%d", (g*50+i)%numTags)
				pts, err := db.QueryAll("hc", tag, base-1, base+int64(numTags)+1, nil)
				if err != nil {
					errCh <- err
					return
				}
				if len(pts) != 1 {
					errCh <- fmt.Errorf("tag %s: %d points, want 1", tag, len(pts))
					return
				}
				if v, _ := pts[0].V.AsInt64(); v != int64((g*50+i)%numTags) {
					errCh <- fmt.Errorf("tag %s: value %d", tag, v)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestQueryWindow_AvgNumericFastPath verifies the numeric avg fast path: for
// uniform int64/float64 windows it must produce exactly the historical
// incremental-mean values, and UInt64 windows (which keep the generic variant
// path) must agree for values below 2^63.
func TestQueryWindow_AvgNumericFastPath(t *testing.T) {
	// Float64 window (fast path): running mean of 0..9 step 1 = 4.5 exactly.
	db, base := openQueryTestDB(t, ColumnTypeFloat)
	for i := int64(0); i < 100; i++ {
		if _, err := db.Write("it", "cpu", base+i, variant.NewFloat64(float64(i))); err != nil {
			t.Fatal(err)
		}
	}
	pts, err := db.Query("it", "cpu", base-1, base+200, 10, AvgFusion, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(pts) != 10 {
		t.Fatalf("float windows = %d, want 10", len(pts))
	}
	for k, p := range pts {
		v, _ := p.V.AsFloat64()
		want := float64(k*10) + 4.5
		if math.Abs(v-want) > 1e-9 {
			t.Fatalf("float window %d avg = %v, want %v", k, v, want)
		}
	}

	// Int64 window (fast path): incremental int division keeps the first value
	// (diffs < count), so window k averages to 10k exactly.
	db2, base2 := openQueryTestDB(t, ColumnTypeInt)
	writeRange(t, db2, "it", "cpu", base2, 100)
	pts2, err := db2.Query("it", "cpu", base2-1, base2+200, 10, AvgFusion, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(pts2) != 10 {
		t.Fatalf("int windows = %d, want 10", len(pts2))
	}
	for k, p := range pts2 {
		v, _ := p.V.AsInt64()
		if v != int64(k*10) {
			t.Fatalf("int window %d avg = %d, want %d", k, v, k*10)
		}
	}

	// UInt64 window (generic path): same incremental result for values < 2^63.
	db3, base3 := openQueryTestDB(t, ColumnTypeInt)
	for i := int64(0); i < 100; i++ {
		if _, err := db3.Write("it", "cpu", base3+i, variant.NewUInt64(uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	pts3, err := db3.Query("it", "cpu", base3-1, base3+200, 10, AvgFusion, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(pts3) != 10 {
		t.Fatalf("uint windows = %d, want 10", len(pts3))
	}
	for k, p := range pts3 {
		v, _ := p.V.AsInt64()
		if v != int64(k*10) {
			t.Fatalf("uint window %d avg = %d, want %d", k, v, k*10)
		}
	}
}
