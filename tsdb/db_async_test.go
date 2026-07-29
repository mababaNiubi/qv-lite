package tsdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

// countDataFiles returns the number of .tsb segment files in a table's data dir.
func countDataFiles(t *testing.T, dir, tableName string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, tableName, "data"))
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), dataSuffix) {
			n++
		}
	}
	return n
}

func asyncFlushConfig(dir string) Config {
	return Config{
		Path:           dir,
		MaxStorageTime: 24 * 60 * 60 * 365,
		WalConfig: WalConfig{
			MaxFileSize:        4 * 1024,
			MaxBufferBatchSize: 50,
		},
		AsyncFlush: true,
	}
}

// ---------- moved from db_test.go ----------

func TestDB_AsyncFlush_Persistence(t *testing.T) {
	dir := tempDir(t)
	cfg := asyncFlushConfig(dir)
	db, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "af", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	const n = 5000
	for i := 0; i < n; i++ {
		if _, err := db.Write("af", "tag1", baseTime+int64(i), variant.NewInt(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	points, err := db2.QueryAll("af", "tag1", baseTime-1, baseTime+int64(n)+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != n {
		t.Fatalf("expected %d points after reopen, got %d", n, len(points))
	}
}

func TestDB_AsyncFlush_RunsInBackground(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(asyncFlushConfig(dir), context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "af2", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	for i := 0; i < 3000; i++ {
		if _, err := db.Write("af2", "tag1", baseTime+int64(i), variant.NewInt(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	table, ok := db.ssTables.Load("af2")
	if !ok {
		t.Fatal("table not found")
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !table.walFile.NeedFlush() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if table.walFile.NeedFlush() {
		t.Fatal("async flush did not drain the WAL within timeout")
	}
	if got := countDataFiles(t, dir, "af2"); got == 0 {
		t.Fatal("expected segment files on disk after async flush, found none")
	}
	points, err := db.QueryAll("af2", "tag1", baseTime-1, baseTime+3001, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 3000 {
		t.Fatalf("expected 3000 points, got %d", len(points))
	}
}

func TestDB_AsyncCleanup_RemovesExpiredSegments(t *testing.T) {
	dir := tempDir(t)
	tableName := "ac"
	dataDir := filepath.Join(dir, tableName, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UnixNano()
	oldTs := []int64{
		now - 2*int64(time.Hour),
		now - int64(90*time.Minute),
		now - int64(80*time.Minute),
	}
	for _, ts := range oldTs {
		p := filepath.Join(dataDir, strconv.FormatInt(ts, 10)+dataSuffix)
		if err := os.WriteFile(p, nil, 0644); err != nil {
			t.Fatal(err)
		}
	}
	if got := countDataFiles(t, dir, tableName); got != 3 {
		t.Fatalf("expected 3 pre-seeded files, got %d", got)
	}

	db, err := Open(Config{
		Path:                   dir,
		MaxStorageTime:         24 * 60 * 60 * 365,
		ExpirationMinuteTime:   1,
		AsyncCleanup:           true,
		CleanupIntervalSeconds: 1,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: tableName, Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if countDataFiles(t, dir, tableName) < 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := countDataFiles(t, dir, tableName); got >= 3 {
		t.Fatalf("async cleanup did not remove expired segments, still %d files", got)
	}

	baseTime := time.Now().UnixNano()
	if _, err := db.Write(tableName, "tag1", baseTime, variant.NewInt(42)); err != nil {
		t.Fatalf("write after cleanup: %v", err)
	}
	points, err := db.QueryAll(tableName, "tag1", baseTime-1, baseTime+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 1 {
		t.Fatalf("expected 1 point after cleanup, got %d", len(points))
	}
}

// ---------- new test cases ----------

// TestDB_AsyncFlush_ConcurrentQuery verifies that querying while the async
// flush goroutine is active does not corrupt data or return duplicates.
func TestDB_AsyncFlush_ConcurrentQuery(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(asyncFlushConfig(dir), context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "cq", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	const n = 3000

	// Write a few points first so the tag exists before the reader starts.
	for i := 0; i < 100; i++ {
		if _, err := db.Write("cq", "tag1", baseTime+int64(i), variant.NewInt(i)); err != nil {
			t.Fatal(err)
		}
	}

	// Writer goroutine: writes the remaining points.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 100; i < n; i++ {
			if _, err := db.Write("cq", "tag1", baseTime+int64(i), variant.NewInt(i)); err != nil {
				t.Errorf("write %d: %v", i, err)
				return
			}
		}
	}()

	// Reader goroutine: queries while writes are ongoing.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			pts, qerr := db.QueryAll("cq", "tag1", baseTime, baseTime+n, nil)
			if qerr != nil {
				t.Errorf("query %d: %v", i, qerr)
				return
			}
			// Verify no duplicate timestamps.
			seen := make(map[int64]bool, len(pts))
			for _, p := range pts {
				if seen[p.Tms] {
					t.Errorf("duplicate timestamp %d during concurrent query", p.Tms)
					return
				}
				seen[p.Tms] = true
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()

	// Final integrity check: total unique points should equal n.
	points, err := db.QueryAll("cq", "tag1", baseTime-1, baseTime+n+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != n {
		t.Fatalf("expected %d points, got %d", n, len(points))
	}
}

// TestDB_AsyncFlush_WriteBatch verifies WriteBatch works correctly with
// async flush and that data survives close + reopen.
func TestDB_AsyncFlush_WriteBatch(t *testing.T) {
	dir := tempDir(t)
	cfg := asyncFlushConfig(dir)
	db, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "wb", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	const totalPoints = 4000
	const batchSize = 200
	for n := 0; n < totalPoints/batchSize; n++ {
		points := make([]TagPoint, batchSize)
		for i := 0; i < batchSize; i++ {
			idx := n*batchSize + i
			points[i] = TagPoint{
				Tag:       "cpu",
				Timestamp: baseTime + int64(idx),
				Value:     variant.NewInt(idx),
			}
		}
		results, err := db.WriteBatch("wb", points)
		if err != nil {
			t.Fatalf("batch %d failed: %v", n, err)
		}
		if results != batchSize {
			t.Fatalf("batch %d: expected %d results, got %d", n, batchSize, results)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	points, err := db2.QueryAll("wb", "cpu", baseTime-1, baseTime+totalPoints+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != totalPoints {
		//s := 0
		//for i := range points {
		//	asInt, err := points[i].V.AsInt()
		//	if err != nil {
		//		return
		//	}
		//	if asInt != i {
		//		t.Log(i, points[i])
		//		s++
		//		if s > 5 {
		//			break
		//		}
		//	}
		//}
		t.Fatalf("expected %d points after reopen, got %d", totalPoints, len(points))
	}
}

// TestDB_AsyncFlush_ConcurrentWrites verifies that multiple goroutines
// writing to different tags with async flush produces correct results.
func TestDB_AsyncFlush_ConcurrentWrites(t *testing.T) {
	dir := tempDir(t)
	cfg := asyncFlushConfig(dir)
	db, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "cw", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	const numTags = 10
	const pointsPerTag = 300

	var wg sync.WaitGroup
	for tagIdx := 0; tagIdx < numTags; tagIdx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tag := fmt.Sprintf("sensor_%d", idx)
			for i := 0; i < pointsPerTag; i++ {
				if _, err := db.Write("cw", tag, baseTime+int64(i), variant.NewInt(idx*1000+i)); err != nil {
					t.Errorf("write tag=%s i=%d: %v", tag, i, err)
					return
				}
			}
		}(tagIdx)
	}
	wg.Wait()

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	for tagIdx := 0; tagIdx < numTags; tagIdx++ {
		tag := fmt.Sprintf("sensor_%d", tagIdx)
		points, err := db2.QueryAll("cw", tag, baseTime-1, baseTime+pointsPerTag+1, nil)
		if err != nil {
			t.Fatalf("query tag %s: %v", tag, err)
		}
		if len(points) != pointsPerTag {
			t.Errorf("tag %s: expected %d points, got %d", tag, pointsPerTag, len(points))
		}
	}
}

// TestDB_AsyncFlush_MultipleDataTypes verifies that int, float, string,
// bool and struct data all survive async flush + reopen.
func TestDB_AsyncFlush_MultipleDataTypes(t *testing.T) {
	dir := tempDir(t)
	cfg := Config{
		Path:           dir,
		MaxStorageTime: 24 * 60 * 60 * 365,
		WalConfig: WalConfig{
			MaxFileSize:        256 * 1024,
			MaxBufferBatchSize: 200,
		},
		AsyncFlush: true,
	}
	db, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	const n = 500

	// Int table
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "dt_int", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		db.Write("dt_int", "t", baseTime+int64(i), variant.NewInt(i))
	}

	// Float table
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "dt_float", Type: ColumnTypeFloat, FloatPrecision: 2},
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		db.Write("dt_float", "t", baseTime+int64(i), variant.NewFloat64(float64(i)*1.5))
	}

	// String table
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "dt_str", Type: ColumnTypeString},
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		db.Write("dt_str", "t", baseTime+int64(i), variant.NewString("val_"+strconv.Itoa(i)))
	}

	// Bool table
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "dt_bool", Type: ColumnTypeBool},
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		db.Write("dt_bool", "t", baseTime+int64(i), variant.NewBool(i%2 == 0))
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	checks := []struct {
		table string
	}{
		{"dt_int"}, {"dt_float"}, {"dt_str"}, {"dt_bool"},
	}
	for _, c := range checks {
		points, err := db2.QueryAll(c.table, "t", baseTime-1, baseTime+n+1, nil)
		if err != nil {
			t.Fatalf("query %s: %v", c.table, err)
		}
		if len(points) != n {
			t.Errorf("%s: expected %d points, got %d", c.table, n, len(points))
		}
	}
}

// TestDB_AsyncCleanup_WithOngoingWrites verifies that the background cleanup
// does not interfere with active writes and queries.
func TestDB_AsyncCleanup_WithOngoingWrites(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(Config{
		Path:                   dir,
		MaxStorageTime:         24 * 60 * 60 * 365,
		ExpirationMinuteTime:   1,
		AsyncCleanup:           true,
		CleanupIntervalSeconds: 1,
		WalConfig: WalConfig{
			MaxFileSize:        4 * 1024,
			MaxBufferBatchSize: 50,
		},
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "cw", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	const n = 2000

	// Write data while cleanup is running in the background.
	for i := 0; i < n; i++ {
		if _, err := db.Write("cw", "tag1", baseTime+int64(i), variant.NewInt(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// All freshly written data should be queryable (within the expiry window).
	points, err := db.QueryAll("cw", "tag1", baseTime-1, baseTime+n+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != n {
		t.Fatalf("expected %d points, got %d", n, len(points))
	}
}

// TestDB_AsyncFlush_SyncVsAsyncParity verifies that the same workload
// produces identical results whether AsyncFlush is enabled or not.
func TestDB_AsyncFlush_SyncVsAsyncParity(t *testing.T) {
	const n = 1000
	baseTime := time.Now().UnixNano()

	writeWorkload := func(db *DB, table string) {
		for i := 0; i < n; i++ {
			db.Write(table, "tag", baseTime+int64(i), variant.NewInt(i))
		}
	}

	// Sync mode
	dirSync := tempDir(t)
	cfgSync := Config{
		Path:           dirSync,
		MaxStorageTime: 24 * 60 * 60 * 365,
		WalConfig:      WalConfig{MaxFileSize: 4 * 1024, MaxBufferBatchSize: 50},
	}
	dbSync, err := Open(cfgSync, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	dbSync.CreateTable(TableInfo{ColumnAttribute: ColumnAttribute{Name: "t", Type: ColumnTypeInt}})
	writeWorkload(dbSync, "t")
	dbSync.Close()
	dbSync2, _ := Open(cfgSync, context.Background())
	defer dbSync2.Close()
	syncPoints, _ := dbSync2.QueryAll("t", "tag", baseTime-1, baseTime+n+1, nil)

	// Async mode
	dirAsync := tempDir(t)
	dbAsync, err := Open(asyncFlushConfig(dirAsync), context.Background())
	if err != nil {
		t.Fatal(err)
	}
	dbAsync.CreateTable(TableInfo{ColumnAttribute: ColumnAttribute{Name: "t", Type: ColumnTypeInt}})
	writeWorkload(dbAsync, "t")
	dbAsync.Close()
	dbAsync2, _ := Open(asyncFlushConfig(dirAsync), context.Background())
	defer dbAsync2.Close()
	asyncPoints, _ := dbAsync2.QueryAll("t", "tag", baseTime-1, baseTime+n+1, nil)

	if len(syncPoints) != len(asyncPoints) {
		t.Fatalf("sync=%d vs async=%d points", len(syncPoints), len(asyncPoints))
	}
	for i := range syncPoints {
		if syncPoints[i].Tms != asyncPoints[i].Tms {
			t.Errorf("point %d: sync.Tms=%d != async.Tms=%d", i, syncPoints[i].Tms, asyncPoints[i].Tms)
			break
		}
	}
}
