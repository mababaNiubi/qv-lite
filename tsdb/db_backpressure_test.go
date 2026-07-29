package tsdb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

// TestDB_Backpressure_NoErrorOnFullWAL verifies that when the WAL fills up
// (MaxFileNumber reached), writes block and flush instead of returning
// ErrorWALCacheFull. All data should be preserved.
func TestDB_Backpressure_NoErrorOnFullWAL(t *testing.T) {
	dir := tempDir(t)
	cfg := Config{
		Path:           dir,
		MaxStorageTime: 24 * 60 * 60 * 365,
		WalConfig: WalConfig{
			MaxFileSize:        4 * 1024,
			MaxFileNumber:      2,
			MaxBufferBatchSize: 50,
		},
	}
	db, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "bp", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	const n = 5000
	for i := 0; i < n; i++ {
		ok, err := db.Write("bp", "tag1", baseTime+int64(i), variant.NewInt(i))
		if err != nil {
			t.Fatalf("write %d failed (backpressure should have prevented this): %v", i, err)
		}
		if !ok {
			t.Fatalf("write %d returned ok=false", i)
		}
	}

	points, err := db.QueryAll("bp", "tag1", baseTime-1, baseTime+int64(n)+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != n {
		t.Fatalf("expected %d points, got %d", n, len(points))
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	points, err = db2.QueryAll("bp", "tag1", baseTime-1, baseTime+int64(n)+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != n {
		t.Fatalf("after reopen: expected %d, got %d", n, len(points))
	}
}

// TestDB_Backpressure_AsyncFlush verifies backpressure works with async flush:
// when the background flush can't keep up, writes block instead of erroring.
func TestDB_Backpressure_AsyncFlush(t *testing.T) {
	dir := tempDir(t)
	cfg := Config{
		Path:           dir,
		MaxStorageTime: 24 * 60 * 60 * 365,
		WalConfig: WalConfig{
			MaxFileSize:        4 * 1024,
			MaxFileNumber:      3,
			MaxBufferBatchSize: 50,
		},
		AsyncFlush: true,
	}
	db, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "abp", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	const n = 10000
	for i := 0; i < n; i++ {
		_, err := db.Write("abp", "tag1", baseTime+int64(i), variant.NewInt(i))
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	points, err := db.QueryAll("abp", "tag1", baseTime-1, baseTime+int64(n)+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != n {
		t.Fatalf("expected %d points, got %d", n, len(points))
	}
}

// TestDB_Backpressure_ConcurrentWriters verifies that multiple concurrent
// writers all succeed when the WAL fills up, with backpressure serializing
// their flush attempts.
func TestDB_Backpressure_ConcurrentWriters(t *testing.T) {
	dir := tempDir(t)
	cfg := Config{
		Path:           dir,
		MaxStorageTime: 24 * 60 * 60 * 365,
		WalConfig: WalConfig{
			MaxFileSize:        4 * 1024,
			MaxFileNumber:      2,
			MaxBufferBatchSize: 50,
		},
	}
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
	const numGoroutines = 4
	const pointsPerGoroutine = 2000

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < pointsPerGoroutine; i++ {
				ts := baseTime + int64(gid*pointsPerGoroutine+i)
				_, err := db.Write("cw", "tag1", ts, variant.NewInt(gid*pointsPerGoroutine+i))
				if err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}(g)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent write failed: %v", err)
		}
	}

	total := numGoroutines * pointsPerGoroutine
	points, err := db.QueryAll("cw", "tag1", baseTime-1, baseTime+int64(total)+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != total {
		t.Fatalf("expected %d points, got %d", total, len(points))
	}
}
