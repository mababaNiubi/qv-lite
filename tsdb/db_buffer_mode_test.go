//go:build buffer_mode

// This file contains test cases for the BufferMode configuration.
// Enable with: go test -tags buffer_mode ./tsdb/
//
// Requires the BufferMode implementation in db.go and file_wal.go.
// See docs/BUFFER_MODE_FLUSH_BUG.md for implementation details.

package tsdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

// ---------- Persistence tests for all 3 modes ----------

// TestDB_BufferModeBuffer_Persistence verifies the default "buffer" mode
// preserves all data across close/reopen.
func TestDB_BufferModeBuffer_Persistence(t *testing.T) {
	testBufferModePersistence(t, "buffer")
}

// TestDB_BufferModeClose_Persistence verifies "close" mode (direct disk write)
// preserves all data across close/reopen.
func TestDB_BufferModeClose_Persistence(t *testing.T) {
	testBufferModePersistence(t, "close")
}

// TestDB_BufferModeFlush_Persistence verifies "flush" mode.
//
// KNOWN BUG: This test currently FAILS with 5000-24=4976 points.
// See docs/BUFFER_MODE_FLUSH_BUG.md for root cause analysis.
//
// Missing indices: [200, 401, 602, ...] every 201 records.
// Each file has 201 records (1 SyncToDisk + 4 batches of 50).
// The SyncToDisk entry (first record of each file) is lost.
func TestDB_BufferModeFlush_Persistence(t *testing.T) {
	testBufferModePersistence(t, "flush")
}

func testBufferModePersistence(t *testing.T, mode string) {
	dir := tempDir(t)
	cfg := Config{
		Path:           dir,
		MaxStorageTime: 24 * 60 * 60 * 365,
		WalConfig: WalConfig{
			MaxFileSize:        4 * 1024,
			MaxBufferBatchSize: 50,
			BufferMode:         mode,
		},
	}
	db, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tableName := "t_" + mode
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: tableName, Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	const n = 5000
	for i := 0; i < n; i++ {
		if _, err := db.Write(tableName, "tag1", baseTime+int64(i), variant.NewInt(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// In-session query
	points, err := db.QueryAll(tableName, "tag1", baseTime-1, baseTime+int64(n)+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != n {
		// Find missing indices for diagnostics
		seen := make(map[int64]bool, len(points))
		for _, p := range points {
			seen[p.Tms] = true
		}
		var missing []int
		for i := 0; i < n; i++ {
			if !seen[baseTime+int64(i)] {
				missing = append(missing, i)
			}
		}
		t.Fatalf("[%s] in-session: expected %d, got %d, missing %d: %v",
			mode, n, len(points), len(missing), missing[:min(10, len(missing))])
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and verify
	db2, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	points, err = db2.QueryAll(tableName, "tag1", baseTime-1, baseTime+int64(n)+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != n {
		t.Fatalf("[%s] after reopen: expected %d, got %d", mode, n, len(points))
	}
}

// ---------- Diagnostic tests ----------

// TestDiag_FlushMode_MissingPattern identifies which data points are lost
// in flush mode and verifies the pattern.
func TestDiag_FlushMode_MissingPattern(t *testing.T) {
	dir := tempDir(t)
	cfg := Config{
		Path:           dir,
		MaxStorageTime: 24 * 60 * 60 * 365,
		WalConfig: WalConfig{
			MaxFileSize:        4 * 1024,
			MaxBufferBatchSize: 50,
			BufferMode:         "flush",
		},
	}
	db, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "fb", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	const n = 5000
	for i := 0; i < n; i++ {
		if _, err := db.Write("fb", "tag1", baseTime+int64(i), variant.NewInt(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	all, _ := db.QueryAll("fb", "tag1", baseTime-1, baseTime+int64(n)+1, nil)

	seen := make(map[int64]bool, len(all))
	for _, p := range all {
		seen[p.Tms] = true
	}
	var missing []int
	for i := 0; i < n; i++ {
		if !seen[baseTime+int64(i)] {
			missing = append(missing, i)
		}
	}

	t.Logf("Total: %d, Missing: %d", len(all), len(missing))
	if len(missing) > 0 {
		t.Logf("Missing indices: %v", missing)
		// Verify the pattern: every 201 records
		if len(missing) >= 2 {
			gap := missing[1] - missing[0]
			t.Logf("Gap between missing: %d (expected 201)", gap)
		}
	}

	db.Close()
}

// TestDiag_FlushMode_DataDistribution counts how many points are in
// WAL vs segments to isolate where data is lost.
func TestDiag_FlushMode_DataDistribution(t *testing.T) {
	dir := tempDir(t)
	cfg := Config{
		Path:           dir,
		MaxStorageTime: 24 * 60 * 60 * 365,
		WalConfig: WalConfig{
			MaxFileSize:        4 * 1024,
			MaxBufferBatchSize: 50,
			BufferMode:         "flush",
		},
	}
	db, err := Open(cfg, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "fb", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().UnixNano()
	const n = 5000
	for i := 0; i < n; i++ {
		if _, err := db.Write("fb", "tag1", baseTime+int64(i), variant.NewInt(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	tbl, _ := db.ssTables.Load("fb")
	code, _ := tbl.Meta.Load("tag1")

	walPoints, _ := tbl.walFile.ReadByTime(code, baseTime-1, baseTime+int64(n)+1)
	segPoints, _ := tbl.queryDisk(code, baseTime-1, baseTime+int64(n)+1, CompileCondition(nil))

	t.Logf("WAL points: %d", len(walPoints))
	t.Logf("Segment points: %d", len(segPoints))
	t.Logf("Total: %d (expected %d, missing %d)",
		len(walPoints)+len(segPoints), n, n-len(walPoints)-len(segPoints))

	// Check WAL file details
	if inner, ok := tbl.walFile.(*walFile); ok {
		inner.mutex.Lock()
		t.Logf("WAL files: %d", len(inner.walFiles))
		totalOnDisk := 0
		for i := range inner.walFiles {
			cnt := 0
			forEachWalFile(inner.walFiles[i].fileName, func(tag tagCode, ts int64, v variant.Variant, off int64) bool {
				cnt++
				return true
			})
			t.Logf("  file %d: length=%d, records=%d", i, inner.walFiles[i].length, cnt)
			totalOnDisk += cnt
		}
		t.Logf("Total records on disk: %d", totalOnDisk)
		inner.mutex.Unlock()
	}

	db.Close()
}

// TestDiag_FlushMode_RecordSize calculates the per-record byte size
// to understand file rotation patterns.
func TestDiag_FlushMode_RecordSize(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(Config{
		Path:           dir,
		MaxStorageTime: 24 * 60 * 60 * 365,
		WalConfig: WalConfig{
			MaxFileSize:        4 * 1024,
			MaxBufferBatchSize: 50,
			BufferMode:         "flush",
		},
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.CreateTable(TableInfo{
		ColumnAttribute: ColumnAttribute{Name: "rs", Type: ColumnTypeInt},
	}); err != nil {
		t.Fatal(err)
	}

	// Write 100 points, flush, then check file size
	baseTime := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		db.Write("rs", "tag1", baseTime+int64(i), variant.NewInt(i))
	}

	tbl, _ := db.ssTables.Load("rs")
	if inner, ok := tbl.walFile.(*walFile); ok {
		inner.mutex.Lock()
		for i := range inner.walFiles {
			cnt := 0
			forEachWalFile(inner.walFiles[i].fileName, func(tag tagCode, ts int64, v variant.Variant, off int64) bool {
				cnt++
				return true
			})
			if cnt > 0 {
				perRecord := inner.walFiles[i].length / int64(cnt)
				t.Logf("File %d: %d records, %d bytes, %d bytes/record",
					i, cnt, inner.walFiles[i].length, perRecord)
			}
		}
		inner.mutex.Unlock()
	}
}
