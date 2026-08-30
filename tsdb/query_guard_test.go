package tsdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

// openGuardDB opens a DB with the given query guards and writes n points.
func openGuardDB(t *testing.T, queryTimeout time.Duration, maxQueryPoints int64, n int) (*DB, int64) {
	t.Helper()
	db, err := Open(Config{
		Path:           tempDir(t),
		MaxStorageTime: 24 * 60 * 60 * 365,
		QueryTimeout:   queryTimeout,
		MaxQueryPoints: maxQueryPoints,
		WalConfig:      WalConfig{MaxFileSize: 1 << 20},
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreateTable(TableInfo{ColumnAttribute: ColumnAttribute{Name: "g", Type: ColumnTypeInt}}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UnixNano()
	batch := make([]TagPoint, 0, 4096)
	for i := 0; i < n; i++ {
		batch = append(batch, TagPoint{Tag: "cpu", Timestamp: base + int64(i), Value: variant.NewInt64(int64(i))})
		if len(batch) == 4096 {
			if _, err := db.WriteBatch("g", batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if _, err := db.WriteBatch("g", batch); err != nil {
			t.Fatal(err)
		}
	}
	return db, base
}

// TestQueryTimeout verifies a configured QueryTimeout aborts materialized
// queries with context.DeadlineExceeded. Uses a slow query (100K points on
// disk) with a 1ms timeout: the deadline reliably fires mid-query.
func TestQueryTimeout(t *testing.T) {
	db, base := openGuardDB(t, time.Millisecond, 0, 100_000)
	_, err := db.QueryAll("g", "cpu", base-1, base+200_000, nil)
	if err == nil {
		t.Fatal("expected deadline exceeded error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
}

// TestQueryTimeoutStreaming verifies QueryIter respects a configured timeout
// through the iterator's context.
func TestQueryTimeoutStreaming(t *testing.T) {
	db, base := openGuardDB(t, time.Millisecond, 0, 100_000)
	it, err := db.QueryIter(context.Background(), "g", "cpu", base-1, base+200_000, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	for {
		_, ok, err := it.Next()
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error = %v, want context.DeadlineExceeded", err)
			}
			return
		}
		if !ok {
			t.Fatal("stream completed before the timeout fired")
		}
	}
}

// TestQueryTimeoutExternCtx verifies the caller's cancelled context wins even
// without a configured timeout (existing behavior, guard remains intact).
func TestQueryTimeoutExternCtx(t *testing.T) {
	db, base := openGuardDB(t, 0, 0, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	it, err := db.QueryIter(ctx, "g", "cpu", base-1, base+2000, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	_, _, err = it.Next()
	if err == nil {
		t.Fatal("expected cancellation error from Next")
	}
}

// TestMaxQueryPoints verifies the materialized result is capped.
func TestMaxQueryPoints(t *testing.T) {
	db, base := openGuardDB(t, 0, 100, 1000)
	_, err := db.QueryAll("g", "cpu", base-1, base+2000, nil)
	if err == nil {
		t.Fatal("expected result limit error")
	}
	if !errors.Is(err, ErrorQueryResultLimitExceeded) {
		t.Fatalf("error = %v, want ErrorQueryResultLimitExceeded", err)
	}

	// Window query output is capped as well (100 windows of size 1ns over 1000 points).
	_, err = db.Query("g", "cpu", base-1, base+2000, 1, AvgFusion, nil)
	if err == nil {
		t.Fatal("expected result limit error for window query")
	}
	if !errors.Is(err, ErrorQueryResultLimitExceeded) {
		t.Fatalf("window error = %v, want ErrorQueryResultLimitExceeded", err)
	}
}

// TestMaxQueryPointsUnlimited verifies the default (0) has no limit and no
// per-point overhead path regression.
func TestMaxQueryPointsUnlimited(t *testing.T) {
	db, base := openGuardDB(t, 0, 0, 1000)
	pts, err := db.QueryAll("g", "cpu", base-1, base+2000, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(pts) != 1000 {
		t.Fatalf("points = %d, want 1000", len(pts))
	}
}
