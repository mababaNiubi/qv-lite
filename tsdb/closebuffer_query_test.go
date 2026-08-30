package tsdb

import (
	"context"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

// TestCloseBuffer_AsyncFlush_TailLoss is a regression test for a CloseBuffer
// read-path bug: walPointIter opened only the first WAL file of its snapshot
// and silently skipped every subsequent file (w.remaining was initialized only
// when the handle was first opened), so a query whose snapshot contained
// multiple WAL files lost the points of all files after the first — a
// contiguous tail loss with no error. AsyncFlush makes multi-file snapshots
// common at query time (the flush drains complete files in the background);
// sync flush drains inline, so queries usually see a single data file and the
// bug stayed invisible there.
func TestCloseBuffer_AsyncFlush_TailLoss(t *testing.T) {
	for round := 0; round < 3; round++ {
		db, err := Open(Config{
			Path:           tempDir(t),
			MaxStorageTime: 24 * 60 * 60 * 365,
			WalConfig:      WalConfig{MaxFileSize: 64 * 1024, CloseBuffer: true},
			AsyncFlush:     true,
		}, context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTable(TableInfo{ColumnAttribute: ColumnAttribute{Name: "t", Type: ColumnTypeInt}}); err != nil {
			t.Fatal(err)
		}
		const n = 500000
		base := time.Now().UnixNano()
		for i := 0; i < n; i++ {
			if _, err := db.Write("t", "cpu", base+int64(i), variant.NewInt64(int64(i))); err != nil {
				t.Fatal(err)
			}
		}
		st, _ := db.ssTables.Load("t")
		if st == nil {
			t.Fatal("table not found")
		}
		if err := st.batcher.Flush(); err != nil {
			t.Fatal(err)
		}
		// Query immediately: the async flush is still draining, so the WAL
		// snapshot typically contains many complete files. The old bug dropped
		// every file after the first one here.
		pts, err := db.QueryAll("t", "cpu", base-1, base+int64(n)+1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(pts) != n {
			t.Fatalf("round %d: immediate query got %d/%d points (tail loss)", round, len(pts), n)
		}
		for i, p := range pts {
			if v, _ := p.V.AsInt64(); v != int64(i) {
				t.Fatalf("round %d: point %d value=%d", round, i, v)
			}
		}
		// Drained query must also return everything.
		for st.walFile.NeedFlush() {
			time.Sleep(time.Millisecond)
		}
		pts2, err := db.QueryAll("t", "cpu", base-1, base+int64(n)+1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(pts2) != n {
			t.Fatalf("round %d: drained query got %d/%d points", round, len(pts2), n)
		}
		_ = db.Close()
	}
}
