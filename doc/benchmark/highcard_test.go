package benchmark

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mababaNiubi/qv-lite/tsdb"
	"github.com/mababaNiubi/variant"
)

// TestHighCardMemory writes one point to each of N sparse tags and reports the
// peak heap and bytes-per-tag. It is the M1 regression guard: column encoder
// pre-allocation must not scale with tag count (a 100k-tag table should not
// cost ~96KB x 100k just to hold sparse data). Before M1 this was ~96KB/tag
// (TimeEncoder 32KB + value encoder at MaxBufferBatchSize=4096); after M1 it
// should be a few KB/tag.
//
//	N tags via HIGHCARD_TAGS env (default 5000; keep small for fast runs).
func TestHighCardMemory(t *testing.T) {
	n := 5000
	if v := os.Getenv("HIGHCARD_TAGS"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n <= 0 {
			t.Fatalf("invalid HIGHCARD_TAGS %q", v)
		}
	}
	dir, err := os.MkdirTemp("", "qv_highcard_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := tsdb.Open(tsdb.Config{
		Path:           dir,
		MaxStorageTime: 100 * 365 * 24 * 3600,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(tsdb.TableInfo{ColumnAttribute: tsdb.ColumnAttribute{
		Name: "hc", Type: tsdb.ColumnTypeFloat, FloatPrecision: 4,
	}}); err != nil {
		t.Fatal(err)
	}

	var peak atomic.Uint64
	stop := make(chan struct{})
	go func() {
		tk := time.NewTicker(20 * time.Millisecond)
		defer tk.Stop()
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				runtime.ReadMemStats(&m)
				if a := m.HeapAlloc; a > peak.Load() {
					peak.Store(a)
				}
			}
		}
	}()

	base := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		tag := fmt.Sprintf("tag_%06d", i)
		if _, err := db.Write("hc", tag, base+int64(i), variant.NewFloat64(float64(i))); err != nil {
			t.Fatalf("write tag %d: %v", i, err)
		}
	}
	close(stop)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	peakHeap := peak.Load()
	if ms.HeapAlloc > peakHeap {
		peakHeap = ms.HeapAlloc
	}
	kbPerTag := float64(peakHeap) / 1e3 / float64(n)
	t.Logf("tags=%d peakHeap=%.1fMB (%.2f KB/tag) heapAfterGC=%.1fMB",
		n, float64(peakHeap)/1e6, kbPerTag, float64(ms.HeapAlloc)/1e6)

	// M1 guard: sparse-tag memory must stay well under 16KB/tag. This is
	// deliberately lenient (encoders grow on demand); the real signal is the
	// before/after comparison in the dev log.
	if kbPerTag > 16 {
		t.Errorf("memory per sparse tag too high: %.2f KB/tag (M1 regression guard, want <16KB)", kbPerTag)
	}
}
