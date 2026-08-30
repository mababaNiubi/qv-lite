package tsdb

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/mababaNiubi/variant"
)

// BenchmarkWriteResourceUsage measures the whole local ingestion pipeline:
// caller acceptance, the ordered WAL worker, asynchronous segment flushes and
// Close. Setup and temporary-directory removal are excluded. Besides normal
// benchmark allocation metrics it reports process heap observations so changes
// intended for memory-constrained devices can be judged on retained memory, not
// only cumulative allocations.
func BenchmarkWriteResourceUsage(b *testing.B) {
	tests := []struct {
		name        string
		numTags     int
		pointsPerOp int
		asyncFlush  bool
		queueSize   int
	}{
		{name: "tags1", numTags: 1, pointsPerOp: 5_000_000, asyncFlush: true},
		{name: "tags10000", numTags: 10_000, pointsPerOp: 5_000_000, asyncFlush: true},
		{name: "sparse-tags100000", numTags: 100_000, pointsPerOp: 100_000, asyncFlush: true},
		{name: "tags1-sync-flush", numTags: 1, pointsPerOp: 5_000_000},
		{name: "tags10000-sync-flush", numTags: 10_000, pointsPerOp: 5_000_000},
		{name: "tags10000-async-queue1", numTags: 10_000, pointsPerOp: 5_000_000, asyncFlush: true, queueSize: 1},
	}
	for _, tc := range tests {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			numTags := tc.numTags
			pointsPerOp := tc.pointsPerOp
			tags := prebuiltTags(numTags)
			var (
				acceptTotal  time.Duration
				totalAlloc   uint64
				totalGC      uint32
				maxHeapAlloc uint64
				maxHeapInuse uint64
				maxHeapSys   uint64
			)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)
				dir, err := os.MkdirTemp("", "qvlite_resource_bench_*")
				if err != nil {
					b.Fatal(err)
				}
				db, err := Open(Config{
					Path: dir,
					WalConfig: WalConfig{
						MaxFileSize: 8 << 20,
						CloseBuffer: false,
					},
					IngestConfig: IngestConfig{
						Shards:          16,
						MaxBatchSize:    4096,
						FlushIntervalMs: 5,
						QueueSize:       tc.queueSize,
					},
					AsyncFlush:     tc.asyncFlush,
					MaxStorageTime: 365 * 24 * 60 * 60,
				}, context.Background())
				if err != nil {
					_ = os.RemoveAll(dir)
					b.Fatal(err)
				}
				if err := db.CreateTable(TableInfo{ColumnAttribute: ColumnAttribute{Name: "t", FloatPrecision: 2}}); err != nil {
					_ = db.Close()
					_ = os.RemoveAll(dir)
					b.Fatal(err)
				}

				base := time.Now().UnixNano()
				b.StartTimer()
				acceptStart := time.Now()
				for i := 0; i < pointsPerOp; i++ {
					if _, err := db.Write("t", tags[i%numTags], base+int64(i)*int64(time.Millisecond), variant.NewFloat64(float64(i))); err != nil {
						b.Fatal(err)
					}
				}
				acceptTotal += time.Since(acceptStart)
				b.StopTimer()
				var afterAccept runtime.MemStats
				runtime.ReadMemStats(&afterAccept)

				table, ok := db.ssTables.Load("t")
				if !ok {
					b.Fatal("missing table")
				}
				b.StartTimer()
				if err := table.batcher.Flush(); err != nil {
					b.Fatal(err)
				}
				table.flushWg.Wait()
				b.StopTimer()
				var afterDrain runtime.MemStats
				runtime.ReadMemStats(&afterDrain)
				b.StartTimer()
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()

				var afterClose runtime.MemStats
				runtime.ReadMemStats(&afterClose)
				for _, stats := range []*runtime.MemStats{&afterAccept, &afterDrain, &afterClose} {
					maxHeapAlloc = max(maxHeapAlloc, stats.HeapAlloc)
					maxHeapInuse = max(maxHeapInuse, stats.HeapInuse)
					maxHeapSys = max(maxHeapSys, stats.HeapSys)
				}
				totalAlloc += afterClose.TotalAlloc - before.TotalAlloc
				totalGC += afterClose.NumGC - before.NumGC
				if err := os.RemoveAll(dir); err != nil {
					b.Fatal(err)
				}
			}

			points := float64(pointsPerOp * b.N)
			b.ReportMetric(float64(acceptTotal.Nanoseconds())/points, "accept-ns/point")
			b.ReportMetric(float64(totalAlloc)/points, "total-alloc-B/point")
			b.ReportMetric(float64(totalGC)/float64(b.N), "gc/op")
			b.ReportMetric(float64(maxHeapAlloc)/(1<<20), "peak-heap-alloc-MiB")
			b.ReportMetric(float64(maxHeapInuse)/(1<<20), "peak-heap-inuse-MiB")
			b.ReportMetric(float64(maxHeapSys)/(1<<20), "peak-heap-sys-MiB")
		})
	}
}
