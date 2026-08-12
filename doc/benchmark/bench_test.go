package benchmark

import "testing"

// BenchmarkQuickSuite wires the quick scenarios into the standard go test
// benchmark harness so `-benchmem` and the file-based profiles work:
//
//	go test ./doc/benchmark/ -bench BenchmarkQuickSuite -benchtime=1x -benchmem \
//	   -cpuprofile cpu.out -memprofile mem.out
//
// Each iteration runs one full scenario (write + optional read + close), so a
// benchmark "op" is a complete end-to-end pass.
func BenchmarkQuickSuite(b *testing.B) {
	for _, sc := range QuickSuite() {
		sc := sc
		b.Run(sc.Name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				r, err := RunScenario(sc, HarnessConfig{Label: "bench"})
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(r.WriteRate, "write_pts/s")
				b.ReportMetric(r.ReadRate, "read_pts/s")
				b.ReportMetric(r.Ratio, "ratio")
				b.ReportMetric(r.BytesPerPoint, "bytes/pt")
				b.ReportMetric(float64(r.PeakHeap)/1e6, "peak_heap_MB")
			}
		})
	}
}
