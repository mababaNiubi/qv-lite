package benchmark

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestQuickSuite is a fast smoke test: it runs every quick scenario and checks
// that a report is produced with correct read-back. Run with:
//
//	go test ./doc/benchmark/ -run TestQuickSuite -v
func TestQuickSuite(t *testing.T) {
	for _, sc := range QuickSuite() {
		sc := sc
		t.Run(sc.Name, func(t *testing.T) {
			r, err := RunScenario(sc, HarnessConfig{Label: "test"})
			if err != nil {
				t.Fatalf("scenario failed: %v", err)
			}
			if r.WriteRate <= 0 {
				t.Errorf("write rate <= 0: %v", r.WriteRate)
			}
			if r.OnDiskBytes <= 0 {
				t.Errorf("on-disk bytes <= 0: %v", r.OnDiskBytes)
			}
			if r.Ratio <= 0 {
				t.Errorf("ratio <= 0: %v", r.Ratio)
			}
			if sc.Query == QueryFull && !r.Correct {
				t.Errorf("correct=false: read %d != written %d", r.ReadCount, r.Points)
			}
			t.Logf("%-30s write=%8.0f pts/s read=%8.0f pts/s ratio=%5.2f bytes/pt=%5.2f peakHeap=%6.1fMB segs=%d\n",
				sc.Name, r.WriteRate, r.ReadRate, r.Ratio, r.BytesPerPoint,
				float64(r.PeakHeap)/1e6, r.SegmentCount)
		})
	}
}

// TestBaselineSuite runs the full DefaultSuite. It is skipped unless BENCH_FULL=1
// because it takes minutes. The results are the same artifacts the CLI produces.
//
//	BENCH_FULL=1 go test ./doc/benchmark/ -run TestBaselineSuite -v
func TestBaselineSuite(t *testing.T) {
	if os.Getenv("BENCH_FULL") == "" {
		t.Skip("set BENCH_FULL=1 to run the full default suite")
	}
	for _, sc := range DefaultSuite() {
		sc := sc
		t.Run(sc.Name, func(t *testing.T) {
			r, err := RunScenario(sc, HarnessConfig{Label: "full"})
			if err != nil {
				t.Fatalf("scenario failed: %v", err)
			}
			if sc.Query == QueryFull && !r.Correct {
				t.Errorf("correct=false: read %d != written %d", r.ReadCount, r.Points)
			}
			t.Logf("%-40s write=%8.0f read=%8.0f ratio=%5.2f heap=%6.1fMB bytes/pt=%5.2f segs=%d\n",
				sc.Name, r.WriteRate, r.ReadRate, r.Ratio,
				float64(r.PeakHeap)/1e6, r.BytesPerPoint, r.SegmentCount)
		})
	}
}

// TestCompareGolden sanity-checks the compare tool on a synthetic old/new pair.
func TestCompareGolden(t *testing.T) {
	oldR := RunReport{Scenario: "s1", QueryMode: "full", WriteRate: 1000, ReadRate: 2000, Ratio: 10, PeakHeap: 100, BytesPerPoint: 2, Correct: true}
	newBetter := RunReport{Scenario: "s1", QueryMode: "full", WriteRate: 2000, ReadRate: 4000, Ratio: 12, PeakHeap: 50, BytesPerPoint: 1.6, Correct: true}
	newWorse := RunReport{Scenario: "s1", QueryMode: "full", WriteRate: 100, ReadRate: 200, Ratio: 8, PeakHeap: 500, BytesPerPoint: 3, Correct: true}
	newWrong := RunReport{Scenario: "s1", QueryMode: "full", WriteRate: 1000, ReadRate: 2000, Ratio: 10, PeakHeap: 100, BytesPerPoint: 2, Correct: false}
	newWindow := RunReport{Scenario: "s1", QueryMode: "window", WriteRate: 1000, ReadRate: 2000, Ratio: 10, PeakHeap: 100, BytesPerPoint: 2, Correct: false}

	if d := Compare([]RunReport{oldR}, []RunReport{newBetter}, DefaultThresholds())[0]; !d.Pass {
		t.Errorf("expected PASS for strictly better, got %+v", d)
	}
	if d := Compare([]RunReport{oldR}, []RunReport{newWorse}, DefaultThresholds())[0]; d.Pass {
		t.Errorf("expected FAIL for >20%% regression, got %+v", d)
	}
	if d := Compare([]RunReport{oldR}, []RunReport{newWrong}, DefaultThresholds())[0]; d.Pass {
		t.Errorf("expected FAIL when Correct=false for a full scan, got %+v", d)
	}
	// Non-full-scan scenarios must not be gated on Correct.
	if d := Compare([]RunReport{oldR}, []RunReport{newWindow}, DefaultThresholds())[0]; !d.Pass {
		t.Errorf("expected PASS for window scenario despite Correct=false, got %+v", d)
	}
	// A scenario missing from the new set must be reported as FAIL.
	if d := Compare([]RunReport{oldR}, nil, DefaultThresholds())[0]; d.Pass {
		t.Errorf("expected FAIL for missing scenario, got %+v", d)
	}
}

// TestStallDetection verifies the disk-stall heuristic. Stalls are detected
// cross-run: a run whose write/read rate falls below half the batch median is
// dropped from the aggregated report, so a disk-busy episode affecting one
// repeat does not poison the comparison.
func TestStallDetection(t *testing.T) {
	// Intra-run stall ratio is reported informationally only.
	if r := stallRatio(time.Second, 100, time.Second); r < 50 {
		t.Errorf("expected stall ratio >= 50 for a 100x outlier, got %v", r)
	}
	if r := stallRatio(100*time.Millisecond, 100, time.Second); r >= 50 {
		t.Errorf("expected ratio < 50 for a 10x outlier, got %v", r)
	}
	if r := stallRatio(time.Second, 2, time.Second); r != 0 {
		t.Errorf("expected ratio 0 for <4 calls, got %v", r)
	}

	fast := RunReport{Scenario: "s1", WriteRate: 1000, ReadRate: 2000, Ratio: 10, PeakHeap: 100, Correct: true}
	preFlagged := fast
	preFlagged.WriteRate = 100 // already marked stalled (e.g. detected elsewhere)
	preFlagged.DiskStalled = true
	statSlow := fast
	statSlow.WriteRate = 400 // 2.5x slower than the 1000 median -> auto-flagged

	// A pre-flagged run is dropped; median comes from the clean runs.
	med := MedianReports([]RunReport{fast, fast, preFlagged})
	if med.DiskStalled {
		t.Errorf("expected clean median, got DiskStalled=true")
	}
	if med.StalledRuns != 1 {
		t.Errorf("expected StalledRuns=1, got %d", med.StalledRuns)
	}
	if med.WriteRate != fast.WriteRate {
		t.Errorf("expected median WriteRate=%v (clean), got %v", fast.WriteRate, med.WriteRate)
	}

	// A statistically-slow run is auto-marked and dropped.
	med2 := MedianReports([]RunReport{fast, fast, statSlow})
	if med2.WriteRate != fast.WriteRate {
		t.Errorf("expected median WriteRate=%v after dropping outlier, got %v", fast.WriteRate, med2.WriteRate)
	}
	if med2.StalledRuns != 1 {
		t.Errorf("expected StalledRuns=1, got %d", med2.StalledRuns)
	}

	// No outliers in a consistent batch.
	med3 := MedianReports([]RunReport{fast, fast, fast})
	if med3.DiskStalled || med3.StalledRuns != 0 {
		t.Errorf("expected clean median with 0 stalled, got DiskStalled=%v StalledRuns=%d", med3.DiskStalled, med3.StalledRuns)
	}

	// Every run contaminated: flag the result as unreliable.
	allSlow := []RunReport{preFlagged, preFlagged, preFlagged}
	med4 := MedianReports(allSlow)
	if !med4.DiskStalled {
		t.Errorf("expected DiskStalled=true when every run is contaminated")
	}
	if med4.StalledRuns != 3 {
		t.Errorf("expected StalledRuns=3, got %d", med4.StalledRuns)
	}
}

// formatReport is a compact one-line render of a report for logs/CLI.
func formatReport(r *RunReport) string {
	return fmt.Sprintf(
		"write=%8.0f pts/s (%6.2f MB/s) read=%8.0f pts/s ratio=%5.2f bytes/pt=%5.2f peakHeap=%6.1fMB heapAfterGC=%6.1fMB segs=%d onDisk=%d",
		r.WriteRate, r.WriteMBps, r.ReadRate, r.Ratio, r.BytesPerPoint,
		float64(r.PeakHeap)/1e6, float64(r.HeapAfterGC)/1e6, r.SegmentCount, r.OnDiskBytes)
}
