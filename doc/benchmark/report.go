package benchmark

import (
	"sort"
	"time"
)

// defaultOutlierRegress is the fraction by which a run's write/read rate must
// fall below the batch median to be treated as a disk-stall outlier and
// dropped. 0.5 means a run at less than half the median rate is dropped.
// Legitimate work (e.g. an inline flush) affects every repeat equally and so
// does not create outliers; only a run hit by a disk-busy episode is.
const defaultOutlierRegress = 0.5

// MedianReports aggregates several runs of the same scenario into a single
// report using the median of each numeric field. Runs whose write/read rate
// falls far below the batch median (a disk-busy episode affecting one repeat)
// are marked as stalled and dropped, if at least one clean run exists.
// Otherwise all runs are used and the result is flagged as contaminated.
func MedianReports(rs []RunReport) RunReport {
	if len(rs) == 0 {
		return RunReport{}
	}
	if len(rs) == 1 {
		return rs[0]
	}
	marked := markOutlierRuns(rs, defaultOutlierRegress)
	total := len(marked)
	clean := make([]RunReport, 0, total)
	for _, r := range marked {
		if !r.DiskStalled {
			clean = append(clean, r)
		}
	}
	stalled := total - len(clean)
	if len(clean) == 0 {
		clean = marked // all contaminated; median of everything, flagged below
	}
	out := medianOf(clean)
	out.StalledRuns = stalled
	// A result is clean only when built from clean runs; if every run was
	// contaminated, flag it so the comparison treats it as unreliable.
	out.DiskStalled = stalled > 0 && stalled == total
	return out
}

// markOutlierRuns flags any run whose write or read rate is less than
// (1-maxRegress) of the batch median for that metric. Runs already flagged are
// left untouched. Needs at least 3 runs to call something an outlier.
func markOutlierRuns(rs []RunReport, maxRegress float64) []RunReport {
	if len(rs) < 3 {
		return rs
	}
	out := append([]RunReport(nil), rs...)
	medW := medianF(pluckF(rs, func(r RunReport) float64 { return r.WriteRate }))
	medR := medianF(pluckF(rs, func(r RunReport) float64 { return r.ReadRate }))
	for i := range out {
		if out[i].DiskStalled {
			continue
		}
		if medW > 0 && out[i].WriteRate > 0 && out[i].WriteRate < medW*(1-maxRegress) {
			out[i].DiskStalled = true
			continue
		}
		if medR > 0 && out[i].ReadRate > 0 && out[i].ReadRate < medR*(1-maxRegress) {
			out[i].DiskStalled = true
		}
	}
	return out
}

func medianOf(rs []RunReport) RunReport {
	out := rs[0]
	out.PeakHeap = medianU64(pluckU64(rs, func(r RunReport) uint64 { return r.PeakHeap }))
	out.HeapAfterGC = medianU64(pluckU64(rs, func(r RunReport) uint64 { return r.HeapAfterGC }))
	out.TotalAlloc = medianU64(pluckU64(rs, func(r RunReport) uint64 { return r.TotalAlloc }))
	out.NumGC = uint32(medianInt(pluckInt(rs, func(r RunReport) int { return int(r.NumGC) })))
	out.Elapsed = medianDur(pluckDur(rs, func(r RunReport) time.Duration { return r.Elapsed }))
	out.WriteDur = medianDur(pluckDur(rs, func(r RunReport) time.Duration { return r.WriteDur }))
	out.WriteRate = medianF(pluckF(rs, func(r RunReport) float64 { return r.WriteRate }))
	out.WriteMBps = medianF(pluckF(rs, func(r RunReport) float64 { return r.WriteMBps }))
	out.WriteCount = medianInt(pluckInt(rs, func(r RunReport) int { return r.WriteCount }))
	out.ReadDur = medianDur(pluckDur(rs, func(r RunReport) time.Duration { return r.ReadDur }))
	out.ReadRate = medianF(pluckF(rs, func(r RunReport) float64 { return r.ReadRate }))
	out.ReadCount = medianInt(pluckInt(rs, func(r RunReport) int { return r.ReadCount }))
	out.RawInputBytes = medianInt64(pluckI64(rs, func(r RunReport) int64 { return r.RawInputBytes }))
	out.OnDiskBytes = medianInt64(pluckI64(rs, func(r RunReport) int64 { return r.OnDiskBytes }))
	out.Ratio = medianF(pluckF(rs, func(r RunReport) float64 { return r.Ratio }))
	out.BytesPerPoint = medianF(pluckF(rs, func(r RunReport) float64 { return r.BytesPerPoint }))
	out.SegmentCount = medianInt(pluckInt(rs, func(r RunReport) int { return r.SegmentCount }))
	out.SegmentBytes = medianInt64(pluckI64(rs, func(r RunReport) int64 { return r.SegmentBytes }))
	out.IndexBytes = medianInt64(pluckI64(rs, func(r RunReport) int64 { return r.IndexBytes }))
	out.WalBytes = medianInt64(pluckI64(rs, func(r RunReport) int64 { return r.WalBytes }))
	// Correctness: require every repeated run to pass the self check.
	out.Correct = true
	for _, r := range rs {
		if !r.Correct {
			out.Correct = false
			break
		}
	}
	return out
}

func pluckF(rs []RunReport, f func(RunReport) float64) []float64 {
	out := make([]float64, len(rs))
	for i, r := range rs {
		out[i] = f(r)
	}
	return out
}

func pluckInt(rs []RunReport, f func(RunReport) int) []int {
	out := make([]int, len(rs))
	for i, r := range rs {
		out[i] = f(r)
	}
	return out
}

func pluckI64(rs []RunReport, f func(RunReport) int64) []int64 {
	out := make([]int64, len(rs))
	for i, r := range rs {
		out[i] = f(r)
	}
	return out
}

func pluckU64(rs []RunReport, f func(RunReport) uint64) []uint64 {
	out := make([]uint64, len(rs))
	for i, r := range rs {
		out[i] = f(r)
	}
	return out
}

func pluckDur(rs []RunReport, f func(RunReport) time.Duration) []time.Duration {
	out := make([]time.Duration, len(rs))
	for i, r := range rs {
		out[i] = f(r)
	}
	return out
}

func medianF(vs []float64) float64 {
	s := append([]float64(nil), vs...)
	sort.Float64s(s)
	return s[len(s)/2]
}

func medianInt(vs []int) int {
	s := append([]int(nil), vs...)
	sort.Ints(s)
	return s[len(s)/2]
}

func medianInt64(vs []int64) int64 {
	s := append([]int64(nil), vs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}

func medianU64(vs []uint64) uint64 {
	s := append([]uint64(nil), vs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}

func medianDur(vs []time.Duration) time.Duration {
	s := append([]time.Duration(nil), vs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}
