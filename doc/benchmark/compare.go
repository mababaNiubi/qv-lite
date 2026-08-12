package benchmark

import (
	"fmt"
	"math"
	"sort"
)

// Thresholds controls the PASS/FAIL verdict in Compare. Each value is the
// maximum allowed fractional regression (e.g. 0.20 allows new >= old * 0.80).
// Set a threshold to 0 to forbid any regression for that metric.
type Thresholds struct {
	WriteRateMaxRegress float64 // write throughput (pts/s)
	ReadRateMaxRegress  float64 // read throughput (pts/s)
	RatioMaxRegress     float64 // compression ratio (raw / on_disk)
	HeapMaxRegress      float64 // peak heap bytes
}

// DefaultThresholds allows 20% regression on every metric.
func DefaultThresholds() Thresholds {
	return Thresholds{
		WriteRateMaxRegress: 0.20,
		ReadRateMaxRegress:  0.20,
		RatioMaxRegress:     0.20,
		HeapMaxRegress:      0.20,
	}
}

// Metric is one compared quantity.
type Metric struct {
	Name       string  `json:"name"`
	Old        float64 `json:"old"`
	New        float64 `json:"new"`
	ChangePct  float64 `json:"change_pct"` // (new-old)/old * 100
	MaxRegress float64 `json:"max_regress"`
	LowerBetter bool   `json:"lower_better"` // true: smaller value is better (heap, bytes/pt)
	Pass       bool    `json:"pass"`
}

// Diff is one scenario's comparison result.
type Diff struct {
	Scenario string   `json:"scenario"`
	Correct  bool     `json:"correct"` // new run passed the read-correctness self check
	Metrics  []Metric `json:"metrics"`
	Pass     bool     `json:"pass"`
}

// Compare diffs an old (baseline) and a new report set by scenario name. A
// scenario present in only one set is reported with Pass=false.
func Compare(old, nw []RunReport, th Thresholds) []Diff {
	byName := func(rs []RunReport) map[string]RunReport {
		m := make(map[string]RunReport, len(rs))
		for _, r := range rs {
			m[r.Scenario] = r
		}
		return m
	}
	oldM, newM := byName(old), byName(nw)

	names := make(map[string]bool, len(oldM)+len(newM))
	for n := range oldM {
		names[n] = true
	}
	for n := range newM {
		names[n] = true
	}
	out := make([]Diff, 0, len(names))
	for name := range names {
		o, okO := oldM[name]
		n, okN := newM[name]
		d := Diff{Scenario: name}
		if !okO || !okN {
			d.Pass = false
			out = append(out, d)
			continue
		}
		d.Correct = n.Correct
		d.Metrics = []Metric{
			metric("write_rate_pts_per_sec", o.WriteRate, n.WriteRate, th.WriteRateMaxRegress, false),
			metric("read_rate_pts_per_sec", o.ReadRate, n.ReadRate, th.ReadRateMaxRegress, false),
			metric("compression_ratio", o.Ratio, n.Ratio, th.RatioMaxRegress, false),
			metric("peak_heap_bytes", float64(o.PeakHeap), float64(n.PeakHeap), th.HeapMaxRegress, true),
			// On-disk bytes include WAL residue that varies a few % between
			// runs, so allow 10% growth; real compression regressions show up
			// in the ratio metric at its own threshold.
			metric("bytes_per_point", o.BytesPerPoint, n.BytesPerPoint, 0.10, true),
		}
		// Correctness only gates PASS for full-scan scenarios where a
		// read-back count is defined; QueryWindow/Recent/None have no such check.
		d.Pass = true
		if n.QueryMode == "full" && !n.Correct {
			d.Pass = false
		}
		for _, m := range d.Metrics {
			if !m.Pass {
				d.Pass = false
			}
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Scenario < out[j].Scenario })
	return out
}

func metric(name string, o, n, maxRegress float64, lowerBetter bool) Metric {
	m := Metric{Name: name, Old: o, New: n, MaxRegress: maxRegress, LowerBetter: lowerBetter}
	if o > 0 {
		m.ChangePct = (n - o) / o * 100
	}
	if lowerBetter {
		// Lower is better: pass unless the new value grew beyond the threshold.
		m.Pass = n <= o*(1+maxRegress)
	} else {
		// Higher is better: pass unless the new value dropped beyond the threshold.
		m.Pass = n >= o*(1-maxRegress)
	}
	return m
}

// MetricByName returns the metric with the given name, or nil.
func (d *Diff) MetricByName(name string) *Metric {
	for i := range d.Metrics {
		if d.Metrics[i].Name == name {
			return &d.Metrics[i]
		}
	}
	return nil
}

func pct(m Metric) string {
	if math.IsNaN(m.ChangePct) || math.IsInf(m.ChangePct, 0) {
		return "  n/a "
	}
	if m.Old == 0 {
		return "  n/a "
	}
	return fmt.Sprintf("%+6.1f%%", m.ChangePct)
}

func fmtF(v float64) string {
	if v >= 1e6 {
		return fmt.Sprintf("%10.0f", v)
	}
	return fmt.Sprintf("%10.2f", v)
}
