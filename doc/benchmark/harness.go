package benchmark

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

const defaultTable = "bench"

// HarnessConfig configures one RunScenario invocation.
type HarnessConfig struct {
	Label   string // run label, e.g. "baseline", "v2-m1"
	Version string // implementation version tag, e.g. "head", "m1"
	KeepDir bool   // keep the temp data dir for post-mortem inspection
}

// RunReport is the structured result of one scenario execution (JSON-safe).
type RunReport struct {
	Label        string        `json:"label"`
	Version      string        `json:"version"`
	Scenario     string        `json:"scenario"`
	Started      time.Time     `json:"started"`
	Elapsed      time.Duration `json:"elapsed_ns"`

	Points      int `json:"points"`
	Cardinality int `json:"cardinality"`
	ValueType   string `json:"value_type"`
	TimestampModel string `json:"timestamp_model"`
	BatchSize   int  `json:"batch_size"`
	AsyncFlush  bool `json:"async_flush"`
	QueryMode   string `json:"query_mode"`

	// Write phase
	WriteDur   time.Duration `json:"write_dur_ns"`
	WriteRate  float64       `json:"write_rate_pts_per_sec"` // points / pure write-call time
	WriteMBps  float64       `json:"write_rate_mb_per_sec"`  // raw input bytes / write time
	WriteCount int           `json:"write_count"`            // points actually accepted by the DB

	// Read phase
	ReadDur   time.Duration `json:"read_dur_ns"`
	ReadRate  float64       `json:"read_rate_pts_per_sec"`
	ReadCount int           `json:"read_count"`
	Correct   bool          `json:"correct"` // QueryFull && ReadCount == Points

	// Memory
	PeakHeap    uint64 `json:"peak_heap_bytes"`
	HeapAfterGC uint64 `json:"heap_after_gc_bytes"`
	TotalAlloc  uint64 `json:"total_alloc_bytes"`
	NumGC       uint32 `json:"num_gc"`

	// Compression / disk
	RawInputBytes int64   `json:"raw_input_bytes"`
	OnDiskBytes   int64   `json:"on_disk_bytes"`
	Ratio         float64 `json:"compression_ratio"` // raw / on_disk
	BytesPerPoint float64 `json:"bytes_per_point"`   // on_disk / points
	SegmentCount  int     `json:"segment_count"`
	SegmentBytes  int64   `json:"segment_bytes"`
	IndexBytes    int64   `json:"index_bytes"`
	WalBytes      int64   `json:"wal_bytes"`

	// Applied runtime limits
	AppliedMaxProcs  int   `json:"applied_max_procs"`
	AppliedMemBudget int64 `json:"applied_mem_budget_bytes"`

	// Disk-stall detection. A disk-busy episode inflates one call far above the
	// typical call; stall_ratio = max_call / avg_call. DiskStalled marks a run
	// whose worst write or read call looks like a disk stall.
	MaxWriteCall  time.Duration `json:"max_write_call_ns"`
	WriteCalls    int64         `json:"write_calls"`
	MaxReadCall   time.Duration `json:"max_read_call_ns"`
	ReadCalls     int64         `json:"read_calls"`
	WriteStallRatio float64     `json:"write_stall_ratio"`
	ReadStallRatio  float64     `json:"read_stall_ratio"`
	DiskStalled   bool          `json:"disk_stalled"`
	StalledRuns   int           `json:"stalled_runs"` // set by MedianReports: runs excluded/contaminated

	DataDir string `json:"data_dir,omitempty"` // populated when KeepDir
}

// RunScenario executes a scenario against the tsdb public API and returns a
// measured Report. It is implementation-agnostic: the same code measures the
// current engine and any future rewrite, as long as the public API is stable.
func RunScenario(s Scenario, hc HarnessConfig) (*RunReport, error) {
	r := &RunReport{
		Label:        hc.Label,
		Version:      hc.Version,
		Scenario:     s.Name,
		Started:      time.Now(),
		Points:       s.Points,
		Cardinality:  s.cardinality(),
		ValueType:    s.ValueType.String(),
		TimestampModel: s.TimestampModel.String(),
		BatchSize:    s.BatchSize,
		AsyncFlush:   s.AsyncFlush,
		QueryMode:    s.Query.String(),
	}

	// Apply runtime limits (restore afterwards).
	restoreProcs := applyMaxProcs(r, s.MaxProcs)
	restoreBudget := applyMemBudget(r, s.MemoryBudget)
	defer restoreProcs()
	defer restoreBudget()

	// Temp data dir.
	dir, err := os.MkdirTemp("", "qvbench_*")
	if err != nil {
		return nil, err
	}
	if hc.KeepDir {
		r.DataDir = dir
	} else {
		defer os.RemoveAll(dir)
	}

	// Peak-heap sampler.
	var peak atomic.Uint64
	stopSampler := startHeapSampler(&peak)
	defer stopSampler()

	db, err := tsdb.Open(tsdb.Config{
		Path: dir,
		WalConfig: tsdb.WalConfig{
			MaxFileSize: s.WalSize,
		},
		MaxStorageTime:         100 * 365 * 24 * 3600, // ~100 years, never reject
		AsyncFlush:             s.AsyncFlush,
		SecondaryCompressionName: s.CompressionName,
	}, context.Background())
	if err != nil {
		return nil, err
	}

	if err := db.CreateTable(tableInfo(s)); err != nil {
		db.Close()
		return nil, err
	}

	// ── Write phase (generation excluded from timing) ──
	gen := NewGenerator(&s)
	writeDur, written, maxWriteCall, writeCalls, err := runWrite(db, gen, &s)
	if err != nil {
		db.Close()
		return nil, err
	}
	r.WriteDur = writeDur
	r.WriteCount = written
	r.MaxWriteCall = maxWriteCall
	r.WriteCalls = writeCalls
	r.RawInputBytes = gen.rawInput()
	if writeDur > 0 {
		r.WriteRate = float64(written) / writeDur.Seconds()
		r.WriteMBps = float64(r.RawInputBytes) / (1 << 20) / writeDur.Seconds()
	}
	r.WriteStallRatio = stallRatio(maxWriteCall, writeCalls, writeDur)

	// ── Read phase (optional) ──
	readDur, readCount, maxReadCall, readCalls, err := runQuery(db, gen, &s)
	if err != nil {
		db.Close()
		return nil, err
	}
	r.ReadDur = readDur
	r.ReadCount = readCount
	r.MaxReadCall = maxReadCall
	r.ReadCalls = readCalls
	if readDur > 0 {
		r.ReadRate = float64(readCount) / readDur.Seconds()
	}
	r.ReadStallRatio = stallRatio(maxReadCall, readCalls, readDur)
	if s.Query == QueryFull {
		r.Correct = readCount == s.Points
	}
	// DiskStalled is NOT decided here: a single run cannot tell a legitimately
	// heavy call (e.g. an inline flush, which the current engine does per WAL
	// rotation) apart from a disk stall. Stalls are detected cross-run in
	// MedianReports, where a run whose write/read rate is far below the batch
	// median is marked and dropped.

	// ── Close (flushes all WAL to segments) then measure disk ──
	if err := db.Close(); err != nil {
		return nil, err
	}
	r.OnDiskBytes, r.SegmentBytes, r.IndexBytes, r.WalBytes, r.SegmentCount = measureDisk(dir, defaultTable)
	if r.OnDiskBytes > 0 {
		r.Ratio = float64(r.RawInputBytes) / float64(r.OnDiskBytes)
	}
	if s.Points > 0 {
		r.BytesPerPoint = float64(r.OnDiskBytes) / float64(s.Points)
	}

	// ── Memory after a forced GC (resident set with data on disk) ──
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	// Include the post-run snapshot so very short runs (where the 50ms sampler
	// may have fired only once) still report a non-zero peak.
	peakMax := peak.Load()
	if ms.HeapAlloc > peakMax {
		peakMax = ms.HeapAlloc
	}
	r.PeakHeap = peakMax
	r.HeapAfterGC = ms.HeapAlloc
	r.TotalAlloc = ms.TotalAlloc
	r.NumGC = ms.NumGC
	r.Elapsed = time.Since(r.Started)
	return r, nil
}

// runWrite executes the write phase, timing only the Write/WriteBatch calls.
// It also tracks the longest single call (a disk stall shows up as one call
// taking orders of magnitude longer than the typical call).
func runWrite(db *tsdb.DB, gen *Generator, s *Scenario) (dur time.Duration, written int, maxCall time.Duration, calls int64, err error) {
	if s.BatchSize > 1 {
		points := make([]tsdb.TagPoint, 0, s.BatchSize)
		for gen.Remaining() > 0 {
			points = points[:0]
			for len(points) < s.BatchSize && gen.Remaining() > 0 {
				tag, ts, v, _ := gen.Next()
				points = append(points, tsdb.TagPoint{Tag: tag, Timestamp: ts, Value: v})
			}
			st := time.Now()
			n, err := db.WriteBatch(defaultTable, points)
			d := time.Since(st)
			if d > maxCall {
				maxCall = d
			}
			dur += d
			calls++
			if err != nil {
				return dur, written, maxCall, calls, err
			}
			written += n
		}
		return dur, written, maxCall, calls, nil
	}
	for gen.Remaining() > 0 {
		tag, ts, v, _ := gen.Next()
		st := time.Now()
		ok, err := db.Write(defaultTable, tag, ts, v)
		d := time.Since(st)
		if d > maxCall {
			maxCall = d
		}
		dur += d
		calls++
		if err != nil {
			return dur, written, maxCall, calls, err
		}
		if ok {
			written++
		}
	}
	return dur, written, maxCall, calls, nil
}

// runQuery executes the read phase for the configured QueryMode.
func runQuery(db *tsdb.DB, gen *Generator, s *Scenario) (dur time.Duration, total int, maxCall time.Duration, calls int64, err error) {
	if s.Query == QueryNone {
		return 0, 0, 0, 0, nil
	}
	tags := gen.QueryTags(s)
	start, end := gen.Start(), gen.End()
	query := func(tag string) (int, error) {
		switch s.Query {
		case QueryFull:
			pts, err := db.QueryAll(defaultTable, tag, start, end, nil)
			return len(pts), err
		case QueryRecent:
			recentStart := end - int64(float64(end-start)*0.01)
			pts, err := db.QueryAll(defaultTable, tag, recentStart, end, nil)
			return len(pts), err
		case QueryWindow:
			ws := s.WindowSize
			if ws <= 0 {
				ws = int64(time.Minute)
			}
			pts, err := db.Query(defaultTable, tag, start, end, ws, uint8(tsdb.AvgFusion), nil)
			return len(pts), err
		}
		return 0, nil
	}
	for _, tag := range tags {
		st := time.Now()
		n, err := query(tag)
		d := time.Since(st)
		if d > maxCall {
			maxCall = d
		}
		dur += d
		calls++
		if err != nil {
			return dur, total, maxCall, calls, err
		}
		total += n
	}
	return dur, total, maxCall, calls, nil
}

// stallRatio returns max_call / avg_call, or 0 when the ratio is meaningless
// (no calls, or fewer than minCallSamples calls so the average is unstable).
func stallRatio(maxCall time.Duration, calls int64, total time.Duration) float64 {
	const minCallSamples = 4
	if maxCall <= 0 || calls < minCallSamples || total <= 0 {
		return 0
	}
	avg := total / time.Duration(calls)
	if avg <= 0 {
		return 0
	}
	return float64(maxCall) / float64(avg)
}

// tableInfo maps a Scenario's ValueType to the tsdb table schema. Scalar types
// use ColumnTypeUnknown (auto-adapting encoder); struct uses a fixed schema.
func tableInfo(s Scenario) tsdb.TableInfo {
	ca := tsdb.ColumnAttribute{Name: defaultTable}
	if s.ValueType == StructTwoCol {
		ca.Type = tsdb.ColumnTypeStructure
		ca.Structure = []tsdb.ColumnAttribute{
			{Name: "name", Type: tsdb.ColumnTypeString},
			{Name: "value", Type: tsdb.ColumnTypeFloat},
		}
		return tsdb.TableInfo{ColumnAttribute: ca}
	}
	ca.Type = tsdb.ColumnTypeUnknown
	ca.FloatPrecision = s.FloatPrecision
	if ca.FloatPrecision == 0 {
		ca.FloatPrecision = 4
	}
	return tsdb.TableInfo{ColumnAttribute: ca}
}

// measureDisk walks the table dir summing on-disk bytes and file counts.
func measureDisk(dir, table string) (onDisk, segmentBytes, indexBytes, walBytes int64, segCount int) {
	tableDir := filepath.Join(dir, table)
	_ = filepath.WalkDir(tableDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		onDisk += info.Size()
		switch {
		case strings.HasSuffix(path, ".tsb"):
			segCount++
			segmentBytes += info.Size()
		case strings.HasSuffix(path, ".idx"):
			indexBytes += info.Size()
		case strings.HasSuffix(path, ".wal"):
			walBytes += info.Size()
		}
		return nil
	})
	return
}

// startHeapSampler polls HeapAlloc every 50ms and keeps the observed maximum.
func startHeapSampler(peak *atomic.Uint64) func() {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-done:
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				if a := ms.HeapAlloc; a > peak.Load() {
					peak.Store(a)
				}
			}
		}
	}()
	return func() { close(done) }
}

// applyMaxProcs sets GOMAXPROCS for the run and returns a restore func.
func applyMaxProcs(r *RunReport, n int) func() {
	if n <= 0 {
		r.AppliedMaxProcs = runtime.GOMAXPROCS(0)
		return func() {}
	}
	prev := runtime.GOMAXPROCS(n)
	r.AppliedMaxProcs = n
	return func() { runtime.GOMAXPROCS(prev) }
}

// applyMemBudget sets the Go memory limit (GOMEMLIMIT) for the run and returns
// a restore func. 0 disables the limit, which is the default Go behavior.
func applyMemBudget(r *RunReport, budget int64) func() {
	prev := debug.SetMemoryLimit(budget)
	r.AppliedMemBudget = budget
	return func() { debug.SetMemoryLimit(prev) }
}
