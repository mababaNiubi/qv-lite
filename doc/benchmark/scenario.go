package benchmark

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/mababaNiubi/variant"
)

// ValueType selects the value generator for a scenario.
type ValueType int

const (
	// FloatSlow: slowly-varying temperature-like values (highly compressible).
	FloatSlow ValueType = iota
	// FloatWave: sinusoidal float (medium compressibility).
	FloatWave
	// FloatRandomWalk: gaussian random walk (low compressibility).
	FloatRandomWalk
	// IntCounter: monotonic counter (RLE-friendly).
	IntCounter
	// IntRandom: uniform random int (incompressible).
	IntRandom
	// StringLowCard: rotates among 32 distinct values (dictionary-friendly).
	StringLowCard
	// StringHighEntropy: 32-char random hex (incompressible lower bound).
	StringHighEntropy
	// BoolMostlyTrue: 90% true, 10% false (RLE-friendly).
	BoolMostlyTrue
	// StructTwoCol: {name string, value float64} map rows.
	StructTwoCol
)

func (t ValueType) String() string {
	switch t {
	case FloatSlow:
		return "float-slow"
	case FloatWave:
		return "float-wave"
	case FloatRandomWalk:
		return "float-randomwalk"
	case IntCounter:
		return "int-counter"
	case IntRandom:
		return "int-random"
	case StringLowCard:
		return "string-lowcard"
	case StringHighEntropy:
		return "string-he"
	case BoolMostlyTrue:
		return "bool-mostlytrue"
	case StructTwoCol:
		return "struct-2col"
	}
	return "unknown"
}

// TimestampModel selects how timestamps are spread.
type TimestampModel int

const (
	TSRegular TimestampModel = iota
	TSBursty
	TSJitter
)

func (m TimestampModel) String() string {
	switch m {
	case TSRegular:
		return "regular"
	case TSBursty:
		return "bursty"
	case TSJitter:
		return "jitter"
	}
	return "unknown"
}

// QueryMode selects the read workload after the write phase.
type QueryMode int

const (
	// QueryNone: no read phase (write-only scenario).
	QueryNone QueryMode = iota
	// QueryFull: QueryAll over each tag's full range (reads every written point).
	QueryFull
	// QueryWindow: QueryWindow aggregation over the full range.
	QueryWindow
	// QueryRecent: QueryAll over the last 1% of the time span (dashboard-style).
	QueryRecent
)

func (m QueryMode) String() string {
	switch m {
	case QueryNone:
		return "none"
	case QueryFull:
		return "full"
	case QueryWindow:
		return "window"
	case QueryRecent:
		return "recent"
	}
	return "unknown"
}

// Scenario fully describes one benchmark run. It is deterministic: the same
// Scenario (same Seed) generates the exact same point sequence every run, so
// old/new comparisons are fair.
type Scenario struct {
	Name       string
	Points     int // total points written
	Cardinality int // number of distinct tags
	ValueType  ValueType
	TimestampModel TimestampModel

	Step       int64 // ns between regular points; 0 => 1ms
	BatchSize  int   // 0/1 => single Write; >1 => WriteBatch
	WalSize    int64 // tsdb.WalConfig.MaxFileSize bytes (smaller => more flushes); 0 => default
	// CompressionName is the block-level codec: "zstd" (default), "lz4",
	// "snappy", "gzip", "none". Empty => tsdb default (zstd).
	CompressionName string
	AsyncFlush      bool // tsdb.Config.AsyncFlush
	MaxProcs   int   // runtime.GOMAXPROCS during run; 0 => unchanged
	MemoryBudget int64 // debug.SetMemoryLimit bytes; 0 => unchanged (no limit)

	Query          QueryMode
	QueryTags      int   // number of tags to query; 0 => all
	WindowSize     int64 // QueryWindow window in ns; 0 => 1 minute
	FloatPrecision uint8 // table float precision; 0 => 4

	Seed int64 // 0 => 42
}

func (s *Scenario) step() int64 {
	if s.Step > 0 {
		return s.Step
	}
	return int64(time.Millisecond)
}

func (s *Scenario) span() int64 {
	return int64(s.Points) * s.step()
}

func (s *Scenario) cardinality() int {
	if s.Cardinality < 1 {
		return 1
	}
	return s.Cardinality
}

// Generator produces the point sequence for a Scenario. Point generation is
// excluded from the timed write loop, so it does not distort write throughput.
type Generator struct {
	s    *Scenario
	rng  *rand.Rand
	tags []string
	base int64
	i    int
	raw  int64 // cumulative raw input bytes (12-byte header + variant binary)
}

// NewGenerator returns a deterministic generator for s.
func NewGenerator(s *Scenario) *Generator {
	seed := s.Seed
	if seed == 0 {
		seed = 42
	}
	n := s.cardinality()
	tags := make([]string, n)
	for i := range tags {
		tags[i] = fmt.Sprintf("tag_%06d", i)
	}
	return &Generator{
		s:    s,
		rng:  rand.New(rand.NewSource(seed)),
		tags: tags,
		base: time.Now().UnixNano(),
	}
}

// Start is the base timestamp of the generated range.
func (g *Generator) Start() int64 { return g.base }

// End is the exclusive end of the generated range.
func (g *Generator) End() int64 { return g.base + g.s.span() }

// Remaining reports how many points have not yet been generated.
func (g *Generator) Remaining() int { return g.s.Points - g.i }

// rawInput returns the cumulative raw input bytes generated so far.
func (g *Generator) rawInput() int64 { return g.raw }

// QueryTags returns the subset of tags the read phase will query.
func (g *Generator) QueryTags(s *Scenario) []string {
	n := s.cardinality()
	if s.QueryTags > 0 && s.QueryTags < n {
		n = s.QueryTags
	}
	return g.tags[:n]
}

// Next returns (tag, timestamp, value, rawInputBytes). rawInputBytes is the
// on-disk size of the point in a naive store (12-byte WAL header + variant
// binary), used as the denominator for the compression ratio.
func (g *Generator) Next() (string, int64, variant.Variant, int64) {
	i := g.i
	g.i++
	tag := g.tags[i%len(g.tags)]
	ts := g.nextTS(i)
	v := g.nextValue(tag, i)
	raw := rawPointSize(v)
	g.raw += raw
	return tag, ts, v, raw
}

func (g *Generator) nextTS(i int) int64 {
	step := g.s.step()
	switch g.s.TimestampModel {
	case TSBursty:
		const burst = 64
		k := i / burst
		m := i % burst
		return g.base + int64(k)*step*8 + int64(m)*step
	case TSJitter:
		return g.base + int64(i)*step + int64(g.rng.Int63n(step/4))
	default:
		return g.base + int64(i)*step
	}
}

func (g *Generator) nextValue(tag string, i int) variant.Variant {
	switch g.s.ValueType {
	case FloatSlow:
		// Slow-moving temperature: 20..20.99 drifting with the point index.
		return variant.NewFloat64(20 + float64(i%1000)/100)
	case FloatWave:
		return variant.NewFloat64(50 + 40*math.Sin(float64(i)*0.01))
	case FloatRandomWalk:
		return variant.NewFloat64(50 + g.rng.NormFloat64()*5)
	case IntCounter:
		return variant.NewInt64(int64(i) * 7)
	case IntRandom:
		return variant.NewInt64(g.rng.Int63n(100000))
	case StringLowCard:
		return variant.NewString(fmt.Sprintf("status_%d", g.rng.Intn(32)))
	case StringHighEntropy:
		return variant.NewString(randomHex(g.rng, 32))
	case BoolMostlyTrue:
		return variant.NewBool(g.rng.Intn(100) < 90)
	case StructTwoCol:
		return variant.New(map[string]any{
			"name":  fmt.Sprintf("sensor_%d", i%100),
			"value": float64(i) * 0.5,
		})
	default:
		return variant.NewFloat64(float64(i))
	}
}

// rawPointSize estimates the naive on-disk bytes for a point: 12-byte WAL
// header (key 4 + timestamp 8) + the variant binary payload.
func rawPointSize(v variant.Variant) int64 {
	return int64(12) + int64(len(v.AppendBinary(nil)))
}

func randomHex(rng *rand.Rand, n int) string {
	const hex = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = hex[rng.Intn(len(hex))]
	}
	return string(b)
}
