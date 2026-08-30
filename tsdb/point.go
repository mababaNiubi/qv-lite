package tsdb

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/mababaNiubi/variant"
)

const (
	AvgFusion = iota
	MinFusion
	MaxFusion
)

type ColumnType uint8

const (
	ColumnTypeUnknown ColumnType = iota
	ColumnTypeInt
	ColumnTypeFloat
	ColumnTypeString
	ColumnTypeBool
	ColumnTypeJson
	ColumnTypeStructure
)

type ColumnAttribute struct {
	Name           string            `json:"name"`
	Desc           string            `json:"desc"`
	Type           ColumnType        `json:"type"`
	FloatPrecision uint8             `json:"float_precision"`
	Structure      []ColumnAttribute `json:"structure"`
}

type TableInfo struct {
	ColumnAttribute `json:"column_attribute"`
}

const pointChunkSize = 4096

// TagPoint is a single data point in a batch write.
type TagPoint struct {
	Tag       string
	Timestamp int64
	Value     variant.Variant
}

type Point struct {
	Tms int64
	V   variant.Variant
}

// pointCollector accumulates Points in fixed-size chunks to avoid slice
// reallocation copies during large reads. Each chunk is 4096 points (~64KB).
type pointCollector struct {
	chunks [][]Point
	total  int
}

// pointChunkPool reuses query result chunks with a hard byte cap. A plain
// sync.Pool would keep every chunk a huge query ever pooled until two GC
// cycles run; with no allocation pressure (idle server) that pins the full
// result backing arrays in memory indefinitely. The capped pool drops chunks
// beyond the cap at put time, so retention is deterministic regardless of GC
// timing. A sharded variant keeps the retention semantics while spreading
// concurrent queries across per-shard mutexes (a single global mutex showed
// up as a contention point in concurrent materialized benchmarks; sync.Pool
// was tried instead but its GC-driven clearing destroyed chunk reuse and
// measurably increased allocation under load).
var pointChunkPool = shardedChunkPool{shards: 16, maxBytesPerShard: (64 << 20) / 16}

type cappedChunkPool struct {
	mu       sync.Mutex
	free     [][]Point
	bytes    int
	maxBytes int
}

func (p *cappedChunkPool) get() []Point {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.free); n > 0 {
		c := p.free[n-1]
		p.free = p.free[:n-1]
		p.bytes -= cap(c) * int(unsafe.Sizeof(Point{}))
		return c
	}
	return make([]Point, 0, pointChunkSize)
}

func (p *cappedChunkPool) put(c []Point) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bytes+cap(c)*int(unsafe.Sizeof(Point{})) > p.maxBytes {
		return // drop oversized/excess chunks; GC reclaims them
	}
	p.free = append(p.free, c)
	p.bytes += cap(c) * int(unsafe.Sizeof(Point{}))
}

// shardedChunkPool routes chunk get/put across per-shard capped pools with a
// lock-free round-robin counter, so concurrent queries rarely contend on the
// same shard mutex while total retention stays bounded (shards × per-shard
// cap).
type shardedChunkPool struct {
	once             sync.Once
	shards           int
	maxBytesPerShard int
	shardCursor      atomic.Uint64
	pools            []cappedChunkPool
}

func (p *shardedChunkPool) get() []Point {
	p.ensure()
	i := p.shardCursor.Add(1) % uint64(p.shards)
	return p.pools[i].get()
}

func (p *shardedChunkPool) put(c []Point) {
	p.ensure()
	i := p.shardCursor.Add(1) % uint64(p.shards)
	p.pools[i].put(c)
}

func (p *shardedChunkPool) ensure() {
	// sync.Once: 并发首个查询同时触发初始化时，保证 pools 只被构造一次
	// 且构造结果对所有 goroutine 可见（无同步的懒初始化是 data race）。
	p.once.Do(func() {
		p.pools = make([]cappedChunkPool, p.shards)
		for i := range p.pools {
			p.pools[i].maxBytes = p.maxBytesPerShard
		}
	})
}

var (
	timeDecoderPool     = sync.Pool{New: func() any { return &TimeDecoder{} }}
	intDecoderPool      = sync.Pool{New: func() any { return &IntegerDecoder{} }}
	floatDecoderPool    = sync.Pool{New: func() any { return &FloatDecoder{} }}
	strDecoderPool      = sync.Pool{New: func() any { return &StringDecoder{} }}
	boolDecoderPool     = sync.Pool{New: func() any { return &BooleanDecoder{} }}
	jsonDecoderPool     = sync.Pool{New: func() any { return &JsonDecoder{} }}
	adaptColDecoderPool = sync.Pool{New: func() any { return &AdaptColumnDecoder{} }}
)

func (c *pointCollector) append(p Point) {
	n := len(c.chunks)
	if n == 0 || len(c.chunks[n-1]) >= pointChunkSize {
		c.chunks = append(c.chunks, pointChunkPool.get())
		n++
	}
	c.chunks[n-1] = append(c.chunks[n-1], p)
	c.total++
}

// result flattens all chunks into a single pre-allocated slice and returns
// chunk memory to the pool for reuse. The full backing array is zeroed before
// returning each chunk to the pool so the GC does not scan stale pointers
// inside variant.Variant values.
//
// A nearly-full single chunk (total > half the chunk size) is returned
// directly without the flatten copy: its backing array's tail is zeroed by
// the pool discipline (fresh make or cleared on put), so the extra capacity
// carries no stale pointers. Smaller results keep the flatten path so a
// 100-point query does not pin a 98KB chunk.
func (c *pointCollector) result() []Point {
	if c.total > pointChunkSize/2 && len(c.chunks) == 1 {
		out := c.chunks[0][:c.total]
		c.chunks = c.chunks[:0]
		c.total = 0
		return out
	}
	out := make([]Point, 0, c.total)
	for _, chunk := range c.chunks {
		out = append(out, chunk...)
		clear(chunk)
		chunk = chunk[:0]
		pointChunkPool.put(chunk)
	}
	c.chunks = c.chunks[:0]
	c.total = 0
	return out
}

type Segment struct {
	timeDecoder  *TimeDecoder
	valueDecoder Decoder
}

type PointPack interface {
	// Next advances to the next data point. Returns false if no more points are available.
	Next() bool
	// Read returns the current point's timestamp and value.
	Read() (int64, variant.Variant)

	Reset()
}

func GetAllPointByBytes(attribute []ColumnAttribute, compressedTimeData []byte, compressedValueData []byte) ([]Point, error) {
	points := make([]Point, 0, 256)
	var pack = NewPointDiskPack(attribute, 0, 0)
	defer pack.Reset()
	err := pack.AddSegment(compressedTimeData, compressedValueData)
	if err != nil {
		return points, err
	}
	for pack.Next() {
		tms, value := pack.Read()
		points = append(points, Point{
			Tms: tms,
			V:   value,
		})
	}
	return points, nil
}

type PointDiskPack struct {
	segments   []Segment
	currentIdx int

	attribute []ColumnAttribute

	startTime int64
	endTime   int64
}

func NewPointDiskPack(attribute []ColumnAttribute, startTime int64, endTime int64) *PointDiskPack {
	return &PointDiskPack{
		attribute: attribute,
		startTime: startTime,
		endTime:   endTime,
	}
}

func (p *PointDiskPack) Reset() {
	for i := range p.segments {
		seg := &p.segments[i]
		timeDecoderPool.Put(seg.timeDecoder)
		switch d := seg.valueDecoder.(type) {
		case *IntegerDecoder:
			intDecoderPool.Put(d)
		case *FloatDecoder:
			floatDecoderPool.Put(d)
		case *StringDecoder:
			strDecoderPool.Put(d)
		case *BooleanDecoder:
			boolDecoderPool.Put(d)
		case *JsonDecoder:
			jsonDecoderPool.Put(d)
		case *AdaptColumnDecoder:
			adaptColDecoderPool.Put(d)
		}
	}
	p.segments = p.segments[:0]
	p.currentIdx = 0
}

// AddSegment adds a new data segment containing compressed timestamp and value byte streams.
func (p *PointDiskPack) AddSegment(tmsData []byte, valueData []byte) error {
	if len(tmsData) == 0 || len(valueData) == 0 {
		return nil
	}
	var valueDecoder Decoder
	switch valueData[0] {
	case intUncompressed, intCompressedSimple, intCompressedRLE:
		valueDecoder = intDecoderPool.Get().(*IntegerDecoder)
	case jsonCompressed:
		valueDecoder = jsonDecoderPool.Get().(*JsonDecoder)
	case floatCompressedXDMI:
		valueDecoder = floatDecoderPool.Get().(*FloatDecoder)
	case stringCompressedSnappy, stringCompressedDict:
		valueDecoder = strDecoderPool.Get().(*StringDecoder)
	case booleanCompressedRLEFalse, booleanCompressedRLETrue, booleanCompressedBitPacked:
		valueDecoder = boolDecoderPool.Get().(*BooleanDecoder)
	case columnCompressed:
		valueDecoder = NewColumnDecoder(p.attribute)
	case adaptColumnCompressed:
		valueDecoder = adaptColDecoderPool.Get().(*AdaptColumnDecoder)
	default:
		return errorUnknownValueCompressionType(valueData[0])
	}
	valueDecoder.SetBytes(valueData)
	if valueDecoder.Error() != nil {
		return valueDecoder.Error()
	}
	td := timeDecoderPool.Get().(*TimeDecoder)
	td.Init(tmsData)
	p.segments = append(p.segments, Segment{
		timeDecoder:  td,
		valueDecoder: valueDecoder,
	})

	return nil
}

// Next Attempt to read the next timestamp and value, automatically switch to the next shard
func (p *PointDiskPack) Next() bool {
	for p.currentIdx < len(p.segments) {
		seg := &p.segments[p.currentIdx]

		timeOK := seg.timeDecoder.Next()
		valueOK := seg.valueDecoder.Next()

		if !timeOK || !valueOK {
			p.currentIdx++
			continue // The current shard has ended, try the next one
		}

		if p.endTime > 0 {
			tms := seg.timeDecoder.Read()
			if tms > p.endTime || tms < p.startTime {
				// Next() has already advanced both decoders past this point,
				// but the caller will never Read() it. IntegerDecoder's delta
				// chain updates its accumulator (prev) only inside Read, so a
				// filtered point would desync every later value by one delta
				// (value 0 lost, value 1 duplicated). Consume and discard the
				// value to keep decoder state in lockstep with the time
				// stream. Read is side-effect free for the other decoders.
				seg.valueDecoder.Read()
				continue
			}
		}

		return true
	}

	return false
}

// Read returns the current timestamp and value.
func (p *PointDiskPack) Read() (int64, variant.Variant) {
	seg := &p.segments[p.currentIdx]
	return seg.timeDecoder.Read(), seg.valueDecoder.Read()
}

type PointCachePack struct {
	currentIdx int
	points     []Point
}

func NewPointCachePack(points []Point) PointPack {
	return &PointCachePack{
		currentIdx: -1,
		points:     points,
	}
}

func (p *PointCachePack) Reset() {
	p.currentIdx = -1
	p.points = nil
}

func (p *PointCachePack) Next() bool {
	p.currentIdx++
	return p.currentIdx < len(p.points)
}

func (p *PointCachePack) Read() (int64, variant.Variant) {
	pt := p.points[p.currentIdx]
	return pt.Tms, pt.V
}
