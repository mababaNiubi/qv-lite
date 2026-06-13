package tsdb

import (
	"sync"

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

var pointChunkPool = sync.Pool{
	New: func() any {
		return make([]Point, 0, pointChunkSize)
	},
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
		c.chunks = append(c.chunks, pointChunkPool.Get().([]Point))
		n++
	}
	c.chunks[n-1] = append(c.chunks[n-1], p)
	c.total++
}

// result flattens all chunks into a single pre-allocated slice and returns
// chunk memory to the pool for reuse. The full backing array is zeroed before
// returning each chunk to the pool so the GC does not scan stale pointers
// inside variant.Variant values.
func (c *pointCollector) result() []Point {
	out := make([]Point, 0, c.total)
	for _, chunk := range c.chunks {
		out = append(out, chunk...)
		clear(chunk)
		chunk = chunk[:0]
		pointChunkPool.Put(chunk)
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
	case stringCompressedSnappy:
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
		points: points,
	}
}

func (p *PointCachePack) Reset() {
	p.currentIdx = 0
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
