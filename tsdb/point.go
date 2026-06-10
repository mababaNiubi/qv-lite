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
}

func GetAllPointByBytes(attribute []ColumnAttribute, compressedTimeData []byte, compressedValueData []byte, cond any) ([]Point, error) {
	points := make([]Point, 0, 256)
	var pack = NewPointPackImpl(attribute)
	err := pack.AddSegment(compressedTimeData, compressedValueData)
	if err != nil {
		return points, err
	}
	for pack.Next() {
		tms, value := pack.Read()
		// Evaluate condition filter.
		condition, err := evalAnyCondition(cond, value)
		if err != nil {
			return nil, err
		}
		if condition {
			points = append(points, Point{
				Tms: tms,
				V:   value,
			})
		}
	}
	return points, nil
}

type PointPackImpl struct {
	segments     []Segment
	currentIdx   int
	currentTs    int64
	currentValue variant.Variant

	cacheTms   []int64
	cacheValue []variant.Variant

	attribute []ColumnAttribute
}

func NewPointPackImpl(attribute []ColumnAttribute) *PointPackImpl {
	return &PointPackImpl{
		attribute: attribute,
	}
}

func (p *PointPackImpl) AddCacheSegment(cacheTms []int64, cacheValue []variant.Variant) {
	p.cacheTms = cacheTms
	p.cacheValue = cacheValue
}

func (p *PointPackImpl) Reset() {
	p.segments = p.segments[:0]
	p.currentIdx = 0
	p.cacheTms = nil
	p.cacheValue = nil
}

// AddSegment adds a new data segment containing compressed timestamp and value byte streams.
func (p *PointPackImpl) AddSegment(tmsData []byte, valueData []byte) error {
	if len(tmsData) == 0 || len(valueData) == 0 {
		return nil
	}
	var valueDecoder Decoder
	switch valueData[0] {
	case intUncompressed, intCompressedSimple, intCompressedRLE:
		valueDecoder = &IntegerDecoder{}
	case jsonCompressed:
		valueDecoder = &JsonDecoder{}
	case floatCompressedXDMI:
		valueDecoder = &FloatDecoder{}
	case stringCompressedSnappy:
		valueDecoder = &StringDecoder{}
	case booleanCompressedRLEFalse, booleanCompressedRLETrue, booleanCompressedBitPacked:
		valueDecoder = &BooleanDecoder{}
	case columnCompressed:
		valueDecoder = NewColumnDecoder(p.attribute)
	case adaptColumnCompressed:
		valueDecoder = &AdaptColumnDecoder{}
	default:
		return errorUnknownValueCompressionType(valueData[0])
	}
	valueDecoder.SetBytes(valueData)
	if valueDecoder.Error() != nil {
		return valueDecoder.Error()
	}
	td := &TimeDecoder{}
	td.Init(tmsData)
	p.segments = append(p.segments, Segment{
		timeDecoder:  td,
		valueDecoder: valueDecoder,
	})

	return nil
}

// Next Attempt to read the next timestamp and value, automatically switch to the next shard
func (p *PointPackImpl) Next() bool {
	for ; p.currentIdx < len(p.segments); p.currentIdx++ {
		seg := &p.segments[p.currentIdx]

		timeOK := seg.timeDecoder.Next()
		valueOK := seg.valueDecoder.Next()

		if !timeOK || !valueOK {
			continue // The current shard has ended, try the next one
		}

		p.currentTs = seg.timeDecoder.Read()
		p.currentValue = seg.valueDecoder.Read()
		return true
	}
	if p.currentIdx-len(p.segments) < len(p.cacheTms) {
		index := p.currentIdx - len(p.segments)
		p.currentTs = p.cacheTms[index]
		p.currentValue = p.cacheValue[index]
		p.currentIdx++
		return true
	}
	return false
}

// Read returns the current timestamp and value.
func (p *PointPackImpl) Read() (int64, variant.Variant) {
	return p.currentTs, p.currentValue
}
