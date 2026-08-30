package tsdb

import (
	"sync"

	"github.com/mababaNiubi/variant"
)

// ingestSeries is the stable per-tag state retained for the lifetime of the
// table batcher. Only runWorker mutates code/preparedPoints/encoderWarmed;
// callers touch its point buffers while holding the owning shard mutex.
type ingestSeries struct {
	tag            string
	hash           uint64
	code           tagCode
	preparedPoints int
	encoderWarmed  bool

	// Sparse high-cardinality optimization: most tags appear in only one
	// active/frozen buffer. Keep that first buffer inline and allocate the other
	// QueueSize+2 slots only if the tag actually crosses a batch boundary. Eager
	// allocation here previously multiplied every one-shot tag by the full queue
	// depth and caused the sharp high-cardinality memory cliff.
	primaryBufferID int
	primaryBuffer   rawTagBuffer
	otherBuffers    []rawTagBuffer
}

// bufferFor maps a stable ingestBuffer ID to this series' matching tag buffer.
// Once secondary storage is created it never moves, so pointers stored in a
// frozen batch remain valid while callers write into another buffer ID.
func (s *ingestSeries) bufferFor(id, bufferCount int) *rawTagBuffer {
	if s.primaryBufferID < 0 {
		s.primaryBufferID = id
		s.primaryBuffer.series = s
		return &s.primaryBuffer
	}
	if id == s.primaryBufferID {
		return &s.primaryBuffer
	}
	if s.otherBuffers == nil {
		s.otherBuffers = make([]rawTagBuffer, bufferCount-1)
		for i := range s.otherBuffers {
			s.otherBuffers[i].series = s
		}
	}
	otherID := id
	if id > s.primaryBufferID {
		otherID--
	}
	return &s.otherBuffers[otherID]
}

// rawWritePoint omits both tag string and tagCode. The owning rawTagBuffer
// already identifies one series, saving repeated tag storage for every point.
type rawWritePoint struct {
	timestamp int64
	value     variant.Variant
}

// rawTagBuffer groups one series' points inside one reusable ingestBuffer.
// ordered stays true for the common monotonically increasing case, allowing
// prepareRawWriteBatch to skip sorting entirely.
type rawTagBuffer struct {
	series       *ingestSeries
	points       []rawWritePoint
	lastTms      int64
	ordered      bool
	hasReference bool
}

// ingestTagCacheEntry is one slot in a direct-mapped best-effort cache. The tag
// string is checked on every hit; collisions fall back to ingestShard.series.
type ingestTagCacheEntry struct {
	tag    string
	series *ingestSeries
}

// ingestBuffer is a reusable shard-local generation. Its stable ID selects the
// matching rawTagBuffer in every ingestSeries; used makes recycle proportional
// to tags touched by the batch rather than total table cardinality.
type ingestBuffer struct {
	id   int
	used []*rawTagBuffer
}

type ingestShard struct {
	mu          sync.Mutex
	series      map[string]*ingestSeries
	tagCache    []ingestTagCacheEntry
	active      ingestBuffer
	free        []ingestBuffer
	activeCount int
}

// frozenWriteBatch owns one ingestBuffer from every shard until runWorker has
// committed it. done is closed after buffers are recycled; waiting on it is the
// visibility barrier used by Flush and Close.
type frozenWriteBatch struct {
	shards []ingestBuffer
	count  int
	done   chan struct{}
	err    error
}

func newIngestBuffer(id int) ingestBuffer {
	return ingestBuffer{id: id}
}

// variantRetainsHeap mirrors Variant's reference-bearing cases. Numeric values
// can be overwritten safely when a pooled slice is reused; strings, lists and
// maps must be cleared or a mostly idle high-cardinality buffer would retain
// the caller's object graph indefinitely.
func variantRetainsHeap(value variant.Variant) bool {
	switch value.Type() {
	case variant.TypeString, variant.TypeList, variant.TypeMap:
		return true
	default:
		return false
	}
}
