package tsdb

import (
	"testing"
	"unsafe"

	"github.com/mababaNiubi/variant"
)

func TestPointCachePackReadsEveryPoint(t *testing.T) {
	points := []Point{
		{Tms: 10, V: variant.NewInt64(1)},
		{Tms: 20, V: variant.NewInt64(2)},
		{Tms: 30, V: variant.NewInt64(3)},
	}
	pack := NewPointCachePack(points)

	var got []int64
	for pack.Next() {
		tms, _ := pack.Read()
		got = append(got, tms)
	}

	if len(got) != len(points) {
		t.Fatalf("read %d points, want %d: %v", len(got), len(points), got)
	}
	for i := range points {
		if got[i] != points[i].Tms {
			t.Fatalf("point %d timestamp = %d, want %d", i, got[i], points[i].Tms)
		}
	}
}

func TestPointCachePackEmpty(t *testing.T) {
	if NewPointCachePack(nil).Next() {
		t.Fatal("empty cache pack unexpectedly returned a point")
	}
}

// TestPointChunkPoolCap verifies the capped pool drops chunks beyond its byte
// budget instead of retaining the whole result of a huge query.
func TestPointChunkPoolCap(t *testing.T) {
	chunkBytes := pointChunkSize * int(unsafe.Sizeof(Point{}))
	pool := cappedChunkPool{maxBytes: chunkBytes * 3} // room for 3 chunks
	var chunks [][]Point
	for i := 0; i < 10; i++ {
		c := pool.get()
		if cap(c) != pointChunkSize {
			t.Fatalf("chunk cap = %d, want %d", cap(c), pointChunkSize)
		}
		chunks = append(chunks, c)
	}
	for _, c := range chunks {
		pool.put(c)
	}
	if len(pool.free) != 3 {
		t.Fatalf("pool retained %d chunks, want 3 (cap enforced)", len(pool.free))
	}
	// Re-get keeps returning valid chunks.
	for i := 0; i < 5; i++ {
		if c := pool.get(); cap(c) != pointChunkSize {
			t.Fatalf("reuse chunk cap = %d", cap(c))
		}
	}
}
