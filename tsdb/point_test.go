package tsdb

import (
	"testing"

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
