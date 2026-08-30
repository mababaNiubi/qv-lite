package tsdb

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestFileIndexMatchesBlocksByTagAndTime(t *testing.T) {
	idx := newFileIndex(6)
	for _, block := range []BlockIndexEntry{
		{Attribute: 1, MinTime: 0, MaxTime: 9, Offset: 100},
		{Attribute: 2, MinTime: 0, MaxTime: 9, Offset: 200},
		{Attribute: 1, MinTime: 10, MaxTime: 19, Offset: 300},
		{Attribute: 2, MinTime: 10, MaxTime: 19, Offset: 400},
		{Attribute: 1, MinTime: 20, MaxTime: 29, Offset: 500},
	} {
		idx.appendBlock(block)
	}

	tests := []struct {
		name       string
		tag        tagCode
		start, end int64
		want       []int
	}{
		{name: "single block", tag: 1, start: 10, end: 19, want: []int{2}},
		{name: "overlapping boundaries", tag: 1, start: 9, end: 20, want: []int{0, 2, 4}},
		{name: "other tag", tag: 2, start: 0, end: 100, want: []int{1, 3}},
		{name: "missing tag", tag: 3, start: 0, end: 100, want: nil},
		{name: "outside range", tag: 1, start: 30, end: 40, want: []int{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := idx.matchingBlockPositions(tt.tag, tt.start, tt.end)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("positions = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileIndexOutOfOrderTagFallsBackToGroupedScan(t *testing.T) {
	idx := newFileIndex(2)
	idx.appendBlock(BlockIndexEntry{Attribute: 1, MinTime: 20, MaxTime: 29, Offset: 100})
	idx.appendBlock(BlockIndexEntry{Attribute: 1, MinTime: 0, MaxTime: 9, Offset: 200})

	got := idx.matchingBlockPositions(1, 0, 10)
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("positions = %v, want [1]", got)
	}
}

func TestFileIndexPersistenceRebuildsTagGroups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.idx")
	want := newFileIndex(3)
	want.MinTime = 10
	want.MaxTime = 39
	want.appendBlock(BlockIndexEntry{Attribute: 1, MinTime: 10, MaxTime: 19, Offset: 100, DataSize: 11})
	want.appendBlock(BlockIndexEntry{Attribute: 2, MinTime: 20, MaxTime: 29, Offset: 200, DataSize: 22})
	want.appendBlock(BlockIndexEntry{Attribute: 1, MinTime: 30, MaxTime: 39, Offset: 300, DataSize: 33})

	if err := writeIndexFile(path, want); err != nil {
		t.Fatalf("writeIndexFile: %v", err)
	}
	got := readIndexFile(path)
	if got == nil {
		t.Fatal("readIndexFile returned nil")
	}
	if got.MinTime != want.MinTime || got.MaxTime != want.MaxTime || !reflect.DeepEqual(got.Blocks, want.Blocks) {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got, want)
	}
	if positions := got.matchingBlockPositions(1, 15, 35); !reflect.DeepEqual(positions, []int{0, 2}) {
		t.Fatalf("rebuilt tag positions = %v, want [0 2]", positions)
	}
}
