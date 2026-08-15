package tsdb

import (
	"fmt"
	"sort"
	"testing"

	"github.com/mababaNiubi/variant"
)

// buildInterleavedChunk builds a per-tag-monotonic but interleaved chunk:
// `tags` distinct tags written round-robin, each tag's timestamps increasing.
func buildInterleavedChunk(tags, entries int) []walDataEntry {
	chunk := make([]walDataEntry, 0, entries)
	for i := 0; i < entries; i++ {
		chunk = append(chunk, walDataEntry{
			Key:       tagCode(i % tags),
			Timestamp: int64(i),
			Value:     variant.NewInt64(int64(i)),
		})
	}
	return chunk
}

func freshWalForBench(tags int) *walFile {
	return &walFile{
		tagMaxTimestamp: make(map[tagCode]int64, tags),
		tagLastValue:    make(map[tagCode]variant.Variant, tags),
	}
}

// BenchmarkDedupInterleaved_PerEntry measures the interleaved no-sort path
// (dedupPerEntry: per-entry shared-map loop). This is what processDedupChunk
// dispatches to for per-tag-monotonic unsorted chunks.
func BenchmarkDedupInterleaved_PerEntry(b *testing.B) {
	for _, tc := range []struct{ tags, entries int }{
		{64, 4096}, {256, 4096}, {4096, 4096},
	} {
		b.Run(fmt.Sprintf("tags%d_entries%d", tc.tags, tc.entries), func(b *testing.B) {
			orig := buildInterleavedChunk(tc.tags, tc.entries)
			ws := freshWalForBench(tc.tags)
			var batch []byte
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch = batch[:0]
				chunk := make([]walDataEntry, len(orig))
				copy(chunk, orig)
				var length int64
				batch, length, _ = ws.dedupPerEntry(chunk, batch, length)
				_ = length
			}
		})
	}
}

// BenchmarkDedupInterleaved_SortRuns measures the alternative: sort the
// interleaved chunk first, then run the run-batched dedupRuns path. Only wins
// at extreme cardinality (≈one tag per entry), where the sort pays off.
func BenchmarkDedupInterleaved_SortRuns(b *testing.B) {
	for _, tc := range []struct{ tags, entries int }{
		{64, 4096}, {256, 4096}, {4096, 4096},
	} {
		b.Run(fmt.Sprintf("tags%d_entries%d", tc.tags, tc.entries), func(b *testing.B) {
			orig := buildInterleavedChunk(tc.tags, tc.entries)
			ws := freshWalForBench(tc.tags)
			var batch []byte
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch = batch[:0]
				chunk := make([]walDataEntry, len(orig))
				copy(chunk, orig)
				sort.Slice(chunk, func(i, j int) bool {
					if chunk[i].Key != chunk[j].Key {
						return chunk[i].Key < chunk[j].Key
					}
					return chunk[i].Timestamp < chunk[j].Timestamp
				})
				var length int64
				batch, length, _ = ws.dedupRuns(chunk, batch, length)
				_ = length
			}
		})
	}
}
