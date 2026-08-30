package tsdb

import (
	"fmt"
	"testing"

	"github.com/mababaNiubi/variant"
)

// buildGroupedChunk builds the prepared shape produced by tableBatcher: one
// contiguous timestamp-ordered run per tag.
func buildGroupedChunk(tags, entries int) []walDataEntry {
	chunk := make([]walDataEntry, 0, entries)
	for tag := 0; tag < tags; tag++ {
		for i := tag; i < entries; i += tags {
			chunk = append(chunk, walDataEntry{
				Key:       tagCode(tag),
				Timestamp: int64(i),
				Value:     variant.NewInt64(int64(i)),
			})
		}
	}
	return chunk
}

func freshWalForBench(tags int) *walFile {
	return &walFile{
		tagMaxTimestamp: make(map[tagCode]int64, tags),
		tagLastValue:    make(map[tagCode]variant.Variant, tags),
	}
}

// BenchmarkDedupPreparedRuns measures the only WAL write shape accepted after
// accumulation and ordering moved into tableBatcher.
func BenchmarkDedupPreparedRuns(b *testing.B) {
	for _, tc := range []struct{ tags, entries int }{
		{64, 4096}, {256, 4096}, {4096, 4096},
	} {
		b.Run(fmt.Sprintf("tags%d_entries%d", tc.tags, tc.entries), func(b *testing.B) {
			orig := buildGroupedChunk(tc.tags, tc.entries)
			ws := freshWalForBench(tc.tags)
			var batch []byte
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch = batch[:0]
				chunk := make([]walDataEntry, len(orig))
				copy(chunk, orig)
				var length int64
				batch, length, _ = ws.dedupRuns(chunk, batch, length, nil)
				_ = length
			}
		})
	}
}
