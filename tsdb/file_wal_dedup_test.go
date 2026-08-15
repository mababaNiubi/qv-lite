package tsdb

import (
	"testing"

	"github.com/mababaNiubi/variant"
)

// reopenWal loads the WAL back from disk after Close, giving the deduped view
// (only entries actually written to the .wal files remain).
func reopenWal(t *testing.T, dir string, cfg WalConfig) WalFile {
	t.Helper()
	wf, err := NewWalFile(dir, 0, 0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { wf.Close() })
	return wf
}

func readAll(wf WalFile, tag tagCode, lo, hi int64) []int64 {
	pts, err := wf.ReadByTime(tag, lo, hi)
	if err != nil {
		return nil
	}
	out := make([]int64, 0, len(pts))
	for _, p := range pts {
		v, _ := p.V.AsInt64()
		out = append(out, v)
	}
	return out
}

func eqInt64(t *testing.T, got, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d: got %v want %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("[%d]=%d want %d: got %v want %v", i, got[i], want[i], got, want)
		}
	}
}

// TestDedupRunPath verifies the sorted-chunk fast path (dedupRuns): an
// out-of-order single-tag chunk is sorted and all points are kept; a later
// chunk with older points has them deduped against the running max.
func TestDedupRunPath(t *testing.T) {
	dir := tempDir(t)
	cfg := WalConfig{MaxFileSize: maxSegmentSize, MaxBufferBatchSize: 5, MaxFileNumber: 100}
	wf, err := NewWalFile(dir, 0, 0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	tag := tagCode(1)

	// Chunk 1 (5 entries, out of order → sorted → dedupRuns): all kept.
	for _, ts := range []int64{100, 50, 75, 80, 90} {
		if _, _, err := wf.Write(tag, ts, variant.NewInt64(ts)); err != nil {
			t.Fatal(err)
		}
	}
	// Chunk 2 (5 entries): 30,60 < running max 100 → dropped.
	for _, ts := range []int64{60, 200, 30, 150, 250} {
		if _, _, err := wf.Write(tag, ts, variant.NewInt64(ts)); err != nil {
			t.Fatal(err)
		}
	}

	if ts, _, ok := wf.GetTagMaxTimestamp(tag); !ok || ts != 250 {
		t.Fatalf("max timestamp = %d (ok=%v), want 250", ts, ok)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}

	rw := reopenWal(t, dir, cfg)
	eqInt64(t, readAll(rw, tag, 0, 1000), []int64{50, 75, 80, 90, 100, 150, 200, 250})
}

// TestDedupInterleavedPath verifies the interleaved per-tag-monotonic path
// (dedupPerEntry, the no-sort high-cardinality case): two tags interleaved with
// non-decreasing timestamps are deduped correctly, and old cross-chunk points
// are dropped per tag.
func TestDedupInterleavedPath(t *testing.T) {
	dir := tempDir(t)
	cfg := WalConfig{MaxFileSize: maxSegmentSize, MaxBufferBatchSize: 5, MaxFileNumber: 100}
	wf, err := NewWalFile(dir, 0, 0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	const tagA, tagB = tagCode(1), tagCode(2)

	// Interleaved monotonic chunk: [A:10 B:10 A:20 B:20 A:30] → dedupPerEntry, all kept.
	for _, e := range []struct {
		k tagCode
		v int64
	}{{tagA, 10}, {tagB, 10}, {tagA, 20}, {tagB, 20}, {tagA, 30}} {
		if _, _, err := wf.Write(e.k, e.v, variant.NewInt64(e.v)); err != nil {
			t.Fatal(err)
		}
	}
	// Second chunk: A:5,B:15 older than A:30,B:20 → dropped; rest kept.
	for _, e := range []struct {
		k tagCode
		v int64
	}{{tagA, 5}, {tagB, 15}, {tagA, 35}, {tagB, 25}, {tagA, 40}} {
		if _, _, err := wf.Write(e.k, e.v, variant.NewInt64(e.v)); err != nil {
			t.Fatal(err)
		}
	}

	if ts, _, ok := wf.GetTagMaxTimestamp(tagA); !ok || ts != 40 {
		t.Fatalf("A max = %d (ok=%v), want 40", ts, ok)
	}
	if ts, _, ok := wf.GetTagMaxTimestamp(tagB); !ok || ts != 25 {
		t.Fatalf("B max = %d (ok=%v), want 25", ts, ok)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}

	rw := reopenWal(t, dir, cfg)
	eqInt64(t, readAll(rw, tagA, 0, 1000), []int64{10, 20, 30, 35, 40})
	eqInt64(t, readAll(rw, tagB, 0, 1000), []int64{10, 20, 25})
}

// TestDedupWindow applies dedupWindowMs across chunk boundaries on both paths
// and checks that the equal-value dedup window still drops repeats.
func TestDedupWindow(t *testing.T) {
	dir := tempDir(t)
	cfg := WalConfig{MaxFileSize: maxSegmentSize, MaxBufferBatchSize: 5, MaxFileNumber: 100}
	wf, err := NewWalFile(dir, 1000, 0, cfg) // dedupWindowMs = 1000
	if err != nil {
		t.Fatal(err)
	}
	tag := tagCode(1)
	base := int64(1000000)
	// Same value within the 1000ms window → dropped.
	for _, ts := range []int64{base, base + 100, base + 200, base + 300, base + 400} {
		if _, _, err := wf.Write(tag, ts, variant.NewInt64(42)); err != nil {
			t.Fatal(err)
		}
	}
	// New value still inside the window → kept (value differs).
	if _, _, err := wf.Write(tag, base+500, variant.NewInt64(43)); err != nil {
		t.Fatal(err)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}

	rw := reopenWal(t, dir, cfg)
	// Only the first 42 and the 43 survive; the other four 42s are deduped.
	eqInt64(t, readAll(rw, tag, 0, 2000000), []int64{42, 43})
}

// TestDedupHighCardinality writes many rotating tags (each in monotonic order)
// so chunks take the interleaved dedupPerEntry path, then verifies every point
// survives with per-tag ordering intact.
func TestDedupHighCardinality(t *testing.T) {
	dir := tempDir(t)
	cfg := WalConfig{MaxFileSize: maxSegmentSize, MaxBufferBatchSize: 64, MaxFileNumber: 1000}
	wf, err := NewWalFile(dir, 0, 0, cfg)
	if err != nil {
		t.Fatal(err)
	}

	const tags = 100
	const perTag = 20
	base := int64(1000000000000)
	// Zigzag tag order within each round (0,99,1,98,...) so each 64-entry chunk
	// is interleaved (per-tag monotonic but not sorted) → dedupPerEntry path.
	order := make([]int, 0, tags)
	for i := 0; i < tags/2; i++ {
		order = append(order, i, tags-1-i)
	}
	for round := 0; round < perTag; round++ {
		for _, tag := range order {
			ts := base + int64(round*1000) + int64(tag)
			if _, _, err := wf.Write(tagCode(tag+1), ts, variant.NewInt64(ts)); err != nil {
				t.Fatal(err)
			}
		}
	}

	for tag := 0; tag < tags; tag++ {
		if ts, _, ok := wf.GetTagMaxTimestamp(tagCode(tag + 1)); !ok || ts != base+int64((perTag-1)*1000)+int64(tag) {
			t.Fatalf("tag %d max = %d (ok=%v)", tag, ts, ok)
		}
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}

	rw := reopenWal(t, dir, cfg)
	hi := base + int64(1<<40)
	for tag := 0; tag < tags; tag++ {
		got := readAll(rw, tagCode(tag+1), 0, hi)
		want := make([]int64, perTag)
		for i := range want {
			want[i] = base + int64(i*1000) + int64(tag)
		}
		eqInt64(t, got, want)
	}
}
