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

// TestDedupRunPath verifies the prepared-batch path (dedupRuns). With no
// dedup or sampling policy configured, late points in a later batch are kept;
// the WAL marks that uncommon file for flush-time ordering.
func TestDedupRunPath(t *testing.T) {
	dir := tempDir(t)
	cfg := WalConfig{MaxFileSize: maxSegmentSize, MaxBufferBatchSize: 5, MaxFileNumber: 100}
	wf, err := NewWalFile(dir, 0, 0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	tag := tagCode(1)

	// Every input batch already satisfies the batcher contract: one ordered run
	// per tag. Late points in batch 2 are relative to batch 1, not within a run.
	batch := make([]walDataEntry, 0, 5)
	for _, ts := range []int64{50, 75, 80, 90, 100} {
		batch = append(batch, walDataEntry{Key: tag, Timestamp: ts, Value: variant.NewInt64(ts)})
	}
	if _, err := wf.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	// Batch 2 contains late points 30 and 60; both must remain durable.
	batch = batch[:0]
	for _, ts := range []int64{30, 60, 150, 200, 250} {
		batch = append(batch, walDataEntry{Key: tag, Timestamp: ts, Value: variant.NewInt64(ts)})
	}
	if _, err := wf.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}

	if ts, _, ok := wf.GetTagMaxTimestamp(tag); !ok || ts != 250 {
		t.Fatalf("max timestamp = %d (ok=%v), want 250", ts, ok)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}

	rw := reopenWal(t, dir, cfg)
	eqInt64(t, readAll(rw, tag, 0, 1000), []int64{30, 50, 60, 75, 80, 90, 100, 150, 200, 250})
}

// TestDedupGroupedTags verifies that multiple contiguous tag runs need not be
// ordered by tagCode, and late points in a later batch remain visible when no
// filtering policy is configured.
func TestDedupGroupedTags(t *testing.T) {
	dir := tempDir(t)
	cfg := WalConfig{MaxFileSize: maxSegmentSize, MaxBufferBatchSize: 5, MaxFileNumber: 100}
	wf, err := NewWalFile(dir, 0, 0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	const tagA, tagB = tagCode(1), tagCode(2)

	// Tag B precedes tag A, proving tagCode order itself is not required.
	batch := make([]walDataEntry, 0, 5)
	for _, e := range []struct {
		k tagCode
		v int64
	}{{tagB, 10}, {tagB, 20}, {tagA, 10}, {tagA, 20}, {tagA, 30}} {
		batch = append(batch, walDataEntry{Key: e.k, Timestamp: e.v, Value: variant.NewInt64(e.v)})
	}
	if _, err := wf.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	// Second batch: A:5,B:15 are late but must not be silently dropped.
	batch = batch[:0]
	for _, e := range []struct {
		k tagCode
		v int64
	}{{tagA, 5}, {tagA, 35}, {tagA, 40}, {tagB, 15}, {tagB, 25}} {
		batch = append(batch, walDataEntry{Key: e.k, Timestamp: e.v, Value: variant.NewInt64(e.v)})
	}
	if _, err := wf.WriteBatch(batch); err != nil {
		t.Fatal(err)
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
	eqInt64(t, readAll(rw, tagA, 0, 1000), []int64{5, 10, 20, 30, 35, 40})
	eqInt64(t, readAll(rw, tagB, 0, 1000), []int64{10, 15, 20, 25})
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

// TestDedupHighCardinality writes many tags as single-entry prepared batches,
// then verifies every point survives with per-tag ordering intact.
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
	// Zigzag tag order exercises arbitrary tagCode order across batches.
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
