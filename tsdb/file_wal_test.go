package tsdb

import (
	"github.com/mababaNiubi/variant"
	"testing"
)

func TestReadByTime_OutOfOrderWrite(t *testing.T) {
	dir := tempDir(t)
	wf, err := NewWalFile(dir, false, 64*1024*1024, 10, 0, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer wf.Close()

	tag := tagCode(1)

	// Write timestamps out of order into the active (unsorted) chunk.
	wf.Write(tag, 100, variant.NewInt64(100))
	wf.Write(tag, 50, variant.NewInt64(50))
	wf.Write(tag, 75, variant.NewInt64(75))

	tms, vals, err := wf.ReadByTime(tag, 0, 200)
	if err != nil {
		t.Fatal(err)
	}

	if len(tms) != 3 {
		t.Fatalf("expected 3 results, got %d", len(tms))
	}
	for i := 1; i < len(tms); i++ {
		if tms[i] < tms[i-1] {
			t.Errorf("timestamps not sorted: tms[%d]=%d > tms[%d]=%d", i-1, tms[i-1], i, tms[i])
		}
	}
	expected := []int64{50, 75, 100}
	for i := range expected {
		if tms[i] != expected[i] {
			t.Errorf("tms[%d]=%d, want %d", i, tms[i], expected[i])
		}
		if v, _ := vals[i].AsInt64(); v != expected[i] {
			t.Errorf("vals[%d]=%d, want %d", i, v, expected[i])
		}
	}
}

func TestReadByTime_MultipleChunks(t *testing.T) {
	dir := tempDir(t)
	// Small batch size so flush triggers quickly.
	wf, err := NewWalFile(dir, false, 64*1024*1024, 10, 0, 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer wf.Close()

	tag := tagCode(1)

	// Write 5 entries — this will trigger flushPending (creates a sorted flushed chunk).
	for _, ts := range []int64{30, 10, 50, 20, 40} {
		wf.Write(tag, ts, variant.NewInt64(ts))
	}
	// Write 3 more entries into a new active chunk (unsorted).
	for _, ts := range []int64{90, 70, 80} {
		wf.Write(tag, ts, variant.NewInt64(ts))
	}

	tms, vals, err := wf.ReadByTime(tag, 0, 100)
	if err != nil {
		t.Fatal(err)
	}

	if len(tms) != 8 {
		t.Fatalf("expected 8 results, got %d", len(tms))
	}
	for i := 1; i < len(tms); i++ {
		if tms[i] < tms[i-1] {
			t.Errorf("timestamps not sorted at index %d: %d > %d", i, tms[i-1], tms[i])
		}
	}
	// Verify all expected timestamps are present.
	seen := make(map[int64]bool)
	for _, ts := range tms {
		seen[ts] = true
	}
	for _, exp := range []int64{10, 20, 30, 40, 50, 70, 80, 90} {
		if !seen[exp] {
			t.Errorf("missing timestamp %d", exp)
		}
	}
	_ = vals
}

func TestReadByTime_TimeRange(t *testing.T) {
	dir := tempDir(t)
	wf, err := NewWalFile(dir, false, 64*1024*1024, 10, 0, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer wf.Close()

	tag := tagCode(1)

	// Write out of order into the active chunk.
	wf.Write(tag, 100, variant.NewInt64(100))
	wf.Write(tag, 50, variant.NewInt64(50))
	wf.Write(tag, 75, variant.NewInt64(75))
	wf.Write(tag, 200, variant.NewInt64(200))
	wf.Write(tag, 150, variant.NewInt64(150))

	// Query only [60, 160] — should include 75, 100, 150 in sorted order.
	tms, vals, err := wf.ReadByTime(tag, 60, 160)
	if err != nil {
		t.Fatal(err)
	}

	if len(tms) != 3 {
		t.Fatalf("expected 3 results in range [60,160], got %d: %v", len(tms), tms)
	}
	expected := []int64{75, 100, 150}
	for i := range expected {
		if tms[i] != expected[i] {
			t.Errorf("tms[%d]=%d, want %d", i, tms[i], expected[i])
		}
		if v, _ := vals[i].AsInt64(); v != expected[i] {
			t.Errorf("vals[%d]=%d, want %d", i, v, expected[i])
		}
	}
}
