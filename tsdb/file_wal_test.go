package tsdb

import (
	"testing"

	"github.com/mababaNiubi/variant"
)

func TestReadByTime_LateBatch(t *testing.T) {
	dir := tempDir(t)
	wf, err := NewWalFile(dir, 0, 0, WalConfig{
		MaxFileSize:        maxSegmentSize,
		MaxBufferBatchSize: 5,
		MaxFileNumber:      100,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer wf.Close()

	tag := tagCode(1)

	// Each batch is internally ordered. The second batch is late relative to
	// the first, which still exercises read-time ordering without asking WAL to
	// repair a malformed prepared batch.
	_, err = wf.WriteBatch([]walDataEntry{
		{Key: tag, Timestamp: 50, Value: variant.NewInt64(50)},
		{Key: tag, Timestamp: 75, Value: variant.NewInt64(75)},
		{Key: tag, Timestamp: 100, Value: variant.NewInt64(100)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = wf.WriteBatch([]walDataEntry{
		{Key: tag, Timestamp: 25, Value: variant.NewInt64(25)},
	}); err != nil {
		t.Fatal(err)
	}

	points, err := wf.ReadByTime(tag, 0, 200)
	if err != nil {
		t.Fatal(err)
	}

	if len(points) != 4 {
		t.Fatalf("expected 4 results, got %d", len(points))
	}
	for i := 1; i < len(points); i++ {
		if points[i].Tms < points[i-1].Tms {
			t.Errorf("timestamps not sorted: tms[%d]=%d > tms[%d]=%d", i-1, points[i-1].Tms, i, points[i].Tms)
		}
	}
	expected := []int64{25, 50, 75, 100}
	for i := range expected {
		if points[i].Tms != expected[i] {
			t.Errorf("tms[%d]=%d, want %d", i, points[i].Tms, expected[i])
		}
		if v, _ := points[i].V.AsInt64(); v != expected[i] {
			t.Errorf("vals[%d]=%d, want %d", i, v, expected[i])
		}
	}
}

func TestReadByTime_MultipleChunks(t *testing.T) {
	dir := tempDir(t)
	// Small batch size so flush triggers quickly.
	wf, err := NewWalFile(dir, 0, 0, WalConfig{
		MaxFileSize:        maxSegmentSize,
		MaxBufferBatchSize: 5,
		MaxFileNumber:      10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer wf.Close()

	tag := tagCode(1)

	first := make([]walDataEntry, 0, 5)
	for _, ts := range []int64{10, 20, 30, 40, 50} {
		first = append(first, walDataEntry{Key: tag, Timestamp: ts, Value: variant.NewInt64(ts)})
	}
	if _, err = wf.WriteBatch(first); err != nil {
		t.Fatal(err)
	}
	second := make([]walDataEntry, 0, 3)
	for _, ts := range []int64{70, 80, 90} {
		second = append(second, walDataEntry{Key: tag, Timestamp: ts, Value: variant.NewInt64(ts)})
	}
	if _, err = wf.WriteBatch(second); err != nil {
		t.Fatal(err)
	}

	points, err := wf.ReadByTime(tag, 0, 100)
	if err != nil {
		t.Fatal(err)
	}

	if len(points) != 8 {
		t.Fatalf("expected 8 results, got %d", len(points))
	}
	for i := 1; i < len(points); i++ {
		if points[i].Tms < points[i-1].Tms {
			t.Errorf("timestamps not sorted at index %d: %d > %d", i, points[i-1].Tms, points[i].Tms)
		}
	}
	// Verify all expected timestamps are present.
	seen := make(map[int64]bool)
	for _, p := range points {
		seen[p.Tms] = true
	}
	for _, exp := range []int64{10, 20, 30, 40, 50, 70, 80, 90} {
		if !seen[exp] {
			t.Errorf("missing timestamp %d", exp)
		}
	}
}

func TestReadByTime_TimeRange(t *testing.T) {
	dir := tempDir(t)
	wf, err := NewWalFile(dir, 0, 0, WalConfig{
		MaxFileSize:        maxSegmentSize,
		MaxBufferBatchSize: 5,
		MaxFileNumber:      100,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer wf.Close()

	tag := tagCode(1)

	_, err = wf.WriteBatch([]walDataEntry{
		{Key: tag, Timestamp: 50, Value: variant.NewInt64(50)},
		{Key: tag, Timestamp: 75, Value: variant.NewInt64(75)},
		{Key: tag, Timestamp: 100, Value: variant.NewInt64(100)},
		{Key: tag, Timestamp: 150, Value: variant.NewInt64(150)},
		{Key: tag, Timestamp: 200, Value: variant.NewInt64(200)},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Query only [60, 160] — should include 75, 100, 150 in sorted order.
	points, err := wf.ReadByTime(tag, 60, 160)
	if err != nil {
		t.Fatal(err)
	}

	if len(points) != 3 {
		t.Fatalf("expected 3 results in range [60,160], got %d: %v", len(points), points)
	}
	expected := []int64{75, 100, 150}
	for i := range expected {
		if points[i].Tms != expected[i] {
			t.Errorf("tms[%d]=%d, want %d", i, points[i].Tms, expected[i])
		}
		if v, _ := points[i].V.AsInt64(); v != expected[i] {
			t.Errorf("vals[%d]=%d, want %d", i, v, expected[i])
		}
	}
}

func TestCloseBufferRecoversLastPoint(t *testing.T) {
	dir := tempDir(t)
	cfg := WalConfig{
		MaxFileSize:        maxSegmentSize,
		MaxBufferBatchSize: 5,
		MaxFileNumber:      100,
		CloseBuffer:        true,
	}
	wf, err := NewWalFile(dir, 0, 0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	tag := tagCode(1)
	if _, err := wf.WriteBatch([]walDataEntry{
		{Key: tag, Timestamp: 10, Value: variant.NewInt64(10)},
		{Key: tag, Timestamp: 20, Value: variant.NewInt64(20)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewWalFile(dir, 0, 0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	maxTs, maxValue, ok := reopened.GetTagMaxTimestamp(tag)
	if !ok || maxTs != 20 {
		t.Fatalf("recovered max timestamp=%d ok=%v, want 20/true", maxTs, ok)
	}
	value, _ := maxValue.AsInt64()
	if value != 20 {
		t.Fatalf("recovered last value=%d, want 20", value)
	}
}

func TestCompleteWALFilesTrackOnlyRetainedLateData(t *testing.T) {
	dir := tempDir(t)
	cfg := WalConfig{
		// Every non-empty batch rotates, making its ordering state observable as
		// an immutable complete file.
		MaxFileSize:        1,
		MaxBufferBatchSize: 8,
		MaxFileNumber:      10,
	}
	wf, err := NewWalFile(dir, 0, 0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	tag := tagCode(1)
	if _, err := wf.WriteBatch([]walDataEntry{
		{Key: tag, Timestamp: 10, Value: variant.NewInt64(10)},
		{Key: tag, Timestamp: 20, Value: variant.NewInt64(20)},
	}); err != nil {
		t.Fatal(err)
	}
	if count, needsSort := wf.completeFileState(); count != 1 || needsSort {
		t.Fatalf("ordered state=(count=%d sort=%v), want (1 false)", count, needsSort)
	}

	if _, err := wf.WriteBatch([]walDataEntry{
		{Key: tag, Timestamp: 5, Value: variant.NewInt64(5)},
		{Key: tag, Timestamp: 30, Value: variant.NewInt64(30)},
	}); err != nil {
		t.Fatal(err)
	}
	if count, needsSort := wf.completeFileState(); count != 2 || !needsSort {
		t.Fatalf("late state=(count=%d sort=%v), want (2 true)", count, needsSort)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}

	// The marker is reconstructed from WAL contents after a restart; it is not
	// dependent on volatile process state.
	reopened, err := NewWalFile(dir, 0, 0, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if count, needsSort := reopened.completeFileState(); count != 2 || !needsSort {
		t.Fatalf("recovered state=(count=%d sort=%v), want (2 true)", count, needsSort)
	}
}
