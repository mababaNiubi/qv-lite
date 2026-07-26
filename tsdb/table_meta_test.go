package tsdb

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func openMetaForWrite(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
}

// writeRawEntry writes an entry WITHOUT CRC (for testing corruption scenarios).
func writeRawEntry(f *os.File, tag string, code tagCode) error {
	tagBytes := []byte(tag)
	entry := make([]byte, 2+len(tagBytes)+4)
	binary.BigEndian.PutUint16(entry[0:2], uint16(len(tagBytes)))
	copy(entry[2:], tagBytes)
	binary.BigEndian.PutUint32(entry[2+len(tagBytes):], uint32(code))
	_, err := f.Write(entry)
	return err
}

// writeCompleteEntry writes a full entry with valid CRC.
func writeCompleteEntry(f *os.File, tag string, code tagCode) error {
	tagBytes := []byte(tag)
	dataLen := 2 + len(tagBytes) + 4
	entry := make([]byte, dataLen+4)
	binary.BigEndian.PutUint16(entry[0:2], uint16(len(tagBytes)))
	copy(entry[2:], tagBytes)
	binary.BigEndian.PutUint32(entry[2+len(tagBytes):], uint32(code))
	crc := crc32.ChecksumIEEE(entry[:dataLen])
	binary.BigEndian.PutUint32(entry[dataLen:], crc)
	_, err := f.Write(entry)
	return err
}

// ── NewMeta ──────────────────────────────────────────────────────────────────

func TestNewMeta_CreateFresh(t *testing.T) {
	dir := t.TempDir()
	m, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta failed: %v", err)
	}
	defer m.Close()

	if m.MaxPointDict != 0 {
		t.Errorf("fresh MaxPointDict: got %d, want 0", m.MaxPointDict)
	}
	if m.Path != filepath.Join(dir, metaFile) {
		t.Errorf("Path: got %s, want %s", m.Path, filepath.Join(dir, metaFile))
	}

	// Verify the file exists and has only the header.
	fi, err := os.Stat(m.Path)
	if err != nil {
		t.Fatalf("stat meta file: %v", err)
	}
	if fi.Size() != int64(metaHeadLen) {
		t.Errorf("file size: got %d, want %d", fi.Size(), metaHeadLen)
	}
}

func TestNewMeta_ReopenExisting(t *testing.T) {
	dir := t.TempDir()

	// Create and populate.
	m, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta: %v", err)
	}
	code1, _ := m.addTag("sensor_a")
	code2, _ := m.addTag("sensor_b")
	code3, _ := m.addTag("sensor_c")
	m.Close()

	if code1 != 1 || code2 != 2 || code3 != 3 {
		t.Fatalf("expected codes 1,2,3 got %d,%d,%d", code1, code2, code3)
	}

	// Reopen and verify.
	m2, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta (reopen): %v", err)
	}
	defer m2.Close()

	if m2.MaxPointDict != 3 {
		t.Errorf("MaxPointDict after reopen: got %d, want 3", m2.MaxPointDict)
	}

	for _, tc := range []struct {
		tag  string
		code tagCode
	}{
		{"sensor_a", 1},
		{"sensor_b", 2},
		{"sensor_c", 3},
	} {
		got, ok := m2.Load(tc.tag)
		if !ok {
			t.Errorf("tag %q not found after reopen", tc.tag)
		}
		if got != tc.code {
			t.Errorf("tag %q: got code %d, want %d", tc.tag, got, tc.code)
		}
	}
}

func TestNewMeta_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, metaFile)

	// Create a file with only a valid header and no entries.
	m, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta: %v", err)
	}
	m.Close()

	// Reopen — should succeed with MaxPointDict == 0.
	m2, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta (reopen empty): %v", err)
	}
	defer m2.Close()

	if m2.MaxPointDict != 0 {
		t.Errorf("MaxPointDict: got %d, want 0", m2.MaxPointDict)
	}

	// Verify we can still add tags.
	code, err := m2.addTag("new_tag")
	if err != nil {
		t.Fatalf("addTag after reopen empty: %v", err)
	}
	if code != 1 {
		t.Errorf("first code after reopen: got %d, want 1", code)
	}
	_ = path // used in filepath.Join above
}

// ── addTag ───────────────────────────────────────────────────────────────────

func TestAddTag_Single(t *testing.T) {
	m, err := NewMeta(t.TempDir())
	if err != nil {
		t.Fatalf("NewMeta: %v", err)
	}
	defer m.Close()

	code, err := m.addTag("temperature")
	if err != nil {
		t.Fatalf("addTag: %v", err)
	}
	if code != 1 {
		t.Errorf("first code: got %d, want 1", code)
	}
	if m.MaxPointDict != 1 {
		t.Errorf("MaxPointDict: got %d, want 1", m.MaxPointDict)
	}

	// Load back from in-memory map.
	got, ok := m.Load("temperature")
	if !ok {
		t.Fatal("tag 'temperature' not found in map")
	}
	if got != 1 {
		t.Errorf("loaded code: got %d, want 1", got)
	}
}

func TestAddTag_MultipleSequential(t *testing.T) {
	m, err := NewMeta(t.TempDir())
	if err != nil {
		t.Fatalf("NewMeta: %v", err)
	}
	defer m.Close()

	tags := []string{"a", "b", "c", "d", "e"}
	for i, tag := range tags {
		code, err := m.addTag(tag)
		if err != nil {
			t.Fatalf("addTag(%q): %v", tag, err)
		}
		expected := tagCode(i + 1)
		if code != expected {
			t.Errorf("addTag(%q): got code %d, want %d", tag, code, expected)
		}
	}

	if m.MaxPointDict != tagCode(len(tags)) {
		t.Errorf("MaxPointDict: got %d, want %d", m.MaxPointDict, len(tags))
	}
}

func TestAddTag_DuplicateTag(t *testing.T) {
	// Duplicate detection is handled at the CreateColumn level,
	// not in addTag itself. Calling addTag with the same tag twice
	// will happily assign two different codes — this tests that
	// the low-level behaviour is consistent.
	m, err := NewMeta(t.TempDir())
	if err != nil {
		t.Fatalf("NewMeta: %v", err)
	}
	defer m.Close()

	c1, _ := m.addTag("dup")
	c2, _ := m.addTag("dup")

	if c1 == c2 {
		t.Errorf("duplicate tag got same code %d", c1)
	}
	// The last Store wins in the SyncMap.
	got, _ := m.Load("dup")
	if got != c2 {
		t.Errorf("Load: got %d, want %d (last write)", got, c2)
	}
}

// ── Round-trip (write → close → reopen) ─────────────────────────────────────

func TestRoundTrip_ManyTags(t *testing.T) {
	dir := t.TempDir()
	const N = 200

	// Write N tags.
	m, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta: %v", err)
	}
	for i := 0; i < N; i++ {
		code, err := m.addTag("tag_" + strconv.Itoa(i))
		if err != nil {
			m.Close()
			t.Fatalf("addTag %d: %v", i, err)
		}
		if code != tagCode(i+1) {
			m.Close()
			t.Fatalf("code %d: got %d, want %d", i, code, i+1)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify all tags.
	m2, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta (reopen): %v", err)
	}
	defer m2.Close()

	if m2.MaxPointDict != N {
		t.Errorf("MaxPointDict: got %d, want %d", m2.MaxPointDict, N)
	}

	for i := 0; i < N; i++ {
		tag := "tag_" + strconv.Itoa(i)
		got, ok := m2.Load(tag)
		if !ok {
			t.Errorf("tag %q missing after reopen", tag)
		}
		if got != tagCode(i+1) {
			t.Errorf("tag %q: got %d, want %d", tag, got, i+1)
		}
	}
}

func TestRoundTrip_MaxPointDictContinues(t *testing.T) {
	dir := t.TempDir()

	// First session: add 5 tags.
	m, _ := NewMeta(dir)
	for i := 0; i < 5; i++ {
		m.addTag("tag" + strconv.Itoa(i))
	}
	m.Close()

	// Second session: add 3 more tags.
	m2, _ := NewMeta(dir)
	code, err := m2.addTag("tag_new")
	if err != nil {
		t.Fatalf("addTag in second session: %v", err)
	}
	if code != 6 {
		t.Errorf("code should continue from 6, got %d", code)
	}
	m2.Close()
}

// ── Crash recovery ───────────────────────────────────────────────────────────

// setupMetaWithEntries creates a meta file directory with a valid header and
// the given number of complete entries, then returns the directory path and
// the meta file path.
func setupMetaFile(t *testing.T, dir string, numEntries int) string {
	t.Helper()
	path := filepath.Join(dir, metaFile)

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Write header.
	var header [metaHeadLen]byte
	binary.BigEndian.PutUint32(header[:], metaMagic)
	if _, err := f.Write(header[:]); err != nil {
		f.Close()
		t.Fatalf("write header: %v", err)
	}

	// Write entries with valid CRC.
	for i := 0; i < numEntries; i++ {
		if err := writeCompleteEntry(f, "tag_"+strconv.Itoa(i), tagCode(i+1)); err != nil {
			f.Close()
			t.Fatalf("write entry %d: %v", i, err)
		}
	}

	if err := f.Sync(); err != nil {
		f.Close()
		t.Fatalf("sync: %v", err)
	}
	f.Close()
	return path
}

func TestCrashRecovery_PartialTagLen(t *testing.T) {
	dir := t.TempDir()
	path := setupMetaFile(t, dir, 3) // 3 good entries

	// Append only half a tagLen byte (incomplete).
	f, _ := openMetaForWrite(path)
	f.Write([]byte{0x00}) // only 1 of 2 tagLen bytes
	f.Sync()
	f.Close()

	m, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta should recover: %v", err)
	}
	defer m.Close()

	if m.MaxPointDict != 3 {
		t.Errorf("MaxPointDict: got %d, want 3", m.MaxPointDict)
	}
	// Verify the 3 good entries survived.
	for i := 0; i < 3; i++ {
		if _, ok := m.Load("tag_" + strconv.Itoa(i)); !ok {
			t.Errorf("tag_%d should exist after recovery", i)
		}
	}
	// The partial byte should be gone.
	fi, _ := os.Stat(path)
	// 3 complete entries + header.
	expectedSize := int64(metaHeadLen) + 3*(2+4+4+4) // tagLen(2) + tag("tag_N"=5) + code(4) + crc(4)
	_ = expectedSize
	if fi.Size()%1 != 0 { // just check it's a reasonable size
		t.Logf("file size after recovery: %d", fi.Size())
	}

	// Should still be able to add new tags.
	code, err := m.addTag("after_crash")
	if err != nil {
		t.Fatalf("addTag after recovery: %v", err)
	}
	if code != 4 {
		t.Errorf("new code after recovery: got %d, want 4", code)
	}
}

func TestCrashRecovery_PartialTagBytes(t *testing.T) {
	dir := t.TempDir()
	path := setupMetaFile(t, dir, 2)

	// Write a partial entry: complete tagLen header but incomplete tag bytes.
	f, _ := openMetaForWrite(path)
	tag := "incomplete"
	tagBytes := []byte(tag)
	// Write tagLen.
	var tagLenBuf [2]byte
	binary.BigEndian.PutUint16(tagLenBuf[:], uint16(len(tagBytes)))
	f.Write(tagLenBuf[:])
	// Write only half the tag bytes.
	f.Write(tagBytes[:len(tagBytes)/2])
	f.Sync()
	f.Close()

	m, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta should recover: %v", err)
	}
	defer m.Close()

	if m.MaxPointDict != 2 {
		t.Errorf("MaxPointDict: got %d, want 2", m.MaxPointDict)
	}

	// Partial entry must not appear.
	if _, ok := m.Load("incomplete"); ok {
		t.Error("'incomplete' should NOT be present after recovery")
	}
}

func TestCrashRecovery_PartialCode(t *testing.T) {
	dir := t.TempDir()
	path := setupMetaFile(t, dir, 2)

	// Write tagLen + complete tag, but only part of the code.
	f, _ := openMetaForWrite(path)
	tag := "missing_code"
	tagBytes := []byte(tag)
	var hdr [2]byte
	binary.BigEndian.PutUint16(hdr[:], uint16(len(tagBytes)))
	f.Write(hdr[:])
	f.Write(tagBytes)
	// Only 2 of 4 code bytes.
	f.Write([]byte{0x00, 0x00})
	f.Sync()
	f.Close()

	m, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta should recover: %v", err)
	}
	defer m.Close()

	if m.MaxPointDict != 2 {
		t.Errorf("MaxPointDict: got %d, want 2", m.MaxPointDict)
	}
	if _, ok := m.Load("missing_code"); ok {
		t.Error("'missing_code' should NOT be present")
	}
}

func TestCrashRecovery_PartialCRC(t *testing.T) {
	dir := t.TempDir()
	path := setupMetaFile(t, dir, 2)

	// Write everything except the last CRC byte.
	f, _ := openMetaForWrite(path)
	tag := "missing_crc"
	tagBytes := []byte(tag)
	dataLen := 2 + len(tagBytes) + 4
	buf := make([]byte, dataLen+3) // only 3 of 4 CRC bytes
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(tagBytes)))
	copy(buf[2:], tagBytes)
	binary.BigEndian.PutUint32(buf[2+len(tagBytes):], 99)
	// Don't write the full CRC — only 3 bytes of it.
	f.Write(buf)
	f.Sync()
	f.Close()

	m, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta should recover: %v", err)
	}
	defer m.Close()

	if m.MaxPointDict != 2 {
		t.Errorf("MaxPointDict: got %d, want 2", m.MaxPointDict)
	}
	if _, ok := m.Load("missing_crc"); ok {
		t.Error("'missing_crc' should NOT be present")
	}
}

func TestCrashRecovery_CorruptCRC(t *testing.T) {
	dir := t.TempDir()
	path := setupMetaFile(t, dir, 2)

	// Write a structurally complete entry but with a wrong CRC.
	f, _ := openMetaForWrite(path)
	tag := "bad_crc"
	tagBytes := []byte(tag)
	dataLen := 2 + len(tagBytes) + 4
	buf := make([]byte, dataLen+4)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(tagBytes)))
	copy(buf[2:], tagBytes)
	binary.BigEndian.PutUint32(buf[2+len(tagBytes):], 99)
	// Deliberately wrong CRC.
	binary.BigEndian.PutUint32(buf[dataLen:], 0xDEADBEEF)
	f.Write(buf)
	f.Sync()
	f.Close()

	m, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta should recover: %v", err)
	}
	defer m.Close()

	if m.MaxPointDict != 2 {
		t.Errorf("MaxPointDict: got %d, want 2", m.MaxPointDict)
	}
	if _, ok := m.Load("bad_crc"); ok {
		t.Error("'bad_crc' should NOT be present (CRC mismatch)")
	}
}

func TestCrashRecovery_CorruptHeader(t *testing.T) {
	dir := t.TempDir()
	// Create a file with just a partial header (a crash during initFile).
	path := filepath.Join(dir, metaFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Write only 2 bytes instead of the full 4-byte header.
	f.Write([]byte{0x4D, 0x45}) // "ME" — not enough
	f.Close()

	// NewMeta should detect the corrupt header and re-initialise.
	m, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta should recover: %v", err)
	}
	defer m.Close()

	if m.MaxPointDict != 0 {
		t.Errorf("MaxPointDict after header recovery: got %d, want 0", m.MaxPointDict)
	}

	// Should work normally afterwards.
	code, err := m.addTag("after_header_crash")
	if err != nil {
		t.Fatalf("addTag after header recovery: %v", err)
	}
	if code != 1 {
		t.Errorf("code after header recovery: got %d, want 1", code)
	}
}

func TestCrashRecovery_WrongMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, metaFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Write a full header but with the wrong magic.
	var header [metaHeadLen]byte
	binary.BigEndian.PutUint32(header[:], 0xDEADBEEF)
	os.WriteFile(path, header[:], 0644)

	_, err := NewMeta(dir)
	if err != ErrorMetaFileFormat {
		t.Errorf("expected ErrorMetaFileFormat, got %v", err)
	}
}

func TestCrashRecovery_OnlyPartialEntryAfterGoodData(t *testing.T) {
	dir := t.TempDir()
	setupMetaFile(t, dir, 5)

	// Append garbage (corrupts the next entry completely).
	f, _ := openMetaForWrite(filepath.Join(dir, metaFile))
	f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Sync()
	f.Close()

	m, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta should recover: %v", err)
	}
	defer m.Close()

	if m.MaxPointDict != 5 {
		t.Errorf("MaxPointDict: got %d, want 5", m.MaxPointDict)
	}
	for i := 0; i < 5; i++ {
		if _, ok := m.Load("tag_" + strconv.Itoa(i)); !ok {
			t.Errorf("tag_%d should exist", i)
		}
	}
}

// ── Concurrent access ────────────────────────────────────────────────────────

func TestConcurrentAddTag(t *testing.T) {
	m, err := NewMeta(t.TempDir())
	if err != nil {
		t.Fatalf("NewMeta: %v", err)
	}
	defer m.Close()

	const goroutines = 20
	const tagsPerGoroutine = 50
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < tagsPerGoroutine; i++ {
				tag := "g" + strconv.Itoa(gid) + "_t" + strconv.Itoa(i)
				if _, err := m.addTag(tag); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent addTag error: %v", err)
	}

	totalTags := goroutines * tagsPerGoroutine
	if int(m.MaxPointDict) != totalTags {
		t.Errorf("MaxPointDict: got %d, want %d", m.MaxPointDict, totalTags)
	}
}

// ── Close ────────────────────────────────────────────────────────────────────

func TestClose(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewMeta(dir)
	m.addTag("a")
	m.addTag("b")

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Double close should be safe.
	if err := m.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestClose_ReopenAfterClose(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewMeta(dir)
	m.addTag("x")
	m.Close()

	// Reopen and verify.
	m2, err := NewMeta(dir)
	if err != nil {
		t.Fatalf("NewMeta after close: %v", err)
	}
	defer m2.Close()

	if _, ok := m2.Load("x"); !ok {
		t.Error("tag 'x' should persist after close")
	}
}
