package tsdb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"path/filepath"
	"testing"
)

func TestNewFileWriter(t *testing.T) {
	testFile := filepath.Join(t.TempDir(), "test_new_writer.txt")
	writer := NewFileWriter(testFile, NoCompressor{}).(*fileWriter)

	if writer.filePath != testFile {
		t.Errorf("filePath mismatch: got %q, want %q", writer.filePath, testFile)
	}
	if !writer.closed {
		t.Error("new writer should be in closed state")
	}
}

func TestBasicWriteAndCommit(t *testing.T) {
	testFile := filepath.Join(t.TempDir(), "test_basic_write.txt")
	writer := NewFileWriter(testFile, NoCompressor{}).(*fileWriter)

	if err := writer.OpenWriter(); err != nil {
		t.Fatalf("OpenWriter failed: %v", err)
	}
	defer writer.Cleanup()

	data := []byte("hello tsdb writer test")
	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write length: got %d, want %d", n, len(data))
	}
	if writer.Size() != int64(len(data)) {
		t.Errorf("Size: got %d, want %d", writer.Size(), len(data))
	}

	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Read back via BlockFile to verify
	result := readTestFile(t, testFile, NoCompressor{})
	if string(result) != string(data) {
		t.Errorf("file content mismatch: got %q, want %q", result, data)
	}
}

func TestCompression(t *testing.T) {
	testFile := filepath.Join(t.TempDir(), "test_compression.txt")
	writer := NewFileWriter(testFile, SnappyCompressor{}).(*fileWriter)

	if err := writer.OpenWriter(); err != nil {
		t.Fatalf("OpenWriter failed: %v", err)
	}
	defer writer.Cleanup()

	// Build a properly formatted block matching glowWrite output format:
	// SegmentHeader + [8B valueLength] + valueData + timeData
	valueData := []byte("compressed content with repetition compressed content with repetition")
	header := SegmentHeader{
		MinTime:   100,
		MaxTime:   200,
		Attribute: 1,
		DataSize:  int64(8 + len(valueData)),
		Crc:       crc32.ChecksumIEEE(valueData),
	}
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, header)
	binary.Write(&buf, binary.BigEndian, uint64(len(valueData)))
	buf.Write(valueData)

	_, err := writer.Write(buf.Bytes())
	if err != nil {
		t.Fatalf("WriteBlock failed: %v", err)
	}
	if err := writer.FlushBlock(); err != nil {
		t.Fatalf("FlushBlock failed: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	reader := NewFileReader(testFile, SnappyCompressor{}, nil)
	if err := reader.OpenReader(); err != nil {
		t.Fatalf("OpenReader failed: %v", err)
	}
	defer reader.CloseReader()

	head, _, compressedValueData, err := reader.NextRead(
		func(h SegmentHeader) bool { return true },
		&TableInfo{ColumnAttribute: ColumnAttribute{Type: ColumnTypeInt}},
	)
	if err != nil {
		t.Fatalf("NextRead failed: %v", err)
	}
	if head == nil {
		t.Fatal("expected a block")
	}
	// compressedValueData should be the value portion
	if string(compressedValueData) != string(valueData) {
		t.Errorf("content mismatch: got %q, want %q", compressedValueData, valueData)
	}
}

func TestRollback(t *testing.T) {
	testFile := filepath.Join(t.TempDir(), "test_rollback.txt")
	writer := NewFileWriter(testFile, NoCompressor{}).(*fileWriter)

	// First write and commit
	if err := writer.OpenWriter(); err != nil {
		t.Fatalf("OpenWriter failed: %v", err)
	}
	firstData := []byte("first part")
	if _, err := writer.Write(firstData); err != nil {
		t.Fatalf("first Write failed: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("first Commit failed: %v", err)
	}

	// Second write and commit
	if err := writer.OpenWriter(); err != nil {
		t.Fatalf("second OpenWriter failed: %v", err)
	}
	secondData := []byte("second part")
	if _, err := writer.Write(secondData); err != nil {
		t.Fatalf("second Write failed: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("second Commit failed: %v", err)
	}

	// Verify full content before rollback
	fullContent := readTestFile(t, testFile, NoCompressor{})
	expectedFull := string(firstData) + string(secondData)
	if string(fullContent) != expectedFull {
		t.Errorf("before rollback: got %q, want %q", fullContent, expectedFull)
	}

	// Rollback and verify
	if err := writer.RollbackLastCommit(); err != nil {
		t.Fatalf("RollbackLastCommit failed: %v", err)
	}

	rolledBackContent := readTestFile(t, testFile, NoCompressor{})
	if string(rolledBackContent) != string(firstData) {
		t.Errorf("after rollback: got %q, want %q", rolledBackContent, firstData)
	}
}

func TestFileWriteErrorHandling(t *testing.T) {
	// Write to a finalized writer should fail
	testFile := filepath.Join(t.TempDir(), "test_error.txt")
	writer := NewFileWriter(testFile, NoCompressor{}).(*fileWriter)
	if err := writer.OpenWriter(); err != nil {
		t.Fatalf("OpenWriter failed: %v", err)
	}
	if err := writer.Cleanup(); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	_, err := writer.Write([]byte("data"))
	if err == nil {
		t.Error("Write to closed writer should return an error")
	}
}

// ─── Integration tests for all compressors ─────────────────────────────

func TestWriteBlock_AllCompressors(t *testing.T) {
	compressors := map[string]BlockCompressor{
		"snappy": SnappyCompressor{},
		"lz4":    LZ4Compressor{},
		"gzip":   GzipCompressor{},
		"zstd":   ZstdCompressor{},
	}

	for cName, c := range compressors {
		t.Run(cName, func(t *testing.T) {
			testFile := filepath.Join(t.TempDir(), "test_block_"+cName)
			w := NewFileWriter(testFile, c).(*fileWriter)
			if err := w.OpenWriter(); err != nil {
				t.Fatalf("OpenWriter failed: %v", err)
			}
			defer w.Cleanup()

			valueData := []byte("integration test data with some repetition integration test data with some repetition")
			header := SegmentHeader{
				MinTime:   200,
				MaxTime:   300,
				Attribute: 2,
				DataSize:  int64(8 + len(valueData)),
				Crc:       crc32.ChecksumIEEE(valueData),
			}
			var buf bytes.Buffer
			binary.Write(&buf, binary.BigEndian, header)
			binary.Write(&buf, binary.BigEndian, uint64(len(valueData)))
			buf.Write(valueData)

			_, err := w.Write(buf.Bytes())
			if err != nil {
				t.Fatalf("WriteBlock failed: %v", err)
			}
			if err := w.FlushBlock(); err != nil {
				t.Fatalf("FlushBlock failed: %v", err)
			}
			if err := w.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Read back and verify
			r := NewFileReader(testFile, c, nil)
			if err := r.OpenReader(); err != nil {
				t.Fatalf("OpenReader failed: %v", err)
			}
			defer r.CloseReader()

			head, _, cvData, err := r.NextRead(
				func(h SegmentHeader) bool { return true },
				&TableInfo{ColumnAttribute: ColumnAttribute{Type: ColumnTypeInt}},
			)
			if err != nil {
				t.Fatalf("NextRead failed: %v", err)
			}
			if head == nil {
				t.Fatal("expected a block")
			}
			if string(cvData) != string(valueData) {
				t.Errorf("content mismatch: got %q, want %q", cvData, valueData)
			}
		})
	}
}

func readTestFile(t *testing.T, path string, compressor BlockCompressor) []byte {
	t.Helper()
	bf, err := OpenBlockFile(path, compressor, BlockSizeDef)
	if err != nil {
		t.Fatalf("OpenBlockFile failed: %v", err)
	}
	defer bf.Close()
	if bf.Size() == 0 {
		return nil
	}
	data := make([]byte, bf.Size())
	if _, err = io.ReadFull(bf, data); err != nil {
		t.Fatalf("ReadFull failed: %v", err)
	}
	return data
}
