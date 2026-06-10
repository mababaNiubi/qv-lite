package tsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenBlockFile_New(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new.bf")
	bf, err := OpenBlockFile(path, NoCompressor{}, 0)
	if err != nil {
		t.Fatalf("OpenBlockFile failed: %v", err)
	}
	defer bf.Close()

	if bf.Size() != 0 {
		t.Errorf("new file size: got %d, want 0", bf.Size())
	}
	if bf.DataEnd() != HeaderSize {
		t.Errorf("new file DataEnd: got %d, want %d", bf.DataEnd(), HeaderSize)
	}
	raw, _ := os.ReadFile(path)
	if len(raw) < int(HeaderSize) {
		t.Fatalf("file too short: %d bytes", len(raw))
	}
	magic := binary.LittleEndian.Uint32(raw[0:4])
	if magic != Magic {
		t.Errorf("magic: got %x, want %x", magic, Magic)
	}
}

func TestOpenBlockFile_Reopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.bf")
	data := []byte("persistent data")

	bf, _ := OpenBlockFile(path, NoCompressor{}, 128)
	bf.Write(data)
	bf.Close()

	bf2, err := OpenBlockFile(path, NoCompressor{}, 128)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer bf2.Close()

	if bf2.Size() != int64(len(data)) {
		t.Errorf("Size: got %d, want %d", bf2.Size(), len(data))
	}
	buf := make([]byte, len(data))
	n, err := io.ReadFull(bf2, buf)
	if err != nil {
		t.Fatalf("ReadFull failed: %v", err)
	}
	if n != len(data) || string(buf) != string(data) {
		t.Errorf("content: got %q, want %q", buf[:n], data)
	}
}

func TestBlockFile_WriteCloseRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wcr.bf")
	data := []byte("hello block file")

	bf, _ := OpenBlockFile(path, NoCompressor{}, 1024)
	bf.Write(data)
	bf.Close()

	bf2, _ := OpenBlockFile(path, NoCompressor{}, 1024)
	defer bf2.Close()
	bf2.Seek(0, io.SeekStart)

	buf := make([]byte, len(data))
	n, _ := io.ReadFull(bf2, buf)
	if n != len(data) || string(buf) != string(data) {
		t.Errorf("round-trip: got %q, want %q", buf[:n], data)
	}
}

func TestBlockFile_Seek(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seek.bf")
	bf, _ := OpenBlockFile(path, NoCompressor{}, 1024)
	defer bf.Close()

	data := []byte("0123456789abcdef")
	bf.Write(data)
	bf.Close()

	bf2, _ := OpenBlockFile(path, NoCompressor{}, 1024)
	defer bf2.Close()

	// SeekStart
	pos, _ := bf2.Seek(3, io.SeekStart)
	if pos != 3 {
		t.Errorf("SeekStart: got %d, want 3", pos)
	}
	c := make([]byte, 1)
	bf2.Read(c)
	if c[0] != '3' {
		t.Errorf("after SeekStart: got %c, want 3", c[0])
	}

	// SeekCurrent
	bf2.Seek(5, io.SeekCurrent)
	c2 := make([]byte, 1)
	bf2.Read(c2)
	if c2[0] != '9' {
		t.Errorf("after SeekCurrent: got %c, want 9", c2[0])
	}

	// SeekEnd
	bf2.Seek(-2, io.SeekEnd)
	c3 := make([]byte, 1)
	bf2.Read(c3)
	if c3[0] != 'e' {
		t.Errorf("after SeekEnd: got %c, want e", c3[0])
	}
}

func TestBlockFile_Sync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.bf")
	bf, _ := OpenBlockFile(path, NoCompressor{}, 4096)
	defer bf.Close()

	bf.Write([]byte("before sync"))
	if err := bf.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	bf.Close()

	bf2, _ := OpenBlockFile(path, NoCompressor{}, 4096)
	defer bf2.Close()
	if bf2.Size() != 11 {
		t.Errorf("after Close Size: got %d, want 11", bf2.Size())
	}
}

func TestBlockFile_Drop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drop.bf")
	bf, _ := OpenBlockFile(path, NoCompressor{}, 4096)
	bf.Write([]byte("data to discard"))
	bf.Drop()

	bf2, err := OpenBlockFile(path, NoCompressor{}, 4096)
	if err != nil {
		t.Fatalf("reopen after Drop failed: %v", err)
	}
	defer bf2.Close()
	if bf2.Size() != 0 {
		t.Errorf("after Drop Size: got %d, want 0", bf2.Size())
	}
}

func TestBlockFile_CrashRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash.bf")

	// Step 1: Write and Close
	bf, _ := OpenBlockFile(path, NoCompressor{}, 256)
	bf.Write([]byte("safe data block"))
	bf.Close()

	// Step 2: Open, write more, Sync (flushes buffer + writes header), then crash (Drop)
	bf2, _ := OpenBlockFile(path, NoCompressor{}, 10)
	bf2.Write([]byte("lost"))
	bf2.Sync()
	bf2.Drop()

	// Step 3: Reopen — should recover flushed blocks via rebuildIndex
	bf3, err := OpenBlockFile(path, NoCompressor{}, 256)
	if err != nil {
		t.Fatalf("reopen after crash failed: %v", err)
	}
	defer bf3.Close()

	expected := "safe data blocklost"
	if bf3.Size() != int64(len(expected)) {
		t.Errorf("after recovery Size: got %d, want %d", bf3.Size(), len(expected))
	}
	buf := make([]byte, bf3.Size())
	io.ReadFull(bf3, buf)
	if string(buf) != expected {
		t.Errorf("content: got %q, want %q", buf, expected)
	}
}

func TestBlockFile_ReadPastEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "eof.bf")
	bf, _ := OpenBlockFile(path, NoCompressor{}, 128)
	bf.Write([]byte("short"))
	bf.Close()

	bf2, _ := OpenBlockFile(path, NoCompressor{}, 128)
	defer bf2.Close()
	buf := make([]byte, 100)
	n, err := bf2.Read(buf)
	if err != io.EOF && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Errorf("read len: got %d, want 5", n)
	}
}

func TestBlockFile_WriteAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "closed.bf")
	bf, _ := OpenBlockFile(path, NoCompressor{}, 128)
	bf.Close()

	_, err := bf.Write([]byte("data"))
	if err == nil {
		t.Error("Write after Close should return error")
	}
}

func TestBlockFile_CRC(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crc.bf")

	bf, _ := OpenBlockFile(path, SnappyCompressor{}, 256)
	data := []byte("data with crc check")
	bf.Write(data)
	bf.Close()

	// Corrupt the compressed data
	f, _ := os.OpenFile(path, os.O_RDWR, 0644)
	f.Seek(HeaderSize+5, io.SeekStart)
	f.Write([]byte{0xFF, 0xFF, 0xFF})
	f.Close()

	bf2, _ := OpenBlockFile(path, SnappyCompressor{}, 256)
	defer bf2.Close()
	buf := make([]byte, len(data))
	_, err := io.ReadFull(bf2, buf)
	if err == nil {
		t.Error("CRC check should have failed after corruption")
	}
}

func TestBlockFile_SizeAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "size.bf")
	bf, _ := OpenBlockFile(path, SnappyCompressor{}, 256)
	bf.Write([]byte("size test"))
	bf.Close()

	bf2, _ := OpenBlockFile(path, SnappyCompressor{}, 256)
	defer bf2.Close()
	if bf2.Size() != 9 {
		t.Errorf("Size: got %d, want 9", bf2.Size())
	}
	buf := make([]byte, bf2.Size())
	io.ReadFull(bf2, buf)
	if string(buf) != "size test" {
		t.Errorf("content: got %q, want %q", buf, "size test")
	}
}

func TestBlockFile_MultiCompressor(t *testing.T) {
	compressors := map[string]BlockCompressor{
		"none":   NoCompressor{},
		"snappy": SnappyCompressor{},
		"lz4":    LZ4Compressor{},
		"gzip":   GzipCompressor{},
		"zstd":   ZstdCompressor{},
	}
	data := []byte("multi compressor test data block")

	for name, c := range compressors {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "multi_"+name+".bf")

			bf, _ := OpenBlockFile(path, c, 4096)
			bf.Write(data)
			bf.Close()

			bf2, _ := OpenBlockFile(path, c, 4096)
			defer bf2.Close()

			if bf2.Size() != int64(len(data)) {
				t.Errorf("Size: got %d, want %d", bf2.Size(), len(data))
			}
			buf := make([]byte, len(data))
			if _, err := io.ReadFull(bf2, buf); err != nil {
				t.Fatalf("ReadFull failed: %v", err)
			}
			if string(buf) != string(data) {
				t.Fatalf("mismatch: got %q, want %q", buf, data)
			}
		})
	}
}

func TestBlockFile_LargeWriteRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large write/read test in short mode")
	}
	// Note: this test requires fixing the Seek hang bug to pass.
	dataSize := 1*1024*1024 + 124
	for _, tc := range []struct {
		name     string
		comp     BlockCompressor
		dataSize int
	}{
		{"none_1MB", NoCompressor{}, dataSize},
		{"snappy_1MB", SnappyCompressor{}, dataSize},
		{"lz4_1MB", LZ4Compressor{}, dataSize},
		{"gzip_1MB", GzipCompressor{}, dataSize},
		{"zstd_1MB", ZstdCompressor{}, dataSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "large_"+tc.name+".bf")
			data := make([]byte, tc.dataSize)
			for i := range data {
				data[i] = byte(i % 256)
			}

			// Write
			bfWrite, _ := OpenBlockFile(path, tc.comp, BlockSizeDef)
			n, err := bfWrite.Write(data)
			if err != nil {
				t.Fatalf("Write error: %v", err)
			}
			if n != tc.dataSize {
				t.Errorf("Write: got %d, want %d", n, tc.dataSize)
			}
			bfWrite.Close()

			// Check size on disk
			fi, _ := os.Stat(path)
			physicalSize := fi.Size()
			ratio := float64(physicalSize) / float64(tc.dataSize)
			t.Logf("Compressor=%s logical=%d physical=%d ratio=%.3f",
				tc.name, tc.dataSize, physicalSize, ratio)

			// Reopen and read back
			bfRead, _ := OpenBlockFile(path, tc.comp, BlockSizeDef)
			defer bfRead.Close()

			if bfRead.Size() != int64(tc.dataSize) {
				t.Fatalf("Size mismatch: got %d, want %d", bfRead.Size(), tc.dataSize)
			}

			// Verify segments: read 4KB each from head, middle, and tail.
			chunkSize := 4 * 1024
			verifySegment := func(offset int64, label string) {
				bfRead.Seek(offset, io.SeekStart)
				buf := make([]byte, chunkSize)
				io.ReadFull(bfRead, buf)
				for i := 0; i < chunkSize; i++ {
					want := byte((int(offset) + i) % 256)
					if buf[i] != want {
						t.Fatalf("%s byte %d: got %d, want %d", label, int(offset)+i, buf[i], want)
					}
				}
			}
			verifySegment(0, "head")
			verifySegment(int64(tc.dataSize/2), "mid")
			verifySegment(int64(tc.dataSize-chunkSize), "tail")
		})
	}
}

func TestBlockFile_ManySmallWrites(t *testing.T) {
	// Small writes should not trigger a flush; verify that Close correctly flushes and indexes.
	path := filepath.Join(t.TempDir(), "small_writes.bf")
	bf, _ := OpenBlockFile(path, NoCompressor{}, BlockSizeDef)
	defer bf.Close()

	nWrites := 100
	writeSize := 5000

	var expected bytes.Buffer
	for i := 0; i < nWrites; i++ {
		chunk := make([]byte, writeSize)
		for j := range chunk {
			chunk[j] = byte((i + j) % 256)
		}
		expected.Write(chunk)
		n, err := bf.Write(chunk)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
		if n != writeSize {
			t.Errorf("Write %d returned %d, want %d", i, n, writeSize)
		}
	}
	expectedData := expected.Bytes()
	totalSize := int64(len(expectedData))

	if bf.Size() != totalSize {
		t.Errorf("Size before Close: got %d, want %d", bf.Size(), totalSize)
	}
	bf.Close()

	// Reopen and verify.
	bf2, _ := OpenBlockFile(path, NoCompressor{}, BlockSizeDef)
	defer bf2.Close()

	if bf2.Size() != totalSize {
		t.Fatalf("Size after reopen: got %d, want %d", bf2.Size(), totalSize)
	}

	result := make([]byte, totalSize)
	io.ReadFull(bf2, result)
	for i := range result {
		if result[i] != expectedData[i] {
			t.Fatalf("mismatch at %d: got %d, want %d", i, result[i], expectedData[i])
		}
	}
}

// ─── 500MB Throughput Test ──────────────────────────────────────
func TestThroughput_500MB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 500MB throughput test in short mode")
	}

	const dataSize = 500 * 1024 * 1024
	t.Logf("=== 500MB Throughput Test ===")

	compressors := []struct {
		name string
		comp BlockCompressor
	}{
		{"none", NoCompressor{}},
		{"snappy", SnappyCompressor{}},
		{"lz4", LZ4Compressor{}},
		{"gzip", GzipCompressor{}},
		{"zstd", ZstdCompressor{}},
	}

	for _, c := range compressors {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "thru_"+c.name+".bf")

			// Use a repeating pattern so compressors show meaningful differences.
			data := makePattern(dataSize, func(i int) byte {
				return byte('a' + (i % 26))
			})

			// Measure write speed.
			writeStart := time.Now()
			bf, _ := OpenBlockFile(path, c.comp, BlockSizeDef)
			n, err := bf.Write(data)
			if err != nil {
				t.Fatalf("Write error: %v", err)
			}
			bf.Close()
			writeDuration := time.Since(writeStart)
			writeMBps := float64(n) / writeDuration.Seconds() / (1024 * 1024)

			// Physical file size and compression ratio.
			fi, _ := os.Stat(path)
			physicalSize := fi.Size()
			compressionRatio := float64(physicalSize) / float64(n) * 100

			// Measure read speed.
			readStart := time.Now()
			bf2, _ := OpenBlockFile(path, c.comp, BlockSizeDef)
			buf := make([]byte, n)
			_, err = io.ReadFull(bf2, buf)
			if err != nil {
				t.Fatalf("Read error: %v", err)
			}
			bf2.Close()
			readDuration := time.Since(readStart)
			readMBps := float64(n) / readDuration.Seconds() / (1024 * 1024)

			t.Logf("%-6s: write=%.1f MB/s  read=%.1f MB/s  physical=%d  ratio=%.1f%%",
				c.name, writeMBps, readMBps, physicalSize, compressionRatio)
		})
	}
}

// ─── Compression Ratio Report ─────────────────────────────────

func TestCompressionRatioReport(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compression report in short mode")
	}

	const dataSize = 100 * 1024 * 1024
	compressors := []struct {
		name string
		comp BlockCompressor
	}{
		{"none", NoCompressor{}},
		{"snappy", SnappyCompressor{}},
		{"lz4", LZ4Compressor{}},
		{"gzip", GzipCompressor{}},
		{"zstd", ZstdCompressor{}},
	}

	patterns := []struct {
		name string
		fn   func(int) byte
	}{
		{"random", func(i int) byte { return byte(i % 256) }},
		{"zeros", func(i int) byte { return 0 }},
		{"alpha", func(i int) byte { return byte('a' + (i % 26)) }},
		{"text", func(i int) byte { return byte(32 + (i % 95)) }},
		{"sine", func(i int) byte { return byte(128 + int(127.0*(float64(i%628)*0.01))) }},
	}

	fmt.Println("\n=== Compression Ratio Report ===")
	fmt.Printf("%-8s", "pattern")
	for _, c := range compressors {
		fmt.Printf(" %9s", c.name)
	}
	fmt.Println()

	for _, p := range patterns {
		data := makePattern(dataSize, p.fn)
		fmt.Printf("%-8s", p.name)
		for _, c := range compressors {
			path := filepath.Join(t.TempDir(), fmt.Sprintf("cmp_%s_%s.bf", p.name, c.name))
			bf, _ := OpenBlockFile(path, c.comp, BlockSizeDef)
			bf.Write(data)
			bf.Close()
			fi, _ := os.Stat(path)
			ratio := float64(fi.Size()) / float64(dataSize) * 100
			fmt.Printf(" %9.2f%%", ratio)
			os.Remove(path)
		}
		fmt.Println()
	}
}

func makePattern(size int, fn func(i int) byte) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = fn(i)
	}
	return data
}

// ─── Truncate Tests ───────────────────────────────────────────

func TestBlockFile_TruncateToZero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc0.bf")
	bf, _ := OpenBlockFile(path, NoCompressor{}, 256)
	defer bf.Close()

	bf.Write([]byte("some data to be truncated"))
	if bf.Size() == 0 {
		t.Fatal("file should have data before truncation")
	}

	if err := bf.Truncate(0); err != nil {
		t.Fatalf("Truncate(0) failed: %v", err)
	}
	if bf.Size() != 0 {
		t.Errorf("Size after Truncate(0): got %d, want 0", bf.Size())
	}

	// Can still write after truncate to zero
	bf.Write([]byte("new data"))
	if bf.Size() != 8 {
		t.Errorf("Size after re-write: got %d, want 8", bf.Size())
	}
	bf.Seek(0, io.SeekStart)
	buf := make([]byte, 8)
	io.ReadFull(bf, buf)
	if string(buf) != "new data" {
		t.Errorf("content: got %q, want %q", buf, "new data")
	}
}

func TestBlockFile_TruncateNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc_nop.bf")
	bf, _ := OpenBlockFile(path, NoCompressor{}, 256)
	defer bf.Close()

	bf.Write([]byte("hello world"))
	originalSize := bf.Size()

	// Truncate to size larger than file
	if err := bf.Truncate(100); err != nil {
		t.Fatalf("Truncate(100) should be no-op: %v", err)
	}
	if bf.Size() != originalSize {
		t.Errorf("size changed after no-op truncate: got %d, want %d", bf.Size(), originalSize)
	}
}

func TestBlockFile_TruncateAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc_reopen.bf")

	// Write and close
	bf, _ := OpenBlockFile(path, NoCompressor{}, 32)
	bf.Write([]byte("0123456789ABCDEFGHIJ")) // 20 bytes
	bf.Close()

	// Reopen and truncate
	bf2, _ := OpenBlockFile(path, NoCompressor{}, 32)
	defer bf2.Close()

	if err := bf2.Truncate(10); err != nil {
		t.Fatalf("Truncate(10) failed: %v", err)
	}
	if bf2.Size() != 10 {
		t.Errorf("Size: got %d, want 10", bf2.Size())
	}

	// Read back truncated data
	bf2.Seek(0, io.SeekStart)
	buf := make([]byte, 10)
	io.ReadFull(bf2, buf)
	if string(buf) != "0123456789" {
		t.Errorf("content: got %q, want %q", buf, "0123456789")
	}

	// Can read from truncated file
	bf2.Close()
	bf3, _ := OpenBlockFile(path, NoCompressor{}, 32)
	defer bf3.Close()
	if bf3.Size() != 10 {
		t.Errorf("reopened Size: got %d, want 10", bf3.Size())
	}
}

func TestBlockFile_TruncateMultiBlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc_multi.bf")

	// Write across multiple blocks (blockSize=8)
	bf, _ := OpenBlockFile(path, NoCompressor{}, 8)
	data := []byte("0123456789ABCDEFGHIJ0123456789") // 30 bytes, ~4 blocks
	bf.Write(data)
	bf.Close()

	// Truncate in the middle (at 15 bytes, which is within the 2nd block)
	bf2, _ := OpenBlockFile(path, NoCompressor{}, 8)
	defer bf2.Close()

	if err := bf2.Truncate(15); err != nil {
		t.Fatalf("Truncate(15) failed: %v", err)
	}
	if bf2.Size() != 15 {
		t.Errorf("Size: got %d, want 15", bf2.Size())
	}

	bf2.Seek(0, io.SeekStart)
	buf := make([]byte, 15)
	io.ReadFull(bf2, buf)
	if string(buf) != "0123456789ABCDE" {
		t.Errorf("content: got %q, want %q", buf, "0123456789ABCDE")
	}
}

func TestBlockFile_TruncateAtBlockBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc_boundary.bf")

	bf, _ := OpenBlockFile(path, NoCompressor{}, 8)
	bf.Write([]byte("0123456789abcdef")) // 16 bytes, exactly 2 blocks
	bf.Close()

	// Truncate exactly at block boundary (8 bytes)
	bf2, _ := OpenBlockFile(path, NoCompressor{}, 8)
	defer bf2.Close()

	if err := bf2.Truncate(8); err != nil {
		t.Fatalf("Truncate(8) failed: %v", err)
	}
	if bf2.Size() != 8 {
		t.Errorf("Size: got %d, want 8", bf2.Size())
	}

	bf2.Seek(0, io.SeekStart)
	buf := make([]byte, 8)
	io.ReadFull(bf2, buf)
	if string(buf) != "01234567" {
		t.Errorf("content: got %q, want %q", buf, "01234567")
	}

	// Verify reopen
	bf2.Close()
	bf3, _ := OpenBlockFile(path, NoCompressor{}, 8)
	defer bf3.Close()
	if bf3.Size() != 8 {
		t.Errorf("reopened Size: got %d, want 8", bf3.Size())
	}
}

func TestBlockFile_TruncateWithCompression(t *testing.T) {
	compressors := map[string]BlockCompressor{
		"snappy": SnappyCompressor{},
		"lz4":    LZ4Compressor{},
		"zstd":   ZstdCompressor{},
	}

	data := make([]byte, 500)
	for i := range data {
		data[i] = byte('a' + i%26)
	}

	for name, c := range compressors {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "trunc_"+name+".bf")

			bf, _ := OpenBlockFile(path, c, 128)
			bf.Write(data)
			bf.Close()

			// Truncate at 200 bytes
			bf2, _ := OpenBlockFile(path, c, 128)
			defer bf2.Close()

			if err := bf2.Truncate(200); err != nil {
				t.Fatalf("Truncate(200) failed: %v", err)
			}
			if bf2.Size() != 200 {
				t.Errorf("Size: got %d, want 200", bf2.Size())
			}

			// Verify content
			bf2.Seek(0, io.SeekStart)
			buf := make([]byte, 200)
			io.ReadFull(bf2, buf)
			for i := range buf {
				want := byte('a' + i%26)
				if buf[i] != want {
					t.Fatalf("mismatch at %d: got %d, want %d", i, buf[i], want)
				}
			}

			// Reopen and verify again
			bf2.Close()
			bf3, _ := OpenBlockFile(path, c, 128)
			defer bf3.Close()
			if bf3.Size() != 200 {
				t.Errorf("reopened Size: got %d, want 200", bf3.Size())
			}
		})
	}
}

func TestBlockFile_TruncateAndAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc_append.bf")

	bf, _ := OpenBlockFile(path, NoCompressor{}, 64)
	bf.Write(bytes.Repeat([]byte{'A'}, 200))
	bf.Close()

	// Truncate then append
	bf2, _ := OpenBlockFile(path, NoCompressor{}, 64)
	defer bf2.Close()

	if err := bf2.Truncate(100); err != nil {
		t.Fatalf("Truncate(100) failed: %v", err)
	}
	bf2.Write(bytes.Repeat([]byte{'B'}, 50))

	if bf2.Size() != 150 {
		t.Errorf("Size: got %d, want 150", bf2.Size())
	}

	bf2.Seek(0, io.SeekStart)
	buf := make([]byte, 150)
	io.ReadFull(bf2, buf)

	// First 100 bytes should be 'A'
	for i := 0; i < 100; i++ {
		if buf[i] != 'A' {
			t.Fatalf("position %d: got %c, want A", i, buf[i])
		}
	}
	// Next 50 bytes should be 'B'
	for i := 100; i < 150; i++ {
		if buf[i] != 'B' {
			t.Fatalf("position %d: got %c, want B", i, buf[i])
		}
	}
}

func TestBlockFile_TruncateEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc_empty.bf")
	bf, _ := OpenBlockFile(path, NoCompressor{}, 256)
	defer bf.Close()

	// Truncate empty file (no blocks, no index)
	if err := bf.Truncate(10); err != nil {
		t.Fatalf("Truncate(10) on empty file should be no-op: %v", err)
	}
	if bf.Size() != 0 {
		t.Errorf("Size: got %d, want 0", bf.Size())
	}

	// Truncate to zero on empty file
	if err := bf.Truncate(0); err != nil {
		t.Fatalf("Truncate(0) on empty file: %v", err)
	}
}

func TestBlockFile_TruncateUnflushedData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc_unflushed.bf")

	// Write less than one block (data stays in buffer)
	bf, _ := OpenBlockFile(path, NoCompressor{}, 4096)
	defer bf.Close()
	bf.Write([]byte("unflushed data in buffer"))

	// Truncate should work on data still in buffer
	if err := bf.Truncate(5); err != nil {
		t.Fatalf("Truncate(5) with unflushed data: %v", err)
	}
	if bf.Size() != 5 {
		t.Errorf("Size: got %d, want 5", bf.Size())
	}
	bf.Seek(0, io.SeekStart)
	buf := make([]byte, 5)
	io.ReadFull(bf, buf)
	if string(buf) != "unflu" {
		t.Errorf("content: got %q, want %q", buf, "unflu")
	}
}

func TestBlockFile_TruncateNearBlockBoundary(t *testing.T) {
	testCases := []struct {
		truncSize int64
		expected  string
	}{
		{3, "000"},
		{10, "0000000011"},
		{21, "000000001111111122222"},
		{24, "000000001111111122222222"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("truncate_%d", tc.truncSize), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "trunc_near.bf")

			bf, _ := OpenBlockFile(path, NoCompressor{}, 8)
			bf.Write([]byte("000000001111111122222222")) // 24 bytes, 3 blocks
			bf.Close()

			bf2, _ := OpenBlockFile(path, NoCompressor{}, 8)
			defer bf2.Close()

			if err := bf2.Truncate(tc.truncSize); err != nil {
				t.Fatalf("Truncate(%d) failed: %v", tc.truncSize, err)
			}
			if bf2.Size() != tc.truncSize {
				t.Errorf("Size: got %d, want %d", bf2.Size(), tc.truncSize)
			}

			bf2.Seek(0, io.SeekStart)
			buf := make([]byte, tc.truncSize)
			io.ReadFull(bf2, buf)
			if string(buf) != tc.expected {
				t.Errorf("content: got %q, want %q", buf, tc.expected)
			}
		})
	}
}
