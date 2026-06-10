package tsdb

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"
)

// allCompressors returns all BlockCompressor implementations for testing.
func allCompressors() map[string]BlockCompressor {
	return map[string]BlockCompressor{
		"snappy": SnappyCompressor{},
		"lz4":    LZ4Compressor{},
		"gzip":   GzipCompressor{},
		"zstd":   ZstdCompressor{},
		"none":   NoCompressor{},
	}
}

// ─── Round-trip tests ─────────────────────────────────────────────────

func TestCompressor_RoundTrip(t *testing.T) {
	payloads := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"single_byte", []byte{0x42}},
		{"small_ascii", []byte("hello world")},
		{"repeated_ascii", bytes.Repeat([]byte("AAAA"), 1024)},
		{"random_1k", randomBytes(1024)},
		{"random_64k", randomBytes(64 * 1024)},
		{"zeros_1k", make([]byte, 1024)},
		{"ascii_4k", []byte(fmt.Sprintf("%4096s", "the quick brown fox jumps over the lazy dog."))},
	}

	for cName, c := range allCompressors() {
		for _, p := range payloads {
			t.Run(cName+"/"+p.name, func(t *testing.T) {
				encoded := c.Encode(p.data)
				decoded, err := c.Decode(encoded)
				if err != nil {
					t.Fatalf("Decode failed: %v", err)
				}
				if !bytes.Equal(p.data, decoded) {
					t.Fatalf("round-trip mismatch: len(src)=%d len(decoded)=%d", len(p.data), len(decoded))
				}
			})
		}
	}
}

// ─── Compression ratio comparison ─────────────────────────────────────

func TestCompressor_CompressionRatio(t *testing.T) {
	// Realistic time-series data patterns
	patterns := []struct {
		name string
		data []byte
	}{
		{
			"repeated_pattern_64k",
			bytes.Repeat([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}, 4096),
		},
		{
			"random_8k",
			randomBytes(8192),
		},
		{
			"sequential_ints_8k",
			sequentialIntBytes(2048),
		},
		{
			"zeros_16k",
			make([]byte, 16384),
		},
	}

	for _, p := range patterns {
		for cName, c := range allCompressors() {
			if cName == "none" {
				continue
			}
			encoded := c.Encode(p.data)
			ratio := float64(len(encoded)) / float64(len(p.data)) * 100
			t.Logf("%-8s %-24s: %6d → %6d bytes (%5.1f%%)", cName, p.name, len(p.data), len(encoded), ratio)
		}
	}
}

// ─── Large data test ──────────────────────────────────────────────────

func TestCompressor_LargeData(t *testing.T) {
	data := randomBytes(256 * 1024) // 256KB
	for cName, c := range allCompressors() {
		t.Run(cName, func(t *testing.T) {
			encoded := c.Encode(data)
			decoded, err := c.Decode(encoded)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}
			if !bytes.Equal(data, decoded) {
				t.Fatalf("large data mismatch: len(src)=%d len(decoded)=%d", len(data), len(decoded))
			}
			t.Logf("%s: %d → %d bytes (%.1f%%)", cName, len(data), len(encoded),
				float64(len(encoded))/float64(len(data))*100)
		})
	}
}

// ─── Empty data round-trip ────────────────────────────────────────────

func TestCompressor_EmptyRoundTrip(t *testing.T) {
	for cName, c := range allCompressors() {
		t.Run(cName, func(t *testing.T) {
			encoded := c.Encode(nil)
			decoded, err := c.Decode(encoded)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}
			if len(decoded) != 0 {
				t.Fatalf("expected empty, got %d bytes", len(decoded))
			}
		})
	}
}

// ─── Factory function ─────────────────────────────────────────────────

func TestCompressorByName(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{"snappy", "snappy"},
		{"lz4", "lz4"},
		{"gzip", "gzip"},
		{"zstd", "zstd"},
		{"none", "none"},
		{"", "none"},
		{"bogus", "none"},
	}

	for _, tt := range tests {
		c := CompressorByName(tt.name)
		// Verify by checking a known round-trip
		data := []byte("probe")
		encoded := c.Encode(data)
		decoded, err := c.Decode(encoded)
		if err != nil {
			t.Errorf("CompressorByName(%q): decode failed: %v", tt.name, err)
			continue
		}
		if !bytes.Equal(data, decoded) {
			t.Errorf("CompressorByName(%q): round-trip mismatch", tt.name)
		}
		if tt.expected != "" {
			// Verify by compressing and checking that a known decompressor recovers it
			ref := CompressorByName(tt.expected)
			refDecoded, refErr := ref.Decode(encoded)
			if refErr != nil {
				t.Errorf("CompressorByName(%q): expected %s but cross-decode failed: %v", tt.name, tt.expected, refErr)
				continue
			}
			if !bytes.Equal(data, refDecoded) {
				t.Errorf("CompressorByName(%q): expected %s but cross-decode mismatch", tt.name, tt.expected)
			}
		}
	}
}

// ─── Cross-compressor compatibility ───────────────────────────────────

func TestCompressor_CrossCompatibility(t *testing.T) {
	// Each compressor should be self-consistent (its own Decode can read its Encode).
	data := []byte("cross-compatibility test data with some repetition cross-compatibility test data")
	for cName, c := range allCompressors() {
		t.Run(cName, func(t *testing.T) {
			for i := 0; i < 10; i++ {
				encoded := c.Encode(data)
				decoded, err := c.Decode(encoded)
				if err != nil {
					t.Fatalf("iter %d: Decode failed: %v", i, err)
				}
				if !bytes.Equal(data, decoded) {
					t.Fatalf("iter %d: mismatch", i)
				}
			}
		})
	}
}

// ─── helpers ──────────────────────────────────────────────────────────

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func sequentialIntBytes(n int) []byte {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.Write([]byte{
			byte(i),
			byte(i >> 8),
			byte(i >> 16),
			byte(i >> 24),
		})
	}
	return buf.Bytes()
}
