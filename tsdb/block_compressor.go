package tsdb

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// BlockCompressor handles block-level compression for file segments.
// Compressor implementations follow a File-like pattern: they encapsulate
// the on-disk block format so fileWriter/fileReader don't branch on compression type.
type BlockCompressor interface {
	// Encode compresses src and returns the compressed bytes.
	Encode(src []byte) []byte
	// Decode decompresses src and returns decompressed bytes or an error.
	Decode(src []byte) ([]byte, error)
}

// ─── Snappy ───────────────────────────────────────────────────────────

// SnappyCompressor uses Snappy for block-level compression with a 4-byte LE size header.
type SnappyCompressor struct{}

func (s SnappyCompressor) Encode(src []byte) []byte {
	return snappy.Encode(nil, src)
}

func (s SnappyCompressor) Decode(src []byte) ([]byte, error) {
	return snappy.Decode(nil, src)
}

// ─── LZ4 ──────────────────────────────────────────────────────────────

// LZ4Compressor uses LZ4 frame format for block-level compression.
type LZ4Compressor struct{}

func (LZ4Compressor) Encode(src []byte) []byte {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	w.Write(src)
	w.Close()
	return buf.Bytes()
}

func (LZ4Compressor) Decode(src []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(src))
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ─── Gzip ─────────────────────────────────────────────────────────────

// GzipCompressor uses gzip for block-level compression.
type GzipCompressor struct{}

func (GzipCompressor) Encode(src []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write(src)
	w.Close()
	return buf.Bytes()
}

func (GzipCompressor) Decode(src []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ─── Zstd ─────────────────────────────────────────────────────────────

// ZstdCompressor uses zstd for block-level compression.
type ZstdCompressor struct{}

var reusableZstdEncoder struct {
	sync.Once
	encoder *zstd.Encoder
}

var reusableZstdDecoder struct {
	sync.Once
	decoder *zstd.Decoder
}

func blockZstdEncoder() *zstd.Encoder {
	reusableZstdEncoder.Do(func() {
		encoder, err := zstd.NewWriter(nil,
			zstd.WithEncoderConcurrency(1),
			zstd.WithWindowSize(1<<20),
			zstd.WithLowerEncoderMem(true),
			zstd.WithZeroFrames(true),
		)
		if err != nil {
			panic("failed to create block zstd encoder: " + err.Error())
		}
		reusableZstdEncoder.encoder = encoder
	})
	return reusableZstdEncoder.encoder
}

func blockZstdDecoder() *zstd.Decoder {
	reusableZstdDecoder.Do(func() {
		decoder, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxMemory(1<<30),
		)
		if err != nil {
			panic("failed to create block zstd decoder: " + err.Error())
		}
		reusableZstdDecoder.decoder = decoder
	})
	return reusableZstdDecoder.decoder
}

func (ZstdCompressor) Encode(src []byte) []byte {
	// BlockFile emits 64 KiB independent frames. A single-worker Encoder is
	// enough for each call and is safe for concurrent EncodeAll calls; retaining
	// it avoids rebuilding the dependency's CPU-wide weakly held encoder after
	// every memory-pressure GC.
	return blockZstdEncoder().EncodeAll(src, nil)
}

func (ZstdCompressor) Decode(src []byte) ([]byte, error) {
	return blockZstdDecoder().DecodeAll(src, nil)
}

// ─── NoCompressor ─────────────────────────────────────────────────────

// NoCompressor is an identity compressor that passes data through unchanged.
type NoCompressor struct{}

func (n NoCompressor) Encode(src []byte) []byte {
	return append([]byte(nil), src...)
}

func (n NoCompressor) Decode(src []byte) ([]byte, error) {
	return append([]byte(nil), src...), nil
}

// CompressorByName returns a BlockCompressor by name.
// Supported names: "snappy", "lz4", "gzip", "zstd", "none".
// Unknown names default to NoCompressor.
func CompressorByName(name string) BlockCompressor {
	switch name {
	case "snappy":
		return SnappyCompressor{}
	case "lz4":
		return LZ4Compressor{}
	case "gzip":
		return GzipCompressor{}
	case "zstd":
		return ZstdCompressor{}
	default:
		return NoCompressor{}
	}
}
