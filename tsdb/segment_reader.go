package tsdb

import (
	"encoding/binary"
	"io"
)

type FileReader interface {
	OpenReader() error
	CloseReader()
	NextReadFilter(attribute tagCode, starTime int64, endTime int64, tableInfo *TableInfo) (*SegmentHeader, []byte, []byte, error)
	NextRead(checkHead func(SegmentHeader) bool, tableInfo *TableInfo) (*SegmentHeader, []byte, []byte, error)
	ReadAt(offset int64, tableInfo *TableInfo) (*SegmentHeader, []byte, []byte, error)
	GetReadEffectiveSize() int64
}

type fileReader struct {
	filePath            string
	bf                  *BlockFile
	readEffectiveOffset int64
	compressor          BlockCompressor
	needSeek            bool
	cache               *readerCache
}

func NewFileReader(filePath string, compressor BlockCompressor, cache *readerCache) FileReader {
	return &fileReader{
		filePath:   filePath,
		compressor: compressor,
		cache:      cache,
	}
}

func (r *fileReader) OpenReader() error {
	if r.bf != nil {
		// Query-internal segment switch: reuse this reader (and its decode
		// buffer) for the new file instead of round-tripping through the
		// shared reader cache, which can miss under concurrent queries and
		// forces a fresh open + index load per segment.
		if r.bf.Path() != r.filePath {
			if err := r.bf.Rebind(r.filePath, r.compressor); err != nil {
				// The rebind may have closed the old file before failing on
				// the new one; a half-bound BlockFile must not be released
				// back to the shared cache for a later query to pick up.
				r.bf.Drop()
				r.bf = nil
				return err
			}
		}
		r.readEffectiveOffset = 0
		r.needSeek = true
		return nil
	}
	if r.cache != nil {
		if bf := r.cache.acquire(r.filePath); bf != nil {
			r.bf = bf
			r.readEffectiveOffset = 0
			r.needSeek = true
			return nil
		}
	}
	bf, err := OpenBlockFile(r.filePath, r.compressor, BlockSizeDef)
	if err != nil {
		return err
	}
	r.bf = bf
	r.readEffectiveOffset = 0
	r.needSeek = true
	return nil
}

func (r *fileReader) CloseReader() {
	if r.bf == nil {
		return
	}
	if r.cache != nil {
		r.cache.release(r.filePath, r.bf)
	} else {
		r.bf.Drop()
	}
	r.bf = nil
	r.readEffectiveOffset = 0
}

// readBlockRef returns a reference to the decompressed block data starting at
// logical offset off, or a copied slice when the range spans compressed-block
// boundaries (rare: tsdb blocks are far smaller than the 64 KiB block size).
// The reference aliases the BlockFile's decode buffer and stays valid until
// the next read on it — the caller must consume it before reading again.
func (r *fileReader) readBlockRef(off int64, size int) ([]byte, error) {
	data, err := r.bf.ReadBlockFrom(off)
	if err != nil {
		return nil, err
	}
	if len(data) >= size {
		return data[:size], nil
	}
	// Cross compressed-block boundary: assemble a contiguous copy.
	out := make([]byte, size)
	n := copy(out, data)
	for n < size {
		more, err := r.bf.ReadBlockFrom(off + int64(n))
		if err != nil {
			return nil, err
		}
		if len(more) == 0 {
			return nil, io.ErrUnexpectedEOF
		}
		n += copy(out[n:], more)
	}
	return out, nil
}

// readSegmentHeader parses the segment block header at logical offset off.
// When the whole segment block fits in the current decompressed reference the
// payload is returned as a zero-copy slice; otherwise payload is nil and the
// caller must fetch it via readBlockRef (the block spans a compressed-block
// boundary). A nil header means the stream ended (attribute == 0).
func (r *fileReader) readSegmentHeader(off int64) (*SegmentHeader, []byte, error) {
	data, err := r.bf.ReadBlockFrom(off)
	if err != nil {
		return nil, nil, err
	}
	if len(data) < segmentHeaderRawSize {
		data, err = r.readBlockRef(off, segmentHeaderRawSize)
		if err != nil {
			return nil, nil, err
		}
	}
	var header SegmentHeader
	decodeSegmentHeader(data[:segmentHeaderRawSize], &header)
	if header.Attribute == 0 {
		return nil, nil, nil
	}
	total := segmentHeaderRawSize + int(header.DataSize)
	if len(data) >= total {
		return &header, data[segmentHeaderRawSize:total], nil
	}
	return &header, nil, nil
}

func (r *fileReader) NextRead(checkHead func(SegmentHeader) bool, tableInfo *TableInfo) (*SegmentHeader, []byte, []byte, error) {
	if r.bf == nil {
		return nil, nil, nil, ErrorReaderNotOpened
	}
	for {
		if r.readEffectiveOffset >= r.bf.Size() {
			return nil, nil, nil, nil
		}
		if r.needSeek {
			if _, err := r.bf.Seek(r.readEffectiveOffset, io.SeekStart); err != nil {
				return nil, nil, nil, err
			}
			r.needSeek = false
		}
		blockStart := r.readEffectiveOffset
		header, payload, err := r.readSegmentHeader(blockStart)
		if err != nil {
			if err == io.EOF {
				return nil, nil, nil, nil
			}
			return nil, nil, nil, err
		}
		if header == nil {
			return nil, nil, nil, nil
		}

		r.readEffectiveOffset += segmentHeaderSize
		if !checkHead(*header) {
			r.readEffectiveOffset += header.DataSize
			r.needSeek = true
			continue
		}

		if payload == nil {
			payload, err = r.readBlockRef(blockStart, segmentHeaderRawSize+int(header.DataSize))
			if err != nil {
				return nil, nil, nil, err
			}
			payload = payload[segmentHeaderRawSize:]
		}
		r.readEffectiveOffset += header.DataSize
		return r.parseBlock(header, payload, tableInfo)
	}
}

func (r *fileReader) NextReadFilter(attribute tagCode, starTime, endTime int64, tableInfo *TableInfo) (*SegmentHeader, []byte, []byte, error) {
	f := func(header SegmentHeader) bool {
		if header.Attribute != attribute {
			return false
		}
		if header.MaxTime != 0 && header.MaxTime < starTime {
			return false
		}
		if header.MinTime > endTime {
			return false
		}
		return true
	}
	return r.NextRead(f, tableInfo)
}

func (r *fileReader) GetReadEffectiveSize() int64 { return r.readEffectiveOffset }

func (r *fileReader) ReadAt(offset int64, tableInfo *TableInfo) (*SegmentHeader, []byte, []byte, error) {
	if r.bf == nil {
		if r.cache != nil {
			if bf := r.cache.acquire(r.filePath); bf != nil {
				r.bf = bf
			}
		}
		if r.bf == nil {
			bf, err := OpenBlockFile(r.filePath, r.compressor, BlockSizeDef)
			if err != nil {
				return nil, nil, nil, err
			}
			r.bf = bf
		}
	}
	r.needSeek = true

	header, payload, err := r.readSegmentHeader(offset)
	if err != nil {
		return nil, nil, nil, err
	}
	if header == nil {
		return nil, nil, nil, nil
	}
	if payload == nil {
		payload, err = r.readBlockRef(offset, segmentHeaderRawSize+int(header.DataSize))
		if err != nil {
			return nil, nil, nil, err
		}
		payload = payload[segmentHeaderRawSize:]
	}
	return r.parseBlock(header, payload, tableInfo)
}

// parseBlock splits the raw block payload into (time bytes, value bytes) and
// performs range/format checks. CRC is deliberately not re-verified here: the
// BlockFile layer already validates the enclosing compressed block's CRC
// before the payload reaches this point (the value CRC in the segment header
// covers a subset of the same bytes), so a second pass would only double the
// checksum cost per block.
func (r *fileReader) parseBlock(header *SegmentHeader, data []byte, tableInfo *TableInfo) (*SegmentHeader, []byte, []byte, error) {
	if tableInfo.Type == ColumnTypeStructure {
		valueLengthsByteSize := int64(1)
		valueByteLength := int64(0)
		for i := 0; i < len(tableInfo.Structure); i++ {
			valueByteLength += int64(binary.BigEndian.Uint64(data[i*8+1 : (i+1)*8+1]))
			valueLengthsByteSize += 8
		}
		timeDataOffset := valueLengthsByteSize + valueByteLength
		dataValueBytes := data[0:timeDataOffset]
		return header, data[timeDataOffset:], dataValueBytes, nil
	}
	valueByteLength := int64(binary.BigEndian.Uint64(data[0:8]))
	timeDataOffset := 8 + valueByteLength
	dataValueBytes := data[8:timeDataOffset]
	return header, data[timeDataOffset:], dataValueBytes, nil
}
