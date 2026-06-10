package tsdb

import (
	"encoding/binary"
	"hash/crc32"
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
	dataBuf             []byte
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
		var header SegmentHeader
		if err := binary.Read(r.bf, binary.BigEndian, &header); err != nil {
			if err == io.EOF {
				return nil, nil, nil, nil
			}
			return nil, nil, nil, err
		}
		if header.Attribute == 0 {
			return nil, nil, nil, nil
		}

		r.readEffectiveOffset += segmentHeaderSize
		if !checkHead(header) {
			r.readEffectiveOffset += header.DataSize
			r.needSeek = true
			continue
		}

		if cap(r.dataBuf) < int(header.DataSize) {
			r.dataBuf = make([]byte, header.DataSize)
		}
		data := r.dataBuf[:header.DataSize]
		if _, err := io.ReadFull(r.bf, data); err != nil {
			return nil, nil, nil, err
		}
		r.readEffectiveOffset += header.DataSize
		return r.parseBlock(&header, data, tableInfo)
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

	if _, err := r.bf.Seek(offset, io.SeekStart); err != nil {
		return nil, nil, nil, err
	}

	var header SegmentHeader
	if err := binary.Read(r.bf, binary.BigEndian, &header); err != nil {
		return nil, nil, nil, err
	}
	if header.Attribute == 0 {
		return nil, nil, nil, nil
	}
	if cap(r.dataBuf) < int(header.DataSize) {
		r.dataBuf = make([]byte, header.DataSize)
	}
	data := r.dataBuf[:header.DataSize]
	if _, err := io.ReadFull(r.bf, data); err != nil {
		return nil, nil, nil, err
	}
	return r.parseBlock(&header, data, tableInfo)
}

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
		if crc32.ChecksumIEEE(dataValueBytes) != header.Crc {
			return nil, nil, nil, ErrorCRCCheckFailed
		}
		return header, data[timeDataOffset:], dataValueBytes, nil
	}
	valueByteLength := int64(binary.BigEndian.Uint64(data[0:8]))
	timeDataOffset := 8 + valueByteLength
	dataValueBytes := data[8:timeDataOffset]
	if crc32.ChecksumIEEE(dataValueBytes) != header.Crc {
		return nil, nil, nil, ErrorCRCCheckFailed
	}
	return header, data[timeDataOffset:], dataValueBytes, nil
}
