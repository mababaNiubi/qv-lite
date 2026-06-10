package tsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/mababaNiubi/variant"

	"github.com/pierrec/lz4/v4"
)

// JsonEncoder : variant binary format, LZ4-compressed.
//
// Raw data (before compression):
//
//	[0:4]   count: uint32 LE — number of variant entries
//	For each entry:
//	  [0:4]   length: uint32 LE — byte length of marshaled variant
//	  [4..]   variant bytes — output of variant.MarshalBinary()
//
// The raw data is then compressed with LZ4.
//
// Binary layout (final):
//
//	[0]    marker: jsonCompressed (1 byte)
//	[1:]   LZ4-compressed data (decompress to get the raw layout above)
type JsonEncoder struct {
	list []variant.Variant
}

func NewJsonEncoder() *JsonEncoder {
	return &JsonEncoder{
		list: make([]variant.Variant, 0, 64),
	}
}

func (j *JsonEncoder) Write(v variant.Variant) bool {
	j.list = append(j.list, v)
	return true
}

func (j *JsonEncoder) Bytes() ([]byte, error) {
	var raw bytes.Buffer
	// Write count header.
	var countBuf [4]byte
	binary.LittleEndian.PutUint32(countBuf[:], uint32(len(j.list)))
	raw.Write(countBuf[:])

	for _, v := range j.list {
		data, err := v.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("variant marshal failed: %v", err)
		}
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(data)))
		raw.Write(lenBuf[:])
		raw.Write(data)
	}

	var compressedBuf bytes.Buffer
	lz4Writer := lz4.NewWriter(&compressedBuf)
	if _, err := io.Copy(lz4Writer, &raw); err != nil {
		lz4Writer.Close()
		return nil, fmt.Errorf("lz4 compression failed: %v", err)
	}
	if err := lz4Writer.Close(); err != nil {
		return nil, fmt.Errorf("lz4 close failed: %v", err)
	}
	return append([]byte{jsonCompressed}, compressedBuf.Bytes()...), nil
}

func (j *JsonEncoder) Reset() {
	j.list = j.list[:0]
}

// JsonDecoder decodes LZ4-compressed variant binary data.
type JsonDecoder struct {
	list []variant.Variant
	i    int
	err  error
}

func (j *JsonDecoder) SetBytes(compressedData []byte) {
	if len(compressedData) <= 1 || compressedData[0] != jsonCompressed {
		return
	}
	lz4Reader := lz4.NewReader(bytes.NewReader(compressedData[1:]))
	var raw bytes.Buffer
	if _, err := io.Copy(&raw, lz4Reader); err != nil {
		j.err = fmt.Errorf("lz4 decompression failed: %v", err)
		return
	}
	data := raw.Bytes()
	if len(data) < 4 {
		return
	}
	count := binary.LittleEndian.Uint32(data[:4])
	j.list = make([]variant.Variant, 0, count)
	pos := 4
	for i := uint32(0); i < count; i++ {
		if pos+4 > len(data) {
			j.err = fmt.Errorf("truncated data at entry %d", i)
			return
		}
		itemLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4
		if pos+int(itemLen) > len(data) {
			j.err = fmt.Errorf("truncated data at entry %d", i)
			return
		}
		v, _, err := variant.UnmarshalBinary(data[pos : pos+int(itemLen)])
		if err != nil {
			j.err = fmt.Errorf("variant unmarshal failed at entry %d: %v", i, err)
			return
		}
		j.list = append(j.list, v)
		pos += int(itemLen)
	}
	j.i = -1
}

func (j *JsonDecoder) Next() bool {
	if j.err != nil {
		return false
	}
	j.i++
	return j.i < len(j.list)
}

func (j *JsonDecoder) Read() variant.Variant {
	return j.list[j.i]
}

func (j *JsonDecoder) Error() error {
	return j.err
}
