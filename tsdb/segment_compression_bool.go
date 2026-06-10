package tsdb

import (
	"encoding/binary"
	"fmt"

	"github.com/mababaNiubi/variant"
)

// BooleanEncoder Boolean encoding uses two strategies depending on the data pattern:
//
// Bit-packed (booleanCompressedBitPacked):
//
//	Each boolean is packed as 1 bit into consecutive bytes. 8 booleans per byte,
//	MSB first. A trailing partial byte is left-aligned (shifted by 8-i bits).
//
// Run-length (booleanCompressedRLETrue / booleanCompressedRLEFalse):
//
//	All values are identical (all true or all false). The value is inferred from
//	the marker byte; only the count is stored.
//
// Binary layout:
//
//	[0]    marker: booleanCompressedBitPacked / RLETrue / RLEFalse (1 byte)
//	[1:9]  count: uint64 BE — total number of booleans encoded
//	[9:]   payload:
//	         bit-packed: packed bytes (MSB-first, trailing byte left-aligned)
//	         RLE:        empty (count alone determines output)
type BooleanEncoder struct {
	// The encoded bytes
	bytes []byte
	// The current byte being encoded
	b byte
	// The number of booleans packed into b
	i int
	//Determine whether to use RLE
	rle      bool
	lastBool bool
	number   int
	err      error
}

func NewBooleanEncoder() *BooleanEncoder {
	return &BooleanEncoder{
		bytes: make([]byte, 0, 64),
	}
}

func (e *BooleanEncoder) Reset() {
	e.bytes = e.bytes[:0]
	e.b = 0
	e.i = 0
	e.number = 0
}

func (e *BooleanEncoder) Write(bv variant.Variant) bool {
	b, err := bv.AsBool()
	if err != nil {
		e.err = err
		return true
	}
	if e.number == 0 {
		e.lastBool = b
		e.rle = true
	}
	if e.lastBool != b && e.rle == true {
		//Cancel the same value and fill in the byte
		e.rle = false
		for i := 0; i < e.number>>3; i++ {
			if e.lastBool {
				e.bytes = append(e.bytes, 255)
			} else {
				e.bytes = append(e.bytes, 0)
			}
		}
		e.i = e.number % 8
		if e.i != 0 && e.lastBool {
			e.b |= byte((1 << e.i) - 1)
		}
	}
	if !e.rle {
		// offset by one bit and then set
		e.b = e.b << 1
		if b {
			e.b |= 1
		}
		e.i++
		// If there are more than 8 booleans, encode them
		if e.i >= 8 {
			e.bytes = append(e.bytes, e.b)
			e.b = 0
			e.i = 0
		}
	}
	e.number++
	return true
}

func (e *BooleanEncoder) Bytes() ([]byte, error) {
	b := make([]byte, 0, 9+len(e.bytes)+1)
	if !e.rle {
		b = append(b, booleanCompressedBitPacked)
	} else if e.lastBool {
		b = append(b, booleanCompressedRLETrue)
	} else {
		b = append(b, booleanCompressedRLEFalse)
	}
	b = append(b, 0, 0, 0, 0, 0, 0, 0, 0) // placeholder for count
	if !e.rle {
		b = append(b, e.bytes...)
		if e.i > 0 {
			b = append(b, e.b<<(8-e.i))
		}
	}
	binary.BigEndian.PutUint64(b[1:], uint64(e.number))
	return b, nil
}

type BooleanDecoder struct {
	//Determine whether to use RLE
	rle      bool
	number   int
	lastBool bool

	b   []byte
	i   int
	err error
}

func (e *BooleanDecoder) SetBytes(b []byte) {
	if len(b) == 0 {
		return
	}
	switch b[0] {
	case booleanCompressedRLETrue:
		e.rle = true
		e.lastBool = true
	case booleanCompressedRLEFalse:
		e.rle = true
		e.lastBool = false
	case booleanCompressedBitPacked:
		e.rle = false
		e.b = b[9:]
	default:
		e.err = fmt.Errorf("BooleanDecoder: invalid encoding type %d", b[0])
		return
	}
	e.number = int(binary.BigEndian.Uint64(b[1:]))
}

func (e *BooleanDecoder) Next() bool {
	if e.err != nil {
		return false
	}
	e.i++
	return e.i <= e.number
}

func (e *BooleanDecoder) Read() variant.Variant {
	if e.rle {
		return variant.NewBool(e.lastBool)
	}

	index := e.i - 1

	// The mask to select the bit
	mask := byte(1 << uint(7-index&0x7))

	// The packed byte
	v := e.b[index>>3]
	return variant.NewBool(v&mask == mask)
}

func (e *BooleanDecoder) Error() error {
	return e.err
}
