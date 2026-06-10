package tsdb

import (
	"bytes"
	"math"
	"math/bits"

	"github.com/dgryski/go-bitstream"
	"github.com/mababaNiubi/variant"
)

// Float compression strategy (XOR-delta with leading-zero encoding):
//
// Each value is XORed with the previous value. Leading zero bits in the XOR result are counted;
// every group of 4 leading zeros increments a counter stored in 4 bits. This avoids using 6 bits
// to record the leading zero count.
//
// Binary-to-decimal precision relationship: 10^n ≈ 2^(n×log₂10) where log₂10≈3.3219.
// So ~n×3.3219 binary mantissa bits are needed to represent 10^-n decimal precision.
// A float64 mantissa typically uses 3-4 bits per decimal digit.
//
// For example, 123.12 and 123.13 share the first 4 decimal digits (123.1). Since their ratio
// is less than 2, they have the same exponent, meaning ~12 bits (sign+exponent) + 4×4 mantissa
// bits are identical. Hence we count groups of 4 leading zero bits.
//
// Since 3-4 bits can represent one decimal digit of a float64, we can truncate the low mantissa
// bits based on the configured decimal precision. To preserve accuracy after decoding, the
// truncated decimal value is rounded up to the specified number of decimal places.
// For example, 11.21 with the low 42 mantissa bits zeroed becomes 11.203125, which rounds
// back to 11.21 at two decimal places.
//
// With this algorithm, most consecutive, low-precision data compresses to slightly more than
// 1 byte per value: 4 bits (leading-zero count) + effective mantissa bits.

// precision maps decimal places to the number of mantissa bits to retain.
var precision = [20]int{
	0, 4, 7, 10, 14, 17, 20, 24, 27, 30, 34, 37, 40, 44, 47, 50, 54, 57, 60, 64,
}

func NewFloatEncoder(decimalQuantity uint8) Encoder {
	if decimalQuantity == 0 {
		return &ZeroFloatPrecisionEncoder{
			ZFloatEncoder: &ZFloatEncoder{
				decimalQuantity: decimalQuantity,
			},
			decimalQuantity: 0,
			fls:             make([]variant.Variant, 0, 256),
		}
	}
	return &ZFloatEncoder{
		decimalQuantity: decimalQuantity,
	}
}

type ZeroFloatPrecisionEncoder struct {
	fls []variant.Variant

	decimalQuantity uint8
	*ZFloatEncoder
}

func (s *ZeroFloatPrecisionEncoder) Write(value variant.Variant) bool {
	v, err := value.AsFloat64()
	if err != nil {
		return true
	}
	d := decimalPlacesMath(v)
	if d > s.decimalQuantity {
		s.decimalQuantity = d
	}
	s.fls = append(s.fls, value)
	return true
}

func (s *ZeroFloatPrecisionEncoder) Bytes() ([]byte, error) {
	s.ZFloatEncoder.Reset()
	s.ZFloatEncoder.decimalQuantity = s.decimalQuantity
	for i := range s.fls {
		s.ZFloatEncoder.Write(s.fls[i])
	}
	return s.ZFloatEncoder.Bytes()
}

func (s *ZeroFloatPrecisionEncoder) Reset() {
	s.ZFloatEncoder.Reset()
	s.fls = s.fls[:0]
	s.decimalQuantity = 0
}

// decimalPlacesMath computes the number of decimal places mathematically.
// Returns 0 for special values (NaN/±Inf) and integers; returns up to 20 if precision prevents exact detection.
func decimalPlacesMath(num float64) uint8 {
	// Handle special values.
	if math.IsNaN(num) || math.IsInf(num, 0) {
		return 0
	}

	// Integer: return 0 (truncating the fractional part yields the same number).
	if num == math.Trunc(num) {
		return 0
	}

	// Cap at 20 digits (float64 has ~15-17 significant digits; 20 is ample).
	maxDigits := uint8(20)
	current := num
	digits := uint8(0)

	for digits < maxDigits {
		// Multiply by 10 to shift one decimal place.
		current *= 10
		digits++

		// Check if the current value is an integer (equals itself after truncation).
		if current == math.Trunc(current) {
			return digits
		}
	}

	// Still not an integer after max digits (precision limit); return max digits.
	return maxDigits
}

type ZFloatEncoder struct {
	val float64
	err error

	buf             bytes.Buffer
	bw              *bitstream.BitWriter
	decimalQuantity uint8

	hasFirst bool
	finished bool
}

func (s *ZFloatEncoder) Bytes() ([]byte, error) {
	if !s.finished {
		s.finished = true
		s.bw.Flush(bitstream.Zero)
	}
	return s.buf.Bytes(), s.err
}

func (s *ZFloatEncoder) Write(value variant.Variant) bool {
	if s.finished {
		return false
	}
	v, err := value.AsFloat64()
	if err != nil {
		s.err = err
		return true
	}
	if math.IsNaN(v) {
		s.err = ErrorUnsupportedNaN
		return true
	}
	if math.IsInf(v, 0) {
		s.err = ErrorUnsupportedInf
		return true
	}
	// Round before computing XOR delta so encoder and decoder stay symmetric.
	vr := round(v, int(s.decimalQuantity))
	if !s.hasFirst {
		if s.bw == nil {
			s.bw = bitstream.NewWriter(&s.buf)
		}
		s.buf.WriteByte(floatCompressedXDMI)
		s.err = s.bw.WriteBits(uint64(s.decimalQuantity), 5)
		s.val = vr
		s.hasFirst = true
		err = s.bw.WriteBits(math.Float64bits(vr), 64)
		if err != nil {
			s.err = err
		}
		return true
	}

	// Count leading zeros.
	vDelta := math.Float64bits(vr) ^ math.Float64bits(s.val)
	// Write a bit indicating whether the value changed.
	err = s.bw.WriteBit(vDelta != 0)
	if err != nil {
		return false
	}
	if vDelta == 0 {
		return true
	}

	leading := bits.LeadingZeros64(vDelta)
	// Count groups of 4 identical leading bits.
	n := leading / 4
	// Encode the leading-zero group count in 4 bits.
	if n < 0 || n >= 16 {
		n = 15
	}
	err = s.bw.WriteBits(uint64(n), 4)
	if err != nil {
		s.err = err
		return false
	}
	// Truncate the mantissa according to the configured precision.
	// Compute the actual exponent.
	actualExponent := int((math.Float64bits(vr)>>52)&0x7FF) - 1023
	// Number of mantissa bits to retain.
	retainBit := precision[s.decimalQuantity] + actualExponent
	if retainBit >= 52 || s.decimalQuantity == 0 || retainBit <= 0 {
		// Precision calculation out of range; retain full mantissa.
		retainBit = 52
	}
	if n*4 > 12+retainBit {
		// If the identical prefix covers the effective precision bits, skip storing trailing mantissa.
		return true
	}
	// Store the remaining mantissa bits after truncation.
	bitWn := 12 + retainBit - n*4
	err = s.bw.WriteBits(vDelta>>(52-retainBit), bitWn)
	if err != nil {
		s.err = err
		return true
	}
	s.val = vr
	return true
}

func (s *ZFloatEncoder) Reset() {
	s.val = 0
	s.err = nil
	s.buf.Reset()
	if s.bw == nil {
		s.bw = bitstream.NewWriter(&s.buf)
	}
	s.bw.Resume(0x0, 8)
	s.finished = false
	s.hasFirst = false
	s.decimalQuantity = 0
}

type FloatDecoder struct {
	prevBits uint64

	leading uint64

	br BitReader
	b  []byte

	start           bool
	finished        bool
	decimalQuantity uint8
	decPlaces       int     // int(decimalQuantity), pre-computed
	scale           float64 // math.Pow(10, decPlaces), pre-computed; 0 means lossless
	err             error
}

// SetBytes initializes the decoder with b. Must call before calling Next().
func (it *FloatDecoder) SetBytes(b []byte) {
	var v uint64
	if len(b) == 0 {
		it.finished = true
		return
	}
	it.br.Reset(b[1:])
	it.prevBits = v
	it.leading = 0
	it.b = b
	it.start = true
	it.finished = false
	it.err = nil
	bits, err := it.br.ReadBits(5)
	if err != nil {
		it.err = err
		return
	}
	it.decimalQuantity = uint8(bits)
	it.decPlaces = int(it.decimalQuantity)
	if it.decPlaces > 0 {
		it.scale = math.Pow(10, float64(it.decPlaces))
	}
}

// Next returns true if there are remaining values to read.
func (it *FloatDecoder) Next() bool {
	if it.err != nil || it.finished {
		return false
	}

	if it.start {
		it.start = false
		var err error
		it.prevBits, err = it.br.ReadBits(64)
		if err != nil {
			it.err = err
			return false
		}
		return true
	}
	// Check if the value is unchanged.
	if it.br.CanReadBitFast() {
		if !it.br.ReadBitFast() {
			return true
		}
	} else if v, err := it.br.ReadBit(); err != nil {
		it.err = err
		return false
	} else if !v {
		return true
	}

	// Read leading zero count.
	readBits, err := it.br.ReadBits(4)
	if err != nil {
		it.err = err
		return false
	}
	leading := readBits * 4
	if leading < 12 {
		// Read data up to the exponent bits and extract the exponent.
		remainingDigits := uint(12 - leading)
		readBits, err = it.br.ReadBits(remainingDigits)
		if err != nil {
			it.err = err
			return false
		}
		exponentBits := it.prevBits ^ (readBits << 52)
		actualExponent := int(exponentBits>>52&0x7FF) - 1023
		// Number of mantissa bits to retain.
		retainBit := precision[it.decimalQuantity] + actualExponent
		if retainBit >= 52 || it.decimalQuantity == 0 || retainBit <= 0 {
			// Precision calculation out of range; retain full mantissa.
			retainBit = 52
		}
		// Read remaining mantissa bits.
		digi, err := it.br.ReadBits(uint(retainBit))
		if err != nil {
			it.err = err
			return false
		}
		retainBit = 52 - retainBit
		mask := ^uint64(0) << retainBit
		it.prevBits = it.roundBits(it.prevBits&mask ^ (readBits<<52 | (digi << retainBit)))
		return true
	}
	if leading >= 60 {
		// Preserve full precision (only 4 bits differ).
		readBits, err = it.br.ReadBits(4)
		if err != nil {
			it.err = err
			return false
		}
		it.prevBits = it.prevBits ^ readBits
		return true
	}
	// Compute the previous value's exponent (used for precision recovery).
	prevExponent := int((it.prevBits>>52)&0x7FF) - 1023
	// Compute the mantissa bits to retain (consistent with encoder logic).
	retainBitBase := precision[it.decimalQuantity] + prevExponent
	if retainBitBase >= 52 || it.decimalQuantity == 0 || retainBitBase <= 0 {
		retainBitBase = 52 // Out of range; retain full mantissa.
	}
	// Read the remaining significant bits.
	retainBit := 52 - retainBitBase // Right-shift amount (matches encoder).
	if int(leading) > 12+retainBitBase {
		return true
	}
	// Compute effective bit length (number of bits written by the encoder).
	bitsLen := int(64-leading) - retainBit
	readBits, err = it.br.ReadBits(uint(bitsLen))
	if err != nil {
		it.err = err
		return false
	}
	mask := ^uint64(0) << retainBit
	it.prevBits = it.roundBits(it.prevBits&mask ^ (readBits << retainBit))
	return true
}

// roundBits rounds the float64 value encoded in x to it.decPlaces decimal places,
// returning the result as uint64 bits. When decPlaces == 0 (lossless), x is returned
// unchanged, avoiding the float64 conversion entirely.
func (it *FloatDecoder) roundBits(x uint64) uint64 {
	if it.decPlaces == 0 {
		return x
	}
	v := math.Float64frombits(x)
	product := v * it.scale
	if math.IsInf(product, 0) {
		return x
	}
	return math.Float64bits(math.Ceil(product-1e-9) / it.scale)
}

// round rounds num to n decimal places (n >= 0) using ceiling rounding.
// A tiny epsilon is subtracted before Ceil to prevent overshoot when the
// reconstructed value lands just above the exact decimal boundary.
func round(num float64, n int) float64 {
	if n < 0 {
		return num
	}
	scale := math.Pow(10, float64(n))
	if math.IsInf(scale, 1) {
		return num
	}
	product := num * scale
	if math.IsInf(product, 0) {
		return num
	}
	return math.Ceil(product-1e-9) / scale
}

// Values returns the current float64 value.
func (it *FloatDecoder) Values() float64 {
	return math.Float64frombits(it.prevBits)
}

func (it *FloatDecoder) Read() variant.Variant {
	return variant.NewFloat64(math.Float64frombits(it.prevBits))
}

// Error returns the current decoding error.
func (it *FloatDecoder) Error() error {
	return it.err
}
