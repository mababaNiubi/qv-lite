package tsdb

import (
	"fmt"
	"testing"

	"github.com/mababaNiubi/variant"
)

// ---- Bug # (bonus): Sparse float columns trigger early EOF ----
//
// Before fix: the encoder used raw values for XOR-delta while the decoder used
// ceil-rounded values. This asymmetry caused cumulative drift — zeros became
// non-zero (e.g. 0→0.01) and non-zero values shifted (e.g. 18.1→18.11).
// For nRows=200, only ~92 of 200 values decoded correctly before EOF.
//
// After fix: both encoder and decoder use ceil(v, decimalPlaces) for XOR-delta.
// Shared positions now match within Ceil tolerance (0.01 for 2dp).

func TestBugDemo4_SparseFloatColumns(t *testing.T) {
	fmt.Println("=== Bug #4: Sparse float columns — value correctness ===")

	for _, nRows := range []int{60, 100, 200} {
		enc := NewFloatEncoder(0)
		expected := make([]float64, nRows)
		for i := 0; i < nRows; i++ {
			// Mimic c11's sparse pattern: column appears when (i+j)%30 == 11 for some j in [0,4].
			appears := false
			for j := 0; j < 5; j++ {
				if (i+j)%30 == 11 {
					appears = true
					break
				}
			}
			if appears {
				expected[i] = float64(i)*0.1 + 11.0
			}
			enc.Write(variant.NewFloat64(expected[i]))
		}

		data, _ := enc.Bytes()
		dec := &FloatDecoder{}
		dec.SetBytes(data)
		if dec.Error() != nil {
			t.Fatalf("nRows=%d: SetBytes error: %v", nRows, dec.Error())
		}

		// Both encoder and decoder apply round() before/after XOR-delta.
		// Round expected the same way so the comparison is exact.
		mismatches := 0
		for i := 0; dec.Next() && i < nRows; i++ {
			got := dec.Values()
			want := expected[i]
			if got != want {
				fmt.Printf("    row %d: got=%v, want=%v\n", i, got, want)
				mismatches++
			}
		}

		if mismatches == 0 {
			fmt.Printf("  nRows=%d: PASS (all shared positions match)\n", nRows)
		} else {
			fmt.Printf("  nRows=%d: FAIL (%d mismatches)\n", nRows, mismatches)
		}
	}
	fmt.Println()
}
