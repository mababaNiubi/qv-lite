package tsdb

import (
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/mababaNiubi/variant"
)

// ---- Struct mode roundtrip ----

func TestAdaptColumnEncoder_StructRoundTrip(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)
	original := []variant.Variant{
		variant.New(map[string]interface{}{
			"name": "alice", "age": 30, "score": 95.5, "active": true,
		}),
		variant.New(map[string]interface{}{
			"name": "bob", "age": 25, "score": 87.0, "active": false,
		}),
		variant.New(map[string]interface{}{
			"name": "carol", "age": 28, "score": 91.2, "active": true,
		}),
	}

	for _, v := range original {
		if !enc.Write(v) {
			t.Fatal("Write returned false")
		}
	}

	data, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty bytes")
	}

	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	if len(decoded) != len(original) {
		t.Fatalf("count mismatch: got %d, want %d", len(decoded), len(original))
	}
	for i := range original {
		if !original[i].IsEqual(decoded[i]) {
			t.Errorf("row %d mismatch:\n  want: %s\n  got:  %s",
				i, original[i].AsString(), decoded[i].AsString())
		}
	}
}

// ---- Non-struct mode roundtrip ----

func TestAdaptColumnEncoder_NonStructInt(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)
	vals := []variant.Variant{
		variant.NewInt64(10),
		variant.NewInt64(20),
		variant.NewInt64(30),
	}
	for _, v := range vals {
		if !enc.Write(v) {
			t.Fatal("Write returned false")
		}
	}

	data, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	if len(decoded) != len(vals) {
		t.Fatalf("count mismatch: got %d, want %d", len(decoded), len(vals))
	}
	for i := range vals {
		if !vals[i].IsEqual(decoded[i]) {
			t.Errorf("row %d mismatch: want %v, got %v",
				i, vals[i].AsString(), decoded[i].AsString())
		}
	}
}

func TestAdaptColumnEncoder_NonStructFloat(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)
	le := 2590001
	vals := make([]variant.Variant, le)
	for i := range le {
		vals[i] = variant.NewFloat64(float64(i) * 0.01)
	}
	for _, v := range vals {
		if !enc.Write(v) {
			t.Fatal("Write returned false")
		}
	}

	data, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if dec.Error() != nil && dec.Error() != io.EOF {
		t.Fatal(dec.Error())
	}
	if len(decoded) < le {
		t.Fatalf("count mismatch: got %d, want %d", len(decoded), le)
	}
	for i := range vals {
		a, _ := vals[i].AsFloat64()
		b, _ := decoded[i].AsFloat64()
		if math.Abs(a-b) > 0.001 {
			t.Errorf("row %d: want %f, got %f", i, a, b)
		}
	}
}

func TestAdaptColumnEncoder_NonStructString(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)
	vals := []variant.Variant{
		variant.NewString("hello"),
		variant.NewString("world"),
	}
	for _, v := range vals {
		if !enc.Write(v) {
			t.Fatal("Write returned false")
		}
	}

	data, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	if len(decoded) != len(vals) {
		t.Fatalf("count mismatch: got %d, want %d", len(decoded), len(vals))
	}
	for i := range vals {
		if !vals[i].IsEqual(decoded[i]) {
			t.Errorf("row %d: want %s, got %s",
				i, vals[i].AsString(), decoded[i].AsString())
		}
	}
}

func TestAdaptColumnEncoder_NonStructBool(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)
	vals := []variant.Variant{
		variant.NewBool(true),
		variant.NewBool(false),
		variant.NewBool(true),
	}
	for _, v := range vals {
		if !enc.Write(v) {
			t.Fatal("Write returned false")
		}
	}

	data, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	if len(decoded) != len(vals) {
		t.Fatalf("count mismatch: got %d, want %d", len(decoded), len(vals))
	}
	for i := range vals {
		if !vals[i].IsEqual(decoded[i]) {
			t.Errorf("row %d mismatch: want %v, got %v",
				i, vals[i].AsString(), decoded[i].AsString())
		}
	}
}

// ---- Dynamic column addition ----

func TestAdaptColumnEncoder_DynamicColumns(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)

	// Row 1: only "a"
	if !enc.Write(variant.New(map[string]interface{}{"a": 1})) {
		t.Fatal("Write 1 returned false")
	}
	// Row 2: "a" and new column "b"
	if !enc.Write(variant.New(map[string]interface{}{"a": 2, "b": "x"})) {
		t.Fatal("Write 2 returned false")
	}
	// Row 3: "a", "b", and new column "c"
	if !enc.Write(variant.New(map[string]interface{}{"a": 3, "b": "y", "c": 1.5})) {
		t.Fatal("Write 3 returned false")
	}

	data, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	if len(decoded) != 3 {
		t.Fatalf("count mismatch: got %d, want 3", len(decoded))
	}

	// Row 1: "b" and "c" were backfilled as zero values.
	v, _ := decoded[0].MapGet("a")
	if n, _ := v.AsInt64(); n != 1 {
		t.Errorf("row 0 a: want 1, got %d", n)
	}
	// Backfilled string column: empty string.
	if v, ok := decoded[0].MapGet("b"); ok {
		if s := v.AsString(); s != "" {
			t.Errorf("row 0 b: want empty string, got %s", s)
		}
	}
	// Backfilled float column: zero.
	if v, ok := decoded[0].MapGet("c"); ok {
		if f, _ := v.AsFloat64(); !variant.IsFloat64Equal(f, 0) {
			t.Errorf("row 0 c: want 0, got %f", f)
		}
	}

	// Row 3: all columns present.
	v, _ = decoded[2].MapGet("a")
	if n, _ := v.AsInt64(); n != 3 {
		t.Errorf("row 2 a: want 3, got %d", n)
	}
	v, _ = decoded[2].MapGet("b")
	if s := v.AsString(); s != "y" {
		t.Errorf("row 2 b: want y, got %s", s)
	}
}

// ---- Sparse rows (missing columns) ----

func TestAdaptColumnEncoder_SparseRows(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)

	if !enc.Write(variant.New(map[string]interface{}{"x": 1, "y": "a"})) {
		t.Fatal("Write 1")
	}
	if !enc.Write(variant.New(map[string]interface{}{"x": 2})) { // "y" missing
		t.Fatal("Write 2")
	}
	if !enc.Write(variant.New(map[string]interface{}{"y": "c"})) { // "x" missing
		t.Fatal("Write 3")
	}

	data, _ := enc.Bytes()
	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}

	if len(decoded) != 3 {
		t.Fatalf("count: got %d, want 3", len(decoded))
	}

	// Row 2: x present, y backfilled as empty string.
	v1, _ := decoded[1].MapGet("x")
	if n, _ := v1.AsInt64(); n != 2 {
		t.Errorf("row1 x: want 2, got %d", n)
	}
	v2, _ := decoded[1].MapGet("y")
	if s := v2.AsString(); s != "" {
		t.Errorf("row1 y: want empty string, got %s", s)
	}

	// Row 3: x backfilled as zero, y present.
	v3, _ := decoded[2].MapGet("x")
	if n, _ := v3.AsInt64(); n != 0 {
		t.Errorf("row2 x: want 0, got %d", n)
	}
	v4, _ := decoded[2].MapGet("y")
	if s := v4.AsString(); s != "c" {
		t.Errorf("row2 y: want c, got %s", s)
	}
}

// ---- Nested Map ----

func TestAdaptColumnEncoder_NestedMap(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)

	inner1 := variant.New(map[string]interface{}{"k1": 100, "k2": "inner"})
	inner2 := variant.New(map[string]interface{}{"k1": 200, "k2": "inner2"})

	if !enc.Write(variant.New(map[string]interface{}{"id": 1, "meta": inner1.AsInterface()})) {
		t.Fatal("Write 1")
	}
	if !enc.Write(variant.New(map[string]interface{}{"id": 2, "meta": inner2.AsInterface()})) {
		t.Fatal("Write 2")
	}

	data, _ := enc.Bytes()
	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}

	if len(decoded) != 2 {
		t.Fatalf("count: got %d, want 2", len(decoded))
	}

	// Verify nested map in row 0.
	meta0, ok := decoded[0].MapGet("meta")
	if !ok {
		t.Fatal("meta missing in row 0")
	}
	v, _ := meta0.MapGet("k1")
	if n, _ := v.AsInt64(); n != 100 {
		t.Errorf("meta.k1 row0: want 100, got %d", n)
	}

	// Verify nested map in row 1.
	meta1, ok := decoded[1].MapGet("meta")
	if !ok {
		t.Fatal("meta missing in row 1")
	}
	v, _ = meta1.MapGet("k1")
	if n, _ := v.AsInt64(); n != 200 {
		t.Errorf("meta.k1 row1: want 200, got %d", n)
	}
}

// ---- Type change (glow) ----

func TestAdaptColumnEncoder_TypeChange(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)

	// Write an int column.
	if !enc.Write(variant.New(map[string]interface{}{"v": 1})) {
		t.Fatal("Write 1")
	}
	if !enc.Write(variant.New(map[string]interface{}{"v": 2})) {
		t.Fatal("Write 2")
	}
	// Type change: int -> string. Should return false.
	if enc.Write(variant.New(map[string]interface{}{"v": "hello"})) {
		t.Fatal("expected false on type change")
	}

	// Encoder should still have the first 2 rows intact (no partial writes).
	data, _ := enc.Bytes()
	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	if len(decoded) != 2 {
		t.Fatalf("count: got %d, want 2", len(decoded))
	}
	v, _ := decoded[0].MapGet("v")
	if n, _ := v.AsInt64(); n != 1 {
		t.Errorf("row0: want 1, got %d", n)
	}
	v, _ = decoded[1].MapGet("v")
	if n, _ := v.AsInt64(); n != 2 {
		t.Errorf("row1: want 2, got %d", n)
	}
}

// ---- Non-struct type change ----

func TestAdaptColumnEncoder_NonStructTypeChange(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)

	if !enc.Write(variant.NewInt64(10)) {
		t.Fatal("Write int")
	}
	// Type change: int -> string should fail and not corrupt existing data.
	if enc.Write(variant.NewString("hi")) {
		t.Fatal("expected false on non-struct type change")
	}

	// Existing data intact.
	data, _ := enc.Bytes()
	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if len(decoded) != 1 {
		t.Fatalf("count: got %d, want 1", len(decoded))
	}
	n, _ := decoded[0].AsInt64()
	if n != 10 {
		t.Errorf("want 10, got %d", n)
	}
}

// ---- Non-struct -> struct mode switch ----

func TestAdaptColumnEncoder_NonStructToStructSwitch(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)

	// First write a non-Map to enter non-struct mode.
	if !enc.Write(variant.NewInt64(42)) {
		t.Fatal("Write int")
	}
	// Then write a Map — should fail since we can't switch mid-stream.
	if enc.Write(variant.New(map[string]interface{}{"a": 1})) {
		t.Fatal("expected false on mid-stream mode switch")
	}

	// Start fresh: first value is Map, enters struct mode directly.
	enc2 := NewAdaptColumnEncoder(2)
	if !enc2.Write(variant.New(map[string]interface{}{"a": 1})) {
		t.Fatal("Write struct")
	}
	if !enc2.Write(variant.New(map[string]interface{}{"a": 2})) {
		t.Fatal("Write struct 2")
	}

	data, _ := enc2.Bytes()
	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if len(decoded) != 2 {
		t.Fatalf("count: got %d, want 2", len(decoded))
	}
}

// ---- Empty encoder ----

func TestAdaptColumnEncoder_Empty(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)
	data, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 2 {
		t.Fatal("expected at least 2-byte header")
	}

	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}
	if dec.Next() {
		t.Fatal("expected no rows from empty encoder")
	}
}

// ---- Single row ----

func TestAdaptColumnEncoder_SingleRow(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)
	if !enc.Write(variant.New(map[string]interface{}{"x": 1})) {
		t.Fatal("Write")
	}

	data, _ := enc.Bytes()
	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)

	if !dec.Next() {
		t.Fatal("expected one row")
	}
	row := dec.Read()
	v, _ := row.MapGet("x")
	if n, _ := v.AsInt64(); n != 1 {
		t.Errorf("want 1, got %d", n)
	}
	if dec.Next() {
		t.Fatal("expected only one row")
	}
}

// ---- Multiple types in one struct ----

func TestAdaptColumnEncoder_MixedTypes(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)

	original := []variant.Variant{
		variant.New(map[string]interface{}{
			"intV":   int64(10),
			"floatV": 3.14,
			"strV":   "foo",
			"boolV":  true,
		}),
		variant.New(map[string]interface{}{
			"intV":   int64(20),
			"floatV": 2.71,
			"strV":   "bar",
			"boolV":  false,
		}),
	}

	for _, v := range original {
		if !enc.Write(v) {
			t.Fatal("Write returned false")
		}
	}

	data, _ := enc.Bytes()
	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	if len(decoded) != 2 {
		t.Fatalf("count: got %d, want 2", len(decoded))
	}
	for i := range original {
		if !original[i].IsEqual(decoded[i]) {
			t.Errorf("row %d mismatch:\n  want: %s\n  got:  %s",
				i, original[i].AsString(), decoded[i].AsString())
		}
	}
}

// ---- Invalid marker byte ----

func TestAdaptColumnDecoder_InvalidMarker(t *testing.T) {
	dec := &AdaptColumnDecoder{}
	dec.SetBytes([]byte{0xFF, 0x00})
	if dec.Error() == nil {
		t.Fatal("expected error for invalid marker")
	}
}

// ---- Too-short data ----

func TestAdaptColumnDecoder_TooShort(t *testing.T) {
	dec := &AdaptColumnDecoder{}
	dec.SetBytes([]byte{byte(adaptColumnCompressed)})
	if dec.Error() == nil {
		t.Fatal("expected error for too-short data")
	}
}

// ---- Reset ----

func TestAdaptColumnEncoder_Reset(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)
	enc.Write(variant.New(map[string]interface{}{"a": 1}))
	enc.Write(variant.New(map[string]interface{}{"a": 2}))

	enc.Reset()

	// After reset, should behave like fresh encoder.
	if !enc.Write(variant.New(map[string]interface{}{"b": "new"})) {
		t.Fatal("Write after reset")
	}

	data, _ := enc.Bytes()
	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)

	if !dec.Next() {
		t.Fatal("expected one row")
	}
	row := dec.Read()
	v, _ := row.MapGet("b")
	if s := v.AsString(); s != "new" {
		t.Errorf("want new, got %s", s)
	}
	if _, ok := row.MapGet("a"); ok {
		t.Fatal("column 'a' should not exist after reset")
	}
}

// ---- Multi-segment: columns added and removed across segments (glow/restructure) ----

func TestAdaptColumnEncoder_MultiSegmentColumnChurn(t *testing.T) {
	// Simulate 4 consecutive segments where the schema changes each time:
	//   seg1: {a: int, b: string}
	//   seg2: {a: int, c: float64}          — b removed, c added
	//   seg3: {d: bool, e: string}          — a/c removed, d/e added
	//   seg4: {a: int, b: string, d: bool}  — a/b return, d stays, e removed
	//
	// Each segment is a separate encoder (glow restructure).

	type segment struct {
		rows []variant.Variant
	}

	segments := []segment{
		{
			rows: []variant.Variant{
				variant.New(map[string]interface{}{"a": 1, "b": "foo"}),
				variant.New(map[string]interface{}{"a": 2, "b": "bar"}),
				variant.New(map[string]interface{}{"a": 3, "b": "baz"}),
			},
		},
		{
			rows: []variant.Variant{
				variant.New(map[string]interface{}{"a": 10, "c": 1.5}),
				variant.New(map[string]interface{}{"a": 20, "c": 2.5}),
			},
		},
		{
			rows: []variant.Variant{
				variant.New(map[string]interface{}{"d": true, "e": "x"}),
				variant.New(map[string]interface{}{"d": false, "e": "y"}),
				variant.New(map[string]interface{}{"d": true, "e": "z"}),
			},
		},
		{
			rows: []variant.Variant{
				variant.New(map[string]interface{}{"a": 100, "b": "back", "d": false}),
				variant.New(map[string]interface{}{"a": 200, "b": "again", "d": true}),
			},
		},
	}

	for segIdx, seg := range segments {
		enc := NewAdaptColumnEncoder(2)
		for _, v := range seg.rows {
			if !enc.Write(v) {
				t.Fatalf("seg %d: Write returned false", segIdx)
			}
		}

		data, err := enc.Bytes()
		if err != nil {
			t.Fatalf("seg %d: Bytes error: %v", segIdx, err)
		}

		dec := &AdaptColumnDecoder{}
		dec.SetBytes(data)
		if dec.Error() != nil {
			t.Fatalf("seg %d: SetBytes error: %v", segIdx, dec.Error())
		}

		var decoded []variant.Variant
		for dec.Next() {
			decoded = append(decoded, dec.Read())
		}
		if dec.Error() != nil {
			t.Fatalf("seg %d: decode error: %v", segIdx, dec.Error())
		}

		if len(decoded) != len(seg.rows) {
			t.Fatalf("seg %d: count mismatch: got %d, want %d", segIdx, len(decoded), len(seg.rows))
		}
		for i := range seg.rows {
			if !seg.rows[i].IsEqual(decoded[i]) {
				t.Errorf("seg %d row %d mismatch:\n  want: %s\n  got:  %s",
					segIdx, i, seg.rows[i].AsString(), decoded[i].AsString())
			}
		}
	}
}

// ---- Columns appear and disappear within a single segment ----

func TestAdaptColumnEncoder_ColumnAppearDisappearReappear(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)

	// Row 0: {a: 1, b: "x"}        — introduces a(int), b(string)
	if !enc.Write(variant.New(map[string]interface{}{"a": 1, "b": "x"})) {
		t.Fatal("Write 0")
	}
	// Row 1: {a: 2, c: 3.0}        — b missing, c(float) appears
	if !enc.Write(variant.New(map[string]interface{}{"a": 2, "c": 3.0})) {
		t.Fatal("Write 1")
	}
	// Row 2: {b: "y", c: 4.0}      — a missing, b reappears
	if !enc.Write(variant.New(map[string]interface{}{"b": "y", "c": 4.0})) {
		t.Fatal("Write 2")
	}
	// Row 3: {a: 5, b: "z", c: 6.0} — all present
	if !enc.Write(variant.New(map[string]interface{}{"a": 5, "b": "z", "c": 6.0})) {
		t.Fatal("Write 3")
	}

	data, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	if len(decoded) != 4 {
		t.Fatalf("count: got %d, want 4", len(decoded))
	}

	// Row 0: a=1, b="x", c backfilled as 0
	if v, ok := decoded[0].MapGet("a"); ok {
		if n, _ := v.AsInt64(); n != 1 {
			t.Errorf("row0 a: want 1, got %d", n)
		}
	} else {
		t.Error("row0 missing a")
	}
	if v, ok := decoded[0].MapGet("b"); ok {
		if s := v.AsString(); s != "x" {
			t.Errorf("row0 b: want x, got %s", s)
		}
	} else {
		t.Error("row0 missing b")
	}
	if v, ok := decoded[0].MapGet("c"); ok {
		if f, _ := v.AsFloat64(); !variant.IsFloat64Equal(f, 0) {
			t.Errorf("row0 c: want 0, got %f", f)
		}
	}

	// Row 1: a=2, c=3.0, b backfilled as ""
	if v, ok := decoded[1].MapGet("a"); ok {
		if n, _ := v.AsInt64(); n != 2 {
			t.Errorf("row1 a: want 2, got %d", n)
		}
	}
	if v, ok := decoded[1].MapGet("b"); ok {
		if s := v.AsString(); s != "" {
			t.Errorf("row1 b: want empty, got %s", s)
		}
	}
	if v, ok := decoded[1].MapGet("c"); ok {
		if f, _ := v.AsFloat64(); math.Abs(f-3.0) > 0.001 {
			t.Errorf("row1 c: want 3.0, got %f", f)
		}
	}

	// Row 2: b="y", c=4.0, a backfilled as 0
	if v, ok := decoded[2].MapGet("a"); ok {
		if n, _ := v.AsInt64(); n != 0 {
			t.Errorf("row2 a: want 0, got %d", n)
		}
	}
	if v, ok := decoded[2].MapGet("b"); ok {
		if s := v.AsString(); s != "y" {
			t.Errorf("row2 b: want y, got %s", s)
		}
	}
	if v, ok := decoded[2].MapGet("c"); ok {
		if f, _ := v.AsFloat64(); math.Abs(f-4.0) > 0.001 {
			t.Errorf("row2 c: want 4.0, got %f", f)
		}
	}

	// Row 3: all present
	if v, ok := decoded[3].MapGet("a"); ok {
		if n, _ := v.AsInt64(); n != 5 {
			t.Errorf("row3 a: want 5, got %d", n)
		}
	}
	if v, ok := decoded[3].MapGet("b"); ok {
		if s := v.AsString(); s != "z" {
			t.Errorf("row3 b: want z, got %s", s)
		}
	}
	if v, ok := decoded[3].MapGet("c"); ok {
		if f, _ := v.AsFloat64(); math.Abs(f-6.0) > 0.001 {
			t.Errorf("row3 c: want 6.0, got %f", f)
		}
	}
}

// ---- Continuous column growth: one new column per row ----

func TestAdaptColumnEncoder_ContinuousColumnGrowth(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)

	// Add a new column on every row, 10 columns total.
	// Row i has columns col0, col1, ..., coli.
	nCols := 10
	for i := 0; i < nCols; i++ {
		m := make(map[string]interface{})
		for j := 0; j <= i; j++ {
			m[fmt.Sprintf("col%d", j)] = int64(j*100 + i)
		}
		if !enc.Write(variant.New(m)) {
			t.Fatalf("Write %d returned false", i)
		}
	}

	data, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	if len(decoded) != nCols {
		t.Fatalf("count: got %d, want %d", len(decoded), nCols)
	}

	// Row i: columns col0..coli present, col(i+1)..col(n-1) backfilled.
	for i := 0; i < nCols; i++ {
		for j := 0; j < nCols; j++ {
			v, ok := decoded[i].MapGet(fmt.Sprintf("col%d", j))
			if !ok {
				t.Errorf("row %d missing col%d", i, j)
				continue
			}
			n, _ := v.AsInt64()
			if j <= i {
				if n != int64(j*100+i) {
					t.Errorf("row %d col%d: want %d, got %d", i, j, j*100+i, n)
				}
			} else {
				if n != 0 {
					t.Errorf("row %d col%d: want backfill 0, got %d", i, j, n)
				}
			}
		}
	}
}

// ---- Columns shrink: progressively fewer columns per row ----

func TestAdaptColumnEncoder_ColumnShrink(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)

	// Row 0: all 5 columns present
	if !enc.Write(variant.New(map[string]interface{}{
		"a": 1, "b": "b0", "c": 1.0, "d": true, "e": "e0",
	})) {
		t.Fatal("Write 0")
	}
	// Row 1: 4 columns (e missing)
	if !enc.Write(variant.New(map[string]interface{}{
		"a": 2, "b": "b1", "c": 2.0, "d": false,
	})) {
		t.Fatal("Write 1")
	}
	// Row 2: 3 columns (d, e missing)
	if !enc.Write(variant.New(map[string]interface{}{
		"a": 3, "b": "b2", "c": 3.0,
	})) {
		t.Fatal("Write 2")
	}
	// Row 3: 2 columns (c, d, e missing)
	if !enc.Write(variant.New(map[string]interface{}{
		"a": 4, "b": "b3",
	})) {
		t.Fatal("Write 3")
	}
	// Row 4: 1 column (only a)
	if !enc.Write(variant.New(map[string]interface{}{
		"a": 5,
	})) {
		t.Fatal("Write 4")
	}

	data, _ := enc.Bytes()
	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}
	if dec.Error() != nil {
		t.Fatal(dec.Error())
	}

	if len(decoded) != 5 {
		t.Fatalf("count: got %d, want 5", len(decoded))
	}

	// Check each row's present values.
	expectedA := []int64{1, 2, 3, 4, 5}
	expectedB := []string{"b0", "b1", "b2", "b3", ""}
	expectedC := []float64{1.0, 2.0, 3.0, 0, 0}
	expectedD := []bool{true, false, false, false, false}
	expectedE := []string{"e0", "", "", "", ""}

	for i := range 5 {
		v, _ := decoded[i].MapGet("a")
		if n, _ := v.AsInt64(); n != expectedA[i] {
			t.Errorf("row %d a: want %d, got %d", i, expectedA[i], n)
		}
		v, _ = decoded[i].MapGet("b")
		if s := v.AsString(); s != expectedB[i] {
			t.Errorf("row %d b: want %q, got %q", i, expectedB[i], s)
		}
		v, _ = decoded[i].MapGet("c")
		if f, _ := v.AsFloat64(); math.Abs(f-expectedC[i]) > 0.001 {
			t.Errorf("row %d c: want %f, got %f", i, expectedC[i], f)
		}
		v, _ = decoded[i].MapGet("d")
		if b, _ := v.AsBool(); b != expectedD[i] {
			t.Errorf("row %d d: want %v, got %v", i, expectedD[i], b)
		}
		v, _ = decoded[i].MapGet("e")
		if s := v.AsString(); s != expectedE[i] {
			t.Errorf("row %d e: want %q, got %q", i, expectedE[i], s)
		}
	}
}

// ---- Interleaved: column appears, skips rows, appears again ----

func TestAdaptColumnEncoder_InterleavedColumns(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)

	// Row 0: {a: 1}
	if !enc.Write(variant.New(map[string]interface{}{"a": 1})) {
		t.Fatal("Write 0")
	}
	// Row 1: {a: 2, b: "x"}   — b appears
	if !enc.Write(variant.New(map[string]interface{}{"a": 2, "b": "x"})) {
		t.Fatal("Write 1")
	}
	// Row 2: {a: 3}           — b disappears
	if !enc.Write(variant.New(map[string]interface{}{"a": 3})) {
		t.Fatal("Write 2")
	}
	// Row 3: {b: "y"}         — a disappears, b reappears
	if !enc.Write(variant.New(map[string]interface{}{"b": "y"})) {
		t.Fatal("Write 3")
	}
	// Row 4: {a: 5, b: "z"}   — both present
	if !enc.Write(variant.New(map[string]interface{}{"a": 5, "b": "z"})) {
		t.Fatal("Write 4")
	}

	data, _ := enc.Bytes()
	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)

	var decoded []variant.Variant
	for dec.Next() {
		decoded = append(decoded, dec.Read())
	}

	if len(decoded) != 5 {
		t.Fatalf("count: got %d, want 5", len(decoded))
	}

	// Row 0: a=1, b backfilled as ""
	if n, _ := decoded[0].MapGet("a"); true {
		if v, _ := n.AsInt64(); v != 1 {
			t.Errorf("row0 a: want 1, got %d", v)
		}
	}
	if v, ok := decoded[0].MapGet("b"); ok {
		if s := v.AsString(); s != "" {
			t.Errorf("row0 b: want empty, got %s", s)
		}
	}

	// Row 2: a=3, b backfilled ""
	if n, _ := decoded[2].MapGet("a"); true {
		if v, _ := n.AsInt64(); v != 3 {
			t.Errorf("row2 a: want 3, got %d", v)
		}
	}
	if v, ok := decoded[2].MapGet("b"); ok {
		if s := v.AsString(); s != "" {
			t.Errorf("row2 b: want empty, got %s", s)
		}
	}

	// Row 3: a backfilled 0, b="y"
	if n, _ := decoded[3].MapGet("a"); true {
		if v, _ := n.AsInt64(); v != 0 {
			t.Errorf("row3 a: want 0, got %d", v)
		}
	}
	if v, ok := decoded[3].MapGet("b"); ok {
		if s := v.AsString(); s != "y" {
			t.Errorf("row3 b: want y, got %s", s)
		}
	}

	// Row 4: a=5, b="z"
	if n, _ := decoded[4].MapGet("a"); true {
		if v, _ := n.AsInt64(); v != 5 {
			t.Errorf("row4 a: want 5, got %d", v)
		}
	}
	if v, ok := decoded[4].MapGet("b"); ok {
		if s := v.AsString(); s != "z" {
			t.Errorf("row4 b: want z, got %s", s)
		}
	}
}

// ---- Multi-segment with Reset reuse (simulating glow pool) ----

func TestAdaptColumnEncoder_MultiSegmentWithReset(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)

	segments := []struct {
		name string
		rows []variant.Variant
	}{
		{
			name: "seg1-int-string",
			rows: []variant.Variant{
				variant.New(map[string]interface{}{"id": 1, "name": "alice"}),
				variant.New(map[string]interface{}{"id": 2, "name": "bob"}),
				variant.New(map[string]interface{}{"id": 3, "name": "carol"}),
			},
		},
		{
			name: "seg2-float-bool",
			rows: []variant.Variant{
				variant.New(map[string]interface{}{"score": 95.5, "pass": true}),
				variant.New(map[string]interface{}{"score": 60.0, "pass": false}),
			},
		},
		{
			name: "seg3-single-int",
			rows: []variant.Variant{
				variant.New(map[string]interface{}{"val": 100}),
				variant.New(map[string]interface{}{"val": 200}),
				variant.New(map[string]interface{}{"val": 300}),
				variant.New(map[string]interface{}{"val": 400}),
			},
		},
		{
			name: "seg4-many-cols",
			rows: []variant.Variant{
				variant.New(map[string]interface{}{
					"a": int64(1), "b": "x", "c": 0.5, "d": true,
				}),
				variant.New(map[string]interface{}{
					"a": int64(2), "b": "y", "c": 1.5, "d": false,
				}),
			},
		},
	}

	for _, seg := range segments {
		for _, v := range seg.rows {
			if !enc.Write(v) {
				t.Fatalf("%s: Write returned false", seg.name)
			}
		}

		data, err := enc.Bytes()
		if err != nil {
			t.Fatalf("%s: Bytes error: %v", seg.name, err)
		}

		dec := &AdaptColumnDecoder{}
		dec.SetBytes(data)
		if dec.Error() != nil {
			t.Fatalf("%s: SetBytes error: %v", seg.name, dec.Error())
		}

		var decoded []variant.Variant
		for dec.Next() {
			decoded = append(decoded, dec.Read())
		}
		if dec.Error() != nil {
			t.Fatalf("%s: decode error: %v", seg.name, dec.Error())
		}

		if len(decoded) != len(seg.rows) {
			t.Fatalf("%s: count mismatch: got %d, want %d",
				seg.name, len(decoded), len(seg.rows))
		}
		for i := range seg.rows {
			if !seg.rows[i].IsEqual(decoded[i]) {
				t.Errorf("%s row %d mismatch:\n  want: %s\n  got:  %s",
					seg.name, i, seg.rows[i].AsString(), decoded[i].AsString())
			}
		}

		enc.Reset()
	}
}

// ---- Non-struct (empty encoder) ----

func TestAdaptColumnEncoder_NonStructEmpty(t *testing.T) {
	enc := NewAdaptColumnEncoder(2)
	// Don't write anything, just encode.
	data, err := enc.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	// Should be a minimal struct-mode header.
	if len(data) < 2 {
		t.Fatal("expected at least 2 bytes")
	}
	dec := &AdaptColumnDecoder{}
	dec.SetBytes(data)
	if dec.Next() {
		t.Fatal("expected no rows from empty encoder")
	}
}
