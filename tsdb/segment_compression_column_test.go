package tsdb

import (
	"github.com/mababaNiubi/variant"
	"testing"
)

func TestColumnEncoder_RoundTrip(t *testing.T) {
	attribute := []ColumnAttribute{
		{Name: "1", Desc: "tag1", Type: ColumnTypeString},
		{Name: "2", Desc: "tag2", Type: ColumnTypeInt},
		{Name: "3", Desc: "tag3", Type: ColumnTypeFloat},
		{Name: "4", Desc: "tag4", Type: ColumnTypeBool},
	}

	me := NewColumnEncoder(attribute)
	original := make([]variant.Variant, 0)
	testData := []map[string]interface{}{
		{"1": "hello", "2": 10, "3": 3.14, "4": true},
		{"1": "world", "2": 20, "3": 2.71, "4": false},
		{"1": "foo", "2": 30, "3": 1.41, "4": true},
	}

	for _, d := range testData {
		v := variant.New(d)
		if !me.Write(v) {
			t.Fatal("Write returned false")
		}
		original = append(original, v)
	}

	bytes, err := me.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(bytes) == 0 {
		t.Fatal("expected non-empty encoded bytes")
	}

	md := NewColumnDecoder(attribute)
	md.SetBytes(bytes)

	decoded := make([]variant.Variant, 0)
	for md.Next() {
		if md.Error() != nil {
			t.Fatal(md.Error())
		}
		decoded = append(decoded, md.Read())
	}

	if len(decoded) != len(original) {
		t.Fatalf("decoded count mismatch: got %d, want %d", len(decoded), len(original))
	}

	for i := range original {
		if !original[i].IsEqual(decoded[i]) {
			t.Errorf("entry %d mismatch:\n  original: %s\n  decoded:  %s",
				i, original[i].AsString(), decoded[i].AsString())
		}
	}
}

func TestColumnEncoder_SingleEntry(t *testing.T) {
	attribute := []ColumnAttribute{
		{Name: "val", Type: ColumnTypeFloat},
	}

	me := NewColumnEncoder(attribute)
	if !me.Write(variant.New(map[string]variant.Variant{
		"val": variant.NewFloat64(42.0),
	})) {
		t.Fatal("Write returned false")
	}

	bytes, err := me.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	md := NewColumnDecoder(attribute)
	md.SetBytes(bytes)
	if !md.Next() {
		t.Fatal("expected at least one entry")
	}
	decoded := md.Read()
	val, ok := decoded.MapGet("val")
	if !ok {
		t.Fatal("expected key 'val'")
	}
	f, _ := val.AsFloat64()
	if !variant.IsFloat64Equal(f, 42.0) {
		t.Errorf("expected 42.0, got %f", f)
	}
}

func TestColumnEncoder_EmptyInput(t *testing.T) {
	attribute := []ColumnAttribute{
		{Name: "v", Type: ColumnTypeInt},
	}
	me := NewColumnEncoder(attribute)
	// Empty encoder should produce minimal output (header only)
	bytes, err := me.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	// Even with no data written, the encoder produces header bytes
	if len(bytes) == 0 {
		t.Error("expected non-empty bytes for empty input (header)")
	}
}
