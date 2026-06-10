package tsdb

import (
	"testing"

	"github.com/mababaNiubi/variant"
)

func TestJsonEncoder_RoundTrip(t *testing.T) {
	type User struct {
		Name string  `json:"name"`
		Age  int     `json:"age"`
		Sex  bool    `json:"sex"`
		Hi   float32 `json:"hi"`
	}

	e := NewJsonEncoder()
	original := make([]User, 0)
	testData := []User{
		{Name: "Alice", Age: 25, Sex: true, Hi: 1.5},
		{Name: "Bob", Age: 30, Sex: false, Hi: 2.3},
		{Name: "Charlie", Age: 35, Sex: true, Hi: 3.1},
	}

	for _, u := range testData {
		e.Write(variant.New(u))
		original = append(original, u)
	}

	bytes, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(bytes) == 0 {
		t.Fatal("expected non-empty encoded bytes")
	}

	d := &JsonDecoder{}
	d.SetBytes(bytes)

	decoded := make([]User, 0)
	for d.Next() {
		v := d.Read()
		mp, ok := v.AsInterface().(map[string]any)
		if !ok {
			t.Fatal("expected map output")
		}
		u := User{
			Name: mp["name"].(string),
			Age:  int(mp["age"].(int64)),
			Sex:  mp["sex"].(bool),
			Hi:   float32(mp["hi"].(float64)),
		}
		decoded = append(decoded, u)
	}

	if len(decoded) != len(original) {
		t.Fatalf("count mismatch: got %d, want %d", len(decoded), len(original))
	}
	for i := range original {
		o := original[i]
		d := decoded[i]
		if o.Name != d.Name || o.Age != d.Age || o.Sex != d.Sex {
			t.Errorf("entry %d mismatch: got %+v, want %+v", i, d, o)
		}
	}
}

func TestJsonEncoder_SingleValue(t *testing.T) {
	e := NewJsonEncoder()
	e.Write(variant.NewString("hello"))
	bytes, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	d := &JsonDecoder{}
	d.SetBytes(bytes)
	if !d.Next() {
		t.Fatal("expected at least one entry")
	}
	if d.Read().AsString() != "hello" {
		t.Error("value mismatch")
	}
}

func TestJsonEncoder_Empty(t *testing.T) {
	e := NewJsonEncoder()
	bytes, err := e.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(bytes) == 0 {
		t.Fatal("expected non-empty header bytes")
	}
}
