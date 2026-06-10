package tsdb

import (
	"github.com/mababaNiubi/variant"
	"testing"
)

// TestCompareValue covers comparison logic for different value types.
func TestCompareValue(t *testing.T) {
	tests := []struct {
		name        string
		cond        Condition
		columnValue variant.Variant
		want        bool
		wantErr     bool
	}{
		// String type tests
		{
			name:        "string equal",
			cond:        Condition{Type: EqualQueryCondition, Value: variant.NewString("test")},
			columnValue: variant.NewString("test"),
			want:        true,
			wantErr:     false,
		},
		{
			name:        "string not equal",
			cond:        Condition{Type: NotEqualQueryCondition, Value: variant.NewString("other")},
			columnValue: variant.NewString("test"),
			want:        true,
			wantErr:     false,
		},
		{
			name:        "string invalid operator (>)",
			cond:        Condition{Type: GreaterThanQueryCondition, Value: variant.NewString("b")},
			columnValue: variant.NewString("a"),
			want:        false,
			wantErr:     true,
		},

		// Integer type tests
		{
			name:        "int equal",
			cond:        Condition{Type: EqualQueryCondition, Value: variant.NewInt(10)},
			columnValue: variant.NewInt(10),
			want:        true,
			wantErr:     false,
		},
		{
			name:        "int greater than",
			cond:        Condition{Type: GreaterThanQueryCondition, Value: variant.NewInt(5)},
			columnValue: variant.NewInt(10),
			want:        true,
			wantErr:     false,
		},
		{
			name:        "int less than or equal",
			cond:        Condition{Type: LessThanOrEqualQueryCondition, Value: variant.NewInt(10)},
			columnValue: variant.NewInt(10),
			want:        true,
			wantErr:     false,
		},

		// Float type tests
		{
			name:        "float not equal",
			cond:        Condition{Type: NotEqualQueryCondition, Value: variant.New(3.14)},
			columnValue: variant.New(2.71),
			want:        true,
			wantErr:     false,
		},
		{
			name:        "float less than",
			cond:        Condition{Type: LessThanQueryCondition, Value: variant.New(5.0)},
			columnValue: variant.New(3.0),
			want:        true,
			wantErr:     false,
		},

		// List type tests
		{
			name:        "list equal",
			cond:        Condition{Type: EqualQueryCondition, Value: variant.New([]variant.Variant{variant.NewInt(1), variant.NewInt(2)})},
			columnValue: variant.New([]variant.Variant{variant.NewInt(1), variant.NewInt(2)}),
			want:        true,
			wantErr:     false,
		},
		{
			name:        "list invalid operator (>=)",
			cond:        Condition{Type: GreaterThanOrEqualQueryCondition, Value: variant.New([]variant.Variant{})},
			columnValue: variant.New([]variant.Variant{}),
			want:        false,
			wantErr:     true,
		},

		// Map type tests
		{
			name:        "map not equal",
			cond:        Condition{Type: NotEqualQueryCondition, Value: variant.New(map[string]variant.Variant{"k": variant.NewInt(1)})},
			columnValue: variant.New(map[string]variant.Variant{"k": variant.NewInt(2)}),
			want:        true,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CompareValue(tt.cond, tt.columnValue)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompareValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CompareValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestEvalCondition covers column name resolution and value retrieval logic.
func TestEvalCondition(t *testing.T) {
	// Prepare test data.
	flatMap := variant.New(map[string]variant.Variant{
		"age":   variant.NewInt(25),
		"score": variant.New(85.5),
		"name":  variant.NewString("Alice"),
	})
	nestedMap := variant.New(map[string]variant.Variant{
		"user": variant.New(map[string]variant.Variant{
			"profile": variant.New(map[string]variant.Variant{
				"height": variant.NewInt(175),
			}),
		}),
	})
	nonMapData := variant.NewInt(50) // Non-map type data.

	tests := []struct {
		name    string
		cond    Condition
		data    variant.Variant
		want    bool
		wantErr bool
	}{
		{
			name:    "flat map column exists (equal)",
			cond:    Condition{ColumnAttributeName: "age", Type: EqualQueryCondition, Value: variant.NewInt(25)},
			data:    flatMap,
			want:    true,
			wantErr: false,
		},
		{
			name:    "flat map column exists (greater than)",
			cond:    Condition{ColumnAttributeName: "score", Type: GreaterThanQueryCondition, Value: variant.New(80.0)},
			data:    flatMap,
			want:    true,
			wantErr: false,
		},
		{
			name:    "column not found in map",
			cond:    Condition{ColumnAttributeName: "email", Type: EqualQueryCondition, Value: variant.NewString("a@x.com")},
			data:    flatMap,
			want:    false,
			wantErr: true,
		},
		{
			name:    "nested column (user.profile.height)",
			cond:    Condition{ColumnAttributeName: "user.profile.height", Type: LessThanQueryCondition, Value: variant.NewInt(180)},
			data:    nestedMap,
			want:    true,
			wantErr: false,
		},
		{
			name:    "empty column name with non-map data",
			cond:    Condition{ColumnAttributeName: "", Type: GreaterThanQueryCondition, Value: variant.NewInt(40)},
			data:    nonMapData, // Compare data directly (50 > 40).
			want:    true,
			wantErr: false,
		},
		{
			name:    "empty column name with map data (key not exists)",
			cond:    Condition{ColumnAttributeName: "", Type: EqualQueryCondition, Value: variant.NewInt(0)},
			data:    flatMap, // Attempt to get key="" (does not exist).
			want:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EvalCondition(tt.cond, tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("EvalCondition() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("EvalCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestEvalLogicalCondition covers AND/OR logic and nested conditions.
func TestEvalLogicalCondition(t *testing.T) {
	// Define base conditions.
	condAge25 := Condition{ColumnAttributeName: "age", Type: EqualQueryCondition, Value: variant.NewInt(25)}
	condNameAlice := Condition{ColumnAttributeName: "name", Type: EqualQueryCondition, Value: variant.NewString("Alice")}
	condScorePass := Condition{ColumnAttributeName: "score", Type: GreaterThanQueryCondition, Value: variant.NewInt(60)}
	condInvalid := Condition{ColumnAttributeName: "invalid_col", Type: EqualQueryCondition, Value: variant.NewInt(0)} // Non-existent column.

	// Test data.
	data := variant.New(map[string]variant.Variant{
		"age":   variant.NewInt(25),
		"name":  variant.NewString("Alice"),
		"score": variant.NewInt(80),
	})

	// Nested logical conditions.
	nestedOr := LogicalCondition{
		Operator: Or,
		Conditions: []any{
			Condition{ColumnAttributeName: "score", Type: LessThanQueryCondition, Value: variant.NewInt(50)}, // false
			Condition{ColumnAttributeName: "age", Type: EqualQueryCondition, Value: variant.NewInt(25)},      // true
		},
	}
	nestedAnd := LogicalCondition{
		Operator: And,
		Conditions: []any{
			condAge25,     // true
			nestedOr,      // true
			condScorePass, // true
		},
	}

	tests := []struct {
		name        string
		logicalCond LogicalCondition
		data        variant.Variant
		want        bool
		wantErr     bool
	}{
		{
			name: "AND: all conditions true",
			logicalCond: LogicalCondition{
				Operator:   And,
				Conditions: []any{condAge25, condNameAlice, condScorePass},
			},
			data:    data,
			want:    true,
			wantErr: false,
		},
		{
			name: "AND: one condition false",
			logicalCond: LogicalCondition{
				Operator: And,
				Conditions: []any{
					condAge25,
					Condition{ColumnAttributeName: "name", Type: EqualQueryCondition, Value: variant.NewString("Bob")}, // false
					condScorePass,
				},
			},
			data:    data,
			want:    false,
			wantErr: false,
		},
		{
			name: "AND: with error condition",
			logicalCond: LogicalCondition{
				Operator:   And,
				Conditions: []any{condAge25, condInvalid},
			},
			data:    data,
			want:    false,
			wantErr: true,
		},
		{
			name: "OR: one condition true",
			logicalCond: LogicalCondition{
				Operator: Or,
				Conditions: []any{
					Condition{ColumnAttributeName: "age", Type: EqualQueryCondition, Value: variant.NewInt(30)}, // false
					condNameAlice, // true
					Condition{ColumnAttributeName: "score", Type: LessThanQueryCondition, Value: variant.NewInt(50)}, // false
				},
			},
			data:    data,
			want:    true,
			wantErr: false,
		},
		{
			name: "OR: all conditions false",
			logicalCond: LogicalCondition{
				Operator: Or,
				Conditions: []any{
					Condition{ColumnAttributeName: "age", Type: EqualQueryCondition, Value: variant.NewInt(30)},
					Condition{ColumnAttributeName: "name", Type: EqualQueryCondition, Value: variant.NewString("Bob")},
				},
			},
			data:    data,
			want:    false,
			wantErr: false,
		},
		{
			name: "OR: with error condition",
			logicalCond: LogicalCondition{
				Operator:   Or,
				Conditions: []any{condInvalid, condNameAlice},
			},
			data:    data,
			want:    false,
			wantErr: true,
		},
		{
			name:        "nested logical condition (AND with OR)",
			logicalCond: nestedAnd,
			data:        data,
			want:        true,
			wantErr:     false,
		},
		{
			name: "empty sub-conditions",
			logicalCond: LogicalCondition{
				Operator:   And,
				Conditions: []any{},
			},
			data:    data,
			want:    false,
			wantErr: true,
		},
		{
			name: "unknown logical operator",
			logicalCond: LogicalCondition{
				Operator:   "xor", // Unknown operator.
				Conditions: []any{condAge25},
			},
			data:    data,
			want:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EvalLogicalCondition(tt.logicalCond, tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("EvalLogicalCondition() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("EvalLogicalCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestEvalCondition_NestedMapNotFound tests error handling for missing intermediate map keys.
func TestEvalCondition_NestedMapNotFound(t *testing.T) {
	nestedMap := variant.New(map[string]variant.Variant{
		"user": variant.New(map[string]variant.Variant{
			"profile": variant.New(map[string]variant.Variant{
				"name": variant.NewString("Alice"),
			}),
		}),
	})

	// Intermediate key doesn't exist in path
	cond := Condition{
		ColumnAttributeName: "user.settings.theme",
		Type:                EqualQueryCondition,
		Value:               variant.NewString("dark"),
	}
	_, err := EvalCondition(cond, nestedMap)
	if err == nil {
		t.Error("expected error when intermediate key 'settings' doesn't exist")
	}
}

func TestCompareValue_UInt64(t *testing.T) {
	v := variant.NewUInt64(100)
	r := variant.NewUInt64(50)

	ok, err := CompareValue(Condition{Type: GreaterThanQueryCondition, Value: r}, v)
	if err != nil || !ok {
		t.Errorf("100 > 50 should be true, got ok=%v err=%v", ok, err)
	}

	ok, err = CompareValue(Condition{Type: LessThanQueryCondition, Value: r}, v)
	if err != nil || ok {
		t.Errorf("100 < 50 should be false, got ok=%v err=%v", ok, err)
	}
}

func TestCompareValue_Bool(t *testing.T) {
	ok, err := CompareValue(Condition{Type: EqualQueryCondition, Value: variant.NewBool(true)}, variant.NewBool(false))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("true = false should be false")
	}
}

func TestEvalLogicalCondition_OrWithError(t *testing.T) {
	data := variant.New(map[string]variant.Variant{"x": variant.NewInt(1)})
	invalidCond := Condition{ColumnAttributeName: "nonexistent", Type: EqualQueryCondition, Value: variant.NewInt(0)}
	validCond := Condition{ColumnAttributeName: "x", Type: EqualQueryCondition, Value: variant.NewInt(1)}

	// OR: first condition errors -> should return error immediately
	lc := LogicalCondition{
		Operator:   Or,
		Conditions: []any{invalidCond, validCond},
	}
	_, err := EvalLogicalCondition(lc, data)
	if err == nil {
		t.Error("expected error when OR's first condition errors")
	}
}

func TestEvalCondition_NonMapDataWithColumnName(t *testing.T) {
	// When data is not a map but column name is provided, MapGet on non-map returns false
	v := variant.NewInt(100)
	cond := Condition{
		ColumnAttributeName: "some_col",
		Type:                EqualQueryCondition,
		Value:               variant.NewInt(100),
	}
	_, err := EvalCondition(cond, v)
	if err == nil {
		t.Error("expected error when using column name with non-map data")
	}
}

func TestEvalAnyCondition(t *testing.T) {
	data := variant.New(map[string]variant.Variant{
		"value": variant.NewInt(100),
	})
	validCond := Condition{ColumnAttributeName: "value", Type: EqualQueryCondition, Value: variant.NewInt(100)}
	validLogicalCond := LogicalCondition{
		Operator:   And,
		Conditions: []any{validCond},
	}

	tests := []struct {
		name      string
		inputCond any
		data      variant.Variant
		want      bool
		wantErr   bool
	}{
		{
			name:      "condition is nil",
			inputCond: nil,
			data:      data,
			want:      true, // By design: nil conditions return true.
			wantErr:   false,
		},
		{
			name:      "condition is Condition type",
			inputCond: validCond,
			data:      data,
			want:      true,
			wantErr:   false,
		},
		{
			name:      "condition is LogicalCondition type",
			inputCond: validLogicalCond,
			data:      data,
			want:      true,
			wantErr:   false,
		},
		{
			name:      "condition is invalid type (int)",
			inputCond: 123, // Not a Condition or LogicalCondition type.
			data:      data,
			want:      false,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evalAnyCondition(tt.inputCond, tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("evalAnyCondition() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("evalAnyCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}
