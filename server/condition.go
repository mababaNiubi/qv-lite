package server

import (
	"encoding/json"
	"fmt"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

// conditionWire is the JSON representation of a filter. It supports both a
// single condition and a logical (AND/OR) combination, mirroring the engine's
// tsdb.Condition / tsdb.LogicalCondition types.
//
// Single condition:
//
//	{"column":"value","op":">","value":{...typed value...}}
//
// Logical condition:
//
//	{"op":"and","conditions":[<condition>,<condition>,...]}
type conditionWire struct {
	Column     string            `json:"column"`    // empty = compare against the point value itself
	Op         string            `json:"op"`        // "=","!=",">",">=","<","<="
	Value      json.RawMessage   `json:"value"`     // 原生 JSON 值
	ValueType  string            `json:"valueType"` // "int"/"uint"，value 为字符串数字
	OpLogical  string            `json:"opLogical"` // "and"|"or" when this node is logical
	Conditions []json.RawMessage `json:"conditions,omitempty"`
}

// decodeCondition converts a raw JSON condition into the engine's "any"
// representation (nil, tsdb.Condition, or tsdb.LogicalCondition).
func decodeCondition(raw json.RawMessage) (any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var probe struct {
		OpLogical string `json:"opLogical"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("invalid condition: %w", err)
	}
	if probe.OpLogical != "" {
		return decodeLogical(raw)
	}
	return decodeSingle(raw)
}

func decodeSingle(raw json.RawMessage) (tsdb.Condition, error) {
	var c conditionWire
	if err := json.Unmarshal(raw, &c); err != nil {
		return tsdb.Condition{}, fmt.Errorf("invalid condition: %w", err)
	}
	if len(c.Value) == 0 {
		return tsdb.Condition{}, fmt.Errorf("condition requires a value")
	}
	op := tsdb.ConditionOperator(c.Op)
	switch op {
	case tsdb.OpEqual, tsdb.OpNotEqual, tsdb.OpGreaterThan,
		tsdb.OpGreaterThanOrEqual, tsdb.OpLessThan, tsdb.OpLessThanOrEqual:
	default:
		return tsdb.Condition{}, fmt.Errorf("unsupported condition op %q", c.Op)
	}
	v, err := ValueToVariant(c.Value, c.ValueType)
	if err != nil {
		return tsdb.Condition{}, fmt.Errorf("condition value: %w", err)
	}
	return tsdb.Condition{
		ColumnAttributeName: c.Column,
		Operator:            op,
		Value:               v,
	}, nil
}

func decodeLogical(raw json.RawMessage) (tsdb.LogicalCondition, error) {
	var c conditionWire
	if err := json.Unmarshal(raw, &c); err != nil {
		return tsdb.LogicalCondition{}, fmt.Errorf("invalid logical condition: %w", err)
	}
	if c.OpLogical != string(tsdb.LogicalAnd) && c.OpLogical != string(tsdb.LogicalOr) {
		return tsdb.LogicalCondition{}, fmt.Errorf("unsupported logical op %q", c.OpLogical)
	}
	if len(c.Conditions) == 0 {
		return tsdb.LogicalCondition{}, fmt.Errorf("logical condition requires at least one sub-condition")
	}
	cond := tsdb.LogicalCondition{Op: tsdb.LogicalOperator(c.OpLogical)}
	for _, sub := range c.Conditions {
		decoded, err := decodeCondition(sub)
		if err != nil {
			return tsdb.LogicalCondition{}, err
		}
		cond.Cond = append(cond.Cond, decoded)
	}
	return cond, nil
}
