package tsdb

import (
	"fmt"
	"strings"

	"github.com/mababaNiubi/variant"
)

type ConditionOperator string

const (
	OpNotEqual           ConditionOperator = "!="
	OpEqual              ConditionOperator = "="
	OpGreaterThan        ConditionOperator = ">"
	OpGreaterThanOrEqual ConditionOperator = ">="
	OpLessThan           ConditionOperator = "<"
	OpLessThanOrEqual    ConditionOperator = "<="
)

type LogicalOperator string

const (
	LogicalAnd LogicalOperator = "and"
	LogicalOr  LogicalOperator = "or"
)

type Condition struct {
	// ColumnAttributeName is the name of the column to evaluate.
	ColumnAttributeName string
	// Operator is the condition operator (e.g., "=", ">", "<").
	Operator ConditionOperator
	Value    variant.Variant
}

type LogicalCondition struct {
	Op   LogicalOperator
	Cond []any
}

func EvalCondition(cond Condition, data variant.Variant) (bool, error) {
	if len(cond.ColumnAttributeName) == 0 && data.Type() != variant.TypeMap {
		return CompareValue(cond, data)
	}
	columnAttributeNames := strings.Split(cond.ColumnAttributeName, ".")
	for i := range columnAttributeNames {
		// Resolve column value from data by traversing nested keys.
		columnValue, exists := data.MapGet(columnAttributeNames[i])
		if !exists {
			return false, nil
		}
		if columnValue.Type() == variant.TypeMap {
			data = columnValue
			continue
		}
		return CompareValue(cond, columnValue)
	}
	return false, nil
}

func CompareValue(cond Condition, columnValue variant.Variant) (bool, error) {
	if columnValue.Type() == variant.TypeString || columnValue.Type() == variant.TypeList || columnValue.Type() == variant.TypeMap {
		switch cond.Operator {
		case OpEqual:
			return columnValue.IsEqual(cond.Value), nil
		case OpNotEqual:
			return !columnValue.IsEqual(cond.Value), nil
		default:
			return false, fmt.Errorf("invalid operator: %s", cond.Operator)
		}
	}
	return columnValue.CompareNumberBySymbol(cond.Value, string(cond.Operator))
}

func EvalLogicalCondition(logicalCond LogicalCondition, data variant.Variant) (bool, error) {
	if len(logicalCond.Cond) == 0 {
		return false, ErrorEmptyLogicalCondition
	}

	switch logicalCond.Op {
	case LogicalAnd:
		for _, cond := range logicalCond.Cond {
			result, err := evalAnyCondition(cond, data)
			if err != nil || !result {
				return false, err
			}
		}
		return true, nil
	case LogicalOr:
		for _, cond := range logicalCond.Cond {
			result, err := evalAnyCondition(cond, data)
			if err != nil {
				return false, err
			}
			if result {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("unknown logical operator: %s", logicalCond.Op)
	}
}

func evalAnyCondition(cond any, data variant.Variant) (bool, error) {
	if cond == nil {
		return true, nil
	}
	switch c := cond.(type) {
	case Condition:
		return EvalCondition(c, data)
	case LogicalCondition:
		return EvalLogicalCondition(c, data)
	default:
		return false, fmt.Errorf("invalid condition type: %T", cond)
	}
}
