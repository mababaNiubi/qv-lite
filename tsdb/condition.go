package tsdb

import (
	"fmt"
	"strings"

	"github.com/mababaNiubi/variant"
)

type ConditionOperator string

const (
	NotEqualQueryCondition           ConditionOperator = "!="
	EqualQueryCondition              ConditionOperator = "="
	GreaterThanQueryCondition        ConditionOperator = ">"
	GreaterThanOrEqualQueryCondition ConditionOperator = ">="
	LessThanQueryCondition           ConditionOperator = "<"
	LessThanOrEqualQueryCondition    ConditionOperator = "<="
)

type LogicalOperator string

const (
	And LogicalOperator = "and"
	Or  LogicalOperator = "or"
)

type Condition struct {
	// ColumnAttributeName is the name of the column to evaluate.
	ColumnAttributeName string
	// Type is the condition operator (e.g., "=", ">", "<").
	Type  ConditionOperator
	Value variant.Variant
}

type LogicalCondition struct {
	Operator   LogicalOperator
	Conditions []any
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
			//return false, fmt.Errorf("column %s not found in data", columnAttributeNames[i])
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
		switch cond.Type {
		case EqualQueryCondition:
			return columnValue.IsEqual(cond.Value), nil
		case NotEqualQueryCondition:
			return !columnValue.IsEqual(cond.Value), nil
		default:
			return false, fmt.Errorf("invalid operator: %s", cond.Type)
		}
	}
	return columnValue.CompareNumberBySymbol(cond.Value, string(cond.Type))
}

func EvalLogicalCondition(logicalCond LogicalCondition, data variant.Variant) (bool, error) {
	if len(logicalCond.Conditions) == 0 {
		return false, ErrorEmptyLogicalCondition
	}

	switch logicalCond.Operator {
	case And:
		// All conditions must be satisfied.
		for _, cond := range logicalCond.Conditions {
			result, err := evalAnyCondition(cond, data)
			if err != nil || !result {
				return false, err
			}
		}
		return true, nil
	case Or:
		// At least one condition must be satisfied.
		for _, cond := range logicalCond.Conditions {
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
		return false, fmt.Errorf("unknown logical operator: %s", logicalCond.Operator)
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
