package contracts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type AccumulateMode uint8

const (
	AccumulateModeDefault AccumulateMode = iota
	AccumulateModeAll
	AccumulateModeThreshold
	AccumulateModeTimeout
	AccumulateModeExpression
)

func (m AccumulateMode) String() string {
	switch m {
	case AccumulateModeAll:
		return "all"
	case AccumulateModeThreshold:
		return "threshold"
	case AccumulateModeTimeout:
		return "timeout"
	default:
		return ""
	}
}

type AccumulateCompletion struct {
	Mode       AccumulateMode
	Expression string
}

func ParseAccumulateCompletion(text string) AccumulateCompletion {
	trimmed := strings.TrimSpace(text)
	switch strings.ToLower(trimmed) {
	case "":
		return AccumulateCompletion{Mode: AccumulateModeDefault}
	case "all":
		return AccumulateCompletion{Mode: AccumulateModeAll}
	case "threshold":
		return AccumulateCompletion{Mode: AccumulateModeThreshold}
	case "timeout":
		return AccumulateCompletion{Mode: AccumulateModeTimeout}
	default:
		return AccumulateCompletion{Mode: AccumulateModeExpression, Expression: trimmed}
	}
}

func (c AccumulateCompletion) String() string {
	if c.Mode == AccumulateModeExpression {
		return c.Expression
	}
	return c.Mode.String()
}

type ComputeOperation uint8

const (
	ComputeOpUnknown ComputeOperation = iota
	ComputeOpWeightedAverage
	ComputeOpPickOrAverage
	ComputeOpSum
	ComputeOpMin
	ComputeOpMax
	ComputeOpCount
	ComputeOpLookup
	ComputeOpValidate
)

func ParseComputeOperation(text string) (ComputeOperation, error) {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "weighted_average":
		return ComputeOpWeightedAverage, nil
	case "pick_or_average":
		return ComputeOpPickOrAverage, nil
	case "sum":
		return ComputeOpSum, nil
	case "min":
		return ComputeOpMin, nil
	case "max":
		return ComputeOpMax, nil
	case "count":
		return ComputeOpCount, nil
	case "lookup":
		return ComputeOpUnknown, fmt.Errorf("compute operation %q is internal to policy-sheet value rows; author lookup rows under rules", text)
	case "validate":
		return ComputeOpUnknown, fmt.Errorf("compute operation %q is internal to policy-sheet value rows; author validate rows under rules", text)
	case "":
		return ComputeOpUnknown, nil
	default:
		return ComputeOpUnknown, fmt.Errorf("unsupported compute operation %q", text)
	}
}

func (o ComputeOperation) String() string {
	switch o {
	case ComputeOpWeightedAverage:
		return "weighted_average"
	case ComputeOpPickOrAverage:
		return "pick_or_average"
	case ComputeOpSum:
		return "sum"
	case ComputeOpMin:
		return "min"
	case ComputeOpMax:
		return "max"
	case ComputeOpCount:
		return "count"
	case ComputeOpLookup:
		return "lookup"
	case ComputeOpValidate:
		return "validate"
	default:
		return ""
	}
}

func (o *ComputeOperation) UnmarshalYAML(node *yaml.Node) error {
	if o == nil {
		return nil
	}
	var raw string
	if err := node.Decode(&raw); err != nil {
		return err
	}
	parsed, err := ParseComputeOperation(raw)
	if err != nil {
		return err
	}
	*o = parsed
	return nil
}
