package contracts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

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
	ComputeOpModule
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
	case "compute_module":
		return ComputeOpUnknown, fmt.Errorf("compute operation %q is internal to policy-sheet value rows; author compute_module rows under rules", text)
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
	case ComputeOpModule:
		return "compute_module"
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
