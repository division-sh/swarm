package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeGroupByValue(value yamlsource.Value) (*GroupBySpec, error) {
	fields, err := nodeValueFields(value, "group_by", map[string]struct{}{
		"items_from": {}, "key": {}, "store_as": {},
	})
	if err != nil {
		return nil, err
	}
	var out GroupBySpec
	if err := nodeValueTexts(fields, map[string]*string{
		"items_from": &out.ItemsFrom, "key": &out.Key, "store_as": &out.StoreAs,
	}, true); err != nil {
		return nil, err
	}
	out.ItemsPath = paths.Parse(out.ItemsFrom)
	out.KeyPath = paths.Parse(out.Key)
	out.StorePath = paths.Parse(out.StoreAs)
	return &out, nil
}

func projectNodeFilterValue(value yamlsource.Value) (*FilterSpec, error) {
	fields, err := nodeValueFields(value, "filter", map[string]struct{}{
		"source": {}, "items_from": {}, "condition": {}, "store_as": {},
	})
	if err != nil {
		return nil, err
	}
	var out FilterSpec
	if err := nodeValueTexts(fields, map[string]*string{
		"source": &out.Source, "items_from": &out.ItemsFrom,
		"store_as": &out.StoreAs,
	}, true); err != nil {
		return nil, err
	}
	if condition, present := fields["condition"]; present {
		out.Condition, err = projectNodeScalarExpression(condition, "filter.condition", true)
		if err != nil {
			return nil, err
		}
	}
	out.SourcePath = paths.Parse(out.Source)
	out.ItemsPath = paths.Parse(out.ItemsFrom)
	out.StorePath = paths.Parse(out.StoreAs)
	return &out, nil
}

func projectNodeReduceValue(value yamlsource.Value) (*ReduceSpec, error) {
	fields, err := nodeValueFields(value, "reduce", map[string]struct{}{
		"operation": {}, "source": {}, "items_from": {}, "store_as": {},
	})
	if err != nil {
		return nil, err
	}
	var out ReduceSpec
	if err := nodeValueTexts(fields, map[string]*string{
		"operation": &out.Operation, "source": &out.Source,
		"items_from": &out.ItemsFrom, "store_as": &out.StoreAs,
	}, true, "source", "items_from"); err != nil {
		return nil, err
	}
	out.SourcePath = paths.Parse(out.Source)
	out.ItemsPath = paths.Parse(out.ItemsFrom)
	out.StorePath = paths.Parse(out.StoreAs)
	return &out, nil
}

func projectNodeCountValue(value yamlsource.Value) (*CountSpec, error) {
	fields, err := nodeValueFields(value, "count", map[string]struct{}{
		"source": {}, "items_from": {}, "condition": {}, "store_as": {},
	})
	if err != nil {
		return nil, err
	}
	var out CountSpec
	if err := nodeValueTexts(fields, map[string]*string{
		"source": &out.Source, "items_from": &out.ItemsFrom,
		"store_as": &out.StoreAs,
	}, true, "source", "items_from"); err != nil {
		return nil, err
	}
	if condition, present := fields["condition"]; present {
		out.Condition, err = projectNodeScalarExpression(condition, "count.condition", true)
		if err != nil {
			return nil, err
		}
	}
	out.SourcePath = paths.Parse(out.Source)
	out.ItemsPath = paths.Parse(out.ItemsFrom)
	out.StorePath = paths.Parse(out.StoreAs)
	return &out, nil
}

func projectNodeClearValue(value yamlsource.Value) (*ClearSpec, error) {
	fields, err := nodeValueFields(value, "clear", map[string]struct{}{
		"targets": {},
	})
	if err != nil {
		return nil, err
	}
	values, present := fields["targets"]
	if !present {
		return nil, nil
	}
	targets, err := nodeValueStringSequence(values, "clear.targets")
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, nil
	}
	return &ClearSpec{Targets: targets}, nil
}

func projectNodeClearGatesValue(value yamlsource.Value) ([]string, error) {
	switch value.Presence() {
	case yamlsource.PresenceNull, yamlsource.PresenceEmptyScalar:
		return nil, nil
	case yamlsource.PresenceScalar:
		scalar, err := value.Scalar()
		if err != nil {
			return nil, err
		}
		if scalar.Tag == "!!bool" {
			all, err := nodeValueBool(value, "clear_gates")
			if err != nil {
				return nil, err
			}
			if all {
				return []string{"*"}, nil
			}
			return nil, nil
		}
		return []string{strings.TrimSpace(scalar.Value)}, nil
	case yamlsource.PresenceSequence, yamlsource.PresenceEmptySequence:
		return nodeValueStringSequence(value, "clear_gates")
	default:
		return nil, fmt.Errorf("clear_gates at %s must be a scalar or sequence, got %s", value.Location(), value.Presence())
	}
}
