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
	}, nil)
	if err != nil {
		return nil, err
	}
	var out GroupBySpec
	for _, entry := range []struct {
		key    string
		target *string
	}{
		{"items_from", &out.ItemsFrom}, {"key", &out.Key}, {"store_as", &out.StoreAs},
	} {
		if field, present := fields[entry.key]; present {
			*entry.target, err = nodeValueText(field, "group_by."+entry.key)
			if err != nil {
				return nil, err
			}
			*entry.target = strings.TrimSpace(*entry.target)
		}
	}
	out.ItemsPath = paths.Parse(out.ItemsFrom)
	out.KeyPath = paths.Parse(out.Key)
	out.StorePath = paths.Parse(out.StoreAs)
	return &out, nil
}

func projectNodeFilterValue(value yamlsource.Value) (*FilterSpec, error) {
	fields, err := nodeValueFields(value, "filter", map[string]struct{}{
		"source": {}, "items_from": {}, "condition": {}, "store_as": {},
	}, map[string]string{
		"predicate": "filter.predicate is not executed; use condition",
	})
	if err != nil {
		return nil, err
	}
	var out FilterSpec
	for _, entry := range []struct {
		key    string
		target *string
	}{
		{"source", &out.Source}, {"items_from", &out.ItemsFrom},
		{"condition", &out.Condition}, {"store_as", &out.StoreAs},
	} {
		if field, present := fields[entry.key]; present {
			*entry.target, err = nodeValueText(field, "filter."+entry.key)
			if err != nil {
				return nil, err
			}
			*entry.target = strings.TrimSpace(*entry.target)
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
	}, map[string]string{
		"params": "reduce.params is not executed; use the declared operation and source",
	})
	if err != nil {
		return nil, err
	}
	var out ReduceSpec
	for _, entry := range []struct {
		key    string
		target *string
	}{
		{"operation", &out.Operation}, {"source", &out.Source},
		{"items_from", &out.ItemsFrom}, {"store_as", &out.StoreAs},
	} {
		if field, present := fields[entry.key]; present {
			*entry.target, err = nodeValueText(field, "reduce."+entry.key)
			if err != nil {
				return nil, err
			}
			*entry.target = strings.TrimSpace(*entry.target)
		}
	}
	out.SourcePath = paths.Parse(out.Source)
	out.ItemsPath = paths.Parse(out.ItemsFrom)
	out.StorePath = paths.Parse(out.StoreAs)
	return &out, nil
}

func projectNodeCountValue(value yamlsource.Value) (*CountSpec, error) {
	fields, err := nodeValueFields(value, "count", map[string]struct{}{
		"source": {}, "items_from": {}, "condition": {}, "store_as": {},
	}, nil)
	if err != nil {
		return nil, err
	}
	var out CountSpec
	for _, entry := range []struct {
		key    string
		target *string
	}{
		{"source", &out.Source}, {"items_from", &out.ItemsFrom},
		{"condition", &out.Condition}, {"store_as", &out.StoreAs},
	} {
		if field, present := fields[entry.key]; present {
			*entry.target, err = nodeValueText(field, "count."+entry.key)
			if err != nil {
				return nil, err
			}
			*entry.target = strings.TrimSpace(*entry.target)
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
	}, map[string]string{
		"target": "clear.target is retired; use targets",
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
