package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeAccumulateValue(value yamlsource.Value) (*AccumulateSpec, error) {
	fields, err := nodeValueFields(value, "accumulate", map[string]struct{}{
		"target": {}, "key": {},
	})
	if err != nil {
		return nil, err
	}
	var out AccumulateSpec
	if _, present := fields["key"]; !present {
		return nil, fmt.Errorf("accumulate at %s requires key: <item field>; to collect every arrival, write data_accumulation: {writes: [{op: append, target: state.<field>, value: payload}]}", value.Location())
	}
	if _, present := fields["target"]; !present {
		return nil, fmt.Errorf("accumulate at %s requires target: <declared state list field>", value.Location())
	}
	for _, name := range []string{"target", "key"} {
		scalar, err := fields[name].Scalar()
		if err != nil || scalar.Tag != "!!str" {
			return nil, fmt.Errorf("accumulate.%s at %s must be a field name", name, fields[name].Location())
		}
	}
	if out.Target, err = nodeValueRequiredText(fields["target"], "accumulate.target"); err != nil {
		return nil, err
	}
	if out.Key, err = nodeValueRequiredText(fields["key"], "accumulate.key"); err != nil {
		return nil, err
	}
	if strings.HasPrefix(out.Target, "state.") || strings.HasPrefix(out.Key, "payload.") {
		return nil, fmt.Errorf("accumulate at %s names bare fields: target: %s, key: %s", value.Location(), strings.TrimPrefix(out.Target, "state."), strings.TrimPrefix(out.Key, "payload."))
	}
	return &out, nil
}

func projectNodeFanOutValue(value yamlsource.Value) (*FanOutSpec, error) {
	fields, err := nodeValueFields(value, "fan_out", fanOutFieldOptions)
	if err != nil {
		return nil, err
	}
	var out FanOutSpec
	if err := nodeValueTexts(fields, map[string]*string{
		"items_from": &out.ItemsFrom, "as": &out.As,
	}, true); err != nil {
		return nil, err
	}
	if identity, present := fields["identity"]; present {
		out.Identity, err = projectNodeScalarExpression(identity, "fan_out.identity", false)
		if err != nil {
			return nil, err
		}
	}
	if err := ValidateFanOutAlias(out.As); err != nil {
		return nil, fmt.Errorf("fan_out.%w", err)
	}
	if max, present := fields["max_items"]; present {
		scalar, err := max.Scalar()
		if err != nil || scalar.Tag != "!!int" {
			return nil, nodeValueError(max, fmt.Errorf("fan_out.max_items must be a positive integer when set"))
		}
		out.MaxItemsSet = true
		if err := max.Project(&out.MaxItems); err != nil {
			return nil, fmt.Errorf("fan_out.max_items at %s: %w", max.Location(), err)
		}
	}
	if emit, present := fields["emit"]; present {
		out.Emit, err = projectNodeEmitValue(emit)
		if err != nil {
			return nil, err
		}
	}
	out.ItemsPath = paths.Parse(out.ItemsFrom)
	if _, err := ValidateFanOutItemsSource(out); err != nil {
		return nil, err
	}
	if err := ValidateFanOutMaxItems(out); err != nil {
		return nil, err
	}
	return &out, nil
}
