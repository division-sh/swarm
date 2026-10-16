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
	for _, name := range []string{"target", "key"} {
		field, present := fields[name]
		if !present {
			if name == "key" {
				_, err := out.ItemKeyPath()
				return nil, nodeValueError(value, err)
			}
			return nil, nodeValueError(value, fmt.Errorf("accumulate requires target: <state field>"))
		}
		scalar, err := field.Scalar()
		if err != nil || scalar.Tag != "!!str" || strings.TrimSpace(scalar.Value) == "" {
			return nil, fmt.Errorf("accumulate.%s at %s must be non-empty text", name, field.Location())
		}
	}
	if err := nodeValueTexts(fields, map[string]*string{
		"target": &out.Target, "key": &out.Key,
	}, false, "target", "key"); err != nil {
		return nil, err
	}
	path, err := paths.ParseStrictRelative("state." + out.Target)
	if err != nil || out.Target != strings.TrimSpace(out.Target) || len(path.Segments) != 2 {
		return nil, nodeValueError(fields["target"], fmt.Errorf("accumulate.target %q names one bare state field; write target: %s", out.Target, strings.TrimPrefix(out.Target, "state.")))
	}
	out.KeyPath, err = out.ItemKeyPath()
	if err != nil {
		return nil, nodeValueError(fields["key"], err)
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
