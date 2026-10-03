package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeLoopValue(value yamlsource.Value) (*LoopOperationSpec, error) {
	fields, err := nodeValueFields(value, "handler loop operation", loopOperationFieldOptions)
	if err != nil {
		return nil, err
	}
	out := &LoopOperationSpec{}
	if err := nodeValueTexts(fields, map[string]*string{
		"start": &out.Start, "admit": &out.Admit, "repeat": &out.Repeat,
		"close": &out.Close, "from": &out.From,
	}, true); err != nil {
		return nil, err
	}
	if _, _, err := out.Operation(); err != nil {
		return nil, fmt.Errorf("handler loop operation at %s: %w", value.Location(), err)
	}
	if out.From == "" {
		return nil, fmt.Errorf("handler loop operation from is required at %s", value.Location())
	}
	return out, nil
}

func projectNodeComputeValue(value yamlsource.Value) (*ComputeSpec, error) {
	fields, err := nodeValueFields(value, "compute", computeFieldOptions)
	if err != nil {
		return nil, err
	}
	out := &ComputeSpec{}
	if err := nodeValueTexts(fields, map[string]*string{"store_as": &out.StoreAs, "description": &out.Description}, false); err != nil {
		return nil, err
	}
	if operation, present := fields["operation"]; present {
		name, err := nodeValueText(operation, "compute.operation")
		if err != nil {
			return nil, err
		}
		out.Operation, err = ParseComputeOperation(name)
		if err != nil {
			return nil, fmt.Errorf("compute.operation at %s: %w", operation.Location(), err)
		}
	}
	if keys, present := fields["keys"]; present {
		keyFields, err := nodeValueFields(keys, "compute.keys", map[string]struct{}{
			"dimension_key": {}, "score_keys": {}, "numeric_keys": {},
		})
		if err != nil {
			return nil, err
		}
		if dimension, present := keyFields["dimension_key"]; present {
			out.Keys.DimensionKey, err = nodeValueText(dimension, "compute.keys.dimension_key")
			if err != nil {
				return nil, err
			}
		}
		if scores, present := keyFields["score_keys"]; present {
			out.Keys.ScoreKeys, err = nodeValueStringSequence(scores, "compute.keys.score_keys")
			if err != nil {
				return nil, err
			}
		}
		if numerics, present := keyFields["numeric_keys"]; present {
			out.Keys.NumericKeys, err = nodeValueStringSequence(numerics, "compute.keys.numeric_keys")
			if err != nil {
				return nil, err
			}
		}
	}
	if tiers, present := fields["tiers"]; present {
		items, err := tiers.Sequence()
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			tierFields, err := nodeValueFields(item, "compute.tiers", map[string]struct{}{
				"dimensions": {}, "weight": {},
			})
			if err != nil {
				return nil, err
			}
			var tier ComputeTier
			if dimensions, present := tierFields["dimensions"]; present {
				tier.Dimensions, err = nodeValueStringSequence(dimensions, "compute.tiers.dimensions")
				if err != nil {
					return nil, err
				}
			}
			if weight, present := tierFields["weight"]; present {
				if weight.Presence() != yamlsource.PresenceScalar {
					return nil, fmt.Errorf("compute.tiers.weight at %s must be numeric", weight.Location())
				}
				if err := weight.Project(&tier.Weight); err != nil {
					return nil, err
				}
			}
			out.Tiers = append(out.Tiers, tier)
		}
	}
	if err := validateTieredWeightedAverageSpec(*out); err != nil {
		return nil, err
	}
	return out, nil
}
