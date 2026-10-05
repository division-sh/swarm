package contracts

import (
	"fmt"
	"strings"
)

var ruleFieldOptions = map[string]struct{}{
	"id":                {},
	"description":       {},
	"condition":         {},
	"when":              {},
	"case":              {},
	"range":             {},
	"lookup":            {},
	"validate":          {},
	"compute_module":    {},
	"else":              {},
	"default":           {},
	"advances_to":       {},
	"emit":              {},
	"activity":          {},
	"data_accumulation": {},
	"compute":           {},
	"fan_out":           {},
}

var computeFieldOptions = map[string]struct{}{
	"operation":   {},
	"tiers":       {},
	"keys":        {},
	"store_as":    {},
	"description": {},
}

func validateTieredWeightedAverageSpec(spec ComputeSpec) error {
	if spec.Operation != ComputeOpWeightedAverage || len(spec.Tiers) == 0 {
		return nil
	}
	if strings.TrimSpace(spec.Keys.DimensionKey) == "" {
		return fmt.Errorf("invalid compute spec: weighted_average with tiers requires keys.dimension_key")
	}
	if len(normalizeStrings(spec.Keys.ScoreKeys)) == 0 {
		return fmt.Errorf("invalid compute spec: weighted_average with tiers requires keys.score_keys")
	}
	return nil
}
