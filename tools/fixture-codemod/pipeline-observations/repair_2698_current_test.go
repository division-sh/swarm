package main

import "testing"

func TestRepair2698RecipeConsumptionKeepsExactReceiversMetadataAndMultiplicity(t *testing.T) {
	left := recipe{Family: "native", File: "fixture.go", Function: "Intercept", Before: "original", After: "func (*left) Intercept() {}"}
	right := left
	right.After = "func (*right) Intercept() {}"
	for _, field := range []string{"family", "file", "function", "before", "after", "successor", "removed", "mechanical"} {
		t.Run(field, func(t *testing.T) {
			rows := map[string][]recipe{"Intercept": {left, right}}
			bad := left
			switch field {
			case "family":
				bad.Family += "foreign"
			case "file":
				bad.File += ".foreign"
			case "function":
				bad.Function += "Foreign"
			case "before":
				bad.Before += "foreign"
			case "after":
				bad.After = "func (*foreign) Intercept() {}"
			case "successor":
				bad.Successor = left.After
			case "removed":
				bad.Removed = true
			case "mechanical":
				bad.Mechanical = &mechanicalSnapshot{SourceCommit: "foreign"}
			}
			if consumeRepair2698Recipe(rows, bad) || len(rows["Intercept"]) != 2 {
				t.Fatal("foreign receipt consumed an exact historical occurrence")
			}
			if !consumeRepair2698Recipe(rows, left) || consumeRepair2698Recipe(rows, left) || !consumeRepair2698Recipe(rows, right) || len(rows["Intercept"]) != 0 {
				t.Fatal("same-name receiver or occurrence identity was collapsed")
			}
		})
	}
}
