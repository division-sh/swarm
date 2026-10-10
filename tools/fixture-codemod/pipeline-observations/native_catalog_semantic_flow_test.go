package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func nativeCatalogSemanticFlowPredecessor(row recipe) string {
	if row.Function == "catalogFlowInstanceForCausalFlow" {
		return strings.Replace(row.After, "(workflow catalogWorkflowPersistence,", "(db *sql.DB, workflow catalogWorkflowPersistence,", 1)
	}
	return strings.ReplaceAll(row.After, "catalogFlowInstanceForCausalFlow(h.workflow,", "catalogFlowInstanceForCausalFlow(h.db, h.workflow,")
}

func TestNativeCatalogSemanticFlowRecipesRemoveOnlyUnusedSQLArgument(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-semantic-flow-read" {
			continue
		}
		count++
		if row.Function != "catalogFlowInstanceForCausalFlow" && row.Function != "requireCatalogCreationHandlerOrders" && row.Function != "assertCatalogReplayFixtureOutcome" {
			t.Fatal("unreviewed semantic flow caller")
		}
		want, err := canonicalFunction(row.Before)
		got, parseErr := canonicalFunction(nativeCatalogSemanticFlowPredecessor(row))
		if err != nil || parseErr != nil || got != want {
			t.Fatalf("%s changed beyond unused SQL argument removal: %v / %v", row.Function, err, parseErr)
		}
		for _, pair := range [][2]string{
			{"row.ParentEntityID", "row.EntityID"}, {"if !requireCausal", "if requireCausal"},
			{"catalogRuntimeRunID)", "foreignRunID)"}, {"found, err", "found, ignoredError"},
			{"CurrentState != childState", "CurrentState == childState"},
		} {
			mutant := row
			mutant.After = strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant.After == row.After {
				continue
			}
			got, err := canonicalFunction(nativeCatalogSemanticFlowPredecessor(mutant))
			if err == nil && got == want {
				t.Fatalf("semantic selection/lifecycle assertion mutation accepted: %v", pair)
			}
		}
	}
	if count != 3 {
		t.Fatalf("semantic flow recipes=%d, want three", count)
	}
}
