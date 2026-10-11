package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContinuationRegistrationRecipesConsumeProductionReceipts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-continuation-receipt-unit" {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, replacementFunctionName(t, row))
		want, err := canonicalFunction(row.After)
		got, actualErr := canonicalFunction(actual)
		if err != nil || actualErr != nil || got != want {
			t.Fatalf("continuation receipt timing or assertions changed: %s", row.Function)
		}
		for _, retired := range []string{"OwnerAction", "queueDeliveryContinuationSignal", "PipelinePostCommitActions", "PipelineRollbackActions", "flushPipelinePostCommitActions"} {
			if strings.Contains(row.After, retired) {
				t.Fatalf("continuation registration retained fake transaction authority: %s", retired)
			}
		}
		if !strings.Contains(row.After, "acknowledge()") && !strings.Contains(row.After, "store.SuspendStandingService") {
			t.Fatalf("registration proof bypasses production receipt consumer: %s", row.Function)
		}
	}
	if matched != 4 {
		t.Fatalf("continuation recipes=%d,want4", matched)
	}
	path := "internal/runtime/pipeline/workflow_persistence_fixture_test.go"
	source, err := os.ReadFile(filepath.Join("..", "..", "..", path))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if strings.Contains(string(source), "queueDeliveryContinuationSignal") {
		t.Fatal("retired test-only continuation transaction protocol survived")
	}
}
