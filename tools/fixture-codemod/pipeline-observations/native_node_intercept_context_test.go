package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNodeInterceptRecipesConsumeNativeDeliveryOwners(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-node-intercept-authority-context" {
			continue
		}
		matched++
		got, err := canonicalFunction(row.After)
		actual, actualErr := canonicalFunction(selectedCausalObservationBody(t, row.File, nativePipelineRecipeReplacementName(t, row)))
		if err != nil || actualErr != nil || actual != got {
			t.Fatalf("native node interception differs from its finite snapshot: %s", row.Function)
		}
		for _, token := range []string{"WithPipelinePostCommitActions", "OwnerAction", "newDeliveryAuthorityCoordinator", "seedDeliveryAuthority", "testDB("} {
			if strings.Contains(row.After, token) {
				t.Fatalf("node interceptor still carries obsolete transaction marker: %s", token)
			}
		}
		for _, required := range []string{"nativeReceiverPreparationFixtureForTest(t, backend, open)", "fixture.Store.Outcomes(ctx, id)", "len(outcomes) != 1"} {
			if !strings.Contains(row.After, required) {
				t.Fatalf("native interceptor lost exact ownership/outcome proof: %s", required)
			}
		}
		if strings.Contains(row.Function, "Terminal") {
			for _, required := range []string{"fixture.Store.ClaimDelivery", "fixture.Store.SettleFailure", "runtimedelivery.FailureDeadLetter", "fixture.DeliveryCount", "count != 1", "bus.publishedCount(); got != 0", "if !passthrough"} {
				if !strings.Contains(row.After, required) {
					t.Fatalf("terminal interceptor weakened: %s", required)
				}
			}
		} else if !strings.Contains(row.After, "deliveryAuthorityLogCount(bus.runtimeLogEntries()) != 0") || !strings.Contains(row.After, "if passthrough") {
			t.Fatal("target interceptor weakened consumed-target/no-false-authority-log proof")
		}
	}
	if matched != 2 {
		t.Fatalf("node-context recipes=%d,want2 plus extended native unstamped witness", matched)
	}
}
