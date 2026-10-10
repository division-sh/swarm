package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const busAgentDeliveryCountShape = `func countEventDeliveriesForEvent(t *testing.T,ctx context.Context,selected any,eventID string) int {
t.Helper()
count,err := storetest.CountAgentEventDeliveryStorage(ctx,selected,eventID)
if err != nil { t.Fatalf("count event deliveries for %s: %v",eventID,err) }
return count
}`

func busAgentDeliveryCountPreserved(source string) bool {
	want, err := canonicalFunction(busAgentDeliveryCountShape)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeBusAgentCountPreservesStandaloneAndDeliveredCuts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-agent-delivery-count" {
			continue
		}
		matched++
		if row.Function == "countEventDeliveriesForEvent" {
			if !busAgentDeliveryCountPreserved(row.After) {
				t.Fatal("agent cardinality helper lost exact owner/event, all-state count or errors")
			}
			continue
		}
		if normalizedNativeStandaloneSetup(strings.ReplaceAll(row.Before, "countEventDeliveriesForEvent(t, ctx, db,", "countEventDeliveriesForEvent(t, ctx, pg,")) != row.After {
			t.Fatal("standalone creation/completion, delivery settlement or count expectations changed")
		}
	}
	if matched != 3 {
		t.Fatalf("agent count recipes=%d, want3 plus two updated existing roots", matched)
	}
	actual := selectedCausalObservationBody(t, "internal/runtime/bus/eventbus_publish_test.go", "countEventDeliveriesForEvent")
	for _, pair := range [][2]string{
		{"ctx, selected, eventID", "ctx, foreign, eventID"},
		{"ctx, selected, eventID", "ctx, selected, otherEvent"},
		{"if err != nil", "if false"}, {"return count", "return 0"},
	} {
		mutant := strings.Replace(actual, pair[0], pair[1], 1)
		if mutant == actual || busAgentDeliveryCountPreserved(mutant) {
			t.Fatalf("weakened native agent cardinality admitted: %v", pair)
		}
	}
	owner := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_api_event_storage.go", "CountAgentEventDeliveryStorageForTest")
	if !strings.Contains(owner, "storedelivery.FixtureAgentEventCardinalityTx(ctx, tx, eventID)") || strings.Contains(owner, "SELECT ") {
		t.Fatal("agent count introduced a second query instead of consuming closed delivery ownership")
	}
}
