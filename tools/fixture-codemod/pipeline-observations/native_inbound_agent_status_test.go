package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeInboundAgentStatusPreservesExactPhysicalLookupAndPendingAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Family == "native-inbound-agent-status" {
			t.Fatal("superseded status adapter must not overwrite canonical readback")
		}
	}
	owner := selectedCausalObservationBody(t, "internal/runtime/inbound_readback_test.go", "loadInboundAgentDeliveryStatus")
	for _, required := range []string{"inboundAgentDeliveries(t, ctx, selected, eventID, agentID)", "len(deliveries) != 1", "return deliveries[0].Status"} {
		if !strings.Contains(owner, required) {
			t.Fatalf("exact status lookup/cardinality changed: %s", required)
		}
	}
	for _, name := range []string{"TestInboundGateway_GitHubPausedRuntimePersistsAndReleasesSubscribedDispatch", "TestInboundGateway_SlackPausedRuntimePersistsAndReleasesSubscribedDispatch", "TestInboundGateway_StripePausedRuntimePersistsAndReleasesSubscribedDispatch"} {
		body := selectedCausalObservationBody(t, "internal/runtime/inbound_postgres_test.go", name)
		if !strings.Contains(body, `loadInboundAgentDeliveryStatus(t, ctx, pg, eventID, agentID); got != "pending"`) {
			t.Fatalf("pending cut lost its selected status assertion: %s", name)
		}
	}
}
