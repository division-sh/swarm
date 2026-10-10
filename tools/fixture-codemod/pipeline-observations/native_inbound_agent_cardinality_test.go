package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeInboundAgentCardinalityRetainsExactRoleAndEveryProviderAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Family == "native-inbound-agent-cardinality" {
			t.Fatal("superseded cardinality adapters must not overwrite the upstream read owner")
		}
	}
	owner := selectedCausalObservationBody(t, "internal/runtime/inbound_readback_test.go", "inboundAgentDeliveries")
	for _, required := range []string{"selected.LoadOperatorEvent(ctx, eventID)", "range event.Deliveries", `delivery.SubscriberType == "agent" && delivery.SubscriberID == agentID`, "matches = append(matches, delivery)"} {
		if !strings.Contains(owner, required) {
			t.Fatalf("canonical agent cardinality changed exact event/role/subscriber/context: %s", required)
		}
	}
	count := selectedCausalObservationBody(t, "internal/runtime/inbound_readback_test.go", "countInboundAgentDeliveries")
	if !strings.Contains(count, "return len(inboundAgentDeliveries(t, ctx, selected, eventID, agentID))") {
		t.Fatal("agent count applies a second predicate or status filter")
	}
	for _, name := range []string{"TestInboundGateway_GitHubPausedRuntimePersistsAndReleasesSubscribedDispatch", "TestInboundGateway_SlackPausedRuntimePersistsAndReleasesSubscribedDispatch", "TestInboundGateway_StripePausedRuntimePersistsAndReleasesSubscribedDispatch", "TestInboundGateway_TwilioPostgresPersistsConfiguredManifestDelivery", "TestInboundGateway_ShopifyPostgresPersistsConfiguredManifestDelivery", "TestInboundGateway_TelegramPostgresPersistsConfiguredManifestDelivery", "TestInboundGateway_TypeformAndIntercomPostgresPersistsConfiguredManifestDelivery"} {
		body := selectedCausalObservationBody(t, "internal/runtime/inbound_postgres_test.go", name)
		if strings.Count(body, "countInboundAgentDeliveries(t, ctx, pg,") != 1 || strings.Contains(body, "countPostgresAgentDeliveriesForEvent(") {
			t.Fatalf("%s bypassed the canonical selected cardinality", name)
		}
	}
	cleanup := selectedCausalObservationBody(t, "internal/runtime/inbound_commit_ack_response_test.go", "TestInboundAcknowledgedPublicationCleanupRespondsAndDoesNotRedeliverBothStores")
	for _, required := range []string{"countInboundAgentDeliveries(t, ctx, readback, rawEventID, agentID)", "if deliveries != 1", "acknowledged duplicate must not redeliver", "wrapped.commits != 2", "wrapped.commits != 3"} {
		if !strings.Contains(cleanup, required) {
			t.Fatalf("acknowledgment cleanup/replay assertion lost: %s", required)
		}
	}
}
