package runtime_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type inboundGatewayReadback interface {
	LoadInboundPublicationByIdentity(context.Context, runtimeinbound.Identity) (runtimeinbound.Record, bool, error)
	LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
	ListOperatorEvents(context.Context, operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error)
}

func readInboundPublication(t *testing.T, ctx context.Context, selected inboundGatewayReadback, target runtimepkg.InboundTarget, provider, deliveryID string) runtimeinbound.Record {
	t.Helper()
	identity := inboundTestReceiptIdentity(target, provider, deliveryID)
	record, found, err := selected.LoadInboundPublicationByIdentity(ctx, identity)
	if err != nil || !found {
		t.Fatalf("read inbound publication %+v: found=%t err=%v", identity, found, err)
	}
	marker, err := selected.LoadOperatorEvent(ctx, record.MarkerEventID)
	if err != nil {
		t.Fatalf("read inbound marker %s: %v", record.MarkerEventID, err)
	}
	if marker.EventName != "platform.inbound_recorded" || marker.EntityID != "" ||
		marker.Payload["publication_id"] != record.PublicationID || marker.Payload["provider"] != provider ||
		marker.Payload["provider_event_id"] != deliveryID || marker.Payload["run_id"] != target.RunID ||
		marker.Payload["service_id"] != target.ServiceID || record.ExpectedGeneration != target.Generation ||
		record.OutputCount != len(record.Events) {
		t.Fatalf("inbound receipt/marker mismatch: record=%+v marker=%+v", record, marker)
	}
	return record
}

func inboundEventsWithPayload(t *testing.T, ctx context.Context, selected inboundGatewayReadback, runID, eventName, provider, deliveryID string) []operatorread.OperatorEventFull {
	t.Helper()
	opts := operatorread.OperatorEventListOptions{
		Filter: operatorread.OperatorEventListFilter{RunID: runID, EventName: eventName}, Limit: 100,
	}
	var matches []operatorread.OperatorEventFull
	for {
		page, err := selected.ListOperatorEvents(ctx, opts)
		if err != nil {
			t.Fatalf("list inbound events: %v", err)
		}
		for _, event := range page.Events {
			if event.Payload["provider_event_id"] == deliveryID && (provider == "" || event.Payload["provider"] == provider) {
				matches = append(matches, event)
			}
		}
		if page.NextCursor == "" {
			return matches
		}
		opts.Cursor = page.NextCursor
	}
}

func countInboundMarkers(t *testing.T, ctx context.Context, selected inboundGatewayReadback, target runtimepkg.InboundTarget, provider, deliveryID string) int {
	t.Helper()
	record := readInboundPublication(t, ctx, selected, target, provider, deliveryID)
	markers := inboundEventsWithPayload(t, ctx, selected, "", "platform.inbound_recorded", provider, deliveryID)
	count := 0
	for _, marker := range markers {
		if marker.Payload["publication_id"] != record.PublicationID {
			continue
		}
		if marker.EventID != record.MarkerEventID {
			t.Fatalf("unexpected marker outside exact receipt: %s, want %s", marker.EventID, record.MarkerEventID)
		}
		count++
	}
	return count
}

func loadInboundProviderEventID(t *testing.T, ctx context.Context, selected inboundGatewayReadback, target runtimepkg.InboundTarget, provider, eventName, deliveryID string) string {
	t.Helper()
	record := readInboundPublication(t, ctx, selected, target, provider, deliveryID)
	var eventID string
	var entityID string
	for _, output := range record.Events {
		if output.EventName == eventName {
			if eventID != "" {
				t.Fatalf("duplicate receipt output for %s", eventName)
			}
			eventID = output.EventID
			entityID = output.Event.EntityID()
		}
	}
	if eventID == "" {
		t.Fatalf("receipt lacks output %s: %+v", eventName, record)
	}
	event, err := selected.LoadOperatorEvent(ctx, eventID)
	if err != nil || event.RunID != target.RunID || event.EntityID != entityID || event.EventName != eventName || event.Payload["provider_event_id"] != deliveryID {
		t.Fatalf("inbound output readback mismatch: event=%+v err=%v", event, err)
	}
	return eventID
}

func countInboundProviderEvents(t *testing.T, ctx context.Context, selected inboundGatewayReadback, runID, eventName, deliveryID string) int {
	t.Helper()
	return len(inboundEventsWithPayload(t, ctx, selected, runID, eventName, "", deliveryID))
}

func loadInboundProviderEventPayloadField(t *testing.T, ctx context.Context, selected inboundGatewayReadback, eventID, field string) string {
	t.Helper()
	event, err := selected.LoadOperatorEvent(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := event.Payload[field].(string)
	if !ok {
		t.Fatalf("payload %s is not a string: %+v", field, event.Payload)
	}
	return value
}

func inboundAgentDeliveries(t *testing.T, ctx context.Context, selected inboundGatewayReadback, eventID, agentID string) []operatorread.OperatorEventDelivery {
	t.Helper()
	event, err := selected.LoadOperatorEvent(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}
	var matches []operatorread.OperatorEventDelivery
	for _, delivery := range event.Deliveries {
		if delivery.SubscriberType == "agent" && delivery.SubscriberID == agentID {
			matches = append(matches, delivery)
		}
	}
	return matches
}

func countInboundAgentDeliveries(t *testing.T, ctx context.Context, selected inboundGatewayReadback, eventID, agentID string) int {
	t.Helper()
	return len(inboundAgentDeliveries(t, ctx, selected, eventID, agentID))
}

func loadInboundAgentDeliveryStatus(t *testing.T, ctx context.Context, selected inboundGatewayReadback, eventID, agentID string) string {
	t.Helper()
	deliveries := inboundAgentDeliveries(t, ctx, selected, eventID, agentID)
	if len(deliveries) != 1 {
		t.Fatalf("agent deliveries = %d, want 1", len(deliveries))
	}
	return deliveries[0].Status
}

func countInboundPipelineReceipts(t *testing.T, ctx context.Context, selected inboundGatewayReadback, runID, eventID string) int {
	t.Helper()
	return storetest.ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, eventID).PipelineReceiptCount
}

func countInboundNonPlatformReceipts(t *testing.T, ctx context.Context, selected inboundGatewayReadback, runID, eventID string) int {
	t.Helper()
	return storetest.ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, eventID).NonPlatformReceiptCount
}
