package bus

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func testAgentSubscriptionAdmission(t *testing.T, agentID string, eventTypes ...events.EventType) semanticview.FlowOwnedAgentSubscriptionAdmission {
	return testAgentSubscriptionAdmissionForFlow(t, agentID, "", eventTypes...)
}

func TestEventBusRawSubscribeRejectsQualifiedExactAuthority(t *testing.T) {
	eb, err := NewEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatal(err)
	}
	eventType := events.EventType("producer/task.done")
	if ch := eb.Subscribe("raw-agent", eventType); ch != nil {
		t.Fatalf("raw Subscribe(%q) returned a live channel", eventType)
	}
	if recipients := eb.ResolveSubscribedRecipients(string(eventType)); len(recipients) != 0 {
		t.Fatalf("raw Subscribe(%q) recipients = %#v, want none", eventType, recipients)
	}
}

func TestEventBusRawSubscribeConsumesRootAdmissionForExactAndWildcard(t *testing.T) {
	eb, err := NewEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatal(err)
	}
	for index, eventType := range []events.EventType{"task.ready", "task.*"} {
		agentID := fmt.Sprintf("root-agent-%d", index)
		if ch := eb.Subscribe(agentID, eventType); ch == nil {
			t.Fatalf("raw root Subscribe(%q) returned nil channel", eventType)
		}
		if recipients := eb.ResolveSubscribedRecipients("task.ready"); !slices.Contains(recipients, agentID) {
			t.Fatalf("raw root Subscribe(%q) recipients = %#v, want %s", eventType, recipients, agentID)
		}
	}
}

func TestEventBusTypedAdmissionExecutesSameScopeExactAndWildcard(t *testing.T) {
	eb, err := NewEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatal(err)
	}
	admission := testAgentSubscriptionAdmissionForFlow(t, "reviewer", "review/inst-1",
		events.EventType("task.ready"), events.EventType("task.*"))
	ch := eb.SubscribeAgent(admission)
	if ch == nil {
		t.Fatal("typed admission returned nil channel")
	}
	defer eb.Unsubscribe("reviewer")

	for index, eventType := range []events.EventType{"review/inst-1/task.ready", "review/inst-1/task.done"} {
		evt := eventtest.RootIngress(eventtest.UUID("evt-admitted-"+string(rune('a'+index))), eventType, "test", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())
		if err := eb.Publish(context.Background(), evt); err != nil {
			t.Fatalf("Publish(%s): %v", eventType, err)
		}
		select {
		case got := <-ch:
			if got.ID() != evt.ID() {
				t.Fatalf("received %s, want %s", got.ID(), evt.ID())
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s", eventType)
		}
	}
}

func testAgentSubscriptionAdmissionForFlow(t *testing.T, agentID, flowPath string, eventTypes ...events.EventType) semanticview.FlowOwnedAgentSubscriptionAdmission {
	t.Helper()
	values := make([]string, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		values = append(values, string(eventType))
	}
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID: agentID, FlowPath: flowPath, Subscriptions: values,
	})
	if err != nil {
		t.Fatalf("admit test agent subscriptions: %v", err)
	}
	return admission
}
