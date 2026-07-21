package bus

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func testAgentSubscriptionAdmission(t *testing.T, agentID string, eventTypes ...events.EventType) semanticview.FlowOwnedAgentSubscriptionAdmission {
	return testAgentSubscriptionAdmissionForFlow(t, agentID, "", eventTypes...)
}

func TestEventBusHasNoTokenlessAgentSubscriptionSurface(t *testing.T) {
	eventBusType := reflect.TypeOf((*EventBus)(nil))
	for _, method := range []string{"Subscribe", "SubscribeAgent", "Unsubscribe"} {
		if _, ok := eventBusType.MethodByName(method); ok {
			t.Fatalf("EventBus retains tokenless production method %s", method)
		}
	}
}

func TestEventBusTypedAdmissionExecutesSameScopeExactAndWildcard(t *testing.T) {
	eb, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatal(err)
	}
	admission := testAgentSubscriptionAdmissionForFlow(t, "reviewer", "review/inst-1",
		events.EventType("task.ready"), events.EventType("task.*"))
	ch := subscribeTestAgentAdmission(t, eb, admission)
	if ch == nil {
		t.Fatal("typed admission returned nil channel")
	}
	defer unsubscribeTestAgent(eb, "reviewer")

	for index, eventType := range []events.EventType{"review/inst-1/task.ready", "review/inst-1/task.done"} {
		evt := eventtest.RunCreatingRootIngress(eventtest.UUID("evt-admitted-"+string(rune('a'+index))), eventType, "test", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())
		if err := eb.Publish(context.Background(), evt); err != nil {
			t.Fatalf("Publish(%s): %v", eventType, err)
		}
		select {
		case got := <-ch:
			if got.ID() != evt.ID() {
				t.Fatalf("received %s, want %s", got.ID(), evt.ID())
			}
			if err := got.Complete(); err != nil {
				t.Fatalf("complete %s: %v", eventType, err)
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
