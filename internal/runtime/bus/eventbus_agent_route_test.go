package bus

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func TestEventBusAgentRouteReplacementIsExactFreshAndTokenFenced(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 2}
	oldCh := eb.ReplaceAgentRoute(oldToken, events.EventType("test.old"), events.EventType("test.retained"))
	queued := eventtest.RuntimeControl("queued", events.EventType("test.old"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), queued, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver predecessor event: %v", err)
	}

	newCh := eb.ReplaceAgentRoute(newToken, events.EventType("test.retained"), events.EventType("test.new"))
	if oldCh == newCh {
		t.Fatal("replacement reused predecessor channel")
	}
	if _, ok := eb.channels[events.EventType("test.old")]["agent-a"]; ok {
		t.Fatal("removed predecessor subscription remains routed")
	}
	for _, eventType := range []events.EventType{"test.retained", "test.new"} {
		if got := eb.channels[eventType]["agent-a"]; got != newCh {
			t.Fatalf("route %q channel = %v, want successor channel", eventType, got)
		}
	}
	select {
	case evt := <-newCh:
		t.Fatalf("predecessor queue transferred to successor: %s", evt.ID())
	default:
	}
	if got := eb.Subscribe("agent-a", events.EventType("test.tokenless")); got != newCh {
		t.Fatal("generic subscribe replaced an exact lifecycle route")
	}
	if _, ok := eb.channels[events.EventType("test.tokenless")]["agent-a"]; ok {
		t.Fatal("generic subscribe mutated an exact lifecycle route")
	}
	eb.Unsubscribe("agent-a")
	if got := eb.agentChans["agent-a"]; got != newCh {
		t.Fatal("tokenless unsubscribe removed an exact lifecycle route")
	}

	eb.RemoveAgentRoute(oldToken)
	afterStaleRemove := eventtest.RuntimeControl("successor", events.EventType("test.new"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), afterStaleRemove, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver after stale remove: %v", err)
	}
	select {
	case evt := <-newCh:
		if evt.ID() != "successor" {
			t.Fatalf("successor event id = %q", evt.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("stale predecessor cleanup removed successor route")
	}

	eb.RemoveAgentRoute(newToken)
	if _, ok := eb.agentChans["agent-a"]; ok {
		t.Fatal("exact successor route survived removal")
	}
	select {
	case _, ok := <-oldCh:
		if !ok {
			t.Fatal("detached predecessor channel was closed")
		}
	default:
	}
}

func TestEventBusAgentRouteDeliveryRemainsPendingAfterDequeueUntilCompletion(t *testing.T) {
	eb, err := NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	token := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	ch := eb.ReplaceAgentRoute(token, events.EventType("test.work"))
	evt := eventtest.RuntimeControl("work-1", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	if got := eb.PendingAgentRouteDeliveries(); got != 1 {
		t.Fatalf("pending route deliveries after enqueue = %d, want 1", got)
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("route delivery was not dequeued")
	}
	if got := eb.PendingAgentDeliveries(); got != 0 {
		t.Fatalf("channel-backed pending deliveries after dequeue = %d, want 0", got)
	}
	if got := eb.PendingAgentRouteDeliveries(); got != 1 {
		t.Fatalf("tracked pending route deliveries after dequeue = %d, want 1", got)
	}
	eb.CompleteAgentRouteDelivery(token)
	if got := eb.PendingAgentRouteDeliveries(); got != 0 {
		t.Fatalf("pending route deliveries after completion = %d, want 0", got)
	}
}
