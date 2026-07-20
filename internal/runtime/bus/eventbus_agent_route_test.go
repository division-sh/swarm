package bus

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func TestEventBusAgentRouteReplacementWaitsForExactDequeuedPredecessor(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 2}
	oldCh := eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.work")))
	if oldCh == nil {
		t.Fatal("predecessor route was not installed")
	}
	evt := eventtest.RuntimeControl("work-old", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver predecessor event: %v", err)
	}
	delivery := <-oldCh

	replaced := make(chan (<-chan *LocalDelivery), 1)
	go func() {
		replaced <- eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.work")))
	}()
	select {
	case <-replaced:
		t.Fatal("replacement published before dequeued predecessor completed")
	case <-time.After(25 * time.Millisecond):
	}
	if err := delivery.Complete(); err != nil {
		t.Fatalf("complete predecessor delivery: %v", err)
	}
	var newCh <-chan *LocalDelivery
	select {
	case newCh = <-replaced:
	case <-time.After(time.Second):
		t.Fatal("replacement did not finish after predecessor completion")
	}
	if newCh == nil || newCh == oldCh {
		t.Fatal("replacement did not publish an exact fresh route")
	}

	newEvent := eventtest.RuntimeControl("work-new", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), newEvent, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver successor event: %v", err)
	}
	newDelivery := <-newCh
	if newDelivery.ID() != "work-new" {
		t.Fatalf("successor event id = %q", newDelivery.ID())
	}
	if err := newDelivery.Complete(); err != nil {
		t.Fatalf("complete successor delivery: %v", err)
	}
}

func TestEventBusAgentRouteRemovalWaitsForDequeuedWork(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	token := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	ch := eb.ReplaceAgentRoute(token, testAgentSubscriptionAdmission(t, token.AgentID, events.EventType("test.work")))
	evt := eventtest.RuntimeControl("work-1", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	delivery := <-ch
	done := make(chan struct{})
	go func() { eb.RemoveAgentRoute(token); close(done) }()
	select {
	case <-done:
		t.Fatal("route removal returned before dequeued work completed")
	case <-time.After(25 * time.Millisecond):
	}
	if err := delivery.Complete(); err != nil {
		t.Fatalf("complete route delivery: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("route removal did not join completed work")
	}
}

func TestEventBusAgentRouteBufferedRemovalFailsClosedWithoutDurableHandoff(t *testing.T) {
	eb, err := newScopedTestEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 2}
	eb.ReplaceAgentRoute(oldToken, testAgentSubscriptionAdmission(t, oldToken.AgentID, events.EventType("test.work")))
	evt := eventtest.RuntimeControl("work-buffered", events.EventType("test.work"), "test", "", []byte(`{}`), 0, "run-1", "", events.EventEnvelope{}, time.Now())
	if err := eb.deliverToAgents(context.Background(), evt, []string{"agent-a"}); err != nil {
		t.Fatalf("deliver event: %v", err)
	}
	if got := eb.ReplaceAgentRoute(newToken, testAgentSubscriptionAdmission(t, newToken.AgentID, events.EventType("test.work"))); got != nil {
		t.Fatal("successor route published after unproven buffered handoff")
	}
}
