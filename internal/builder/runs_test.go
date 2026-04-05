package builder

import (
	"context"
	"testing"
	"time"

	runtimepkg "swarm/internal/runtime"
	runtimebus "swarm/internal/runtime/bus"
)

func TestRunHubStartRunPublishesTypedEntityEnvelope(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	hub := newRunHub(func() *runtimepkg.Runtime { return rt }, nil, nil, nil)
	ch := eb.Subscribe("observer", "review.requested")

	if err := hub.startRun(context.Background(), "run-123", map[string]any{
		"review.requested": map[string]any{
			"entity_id": "ent-001",
			"name":      "Telemedicine",
		},
	}, nil); err != nil {
		t.Fatalf("startRun: %v", err)
	}

	select {
	case evt := <-ch:
		if got := evt.EntityID(); got != "ent-001" {
			t.Fatalf("event entity_id = %q, want ent-001", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected run input event to be published")
	}
}

func TestRunHubAwaitCompletion_MarksSessionTerminalWhenCompletionPersistenceFails(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	hub := &runHub{
		sessions: map[string]*runSession{
			"run-123": {
				runID:   "run-123",
				runtime: rt,
				subs:    map[string]func(RunEventEnvelope){},
				events:  []RunEventEnvelope{},
			},
		},
	}

	hub.awaitCompletion("run-123")

	if !hub.isTerminal("run-123") {
		t.Fatal("expected run session to be marked terminal when completion persistence fails")
	}
	session := hub.session("run-123")
	if session == nil {
		t.Fatal("expected run session to remain addressable")
	}
	if len(session.events) == 0 {
		t.Fatal("expected terminal failure event to be emitted")
	}
	last := session.events[len(session.events)-1]
	if got, _ := last["type"].(string); got != "run.failed" {
		t.Fatalf("last event type = %q, want run.failed", got)
	}
	payload, _ := last["payload"].(map[string]any)
	if _, ok := payload["persistence_error"]; !ok {
		t.Fatalf("last payload = %#v, want persistence_error", payload)
	}
}
