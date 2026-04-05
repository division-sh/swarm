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
