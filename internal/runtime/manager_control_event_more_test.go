package runtime

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

func TestAgentManager_ControlEvent_SpinupRequested_SpawnsOpCo(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	am.Run(ctx)
	defer func() { _ = am.Shutdown() }()

	if am.Count() != 0 {
		t.Fatalf("expected empty manager initially")
	}

	payload := []byte(`{"vertical_id":"v1","mandate":{"vertical_id":"v1","founder_notes":"go"}}`)
	if err := bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.spinup_requested"),
		SourceAgent: "tester",
		VerticalID:  "v1",
		Payload:     payload,
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		if am.Count() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected opco agents spawned")
}

