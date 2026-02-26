package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"empireai/internal/events"
)

func TestEventBusRejectsStaleRuntimeEpoch(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})

	previous := CurrentRuntimeEpoch()
	current := BumpRuntimeEpoch()
	if current <= previous {
		t.Fatalf("expected bumped epoch > previous, got current=%d previous=%d", current, previous)
	}

	staleCtx := WithRuntimeEpoch(context.Background(), previous)
	err := bus.Publish(staleCtx, events.Event{
		ID:          "evt-stale",
		Type:        events.EventType("system.started"),
		SourceAgent: "runtime",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now(),
	})
	if !errors.Is(err, ErrStaleRuntimeEpoch) {
		t.Fatalf("expected ErrStaleRuntimeEpoch, got %v", err)
	}

	currentCtx := WithRuntimeEpoch(context.Background(), current)
	if err := bus.Publish(currentCtx, events.Event{
		ID:          "evt-current",
		Type:        events.EventType("system.started"),
		SourceAgent: "runtime",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("publish with current epoch failed: %v", err)
	}
}
