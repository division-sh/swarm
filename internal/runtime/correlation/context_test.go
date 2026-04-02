package correlation

import (
	"context"
	"testing"

	"swarm/internal/events"
)

func TestCorrelateEvent_InheritsRunAndParentWithoutGeneratingTrace(t *testing.T) {
	inbound := events.Event{
		ID:      "evt-parent",
		Type:    events.EventType("task.started"),
		RunID:   "run-123",
	}
	ctx := WithInboundEvent(context.Background(), inbound)

	ctx, child := CorrelateEvent(ctx, events.Event{
		ID:   "evt-child",
		Type: events.EventType("task.completed"),
	})

	if got := child.RunID; got != "run-123" {
		t.Fatalf("child run_id = %q, want run-123", got)
	}
	if got := child.ParentEventID; got != "evt-parent" {
		t.Fatalf("child parent_event_id = %q, want evt-parent", got)
	}
	if got := RunIDFromContext(ctx); got != "run-123" {
		t.Fatalf("context run_id = %q, want run-123", got)
	}
}

func TestCorrelateEvent_GeneratesRunWithoutGeneratingTrace(t *testing.T) {
	ctx, evt := CorrelateEvent(context.Background(), events.Event{
		ID:   "evt-root",
		Type: events.EventType("task.started"),
	})
	if evt.RunID == "" {
		t.Fatal("expected generated run_id")
	}
	if got := RunIDFromContext(ctx); got != evt.RunID {
		t.Fatalf("context run_id = %q, want %q", got, evt.RunID)
	}
	if evt.ParentEventID != "" {
		t.Fatalf("parent_event_id = %q, want empty", evt.ParentEventID)
	}
}
