package correlation

import (
	"context"
	"testing"

	"empireai/internal/events"
)

func TestCorrelateEvent_InheritsInboundTraceAndParent(t *testing.T) {
	inbound := events.Event{
		ID:      "evt-parent",
		Type:    events.EventType("task.started"),
		TraceID: "trace-123",
	}
	ctx := WithInboundEvent(context.Background(), inbound)

	ctx, child := CorrelateEvent(ctx, events.Event{
		ID:   "evt-child",
		Type: events.EventType("task.completed"),
	})

	if got := child.TraceID; got != "trace-123" {
		t.Fatalf("child trace_id = %q, want trace-123", got)
	}
	if got := child.ParentEventID; got != "evt-parent" {
		t.Fatalf("child parent_event_id = %q, want evt-parent", got)
	}
	if got := TraceIDFromContext(ctx); got != "trace-123" {
		t.Fatalf("context trace_id = %q, want trace-123", got)
	}
}

func TestCorrelateEvent_GeneratesTraceWithoutInbound(t *testing.T) {
	_, evt := CorrelateEvent(context.Background(), events.Event{
		ID:   "evt-root",
		Type: events.EventType("task.started"),
	})
	if evt.TraceID == "" {
		t.Fatal("expected generated trace_id")
	}
	if evt.ParentEventID != "" {
		t.Fatalf("parent_event_id = %q, want empty", evt.ParentEventID)
	}
}
