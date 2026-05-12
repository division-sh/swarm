package correlation

import (
	"context"
	"testing"

	"swarm/internal/events"
)

func TestCorrelateEvent_InheritsRunAndParentWithoutGeneratingTrace(t *testing.T) {
	inbound := events.Event{
		ID:    "evt-parent",
		Type:  events.EventType("task.started"),
		RunID: "run-123",
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

func TestCorrelateEvent_UsesTypedRuntimeLineageParent(t *testing.T) {
	ctx := WithRuntimeLineage(context.Background(), RuntimeLineage{
		Owner:               "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage",
		RunID:               "run-fork",
		SubjectEventID:      "evt-selected",
		SubjectEventType:    "validation/validation.package_ready",
		ParentEventID:       "evt-selected",
		RowCategory:         RuntimeLineageRowCategoryRuntimeContainer,
		SelectedForkOwner:   "runtime.run_fork.selected_contract_execution.fork_local_runtime_container",
		Classification:      RuntimeLineageClassificationForkLocal,
		SelectedForkContext: true,
	})

	_, child := CorrelateEvent(ctx, events.Event{
		ID:   "evt-child",
		Type: events.EventType("task.completed"),
	})

	if got := child.RunID; got != "run-fork" {
		t.Fatalf("child run_id = %q, want run-fork", got)
	}
	if got := child.ParentEventID; got != "evt-selected" {
		t.Fatalf("child parent_event_id = %q, want evt-selected", got)
	}
}

func TestCorrelateEvent_DoesNotSelfParentTypedRuntimeLineageSubject(t *testing.T) {
	ctx := WithRuntimeLineage(context.Background(), RuntimeLineage{
		RunID:          "run-fork",
		SubjectEventID: "evt-selected",
		ParentEventID:  "evt-selected",
	})

	_, selected := CorrelateEvent(ctx, events.Event{
		ID:   "evt-selected",
		Type: events.EventType("validation/validation.package_ready"),
	})

	if got := selected.RunID; got != "run-fork" {
		t.Fatalf("selected run_id = %q, want run-fork", got)
	}
	if got := selected.ParentEventID; got != "" {
		t.Fatalf("selected parent_event_id = %q, want empty", got)
	}
}

func TestWithInboundEvent_RefinesTypedRuntimeLineageSubject(t *testing.T) {
	ctx := WithRuntimeLineage(context.Background(), RuntimeLineage{
		Owner:               "owner",
		RunID:               "run-fork",
		SelectedForkContext: true,
	})
	ctx = WithInboundEvent(ctx, events.Event{
		ID:    "evt-selected",
		Type:  events.EventType("task.assigned"),
		RunID: "run-fork",
	})

	lineage, ok := RuntimeLineageFromContext(ctx)
	if !ok {
		t.Fatal("expected runtime lineage")
	}
	if lineage.SubjectEventID != "evt-selected" || lineage.ParentEventID != "evt-selected" || lineage.SubjectEventType != "task.assigned" {
		t.Fatalf("lineage = %#v, want inbound subject/parent", lineage)
	}
}
