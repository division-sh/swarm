package correlation

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"time"
)

func TestCorrelateEvent_InheritsRunAndParentWithoutGeneratingTrace(t *testing.T) {
	inbound := eventtest.RootIngress("evt-parent",
		events.EventType("task.started"), "", "", nil, 0, "run-123", "", events.EventEnvelope{}, time.Time{})

	ctx := WithInboundEvent(context.Background(), inbound)

	ctx, child := CorrelateEvent(ctx, eventtest.RootIngress("evt-child",
		events.EventType("task.completed"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))

	if got := child.RunID(); got != "run-123" {
		t.Fatalf("child run_id = %q, want run-123", got)
	}
	if got := child.ParentEventID(); got != "evt-parent" {
		t.Fatalf("child parent_event_id = %q, want evt-parent", got)
	}
	if got := RunIDFromContext(ctx); got != "run-123" {
		t.Fatalf("context run_id = %q, want run-123", got)
	}
}

func TestCorrelateEvent_DoesNotGenerateRunWithoutAdmission(t *testing.T) {
	ctx, evt := CorrelateEvent(context.Background(), eventtest.RootIngress("evt-root",
		events.EventType("task.started"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if evt.RunID() != "" {
		t.Fatalf("run_id = %q, want empty before admission", evt.RunID())
	}
	if got := RunIDFromContext(ctx); got != "" {
		t.Fatalf("context run_id = %q, want empty before admission", got)
	}
	if evt.ParentEventID() != "" {
		t.Fatalf("parent_event_id = %q, want empty", evt.ParentEventID())
	}
}

func TestCorrelateEvent_PreservesExplicitEventRunID(t *testing.T) {
	ctx, evt := CorrelateEvent(context.Background(), eventtest.RootIngress("evt-child",
		events.EventType("task.started"), "", "", nil, 0, "run-explicit", "", events.EventEnvelope{}, time.Time{}))
	if got := evt.RunID(); got != "run-explicit" {
		t.Fatalf("event run_id = %q, want run-explicit", got)
	}
	if got := RunIDFromContext(ctx); got != "run-explicit" {
		t.Fatalf("context run_id = %q, want run-explicit", got)
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

	_, child := CorrelateEvent(ctx, eventtest.RootIngress("evt-child",
		events.EventType("task.completed"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))

	if got := child.RunID(); got != "run-fork" {
		t.Fatalf("child run_id = %q, want run-fork", got)
	}
	if got := child.ParentEventID(); got != "evt-selected" {
		t.Fatalf("child parent_event_id = %q, want evt-selected", got)
	}
}

func TestCorrelateEvent_DoesNotSelfParentTypedRuntimeLineageSubject(t *testing.T) {
	ctx := WithRuntimeLineage(context.Background(), RuntimeLineage{
		RunID:          "run-fork",
		SubjectEventID: "evt-selected",
		ParentEventID:  "evt-selected",
	})

	_, selected := CorrelateEvent(ctx, eventtest.RootIngress("evt-selected",
		events.EventType("validation/validation.package_ready"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))

	if got := selected.RunID(); got != "run-fork" {
		t.Fatalf("selected run_id = %q, want run-fork", got)
	}
	if got := selected.ParentEventID(); got != "" {
		t.Fatalf("selected parent_event_id = %q, want empty", got)
	}
}

func TestWithInboundEvent_RefinesTypedRuntimeLineageSubject(t *testing.T) {
	ctx := WithRuntimeLineage(context.Background(), RuntimeLineage{
		Owner:               "owner",
		RunID:               "run-fork",
		SelectedForkContext: true,
	})
	ctx = WithInboundEvent(ctx, eventtest.RootIngress("evt-selected",
		events.EventType("task.assigned"), "", "", nil, 0, "run-fork", "", events.EventEnvelope{}, time.Time{}))

	lineage, ok := RuntimeLineageFromContext(ctx)
	if !ok {
		t.Fatal("expected runtime lineage")
	}
	if lineage.SubjectEventID != "evt-selected" || lineage.ParentEventID != "evt-selected" || lineage.SubjectEventType != "task.assigned" {
		t.Fatalf("lineage = %#v, want inbound subject/parent", lineage)
	}
}
