package testcases

import (
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestGenericBundle_AccumulationFanoutPatterns(t *testing.T) {
	bundle := loadGenericSwarmBundle(t)
	created := mustHandler(t, bundle, "intake-router", "item.created")
	if created.Guard == nil || created.Guard.Check != "payload.item_id != ''" {
		t.Fatalf("expected scalar guard on item.created, got %+v", created.Guard)
	}
	if created.FanOut == nil {
		t.Fatal("expected fan_out on item.created")
	}
	if created.FanOut.ItemsFrom != "payload.items" || created.FanOut.Emit.EventType() != "intake/item.processed" {
		t.Fatalf("unexpected fan_out spec: %+v", created.FanOut)
	}
	fannedOut := previewHandler(t, bundle, "intake-router", "item.created", map[string]any{
		"item_id":  "item-123",
		"priority": "urgent",
		"items": []map[string]any{
			{"id": "a", "score": 70.0},
			{"id": "b", "score": 80.0},
			{"id": "c", "score": 90.0},
		},
		"entity_id": "item-123",
	}, runtimepipeline.WorkflowState{
		EntityID: "item-123",
		Stage:    runtimepipeline.NormalizeWorkflowStateID("queued"),
		Status:   "queued",
		Metadata: map[string]any{},
	}, nil)
	if fannedOut.Status != runtimepipeline.HandlerOutcomeFannedOut {
		t.Fatalf("expected fan-out execution, got %+v", fannedOut)
	}
	if fannedOut.FanOutCount != 3 {
		t.Fatalf("expected 3 fan-out items, got %+v", fannedOut)
	}

	processed := mustHandler(t, bundle, "intake-router", "item.processed")
	if processed.Accumulate == nil || processed.Accumulate.Target != "processed_items" {
		t.Fatal("expected open stream accumulation on item.processed")
	}
	completed := previewHandler(t, bundle, "intake-router", "item.processed", map[string]any{
		"entity_id":        "item-123",
		"score":            80.0,
		"expected_workers": 1,
		"result":           map[string]any{"worker": "done"},
		"source":           "worker-a",
		"received_count":   1,
	}, runtimepipeline.WorkflowState{
		EntityID: "item-123",
		Stage:    runtimepipeline.NormalizeWorkflowStateID("collecting"),
		Status:   "collecting",
		Metadata: map[string]any{},
	}, nil)
	if completed.Stage != runtimepipeline.NormalizeWorkflowStateID("collecting") {
		t.Fatalf("stream arrival changed lifecycle state = %+v, want collecting", completed)
	}
	if !hasAll(completed.Emits, "intake/item.review_requested") {
		t.Fatalf("expected intake completion to emit item.review_requested, got %v", completed.Emits)
	}
	if completed.Metadata["intake_ready"] != true {
		t.Fatalf("expected intake_ready state, got %#v", completed.Metadata["intake_ready"])
	}
	if completed.Metadata["needs_revision"] != false {
		t.Fatalf("expected needs_revision state reset, got %#v", completed.Metadata["needs_revision"])
	}
}
