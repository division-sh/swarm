package testcases

import (
	"testing"

	runtimepipeline "empireai/internal/runtime/pipeline"
)

func TestGenericBundle_AccumulationFanoutPatterns(t *testing.T) {
	bundle := loadGenericMASBundle(t)
	created := mustHandler(t, bundle, "intake-router", "item.created")
	if created.Guard == nil || created.Guard.ID != "accept_item" {
		t.Fatalf("expected inline guard id on item.created, got %+v", created.Guard)
	}
	if created.FanOut == nil {
		t.Fatal("expected fan_out on item.created")
	}
	if created.FanOut.ItemsFrom != "payload.items" || created.FanOut.Target != "worker-a" || created.FanOut.EmitPerItem != "item.processed" {
		t.Fatalf("unexpected fan_out spec: %+v", created.FanOut)
	}
	if len(created.Branch) != 1 || created.Branch[0].Then == nil || created.Branch[0].Else == nil {
		t.Fatalf("expected urgent/non-urgent branch on item.created, got %+v", created.Branch)
	}
	fannedOut := previewHandler(t, bundle, "intake-router", "item.created", map[string]any{
		"item_id":   "item-123",
		"priority":  "urgent",
		"items":     []map[string]any{{"id": "a"}, {"id": "b"}, {"id": "c"}},
		"entity_id": "item-123",
	}, runtimepipeline.WorkflowState{
		EntityID:   "item-123",
		Stage:      runtimepipeline.NormalizeWorkflowStateID("queued"),
		Status:     "queued",
		Metadata:   map[string]any{},
	}, nil)
	if fannedOut.Status != runtimepipeline.HandlerOutcomeFannedOut {
		t.Fatalf("expected fan-out execution, got %+v", fannedOut)
	}
	if fannedOut.FanOutCount != 3 {
		t.Fatalf("expected 3 fan-out items, got %+v", fannedOut)
	}

	processed := mustHandler(t, bundle, "intake-router", "item.processed")
	if processed.Accumulate == nil || len(processed.Accumulate.OnComplete) == 0 {
		t.Fatal("expected accumulate.on_complete on item.processed")
	}
	completed := previewHandler(t, bundle, "intake-router", "item.processed", map[string]any{
		"entity_id":        "item-123",
		"expected_workers": 1,
		"result":           map[string]any{"worker": "done"},
		"source":           "worker-a",
		"received_count":   1,
	}, runtimepipeline.WorkflowState{
		EntityID:   "item-123",
		Stage:      runtimepipeline.NormalizeWorkflowStateID("collecting"),
		Status:     "collecting",
		Metadata: map[string]any{
			"received_count": 1,
		},
	}, nil)
	if completed.Stage != runtimepipeline.NormalizeWorkflowStateID("ready") {
		t.Fatalf("expected ready state after accumulation completion, got %+v", completed)
	}
	if !hasAll(completed.Emits, "item.review_requested") {
		t.Fatalf("expected intake completion to emit item.review_requested, got %v", completed.Emits)
	}
	if completed.SetsGate != "intake_ready" {
		t.Fatalf("expected intake_ready gate, got %q", completed.SetsGate)
	}
	if !hasAll(completed.ClearGates, "needs_revision") {
		t.Fatalf("expected needs_revision gate cleared, got %v", completed.ClearGates)
	}
}
