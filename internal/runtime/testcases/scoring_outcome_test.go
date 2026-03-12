package testcases

import (
	"testing"

	runtimepipeline "empireai/internal/runtime/pipeline"
)

func TestGenericBundle_ScoringOutcomePatterns(t *testing.T) {
	bundle := loadGenericMASBundle(t)
	approved := previewHandler(t, bundle, "processing-node", "item.review_requested", map[string]any{
		"entity_id":    "item-123",
		"score":        90.0,
		"quality":      90.0,
		"completeness": 90.0,
		"risk":         80.0,
		"priority":     "urgent",
	}, runtimepipeline.WorkflowState{
		VerticalID: "item-123",
		Stage:      runtimepipeline.NormalizePipelineStage("ready"),
		Status:     "ready",
		Metadata:   map[string]any{},
	}, nil)
	if approved.RuleID != "approve" || approved.Stage != runtimepipeline.NormalizePipelineStage("approved") {
		t.Fatalf("expected approve rule execution, got %+v", approved)
	}
	if !hasAll(approved.Emits, "item.completed") {
		t.Fatalf("expected approval emission, got %v", approved.Emits)
	}
	if got := approved.Metadata["score"]; got == nil {
		t.Fatalf("expected computed score to be stored, got %+v", approved)
	}

	rejected := previewHandler(t, bundle, "processing-node", "item.review_requested", map[string]any{
		"entity_id":    "item-456",
		"score":        45.0,
		"quality":      45.0,
		"completeness": 55.0,
		"risk":         40.0,
	}, runtimepipeline.WorkflowState{
		VerticalID: "item-456",
		Stage:      runtimepipeline.NormalizePipelineStage("ready"),
		Status:     "ready",
		Metadata:   map[string]any{},
	}, nil)
	if rejected.RuleID != "reject" || rejected.Stage != runtimepipeline.NormalizePipelineStage("rejected") {
		t.Fatalf("expected reject rule execution, got %+v", rejected)
	}
	if !hasAll(rejected.Emits, "item.rejected") {
		t.Fatalf("expected rejection emission, got %v", rejected.Emits)
	}
}
