package testcases

import (
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestGenericBundle_BudgetSuppressionPatterns(t *testing.T) {
	bundle := loadGenericSwarmBundle(t)
	allowed := previewHandler(t, bundle, "processing-node", "item.review_requested", map[string]any{
		"entity_id": "item-123",
		"score":     70.0,
	}, runtimepipeline.WorkflowState{
		EntityID: "item-123",
		Stage:    runtimepipeline.NormalizeWorkflowStateID("ready"),
		Status:   "ready",
		Metadata: map[string]any{},
	}, map[string]any{"delivery_enabled": true})
	if allowed.Status == runtimepipeline.HandlerOutcomeBlocked {
		t.Fatalf("expected review guard to pass when delivery is enabled, got %+v", allowed)
	}

	blocked := previewHandler(t, bundle, "processing-node", "item.review_requested", map[string]any{
		"entity_id": "item-456",
		"score":     70.0,
	}, runtimepipeline.WorkflowState{
		EntityID: "item-456",
		Stage:    runtimepipeline.NormalizeWorkflowStateID("ready"),
		Status:   "ready",
		Metadata: map[string]any{},
	}, map[string]any{"delivery_enabled": false})
	if blocked.Status != runtimepipeline.HandlerOutcomeBlocked {
		t.Fatalf("expected review guard to block when delivery is disabled, got %+v", blocked)
	}

	if state, ok := bundle.Policy.Values["budget_state"]; !ok || state.Value != "normal" {
		t.Fatalf("expected normal budget state, got %+v", state)
	}
	if states, ok := bundle.Policy.Values["suppression_states"]; !ok {
		t.Fatal("expected suppression_states policy")
	} else if got, ok := states.Value.([]any); !ok || len(got) != 2 {
		t.Fatalf("expected suppression states list, got %#v", states.Value)
	}
}
