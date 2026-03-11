package testcases

import "testing"

func TestGenericBundle_BudgetSuppressionPatterns(t *testing.T) {
	bundle := loadGenericMASBundle(t)
	handler := mustHandler(t, bundle, "processing-node", "item.review_requested")
	policy := map[string]any{
		"delivery_enabled": true,
	}
	entity := map[string]any{
		"status": "ready",
	}
	payload := map[string]any{
		"score": 70.0,
	}
	if !evaluateGuard(handler.Guard, payload, entity, policy) {
		t.Fatal("expected review guard to pass when delivery is enabled")
	}

	policy["delivery_enabled"] = false
	if evaluateGuard(handler.Guard, payload, entity, policy) {
		t.Fatal("expected review guard to block when delivery is disabled")
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
