package testcases

import "testing"

func TestGenericBundle_MultigateStateMachinePatterns(t *testing.T) {
	bundle := loadGenericSwarmBundle(t)
	review := mustHandler(t, bundle, "processing-node", "item.review_requested")
	if gateName(review.SetsGate) != "score_ready" {
		t.Fatalf("expected score_ready gate, got %+v", review.SetsGate)
	}
	if !hasAll(review.ClearGates, "intake_open") {
		t.Fatalf("expected intake_open to clear on review, got %v", review.ClearGates)
	}
	if len(review.Rules) != 2 {
		t.Fatalf("expected approve/reject rules, got %+v", review.Rules)
	}

	reject := mustHandler(t, bundle, "processing-node", "item.rejected")
	if reject.AdvancesTo != "rejected" {
		t.Fatalf("expected terminal rejection state, got %q", reject.AdvancesTo)
	}
	if !hasAll(reject.ClearGates, "intake_ready", "score_ready", "delivery_ready") {
		t.Fatalf("expected rejection to clear all downstream gates, got %v", reject.ClearGates)
	}

	deliver := mustHandler(t, bundle, "delivery-node", "item.completed")
	if gateName(deliver.SetsGate) != "delivery_ready" {
		t.Fatalf("expected delivery_ready gate, got %+v", deliver.SetsGate)
	}
	if !hasAll(deliver.ClearGates, "intake_ready", "score_ready") {
		t.Fatalf("expected delivery to clear upstream gates, got %v", deliver.ClearGates)
	}
}
