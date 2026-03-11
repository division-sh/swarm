package testcases

import "testing"

func TestGenericBundle_ScoringOutcomePatterns(t *testing.T) {
	bundle := loadGenericMASBundle(t)
	handler := mustHandler(t, bundle, "processing-node", "item.review_requested")

	approvedScore := weightedScore(handler, map[string]any{
		"quality":      90.0,
		"completeness": 90.0,
		"risk":         80.0,
	})
	if approvedScore < 80 {
		t.Fatalf("expected weighted score >= 80, got %.2f", approvedScore)
	}
	approvedRule, ok := chooseRuleForScore(handler, approvedScore)
	if !ok || approvedRule.ID != "approve" || approvedRule.AdvancesTo != "approved" {
		t.Fatalf("expected approve rule, got %+v", approvedRule)
	}
	if !hasAll(approvedRule.Emits.Values(), "item.completed") {
		t.Fatalf("expected approval emission, got %v", approvedRule.Emits.Values())
	}

	rejectedScore := weightedScore(handler, map[string]any{
		"quality":      45.0,
		"completeness": 55.0,
		"risk":         40.0,
	})
	if rejectedScore >= 80 {
		t.Fatalf("expected weighted score < 80, got %.2f", rejectedScore)
	}
	rejectedRule, ok := chooseRuleForScore(handler, rejectedScore)
	if !ok || rejectedRule.ID != "reject" || rejectedRule.AdvancesTo != "rejected" {
		t.Fatalf("expected reject rule, got %+v", rejectedRule)
	}
	if !hasAll(rejectedRule.Emits.Values(), "item.rejected") {
		t.Fatalf("expected rejection emission, got %v", rejectedRule.Emits.Values())
	}
}
