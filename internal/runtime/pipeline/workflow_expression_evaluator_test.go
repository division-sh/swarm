package pipeline

import "testing"

func TestNormalizeWorkflowExpressionStringLiterals(t *testing.T) {
	got, _ := normalizeWorkflowExpression(
		"payload.score >= policy.min_score && (entity.priority == 'high' || payload.override == true)",
		workflowExpressionContext{},
	)
	want := `payload.score >= policy.min_score && (entity.priority == "high" || payload.override == true)`
	if got != want {
		t.Fatalf("normalizeWorkflowExpression(...) = %q, want %q", got, want)
	}
}

func TestRewriteWorkflowExpressionIdentifiers_DoesNotRewriteQuotedStrings(t *testing.T) {
	got := rewriteWorkflowExpressionIdentifiers(`entity.priority == "high" && payload.decision == approve`, map[string]any{})
	want := `entity.priority == "high" && payload.decision == vars.approve`
	if got != want {
		t.Fatalf("rewriteWorkflowExpressionIdentifiers(...) = %q, want %q", got, want)
	}
}
