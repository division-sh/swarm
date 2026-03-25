package pipeline

import "testing"

func TestNormalizeWorkflowExpressionStringLiterals(t *testing.T) {
	got, _, err := normalizeWorkflowExpression(
		"payload.score >= policy.min_score && (entity.priority == 'high' || payload.override == true)",
		workflowExpressionContext{},
	)
	if err != nil {
		t.Fatalf("normalizeWorkflowExpression(...) error = %v", err)
	}
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

func TestNormalizeWorkflowExpression_RewritesQueryEntitiesCount(t *testing.T) {
	got, _, err := normalizeWorkflowExpression(
		`query_entities(name == payload.name).count == 0`,
		workflowExpressionContext{
			Payload: map[string]any{"name": "unique-item"},
		},
	)
	if err != nil {
		t.Fatalf("normalizeWorkflowExpression(...) error = %v", err)
	}
	want := `0 == 0`
	if got != want {
		t.Fatalf("normalizeWorkflowExpression(...) = %q, want %q", got, want)
	}
}

func TestParseWorkflowEntityQueryPredicate_ResolvesPayloadReference(t *testing.T) {
	predicate, err := parseWorkflowEntityQueryPredicate(
		`name == payload.name`,
		workflowExpressionContext{Payload: map[string]any{"name": "candidate-a"}},
	)
	if err != nil {
		t.Fatalf("parseWorkflowEntityQueryPredicate(...) error = %v", err)
	}
	if predicate.Field != "name" || predicate.Op != "==" || predicate.Value != "candidate-a" {
		t.Fatalf("predicate = %#v", predicate)
	}
}

func TestNormalizeWorkflowExpression_PreservesCelLambdaBindings(t *testing.T) {
	got, _, err := normalizeWorkflowExpression(
		`entity.composite_score >= policy.hard_gate_floor && accumulated.filter(d, d.score >= 70 && d.tier == 1).size() >= 2`,
		workflowExpressionContext{},
	)
	if err != nil {
		t.Fatalf("normalizeWorkflowExpression(...) error = %v", err)
	}
	want := `entity.composite_score >= policy.hard_gate_floor && vars.accumulated.filter(d, d.score >= 70 && d.tier == 1).size() >= 2`
	if got != want {
		t.Fatalf("normalizeWorkflowExpression(...) = %q, want %q", got, want)
	}
}
