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

func TestValidateConditionCEL_RequiresExplicitScope(t *testing.T) {
	if err := ValidateConditionCEL(`entity.priority == "high" && payload.decision == approve`, WorkflowConditionContextGuard); err == nil {
		t.Fatal("expected unscoped identifier to fail validation")
	}
}

func TestValidateConditionCEL_RejectsFanOutOutsideDataAccumulationExpressions(t *testing.T) {
	if err := ValidateConditionCEL(`fan_out.count > 0`, WorkflowConditionContextGuard); err == nil {
		t.Fatal("expected fan_out to be rejected in guard conditions")
	}
}

func TestValidateConditionCEL_AllowsItemOnlyInFilterLikeContexts(t *testing.T) {
	if err := ValidateConditionCEL(`item.score > 50`, WorkflowConditionContextFilter); err != nil {
		t.Fatalf("expected filter item scope to validate, got %v", err)
	}
	if err := ValidateConditionCEL(`item.score > 50`, WorkflowConditionContextRule); err == nil {
		t.Fatal("expected item scope to be rejected in rule conditions")
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
		`entity.score >= policy.minimum_score && accumulated.filter(item, item.value >= 70 && item.level == 1).size() >= 2`,
		workflowExpressionContext{},
	)
	if err != nil {
		t.Fatalf("normalizeWorkflowExpression(...) error = %v", err)
	}
	want := `entity.score >= policy.minimum_score && accumulated.filter(item, item.value >= 70 && item.level == 1).size() >= 2`
	if got != want {
		t.Fatalf("normalizeWorkflowExpression(...) = %q, want %q", got, want)
	}
}
