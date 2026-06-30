package pipeline

import (
	"strings"
	"testing"
)

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

func TestValidateConditionCEL_AllowsQueryEntitiesPayloadOperandWithoutRuntimeValue(t *testing.T) {
	if err := ValidateConditionCEL(`query_entities(name == payload.name).count == 0`, WorkflowConditionContextGuard); err != nil {
		t.Fatalf("expected static query_entities payload operand to validate, got %v", err)
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

func TestValidateConditionCEL_AllowsRuleConditionCanonicalRoots(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		wantErr    bool
	}{
		{
			name:       "payload entity policy event",
			expression: `entity.entity_id != "" && payload.score >= policy.threshold && event.entity_id != ""`,
		},
		{
			name:       "query entities",
			expression: `query_entities(name == payload.name).count == 0`,
		},
		{
			name:       "accumulated unavailable",
			expression: `accumulated.size() > 0`,
			wantErr:    true,
		},
		{
			name:       "fan out unavailable",
			expression: `fan_out.count > 0`,
			wantErr:    true,
		},
		{
			name:       "item unavailable",
			expression: `item.score > 50`,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConditionCEL(tt.expression, WorkflowConditionContextRule)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected rule condition %q to fail validation", tt.expression)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected rule condition %q to validate, got %v", tt.expression, err)
			}
		})
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

func TestWorkflowExpressionEvaluator_EvalBoolSurfacesQueryRewriteError(t *testing.T) {
	eval := newWorkflowExpressionEvaluator()
	_, err := eval.EvalBool(`query_entities(name ~= payload.name).count == 0`, workflowExpressionContext{
		Payload: map[string]any{"name": "candidate-a"},
	})
	if err == nil {
		t.Fatal("expected query_entities predicate error")
	}
	if got := err.Error(); !strings.Contains(got, "unsupported query_entities predicate") {
		t.Fatalf("error = %q, want query_entities predicate diagnostic", got)
	}
}

func TestWorkflowExpressionEvaluator_QueryEntitiesMissingScopedOperandFailsClosed(t *testing.T) {
	eval := newWorkflowExpressionEvaluator()
	_, err := eval.EvalBool(`query_entities(name == payload.missing).count == 0`, workflowExpressionContext{
		Payload: map[string]any{"name": "candidate-a"},
	})
	if err == nil {
		t.Fatal("expected missing scoped operand to fail closed")
	}
	if got := err.Error(); !strings.Contains(got, "payload.missing") || !strings.Contains(got, "unavailable") {
		t.Fatalf("error = %q, want missing scoped operand diagnostic", got)
	}
}

func TestWorkflowExpressionEvaluator_QueryEntitiesUnsupportedScopedOperandFailsClosed(t *testing.T) {
	eval := newWorkflowExpressionEvaluator()
	_, err := eval.EvalBool(`query_entities(name == request.id).count == 0`, workflowExpressionContext{})
	if err == nil {
		t.Fatal("expected unsupported scoped operand to fail closed")
	}
	if got := err.Error(); !strings.Contains(got, "unsupported query_entities operand scope") || !strings.Contains(got, "request.id") {
		t.Fatalf("error = %q, want unsupported scoped operand diagnostic", got)
	}
}

func TestWorkflowExpressionEvaluator_QueryEntitiesKeepsQuotedScopedLiteral(t *testing.T) {
	eval := newWorkflowExpressionEvaluator()
	ok, err := eval.EvalBool(`query_entities(name == "payload.missing").count == 0`, workflowExpressionContext{})
	if err != nil {
		t.Fatalf("EvalBool error = %v", err)
	}
	if !ok {
		t.Fatal("expected quoted scoped-looking literal to remain a literal")
	}
}

func TestWorkflowExpressionEvaluator_EvalBoolFailsClosedOnMissingEntityField(t *testing.T) {
	eval := newWorkflowExpressionEvaluator()
	_, err := eval.EvalBool(`entity.score >= 70`, workflowExpressionContext{
		Entity:  map[string]any{},
		Payload: map[string]any{},
		Policy:  map[string]any{},
	})
	if err == nil {
		t.Fatal("expected missing entity field to fail closed")
	}
	if got := err.Error(); got == "" || got == "no such key: score" {
		t.Fatalf("expected explicit lifecycle-safe missing-field error, got %q", got)
	}
}

func TestWorkflowExpressionEvaluator_EvalBoolAllowsHasPresenceCheckOnSparseField(t *testing.T) {
	eval := newWorkflowExpressionEvaluator()
	ok, err := eval.EvalBool(`has(entity.kill_reason)`, workflowExpressionContext{
		Entity:  map[string]any{},
		Payload: map[string]any{},
		Policy:  map[string]any{},
	})
	if err != nil {
		t.Fatalf("EvalBool error = %v", err)
	}
	if ok {
		t.Fatal("expected has(entity.kill_reason) to be false for sparse field")
	}
}

func TestWorkflowExpressionEvaluator_EvalBoolAllowsNullPresenceChecksOnSparseField(t *testing.T) {
	eval := newWorkflowExpressionEvaluator()
	ok, err := eval.EvalBool(`entity.kill_reason == null`, workflowExpressionContext{
		Entity:  map[string]any{},
		Payload: map[string]any{},
		Policy:  map[string]any{},
	})
	if err != nil {
		t.Fatalf("EvalBool == null error = %v", err)
	}
	if !ok {
		t.Fatal("expected sparse field == null to be true")
	}

	ok, err = eval.EvalBool(`entity.kill_reason != null`, workflowExpressionContext{
		Entity:  map[string]any{},
		Payload: map[string]any{},
		Policy:  map[string]any{},
	})
	if err != nil {
		t.Fatalf("EvalBool != null error = %v", err)
	}
	if ok {
		t.Fatal("expected sparse field != null to be false")
	}
}

func TestWorkflowExpressionEvaluator_EvalBoolAllowsHasGuardedTernaryReadOnSparseField(t *testing.T) {
	eval := newWorkflowExpressionEvaluator()
	ok, err := eval.EvalBool(`has(entity.kill_reason) ? entity.kill_reason == "manual" : true`, workflowExpressionContext{
		Entity:  map[string]any{},
		Payload: map[string]any{},
		Policy:  map[string]any{},
	})
	if err != nil {
		t.Fatalf("EvalBool ternary error = %v", err)
	}
	if !ok {
		t.Fatal("expected guarded ternary read to take fallback branch for sparse field")
	}
}

func TestWorkflowExpressionEvaluator_EvalBoolIgnoresEntityRefsInsideStringLiterals(t *testing.T) {
	eval := newWorkflowExpressionEvaluator()
	ok, err := eval.EvalBool(`payload.label == "entity.score"`, workflowExpressionContext{
		Entity:  map[string]any{},
		Payload: map[string]any{"label": "entity.score"},
		Policy:  map[string]any{},
	})
	if err != nil {
		t.Fatalf("EvalBool error = %v", err)
	}
	if !ok {
		t.Fatal("expected quoted entity.* text to stay a string literal, not a missing entity ref")
	}
}

func TestWorkflowExpressionEvaluator_EvalBoolIgnoresNullPresencePatternsInsideStringLiterals(t *testing.T) {
	eval := newWorkflowExpressionEvaluator()
	for _, tc := range []struct {
		expression string
		label      string
	}{
		{expression: `payload.label == "entity.kill_reason == null"`, label: "entity.kill_reason == null"},
		{expression: `payload.label == "entity.kill_reason != null"`, label: "entity.kill_reason != null"},
	} {
		ok, err := eval.EvalBool(tc.expression, workflowExpressionContext{
			Entity:  map[string]any{},
			Payload: map[string]any{"label": tc.label},
			Policy:  map[string]any{},
		})
		if err != nil {
			t.Fatalf("EvalBool(%q) error = %v", tc.expression, err)
		}
		if !ok {
			t.Fatalf("expected quoted null-presence text to stay literal for %q", tc.expression)
		}
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
