package workflowexpr

import "testing"

func TestEvalValueExpression_AllowsNullPresenceCheckOnMissingField(t *testing.T) {
	value, err := EvalValueExpression(`entity.kill_reason == null`, ValueContext{
		Entity: map[string]any{},
	})
	if err != nil {
		t.Fatalf("EvalValueExpression error = %v", err)
	}
	got, ok := value.(bool)
	if !ok {
		t.Fatalf("EvalValueExpression value = %#v (%T), want bool", value, value)
	}
	if !got {
		t.Fatal("expected sparse field == null presence check to evaluate true")
	}
}

func TestEvalValueExpression_FailsClosedOnMissingEntityValueRead(t *testing.T) {
	_, err := EvalValueExpression(`entity.revision_count + 1`, ValueContext{
		Entity: map[string]any{},
	})
	if err == nil {
		t.Fatal("expected missing entity field read to fail closed")
	}
	if got := err.Error(); got == "" || got == "no such key: revision_count" {
		t.Fatalf("expected explicit missing-field error, got %q", got)
	}
}

func TestEvalValueExpression_ExposesFanOutItemAlias(t *testing.T) {
	value, err := EvalValueExpressionWithOptions(`[item]`, ValueContext{
		FanOut: map[string]any{"item": "industry-a"},
	}, ValueExpressionOptions{AllowBareItem: true})
	if err != nil {
		t.Fatalf("EvalValueExpression error = %v", err)
	}
	got, ok := value.([]any)
	if !ok || len(got) != 1 || got[0] != "industry-a" {
		t.Fatalf("EvalValueExpression value = %#v, want [industry-a]", value)
	}
}

func TestEvalValueExpression_RejectsBareItemByDefault(t *testing.T) {
	if err := ValidateValueExpression(`item`); err == nil {
		t.Fatal("expected bare item to be rejected by default")
	}
	_, err := EvalValueExpression(`item`, ValueContext{
		FanOut: map[string]any{"item": "industry-a"},
	})
	if err == nil {
		t.Fatal("expected bare item eval to be rejected by default")
	}
}

func TestValidateValueExpression_RejectsAccumulatedNamespace(t *testing.T) {
	err := ValidateValueExpression(`accumulated.size()`)
	if err == nil {
		t.Fatal("expected accumulated namespace to be rejected for data expressions")
	}
}

func TestExpressionReferencesEntity_IgnoresStringLiterals(t *testing.T) {
	if ExpressionReferencesEntity(`payload.reason == "entity.kill_reason"`) {
		t.Fatal("expected quoted entity reference text to be ignored")
	}
	if !ExpressionReferencesEntity(`has(entity.kill_reason) ? entity.kill_reason : payload.reason`) {
		t.Fatal("expected real entity reference to be detected")
	}
}
