package workflowexpr

import "testing"

func TestEvalDataExpression_AllowsNullPresenceCheckOnMissingField(t *testing.T) {
	value, err := EvalDataExpression(`entity.kill_reason == null`, DataContext{
		Entity: map[string]any{},
	})
	if err != nil {
		t.Fatalf("EvalDataExpression error = %v", err)
	}
	got, ok := value.(bool)
	if !ok {
		t.Fatalf("EvalDataExpression value = %#v (%T), want bool", value, value)
	}
	if !got {
		t.Fatal("expected sparse field == null presence check to evaluate true")
	}
}

func TestEvalDataExpression_FailsClosedOnMissingEntityValueRead(t *testing.T) {
	_, err := EvalDataExpression(`entity.revision_count + 1`, DataContext{
		Entity: map[string]any{},
	})
	if err == nil {
		t.Fatal("expected missing entity field read to fail closed")
	}
	if got := err.Error(); got == "" || got == "no such key: revision_count" {
		t.Fatalf("expected explicit missing-field error, got %q", got)
	}
}

func TestValidateDataExpression_RejectsAccumulatedNamespace(t *testing.T) {
	err := ValidateDataExpression(`accumulated.size()`)
	if err == nil {
		t.Fatal("expected accumulated namespace to be rejected for data expressions")
	}
}
