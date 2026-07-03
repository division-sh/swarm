package workflowexpr

import (
	"strings"
	"testing"
)

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

func TestValidateValueExpression_RejectsRetiredFanOutTarget(t *testing.T) {
	tests := []string{
		`fan_out.target`,
		`fan_out.target.flow_instance`,
		`fan_out["target"]`,
		`fan_out['target']`,
	}
	for _, expression := range tests {
		t.Run(expression, func(t *testing.T) {
			err := ValidateValueExpressionWithOptions(expression, ValueExpressionOptions{AllowBareItem: true})
			if err == nil {
				t.Fatalf("expected %q to reject retired fan_out.target", expression)
			}
			if got := err.Error(); got == "" || !containsAll(got, "fan_out.target", "retired") {
				t.Fatalf("ValidateValueExpressionWithOptions(%q) error = %q, want retired fan_out.target", expression, got)
			}
		})
	}
}

func TestValidateValueExpression_RejectsLegacyEventReceiverProjections(t *testing.T) {
	for _, expression := range []string{
		`event.entity_id`,
		`event.flow_instance`,
		`event.entity_id == "ent-1"`,
	} {
		t.Run(expression, func(t *testing.T) {
			err := ValidateValueExpression(expression)
			if err == nil {
				t.Fatalf("expected %q to reject legacy event receiver projection", expression)
			}
			if !strings.Contains(err.Error(), "unsupported event context reference") {
				t.Fatalf("error = %q, want unsupported event context reference", err.Error())
			}
		})
	}
}

func TestEvalValueExpression_RejectsLegacyEventReceiverProjectionEvenWhenMapContainsValue(t *testing.T) {
	_, err := EvalValueExpression(`event.entity_id`, ValueContext{
		Event: map[string]any{"entity_id": "legacy-ent"},
	})
	if err == nil {
		t.Fatal("expected eval to reject legacy event receiver projection")
	}
	if !strings.Contains(err.Error(), "event.entity_id is unsupported") {
		t.Fatalf("error = %q, want event.entity_id unsupported", err.Error())
	}
}

func TestValidateValueExpression_AllowsSupportedEventContextRefs(t *testing.T) {
	for _, expression := range []string{
		`event.id`,
		`event.type`,
		`event.source.entity_id`,
		`event.source.flow_instance`,
		`event.source.flow_id`,
		`event.target.entity_id`,
		`event.target.flow_instance`,
		`event.target.flow_id`,
		`event.target_set`,
		`event.source_event_id`,
		`event.emitted_at`,
		`event.trigger_event_type`,
		`event.current_state`,
		`event.run_id`,
		`event.scope`,
	} {
		t.Run(expression, func(t *testing.T) {
			if err := ValidateValueExpression(expression); err != nil {
				t.Fatalf("ValidateValueExpression(%q) error = %v", expression, err)
			}
		})
	}
}

func TestValidateValueExpression_AllowsFanOutItemAndStringLiteralTargetText(t *testing.T) {
	tests := []string{
		`fan_out.item.target`,
		`item.target`,
		`"fan_out.target"`,
		`payload.note == "fan_out.target"`,
	}
	for _, expression := range tests {
		t.Run(expression, func(t *testing.T) {
			if err := ValidateValueExpressionWithOptions(expression, ValueExpressionOptions{AllowBareItem: true}); err != nil {
				t.Fatalf("ValidateValueExpressionWithOptions(%q) error = %v", expression, err)
			}
		})
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

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(value, part) {
			return false
		}
	}
	return true
}
