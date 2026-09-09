package workflowexpr

import (
	"reflect"
	"testing"
)

func TestJoinCapturedLoopExpressionMembers(t *testing.T) {
	context := ValueContext{Loop: map[string]any{
		"id": "review", "activation_id": "captured-activation", "revision_id": "captured-revision",
		"attempt": 1, "max_attempts": 3, "flow_id": "private", "revision_field": "private",
	}}
	opts := ValueExpressionOptions{AllowJoin: true}
	for _, field := range []string{"id", "activation_id", "revision_id", "attempt", "max_attempts"} {
		t.Run(field, func(t *testing.T) {
			expression := "loop." + field
			if err := ValidateValueExpressionWithOptions(expression, opts); err != nil {
				t.Fatal(err)
			}
			got, err := EvalValueExpressionWithOptions(expression, context, opts)
			if err != nil || !reflect.DeepEqual(got, context.Loop[field]) {
				t.Fatalf("value=%#v want=%#v err=%v", got, context.Loop[field], err)
			}
		})
	}
	for _, expression := range []string{
		"loop.flow_id", "loop.revision_field", "loop.unknown", "loop", `_loop.revision_id`,
		`_loop["revision_id"]`, `loop["revision_id"]`, `loop[entity.field]`,
		"payload.revision_id", "event.id", "policy.revision_id", "computed.revision_id",
		`loop.attempt.startsWith("1")`, `loop.revision_id > 1`, `loop.max_attempts.unknown`,
	} {
		t.Run(expression, func(t *testing.T) {
			if err := ValidateValueExpressionWithOptions(expression, opts); err == nil {
				t.Fatal("validator accepted forbidden context")
			}
			if _, err := EvalValueExpressionWithOptions(expression, context, opts); err == nil {
				t.Fatal("runtime accepted forbidden context")
			}
		})
	}
	for _, expression := range []string{"loop.attempt == 1", "entity.count == 1"} {
		if err := ValidateValueExpressionWithOptions(expression, ValueExpressionOptions{AllowJoin: true, JoinOnly: true, RequireBool: true}); err == nil {
			t.Fatalf("complete_when admitted %s", expression)
		}
	}
	const literal = `"loop.revision_id _loop.revision_id payload.revision_id"`
	value, err := EvalValueExpressionWithOptions(literal, context, opts)
	if err != nil || value != "loop.revision_id _loop.revision_id payload.revision_id" {
		t.Fatalf("literal changed: %v %v", value, err)
	}
}
