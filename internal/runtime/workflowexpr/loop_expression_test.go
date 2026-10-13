package workflowexpr

import (
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestJoinOptionalResultSchemaAdmission(t *testing.T) {
	opts := ValueExpressionOptions{AllowJoin: true}
	ctx := ValueContext{Join: map[string]any{"completed": int64(1), "results": []any{map[string]any{"label": "ready"}}}}
	for _, expression := range []string{"join.completed", "join.results"} {
		if err := ValidateValueExpressionWithOptions(expression, opts); err != nil {
			t.Fatalf("unspecified result schema: %v", err)
		}
		if _, err := EvalValueExpressionWithOptions(expression, ctx, opts); err != nil {
			t.Fatalf("dynamic result read: %v", err)
		}
	}
	if err := ValidateValueExpressionWithOptions("join.results[0].label", opts); err == nil {
		t.Fatal("unspecified result schema granted closed-record field evidence")
	}
	for _, ref := range []string{" ", "Missing", "text[]", "Cycle"} {
		opts.JoinResultType = runtimecontracts.CatalogTypeReference{Type: ref, Catalog: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{"Cycle": {Fields: map[string]runtimecontracts.TypeFieldSpec{"child": {Type: "Cycle"}}}},
		}}
		if err := ValidateValueExpressionWithOptions("join.completed", opts); err == nil {
			t.Fatalf("supplied invalid reference %q accepted", ref)
		}
		if _, err := EvalValueExpressionWithOptions("join.completed", ctx, opts); err == nil {
			t.Fatalf("supplied invalid reference %q executed", ref)
		}
	}
	if _, err := (runtimecontracts.CatalogTypeReference{}).Resolve(); err == nil {
		t.Fatal("optional join boundary weakened authored empty-reference refusal")
	}
}

func TestJoinCapturedLoopExpressionMembers(t *testing.T) {
	context := ValueContext{Loop: map[string]any{
		"id": "review", "activation_id": "captured-activation", "revision_id": "captured-revision",
		"attempt": int64(1), "max_attempts": int64(3), "flow_id": "private", "revision_field": "private",
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
	const literal = `"loop.revision_id _loop.revision_id payload.revision_id"`
	value, err := EvalValueExpressionWithOptions(literal, context, opts)
	if err != nil || value != "loop.revision_id _loop.revision_id payload.revision_id" {
		t.Fatalf("literal changed: %v %v", value, err)
	}
}
