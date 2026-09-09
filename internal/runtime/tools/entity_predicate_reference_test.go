package tools

import (
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
)

func TestEntityPredicateUsesResolvedFunctionReferences(t *testing.T) {
	schema := testEntityFilterSchema()
	env, err := newEntityFilterEnv(schema)
	if err != nil {
		t.Fatal(err)
	}
	env.Env, err = env.Env.Extend(cel.Function("business.identity", cel.Overload("business_identity_text",
		[]*cel.Type{cel.StringType}, cel.StringType, cel.UnaryBinding(func(value ref.Val) ref.Val { return value }))))
	if err != nil {
		t.Fatal(err)
	}
	program, err := compileEntityFilterExpression(env, `business.identity("ok") == "ok"`, schema)
	if err != nil {
		t.Fatal(err)
	}
	result, _, err := program.Eval(map[string]any{})
	if err != nil || result.Value() != true {
		t.Fatalf("resolved namespace: %v %v", result, err)
	}
	for _, expression := range []string{`optional.of(metadata.regoin).hasValue()`, `metadata.regoin == "us"`} {
		_, err := compileEntityFilterExpression(env, expression, schema)
		if err == nil || !strings.Contains(err.Error(), "did you mean metadata.region?") {
			t.Fatalf("field diagnostic %s: %v", expression, err)
		}
	}
	for _, expression := range []string{`business.unknown("ok") == "ok"`, `optional.ofNonZero("").hasValue()`, `entity.metadata.region == "us"`} {
		if _, err := compileEntityFilterExpression(env, expression, schema); err == nil {
			t.Fatalf("invalid reference accepted: %s", expression)
		}
	}
}

func TestEntityPredicateConstructorsPreservePresenceFailures(t *testing.T) {
	schema := testEntityFilterSchema()
	metadata := schema.Contract.Types.Types["Metadata"]
	region := metadata.Fields["region"]
	region.IsOptional = true
	metadata.Fields["region"] = region
	schema.Contract.Types.Types["Metadata"] = metadata
	for _, expression := range []string{
		`optional.of(metadata).value().region == ""`,
		`optional.ofNonZeroValue(metadata).value().region == ""`,
		`optional.of([metadata]).value().all(x, x.region == "")`,
		`optional.of({"x":metadata}).value()[?"x"].value().region == ""`,
	} {
		_, err := filterEntityStateRowsCEL(expression, nil, schema)
		if err == nil || !strings.Contains(err.Error(), "optional field") {
			t.Fatalf("expected canonical presence error for %s, got %v", expression, err)
		}
	}
}
