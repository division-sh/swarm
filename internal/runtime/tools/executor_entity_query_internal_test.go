package tools

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
)

func TestValidateEntityFilterExpression_AllowsDeclaredNestedLeaf(t *testing.T) {
	schema := testEntityFilterSchema()
	env, err := newEntityFilterEnv(schema)
	if err != nil {
		t.Fatalf("newEntityFilterEnv: %v", err)
	}
	if err := validateEntityFilterExpression(env, `metadata.region == "us"`, schema); err != nil {
		t.Fatalf("validateEntityFilterExpression: %v", err)
	}
}

func TestValidateEntityFilterExpression_SurfacesNearestMatchForUndeclaredLeaf(t *testing.T) {
	schema := testEntityFilterSchema()
	env, err := newEntityFilterEnv(schema)
	if err != nil {
		t.Fatalf("newEntityFilterEnv: %v", err)
	}
	err = validateEntityFilterExpression(env, `metadata.regoin == "us"`, schema)
	if err == nil {
		t.Fatal("expected undeclared filter field to fail")
	}
	if !strings.Contains(err.Error(), "metadata.regoin") {
		t.Fatalf("expected undeclared field in error, got %v", err)
	}
	if !strings.Contains(err.Error(), "did you mean metadata.region?") {
		t.Fatalf("expected nearest-match guidance, got %v", err)
	}
}

func TestValidateEntityFilterExpression_RejectsEntityScopedSelectors(t *testing.T) {
	schema := testEntityFilterSchema()
	env, err := newEntityFilterEnv(schema)
	if err != nil {
		t.Fatalf("newEntityFilterEnv: %v", err)
	}
	err = validateEntityFilterExpression(env, `entity.metadata.region == "us"`, schema)
	if err == nil {
		t.Fatal("expected entity-scoped selector to fail")
	}
	if !strings.Contains(err.Error(), "must not use entity.metadata.region") {
		t.Fatalf("expected entity-scoped selector rejection, got %v", err)
	}
	if !strings.Contains(err.Error(), "use metadata.region instead") {
		t.Fatalf("expected direct selector guidance, got %v", err)
	}
}

func TestFilterEntityStateRowsCEL_RejectsUndeclaredFieldBeforeEvalOnEmptyRows(t *testing.T) {
	schema := testEntityFilterSchema()
	_, err := filterEntityStateRowsCEL(`metadata.regoin == "us"`, nil, schema)
	if err == nil {
		t.Fatal("expected undeclared filter field to fail before CEL evaluation")
	}
	if !strings.Contains(err.Error(), "metadata.regoin") {
		t.Fatalf("expected undeclared field in error, got %v", err)
	}
}

func TestEntityFilterPreservesTopLevelNullComparisons(t *testing.T) {
	schema := testEntityFilterSchema()
	rows := []map[string]any{{"fields": map[string]any{"status": nil, "score": nil}}}
	for _, expression := range []string{`status == null`, `score == null`} {
		got, err := filterEntityStateRowsCEL(expression, rows, schema)
		if err != nil || len(got) != 1 {
			t.Fatalf("%s: rows=%v err=%v", expression, got, err)
		}
	}
}

func TestEntityFilterNumericFamilyExecutesIntAndDoubleCarriers(t *testing.T) {
	schema := testEntityFilterSchema()
	rows := []map[string]any{
		{"fields": map[string]any{"score": int64(8)}},
		{"fields": map[string]any{"score": float64(8)}},
		{"fields": map[string]any{"score": float64(8.25)}},
	}
	for _, expression := range []string{`double(score) > 7.5`, `int(score) == 8`, `score >= 8`, `score != null`} {
		got, err := filterEntityStateRowsCEL(expression, rows, schema)
		if err != nil || len(got) != len(rows) {
			t.Fatalf("%s: %#v %v", expression, got, err)
		}
	}
	for _, expression := range []string{`score.startsWith("8")`, `score + 1.0 > 8.0`, `dyn(score).startsWith("8")`} {
		if _, err := filterEntityStateRowsCEL(expression, nil, schema); err == nil {
			t.Fatalf("invalid expression admitted on empty rows: %s", expression)
		}
	}
}

func testEntityFilterSchema() entityToolSchema {
	return entityToolSchema{
		Defined: true,
		Contract: entityruntime.Contract{
			FlowID:     "review",
			EntityType: "accounts",
			Entity: runtimecontracts.EntityContract{
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"status":   {Type: "text"},
					"score":    {Type: "numeric"},
					"metadata": {Type: "Metadata"},
				},
			},
			Types: runtimecontracts.TypeCatalogDocument{
				Types: map[string]runtimecontracts.NamedTypeDecl{
					"Metadata": {
						Fields: map[string]runtimecontracts.TypeFieldSpec{
							"region": {Type: "text"},
						},
					},
				},
			},
		},
	}
}
