package tools

import (
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/entityruntime"
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
