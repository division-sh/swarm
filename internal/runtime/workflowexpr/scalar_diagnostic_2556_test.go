package workflowexpr

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestScalar2556StructuralPredicateDiagnostic(t *testing.T) {
	env, err := NewEntityPredicateEnv(contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeObject, Fields: []contracts.ResolvedCatalogField{
		{Name: "name", Type: contracts.ResolvedCatalogType{Kind: contracts.CatalogTypeText}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"ready", "${name}", "name == 'ready'"} {
		_, err := env.CompilePredicate(value)
		if value == "name == 'ready'" {
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err == nil || strings.Contains(err.Error(), "quote text or use a declared reference") != (value == "ready") {
			t.Fatalf("unexpected diagnostic for %q: %v", value, err)
		}
	}
}
