package tools

import (
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/entityruntime"
)

func TestEntityToolLeafSelectorNames_GuardsRecursiveNamedTypes(t *testing.T) {
	contract := entityruntime.Contract{
		Entity: runtimecontracts.EntityContract{
			Fields: map[string]runtimecontracts.EntityFieldDecl{
				"status": {Type: "text"},
				"node":   {Type: "node"},
			},
		},
		Types: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"node": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"label": {Type: "text"},
						"next":  {Type: "node"},
					},
				},
			},
		},
	}

	got := entityToolLeafSelectorNames(contract)
	if len(got) != 2 {
		t.Fatalf("entityToolLeafSelectorNames() = %#v, want 2 selectors", got)
	}
	if got[0] != "node.label" || got[1] != "status" {
		t.Fatalf("entityToolLeafSelectorNames() = %#v, want [node.label status]", got)
	}
}
