package tools

import (
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
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

func TestEntityToolWritablePathNames_ExcludesMaterializedFields(t *testing.T) {
	contract := entityruntime.Contract{
		Entity: runtimecontracts.EntityContract{
			Fields: map[string]runtimecontracts.EntityFieldDecl{
				"status": {Type: "text"},
				"scores": {
					Type:            "[DimensionScore]",
					MaterializeFrom: "scoring-node.dimensions_received",
				},
			},
		},
		Types: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"DimensionScore": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"dimension": {Type: "text"},
						"score":     {Type: "integer"},
					},
				},
			},
		},
	}

	got := entityToolWritablePathNames(contract)
	if len(got) != 1 || got[0] != "status" {
		t.Fatalf("entityToolWritablePathNames() = %#v, want [status]", got)
	}
}

func TestEntityToolWritablePathNames_IncludesDeclaredDottedPaths(t *testing.T) {
	contract := entityruntime.Contract{
		Entity: runtimecontracts.EntityContract{
			Fields: map[string]runtimecontracts.EntityFieldDecl{
				"status":   {Type: "text"},
				"metadata": {Type: "Metadata"},
				"scores": {
					Type:            "[DimensionScore]",
					MaterializeFrom: "scoring-node.dimensions_received",
				},
			},
		},
		Types: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"Address": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"city": {Type: "text"},
					},
				},
				"DimensionScore": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"dimension": {Type: "text"},
						"score":     {Type: "integer"},
					},
				},
				"Metadata": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"address": {Type: "Address"},
						"region":  {Type: "text"},
					},
				},
			},
		},
	}

	got := entityToolWritablePathNames(contract)
	want := []string{"metadata", "metadata.address", "metadata.address.city", "metadata.region", "status"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("entityToolWritablePathNames() = %#v, want %#v", got, want)
	}
}
