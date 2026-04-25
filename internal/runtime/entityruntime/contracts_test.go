package entityruntime

import (
	"reflect"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
)

func TestNormalizeFieldValue_AcceptsBuiltInNumberAndJSONTypes(t *testing.T) {
	contract := Contract{
		Entity: runtimecontracts.EntityContract{
			Fields: map[string]runtimecontracts.EntityFieldDecl{
				"score":   {Type: "number"},
				"details": {Type: "json"},
			},
		},
	}

	score, err := NormalizeFieldValue(contract, "score", 91.5)
	if err != nil {
		t.Fatalf("NormalizeFieldValue(score): %v", err)
	}
	if score != 91.5 {
		t.Fatalf("score = %#v, want 91.5", score)
	}

	details, err := NormalizeFieldValue(contract, "details", map[string]any{"summary": "ok"})
	if err != nil {
		t.Fatalf("NormalizeFieldValue(details): %v", err)
	}
	if !reflect.DeepEqual(details, map[string]any{"summary": "ok"}) {
		t.Fatalf("details = %#v", details)
	}
}

func TestMaterialize_AcceptsBracketFormListRefs(t *testing.T) {
	contract := Contract{
		Entity: runtimecontracts.EntityContract{
			Fields: map[string]runtimecontracts.EntityFieldDecl{
				"tags": {Type: "[text]"},
				"spec": {Type: "Spec"},
			},
		},
		Types: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"Spec": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"features": {Type: "[Feature]"},
						"notes":    {Type: "[text]"},
					},
				},
				"Feature": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"name": {Type: "text"},
					},
				},
			},
		},
	}

	materialized, err := Materialize(contract, nil)
	if err != nil {
		t.Fatalf("Materialize defaults: %v", err)
	}
	if !reflect.DeepEqual(materialized["tags"], []any{}) {
		t.Fatalf("tags default = %#v, want empty list", materialized["tags"])
	}
	spec, ok := materialized["spec"].(map[string]any)
	if !ok {
		t.Fatalf("spec default = %#v, want object", materialized["spec"])
	}
	if !reflect.DeepEqual(spec["features"], []any{}) {
		t.Fatalf("spec.features default = %#v, want empty list", spec["features"])
	}
	if !reflect.DeepEqual(spec["notes"], []any{}) {
		t.Fatalf("spec.notes default = %#v, want empty list", spec["notes"])
	}

	features, err := NormalizeFieldValue(contract, "spec.features", []any{map[string]any{"name": "Search"}})
	if err != nil {
		t.Fatalf("NormalizeFieldValue(spec.features): %v", err)
	}
	if !reflect.DeepEqual(features, []any{map[string]any{"name": "Search"}}) {
		t.Fatalf("features = %#v, want normalized feature list", features)
	}

	if _, err := NormalizeFieldValue(contract, "spec.features", map[string]any{"name": "Search"}); err == nil {
		t.Fatalf("NormalizeFieldValue(spec.features object) = nil, want list type error")
	}
	if _, err := NormalizeFieldValue(contract, "spec.features", []any{map[string]any{"unknown": "x"}}); err == nil {
		t.Fatalf("NormalizeFieldValue(spec.features undeclared field) = nil, want named-type field error")
	}
}
