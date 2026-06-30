package entityruntime

import (
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
				"tags":          {Type: "[text]"},
				"legacy_tags":   {Type: "[text][]"},
				"prefixed_tags": {Type: "[][text]"},
				"spec":          {Type: "Spec"},
			},
		},
		Types: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"Spec": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"features":        {Type: "[Feature]"},
						"legacy_features": {Type: "[Feature][]"},
						"notes":           {Type: "[text]"},
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
	if !reflect.DeepEqual(materialized["legacy_tags"], []any{}) {
		t.Fatalf("legacy_tags default = %#v, want empty list", materialized["legacy_tags"])
	}
	if !reflect.DeepEqual(materialized["prefixed_tags"], []any{}) {
		t.Fatalf("prefixed_tags default = %#v, want empty list", materialized["prefixed_tags"])
	}
	spec, ok := materialized["spec"].(map[string]any)
	if !ok {
		t.Fatalf("spec default = %#v, want object", materialized["spec"])
	}
	if !reflect.DeepEqual(spec["features"], []any{}) {
		t.Fatalf("spec.features default = %#v, want empty list", spec["features"])
	}
	if !reflect.DeepEqual(spec["legacy_features"], []any{}) {
		t.Fatalf("spec.legacy_features default = %#v, want empty list", spec["legacy_features"])
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
	legacyFeatures, err := NormalizeFieldValue(contract, "spec.legacy_features", []any{[]any{map[string]any{"name": "Search"}}})
	if err != nil {
		t.Fatalf("NormalizeFieldValue(spec.legacy_features): %v", err)
	}
	if !reflect.DeepEqual(legacyFeatures, []any{[]any{map[string]any{"name": "Search"}}}) {
		t.Fatalf("legacyFeatures = %#v, want normalized nested feature list", legacyFeatures)
	}

	if _, err := NormalizeFieldValue(contract, "spec.features", map[string]any{"name": "Search"}); err == nil {
		t.Fatalf("NormalizeFieldValue(spec.features object) = nil, want list type error")
	}
	if _, err := NormalizeFieldValue(contract, "spec.features", []any{map[string]any{"unknown": "x"}}); err == nil {
		t.Fatalf("NormalizeFieldValue(spec.features undeclared field) = nil, want named-type field error")
	}
}

func TestContainedOperationTarget_ResolvesTypedMapAndListOperations(t *testing.T) {
	contract := Contract{
		Entity: runtimecontracts.EntityContract{
			Fields: map[string]runtimecontracts.EntityFieldDecl{
				"verticals": {Type: "map[text]VerticalState"},
				"queue":     {Type: "[Job]"},
			},
		},
		Types: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"VerticalState": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"status":      {Type: "text"},
						"active_jobs": {Type: "[Job]"},
					},
				},
				"Job": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"id":    {Type: "text"},
						"title": {Type: "text"},
					},
				},
			},
		},
	}

	target, err := ResolveContainedOperationTarget(contract, "entity.verticals.active_jobs", string(ContainedOperationAppend), true, false)
	if err != nil {
		t.Fatalf("ResolveContainedOperationTarget(map append): %v", err)
	}
	if !target.MapScoped || target.RootField != "verticals" || target.ListItemType != "Job" {
		t.Fatalf("target = %+v, want map-scoped verticals active_jobs list", target)
	}
	value, err := NormalizeContainedOperationValue(contract, target, string(ContainedOperationAppend), map[string]any{"id": "job-1", "title": "Build"})
	if err != nil {
		t.Fatalf("NormalizeContainedOperationValue(append): %v", err)
	}
	if !reflect.DeepEqual(value, map[string]any{"id": "job-1", "title": "Build"}) {
		t.Fatalf("value = %#v", value)
	}

	listTarget, err := ResolveContainedOperationTarget(contract, "entity.queue", string(ContainedOperationUpdate), false, true)
	if err != nil {
		t.Fatalf("ResolveContainedOperationTarget(list update): %v", err)
	}
	if listTarget.MapScoped || listTarget.ListItemType != "Job" {
		t.Fatalf("list target = %+v, want direct Job list", listTarget)
	}
}

func TestContainedOperationTarget_FailsClosedForDynamicPathAndWrongShape(t *testing.T) {
	contract := Contract{
		Entity: runtimecontracts.EntityContract{
			Fields: map[string]runtimecontracts.EntityFieldDecl{
				"verticals": {Type: "map[text]VerticalState"},
			},
		},
		Types: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"VerticalState": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"status": {Type: "text"},
					},
				},
			},
		},
	}

	if _, err := ResolveContainedOperationTarget(contract, "entity.verticals[payload.vertical_id]", string(ContainedOperationSet), true, false); err == nil {
		t.Fatal("dynamic bracket target resolved; want fail-closed rejection")
	}
	target, err := ResolveContainedOperationTarget(contract, "entity.verticals", string(ContainedOperationSet), true, false)
	if err != nil {
		t.Fatalf("ResolveContainedOperationTarget(map set): %v", err)
	}
	if _, err := NormalizeContainedOperationValue(contract, target, string(ContainedOperationSet), map[string]any{"missing": "field"}); err == nil {
		t.Fatal("undeclared map value field normalized; want fail-closed rejection")
	}
}
