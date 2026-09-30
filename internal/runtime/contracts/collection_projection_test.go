package contracts

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

func TestA2CollectionProjectionAdmissionAndOrdering(t *testing.T) {
	for _, tc := range []struct {
		name, typ string
		value     any
		want      []any
	}{
		{"list_duplicates", "[integer]", []any{json.Number("9007199254740993"), json.Number("2"), json.Number("2")}, []any{json.Number("9007199254740993"), json.Number("2"), json.Number("2")}},
		{"map_scalar", "map[text]integer", map[string]any{"z": 1, " a ": 2, "a": 3, "": 4}, []any{"", " a ", "a", "z"}},
		{"map_list", "map[text][integer]", map[string]any{"z": []any{2}, "a": []any{1, 1}}, []any{"a", "z"}},
		{"empty_list", "[text]", []any{}, []any{}},
		{"empty_map", "map[text]text", map[string]any{}, []any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := AdmitCollectionProjection(CatalogTypeReference{Type: tc.typ})
			if err != nil {
				t.Fatal(err)
			}
			before, _ := json.Marshal(tc.value)
			got, err := p.Project(tc.value)
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("projection = %#v, %v; want %#v", got, err, tc.want)
			}
			after, _ := json.Marshal(tc.value)
			if string(before) != string(after) {
				t.Fatal("projection mutated meaningful source values")
			}
		})
	}
	for _, typ := range []string{"map[integer]text", "map[boolean][text]", "text", "array", "dynamic", "MissingType"} {
		if _, err := AdmitCollectionProjection(CatalogTypeReference{Type: typ}); err == nil {
			t.Fatalf("admitted %s", typ)
		}
	}
	for _, typ := range []string{"[text]", "map[text]text"} {
		p, err := AdmitCollectionProjection(CatalogTypeReference{Type: typ})
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range []any{nil, 12, "not a collection", map[int]string{1: "one"}} {
			if _, err := p.Project(value); err == nil {
				t.Fatalf("%s accepted %#v", typ, value)
			}
		}
	}
	mapProjection, _ := AdmitCollectionProjection(CatalogTypeReference{Type: "map[text]text"})
	if _, err := mapProjection.Project([]any{"a"}); err == nil {
		t.Fatal("map accepted list")
	}
	listProjection, _ := AdmitCollectionProjection(CatalogTypeReference{Type: "[text]"})
	if _, err := listProjection.Project(map[string]any{"a": "b"}); err == nil {
		t.Fatal("list accepted map")
	}
}

func TestA2CollectionProjectionCatalogAndDigestWitness(t *testing.T) {
	ref := CatalogTypeReference{Type: "map[Key]Record", Catalog: TypeCatalogDocument{
		Scalars: map[string]ScalarTypeDecl{"Key": {Base: "text"}},
		Types:   map[string]NamedTypeDecl{"Record": {Fields: map[string]TypeFieldSpec{"values": {Type: "[integer]"}}}},
	}}
	p, err := AdmitCollectionProjection(ref)
	if err != nil {
		t.Fatal(err)
	}
	if p.ItemType().Kind != CatalogTypeText || p.Type.Value.Name != "Record" {
		t.Fatalf("admission lost type: %#v", p)
	}
	element := FanOutElementRef{FlowPath: ".", Family: "handler", SemanticPath: `handlers["ready"].fan_out`}
	s := FanOutPlanSemantics{ElementRef: element, ItemsFrom: "payload.items", CollectionType: ref, CollectionProjection: p, ItemType: p.ItemType(), ItemAlias: "key", Identity: "key", IdentityDerived: true, MaxItems: 10, Emit: EmitSpec{Event: "item.ready"}}
	digest, err := canonicaljson.Hash(s)
	if err != nil {
		t.Fatal(err)
	}
	planRef := FanOutPlanRef{ElementRef: element, SemanticDigest: digest}
	if err := s.Validate(planRef); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*FanOutPlanSemantics){
		func(s *FanOutPlanSemantics) { s.CollectionProjection.Kind = CollectionListItems },
		func(s *FanOutPlanSemantics) { s.ItemsFrom = "entity.items" },
		func(s *FanOutPlanSemantics) { s.MaxItems++ },
		func(s *FanOutPlanSemantics) { s.SourceAfterWrites = true },
		func(s *FanOutPlanSemantics) { s.ItemAlias = "other" },
		func(s *FanOutPlanSemantics) { s.Emit.Event = "other.ready" },
	} {
		changed := s.Clone()
		change(&changed)
		if err := changed.Validate(planRef); err == nil {
			t.Fatal("contradictory digest witness accepted")
		}
	}
	cloned := s.Clone()
	cloned.CollectionProjection.Type.Value.Fields[0].Name = "changed"
	if s.CollectionProjection.Type.Value.Fields[0].Name != "values" {
		t.Fatal("clone aliases catalog projection")
	}
}
