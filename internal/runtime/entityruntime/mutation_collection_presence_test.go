package entityruntime

import (
	"reflect"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestEntityMutationCollectionAbsence(t *testing.T) {
	contract := Contract{Entity: c.EntityContract{Fields: map[string]c.EntityFieldDecl{
		"items": {Type: "[text]", IsOptional: true}, "by_id": {Type: "map[text]Profile", IsOptional: true},
		"profile": {Type: "Profile", IsOptional: true},
	}}, Types: c.TypeCatalogDocument{Types: map[string]c.NamedTypeDecl{
		"Profile": {Fields: map[string]c.TypeFieldSpec{"id": {Type: "text"}, "items": {Type: "[text]", IsOptional: true}}},
	}}}
	for _, tc := range []struct {
		name  string
		state map[string]any
		op    Mutation
		want  map[string]any
	}{
		{"construct root list", nil, Mutation{Operation: "append", Target: "items", Value: "x"}, map[string]any{"items": []any{"x"}}},
		{"construct root map", nil, Mutation{Operation: "set", Target: "by_id", HasKey: true, Key: "one", Value: map[string]any{"id": "1"}}, map[string]any{"by_id": map[string]any{"one": map[string]any{"id": "1"}}}},
		{"missing named parent", nil, Mutation{Operation: "append", Target: "profile.items", Value: "x"}, nil},
		{"existing named parent", map[string]any{"profile": map[string]any{"id": "1"}}, Mutation{Operation: "append", Target: "profile.items", Value: "x"}, map[string]any{"profile": map[string]any{"id": "1", "items": []any{"x"}}}},
		{"missing map", nil, Mutation{Operation: "append", Target: "by_id.items", HasKey: true, Key: "one", Value: "x"}, nil},
		{"missing map entry", map[string]any{"by_id": map[string]any{}}, Mutation{Operation: "append", Target: "by_id.items", HasKey: true, Key: "one", Value: "x"}, nil},
		{"missing map held list", map[string]any{"by_id": map[string]any{"one": map[string]any{"id": "1"}}}, Mutation{Operation: "append", Target: "by_id.items", HasKey: true, Key: "one", Value: "x"}, nil},
		{"existing map held list", map[string]any{"by_id": map[string]any{"one": map[string]any{"id": "1", "items": []any{}}}}, Mutation{Operation: "append", Target: "by_id.items", HasKey: true, Key: "one", Value: "x"}, map[string]any{"by_id": map[string]any{"one": map[string]any{"id": "1", "items": []any{"x"}}}}},
		{"null map", map[string]any{"by_id": nil}, Mutation{Operation: "set", Target: "by_id", HasKey: true, Key: "one", Value: map[string]any{"id": "1"}}, nil},
		{"wrong kind map", map[string]any{"by_id": []any{}}, Mutation{Operation: "set", Target: "by_id", HasKey: true, Key: "one", Value: map[string]any{"id": "1"}}, nil},
		{"null list", map[string]any{"items": nil}, Mutation{Operation: "append", Target: "items", Value: "x"}, nil},
		{"wrong kind list", map[string]any{"items": ""}, Mutation{Operation: "append", Target: "items", Value: "x"}, nil},
		{"merge missing entry", map[string]any{"by_id": map[string]any{}}, Mutation{Operation: "merge", Target: "by_id", HasKey: true, Key: "one", Value: map[string]any{"id": "1"}}, nil},
		{"delete missing entry", map[string]any{"by_id": map[string]any{}}, Mutation{Operation: "delete", Target: "by_id", HasKey: true, Key: "one"}, nil},
		{"update missing list", nil, Mutation{Operation: "update", Target: "items", HasIndex: true, Index: 0, Value: "x"}, nil},
		{"update missing index", map[string]any{"items": []any{}}, Mutation{Operation: "update", Target: "items", HasIndex: true, Index: 0, Value: "x"}, nil},
		{"append forbidden index", nil, Mutation{Operation: "append", Target: "items", HasIndex: true, Index: 0, Value: "x"}, nil},
		{"delete forbidden index", map[string]any{"by_id": map[string]any{"one": map[string]any{"id": "1"}}}, Mutation{Operation: "delete", Target: "by_id", HasKey: true, Key: "one", HasIndex: true, Index: 0}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := cloneMap(tc.state)
			got, err := ApplyMutations(contract, tc.state, []Mutation{tc.op})
			if tc.want == nil {
				if err == nil {
					t.Fatalf("invalid operation admitted: result=%#v", got)
				}
			} else if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("result=%#v err=%v, want %#v", got, err, tc.want)
			}
			if !reflect.DeepEqual(cloneMap(tc.state), before) {
				t.Fatal("mutation modified the input snapshot")
			}
		})
	}
}
