package entityruntime

import (
	"reflect"
	"strings"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestEntityMutationFinalCandidateEquality(t *testing.T) {
	c := Contract{Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{
		"left":  {Type: "text", IsOptional: true, Refinements: rc.SchemaRefinements{EqualTo: "right"}},
		"right": {Type: "text", IsOptional: true},
	}}}
	for _, tc := range []struct {
		name  string
		state map[string]any
		ops   []Mutation
		valid bool
	}{
		{"paired_set", nil, []Mutation{{Target: "left", Value: "x"}, {Target: "right", Value: "x"}}, true},
		{"half_set", nil, []Mutation{{Target: "left", Value: "x"}}, false},
		{"paired_clear", map[string]any{"left": "x", "right": "x"}, []Mutation{{Operation: MutationClear, Target: "left"}, {Operation: MutationClear, Target: "right"}}, true},
		{"half_clear", map[string]any{"left": "x", "right": "x"}, []Mutation{{Operation: MutationClear, Target: "left"}}, false},
		{"same_value", map[string]any{"left": "x", "right": "x"}, []Mutation{{Target: "left", Value: "x"}}, true},
		{"different_value", map[string]any{"left": "x", "right": "x"}, []Mutation{{Target: "left", Value: "y"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := cloneMap(tc.state)
			_, err := ApplyMutations(c, tc.state, tc.ops)
			if (err == nil) != tc.valid {
				t.Fatalf("mutation error=%v, want valid=%v", err, tc.valid)
			}
			if !reflect.DeepEqual(cloneMap(tc.state), before) {
				t.Fatal("input state changed")
			}
		})
	}
}

func TestEntityMutationPresenceAndOwnership(t *testing.T) {
	c := Contract{Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{
		"required": {Type: "text"}, "optional": {Type: "text", IsOptional: true},
		"fixed":      {Type: "text", IsOptional: true, Immutable: true},
		"projection": {Type: "[text]", MaterializeFrom: "node.items"},
		"profile":    {Type: "Profile", IsOptional: true},
		"items":      {Type: "[text]"}, "lookup": {Type: "map[text]text"},
	}}, Types: rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{
		"Profile": {Fields: map[string]rc.TypeFieldSpec{"name": {Type: "text"}, "note": {Type: "text", IsOptional: true}}},
	}}}
	for _, tc := range []struct {
		name  string
		state map[string]any
		ops   []Mutation
		valid bool
	}{
		{"bare_clear", nil, []Mutation{{Operation: MutationClear, Target: "required"}}, false},
		{"absent_optional_clear", nil, []Mutation{{Operation: MutationClear, Target: "optional"}}, true},
		{"immutable_clear", map[string]any{"fixed": "x"}, []Mutation{{Operation: MutationClear, Target: "fixed"}}, false},
		{"immutable_same", map[string]any{"fixed": "x"}, []Mutation{{Target: "fixed", Value: "x"}}, true},
		{"projection_foreign", nil, []Mutation{{Target: "projection", Value: []any{}}}, false},
		{"projection_owner", nil, []Mutation{{Target: "projection", Value: []any{}, ProjectionSource: "node.items"}}, true},
		{"missing_parent", nil, []Mutation{{Target: "profile.note", Value: "x"}}, false},
		{"clear_missing_parent", nil, []Mutation{{Operation: MutationClear, Target: "profile.note"}}, true},
		{"clear_missing_required_child", nil, []Mutation{{Operation: MutationClear, Target: "profile.name"}}, false},
		{"clear_twice", nil, []Mutation{{Operation: MutationClear, Target: "optional"}, {Operation: MutationClear, Target: "optional"}}, true},
		{"parent_then_child", nil, []Mutation{{Target: "profile", Value: map[string]any{"name": "x"}}, {Target: "profile.note", Value: "y"}}, true},
		{"set_clear_conflict", nil, []Mutation{{Target: "optional", Value: "x"}, {Operation: MutationClear, Target: "optional"}}, false},
		{"ancestor_conflict", map[string]any{"profile": map[string]any{"name": "x"}}, []Mutation{{Target: "profile.note", Value: "y"}, {Operation: MutationClear, Target: "profile"}}, false},
		{"construct_list", nil, []Mutation{{Operation: "append", Target: "items", Value: "x"}}, true},
		{"construct_map", nil, []Mutation{{Operation: "set", Target: "lookup", HasKey: true, Key: "x", Value: "y"}}, true},
		{"null_map", map[string]any{"lookup": nil}, []Mutation{{Operation: "set", Target: "lookup", HasKey: true, Key: "x", Value: "y"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ApplyMutations(c, tc.state, tc.ops)
			if (err == nil) != tc.valid {
				t.Fatalf("mutation error=%v, want valid=%v", err, tc.valid)
			}
		})
	}
}

func TestEntityMutationConflictSurvivesListBoundary(t *testing.T) {
	c := Contract{Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{"note": {Type: "text", IsOptional: true}}}}
	p, err := NewMutationPlan(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Append(Mutation{Target: "note", Value: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := p.Append(Mutation{Operation: MutationClear, Target: "note"}); err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("cross-list conflict=%v", err)
	}
}
