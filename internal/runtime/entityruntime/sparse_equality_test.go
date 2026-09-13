package entityruntime

import (
	"reflect"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestEntitySparseEqualityChainsAndCycles(t *testing.T) {
	for _, closingEdge := range []string{"", "left"} {
		t.Run("closing_edge="+closingEdge, func(t *testing.T) {
			contract := Contract{Entity: c.EntityContract{Fields: map[string]c.EntityFieldDecl{
				"left":   {Type: "text", Refinements: c.SchemaRefinements{EqualTo: "middle"}},
				"middle": {Type: "text", IsOptional: true, Refinements: c.SchemaRefinements{EqualTo: "right"}},
				"right":  {Type: "text", Refinements: c.SchemaRefinements{EqualTo: closingEdge}},
			}}}
			for _, tc := range []struct {
				name   string
				values map[string]any
				valid  bool
			}{
				{"unassigned", map[string]any{}, true},
				{"first_pair_only", map[string]any{"left": "x", "middle": "x"}, false},
				{"second_pair_only", map[string]any{"middle": "x", "right": "x"}, false},
				{"full_equal", map[string]any{"left": "x", "middle": "x", "right": "x"}, true},
				{"third_differs", map[string]any{"left": "x", "middle": "x", "right": "y"}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, err := Initialize(contract, tc.values)
					if (err == nil) != tc.valid || err == nil && !reflect.DeepEqual(got, tc.values) {
						t.Fatalf("values=%#v got=%#v err=%v", tc.values, got, err)
					}
				})
			}
		})
	}
}

func TestEntitySparseEqualityNestedRecords(t *testing.T) {
	for _, required := range []bool{false, true} {
		name := "optional_pair"
		if required {
			name = "required_pair"
		}
		t.Run(name, func(t *testing.T) {
			contract := Contract{
				Entity: c.EntityContract{Fields: map[string]c.EntityFieldDecl{"profile": {Type: "Profile", IsOptional: true}}},
				Types: c.TypeCatalogDocument{Types: map[string]c.NamedTypeDecl{"Profile": {Fields: map[string]c.TypeFieldSpec{
					"name":  {Type: "text"},
					"left":  {Type: "text", IsOptional: !required, Refinements: c.SchemaRefinements{EqualTo: "right"}},
					"right": {Type: "text", IsOptional: !required},
				}}}},
			}
			if got, err := Initialize(contract, nil); err != nil || len(got) != 0 {
				t.Fatalf("absent parent: %#v %v", got, err)
			}
			for _, tc := range []struct {
				name   string
				record map[string]any
				valid  bool
			}{
				{"no_required_name", map[string]any{}, false},
				{"pair_absent", map[string]any{"name": "n"}, !required},
				{"half_pair", map[string]any{"name": "n", "left": "x"}, false},
				{"full_pair", map[string]any{"name": "n", "left": "x", "right": "x"}, true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, err := Initialize(contract, map[string]any{"profile": tc.record})
					if (err == nil) != tc.valid || err == nil && !reflect.DeepEqual(got["profile"], tc.record) {
						t.Fatalf("record=%#v result=%#v err=%v", tc.record, got, err)
					}
				})
			}
			before := map[string]any{"profile": map[string]any{"name": "n", "left": "x", "right": "x"}}
			for _, tc := range []struct {
				name  string
				state map[string]any
				ops   []Mutation
				valid bool
			}{
				{"paired_update", before, []Mutation{{Target: "profile.left", Value: "y"}, {Target: "profile.right", Value: "y"}}, true},
				{"one_update", before, []Mutation{{Target: "profile.left", Value: "y"}}, false},
				{"paired_clear", before, []Mutation{{Operation: MutationClear, Target: "profile.left"}, {Operation: MutationClear, Target: "profile.right"}}, !required},
				{"one_clear", before, []Mutation{{Operation: MutationClear, Target: "profile.left"}}, false},
				{"no_parent_synthesis", nil, []Mutation{{Target: "profile.left", Value: "y"}, {Target: "profile.right", Value: "y"}}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					saved := cloneMap(tc.state)
					_, err := ApplyMutations(contract, tc.state, tc.ops)
					if (err == nil) != tc.valid {
						t.Fatalf("mutation: %v, want valid=%v", err, tc.valid)
					}
					if !reflect.DeepEqual(saved, cloneMap(tc.state)) {
						t.Fatal("mutation changed caller state")
					}
				})
			}
		})
	}
}
