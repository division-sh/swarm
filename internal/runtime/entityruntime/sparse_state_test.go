package entityruntime

import (
	"reflect"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestEntitySparseReadNeverInitializes(t *testing.T) {
	c := Contract{Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{
		"score": {Type: "integer"},
		"label": {Type: "text", Initial: "seed"},
	}}}
	for _, input := range []map[string]any{nil, {}, {"score": int64(0)}, {"label": ""}} {
		got, err := NormalizeState(c, input)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(input) {
			t.Fatalf("read changed presence: input=%#v got=%#v", input, got)
		}
		for key, value := range input {
			if !reflect.DeepEqual(got[key], value) {
				t.Fatalf("read changed %s: %#v", key, got)
			}
		}
	}
}

func TestEntityCreationInitializesOnceAndClearStaysAbsent(t *testing.T) {
	c := Contract{Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{
		"label": {Type: "text", IsOptional: true, Initial: "seed"},
		"score": {Type: "integer"},
	}}}
	created, err := Initialize(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if created["label"] != "seed" || len(created) != 1 {
		t.Fatalf("creation=%#v", created)
	}
	overridden, err := Initialize(c, map[string]any{"label": ""})
	if err != nil || overridden["label"] != "" {
		t.Fatalf("explicit zero override=%#v,%v", overridden, err)
	}
	cleared, err := ApplyMutations(c, created, []Mutation{{Operation: MutationClear, Target: "label"}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		cleared, err = NormalizeState(c, cleared)
		if err != nil || len(cleared) != 0 {
			t.Fatalf("read resurrected initialized field: %#v,%v", cleared, err)
		}
	}
}

func TestEntityNullContainersDoNotBecomeEmptyValues(t *testing.T) {
	c := Contract{Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{
		"items": {Type: "[text]", IsOptional: true}, "lookup": {Type: "map[text]text", IsOptional: true},
	}}}
	for _, state := range []map[string]any{{"items": nil}, {"items": []any(nil)}, {"lookup": map[string]any(nil)}} {
		if _, err := NormalizeState(c, state); err == nil {
			t.Fatalf("null container accepted: %#v", state)
		}
	}
	if _, err := NormalizeState(c, map[string]any{"items": []any{}, "lookup": map[string]any{}}); err != nil {
		t.Fatal(err)
	}
}

func TestEntityAssignedRecordCompleteness(t *testing.T) {
	c := Contract{
		Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{"profile": {Type: "Profile"}}},
		Types: rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{"Profile": {Fields: map[string]rc.TypeFieldSpec{
			"name": {Type: "text"}, "note": {Type: "text", IsOptional: true},
		}}}},
	}
	if _, err := NormalizeState(c, map[string]any{"profile": map[string]any{}}); err == nil {
		t.Fatal("supplied record silently acquired required members")
	}
	if got, err := NormalizeState(c, nil); err != nil || len(got) != 0 {
		t.Fatalf("progressively unassigned root: %#v, %v", got, err)
	}
	got, err := NormalizeState(c, map[string]any{"profile": map[string]any{"name": "valid"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, present := got["profile"].(map[string]any)["note"]; present {
		t.Fatal("optional member was synthesized")
	}
}

func TestEntitySparseEqualityCreation(t *testing.T) {
	for _, mode := range []struct {
		name                string
		left, rightOptional bool
	}{{"bare", false, false}, {"optional", true, true}, {"mixed", false, true}, {"mixed_reverse", true, false}} {
		t.Run(mode.name, func(t *testing.T) {
			for _, tc := range []struct {
				name        string
				values      map[string]any
				left, right any
				valid       bool
			}{
				{"absent", map[string]any{}, nil, nil, true},
				{"left_only", map[string]any{"left": ""}, nil, nil, false},
				{"right_only", map[string]any{"right": ""}, nil, nil, false},
				{"equal", map[string]any{"left": "x", "right": "x"}, nil, nil, true},
				{"explicit_empty", map[string]any{"left": "", "right": ""}, nil, nil, true},
				{"unequal", map[string]any{"left": "x", "right": "y"}, nil, nil, false},
				{"null", map[string]any{"left": nil, "right": nil}, nil, nil, false},
				{"left_initial_only", nil, "seed", nil, false},
				{"right_initial_only", nil, nil, "seed", false},
				{"equal_initials", nil, "seed", "seed", true},
				{"unequal_initials", nil, "seed", "other", false},
				{"binding_completes_initial", map[string]any{"right": "seed"}, "seed", nil, true},
				{"bindings_override_initials", map[string]any{"left": "", "right": ""}, "seed", "seed", true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					contract := Contract{Entity: rc.EntityContract{Fields: map[string]rc.EntityFieldDecl{
						"left":  {Type: "text", IsOptional: mode.left, Initial: tc.left, Refinements: rc.SchemaRefinements{EqualTo: "right"}},
						"right": {Type: "text", IsOptional: mode.rightOptional, Initial: tc.right},
					}}}
					got, err := Initialize(contract, tc.values)
					if (err == nil) != tc.valid {
						t.Fatalf("candidate=%#v error=%v", got, err)
					}
					if err == nil && tc.name == "absent" && len(got) != 0 {
						t.Fatalf("equality fabricated assignment: %#v", got)
					}
					if err == nil {
						for key, value := range tc.values {
							if !reflect.DeepEqual(got[key], value) {
								t.Fatalf("creation replaced explicit %s: %#v -> %#v", key, value, got[key])
							}
						}
					}
				})
			}
		})
	}
}
