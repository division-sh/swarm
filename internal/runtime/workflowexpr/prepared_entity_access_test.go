package workflowexpr

import (
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestPreparedEntityAccessChecksCurrentEntity(t *testing.T) {
	text := runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeText}
	profile := runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeObject, Fields: []runtimecontracts.ResolvedCatalogField{
		{Name: "id", Type: text}, {Name: "note", Type: text, IsOptional: true},
	}}
	entity := runtimecontracts.ResolvedCatalogType{Kind: runtimecontracts.CatalogTypeObject, Fields: []runtimecontracts.ResolvedCatalogField{{Name: "profile", Type: profile}}}
	for _, expression := range []string{
		`state.profile.id`,
		`state.profile.?note.orValue("")`,
		`has(state.profile.note) && state.profile.note != ""`,
		`[{}].exists(state, has(state.note))`,
		`"state.profile.nope" == ""`,
	} {
		t.Run(expression, func(t *testing.T) {
			prepared, err := PrepareValueExpression(expression, ValueExpressionOptions{EntityType: &entity})
			if err != nil {
				t.Fatal(err)
			}
			for _, current := range []map[string]any{
				{"profile": map[string]any{"id": "first", "note": "yes"}},
				{},
				{"profile": map[string]any{"id": "second"}},
				nil,
				{"profile": map[string]any{"id": "restored", "note": "new"}},
			} {
				want := MissingEntityReferences(expression, current)
				got := missingEntityReferencesForAccesses(prepared.entityAccesses, current)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("prepared accesses changed missing paths: got=%v want=%v", got, want)
				}
				result, err := prepared.Eval(ValueContext{Entity: current})
				if len(want) != 0 {
					if expected := "entity field(s) unavailable in expression context: " + strings.Join(want, ", "); err == nil || err.Error() != expected {
						t.Fatalf("current entity check changed: %v, want %s", err, expected)
					}
				} else if err != nil {
					t.Fatal(err)
				} else if expression == `state.profile.id` && result.Value() != current["profile"].(map[string]any)["id"] {
					t.Fatalf("prepared expression retained an earlier value: %#v", result)
				}
			}
		})
	}
}

func BenchmarkPreparedEntityAccesses(b *testing.B) {
	const expression = `state.threshold >= 70 && has(state.profile.note)`
	entity := map[string]any{"threshold": int64(75), "profile": map[string]any{}}
	accesses := entityExpressionAccesses(expression)
	b.Run("parse_each_eval", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			MissingEntityReferences(expression, entity)
		}
	})
	b.Run("prepared_accesses", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			missingEntityReferencesForAccesses(accesses, entity)
		}
	})
}
