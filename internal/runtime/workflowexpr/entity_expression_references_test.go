package workflowexpr

import (
	"reflect"
	"testing"
)

func TestEntityReferencesUseStockCELScopeAndOptionalSelectors(t *testing.T) {
	for _, tc := range []struct {
		expression    string
		refs, missing []string
	}{
		{`entity.profile.?note.orValue("")`, []string{"profile.note"}, nil},
		{`has(entity.profile.note) && entity.profile.note != ""`, []string{"profile.note"}, nil},
		{`(has(entity.profile.note) || has(entity.profile.note)) && entity.profile.note != ""`, []string{"profile.note"}, nil},
		{`has(entity.profile.note) || entity.profile.note != ""`, []string{"profile.note"}, []string{"entity.profile.note"}},
		{`[{}].exists(entity, entity.note != "")`, nil, nil},
		{`[{}].exists(entity, entity.?note.orValue("") != "") && entity.profile.id == "1"`, []string{"profile.id"}, nil},
		{`entity.?profile.optFlatMap(p, p.?note).orValue("")`, []string{"profile"}, nil},
		{`"entity.profile.nope" == ""`, nil, nil},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			if got := EntityReferences(tc.expression); !reflect.DeepEqual(got, tc.refs) {
				t.Fatalf("refs=%v want=%v", got, tc.refs)
			}
			got := MissingEntityReferences(tc.expression, map[string]any{"profile": map[string]any{"id": "1"}})
			if len(got) != len(tc.missing) || len(got) > 0 && !reflect.DeepEqual(got, tc.missing) {
				t.Fatalf("missing=%v want=%v", got, tc.missing)
			}
		})
	}
}

func TestEntityReaderRequiresDeclaredTypeWithoutCapturingShadowedRoots(t *testing.T) {
	for _, expr := range []string{`entity`, `entity.profile`, `entity.?profile`, `has(entity.profile)`} {
		if err := ValidateValueExpression(expr); err == nil {
			t.Fatalf("untyped entity admitted: %s", expr)
		}
	}
	if err := ValidateValueExpression(`[{}].exists(entity, has(entity.note))`); err != nil {
		t.Fatal(err)
	}
}
