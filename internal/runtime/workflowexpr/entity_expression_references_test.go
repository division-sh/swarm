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
		{`state.profile.?note.orValue("")`, []string{"profile.note"}, nil},
		{`has(state.profile.note) && state.profile.note != ""`, []string{"profile.note"}, nil},
		{`(has(state.profile.note) || has(state.profile.note)) && state.profile.note != ""`, []string{"profile.note"}, nil},
		{`has(state.profile.note) || state.profile.note != ""`, []string{"profile.note"}, []string{"state.profile.note"}},
		{`[{}].exists(state, state.note != "")`, nil, nil},
		{`[{}].exists(state, state.?note.orValue("") != "") && state.profile.id == "1"`, []string{"profile.id"}, nil},
		{`state.?profile.optFlatMap(p, p.?note).orValue("")`, []string{"profile"}, nil},
		{`"state.profile.nope" == ""`, nil, nil},
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
	for _, expression := range []string{`entity.note`, `_entity.id`, `gates.ready`, `instance.gates.ready`, `instance.current_state`, `instance.flow_instance`} {
		if err := ValidateValueExpression(expression); err == nil {
			t.Fatalf("retired context admitted: %s", expression)
		}
	}
	for _, expr := range []string{`state`, `state.profile`, `state.?profile`, `has(state.profile)`} {
		if err := ValidateValueExpression(expr); err == nil {
			t.Fatalf("untyped entity admitted: %s", expr)
		}
	}
	if err := ValidateValueExpression(`[{}].exists(state, has(state.note))`); err != nil {
		t.Fatal(err)
	}
}
