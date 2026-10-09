package mutationlog

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestVerifyRunProjectionDomainComparison(t *testing.T) {
	for _, tc := range []struct {
		name            string
		left, right     any
		leftOK, rightOK bool
		same            bool
	}{
		{"absent_null", nil, nil, false, true, false},
		{"integer_double", int64(7), float64(7), true, true, false},
		{"integer_lexeme", int64(7), json.Number("7"), true, true, true},
		{"double_lexeme", float64(7), json.Number("7.0"), true, true, true},
		{"null_empty_object", nil, map[string]any{}, true, true, false},
		{"empty_object_list", map[string]any{}, []any{}, true, true, false},
		{"same_null", nil, nil, true, true, true},
		{"same_empty_object", map[string]any{}, map[string]any{}, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := EntityStateProjection{Fields: map[string]any{}}, EntityStateProjection{Fields: map[string]any{}}
			if tc.leftOK {
				a.Fields["value"] = tc.left
			}
			if tc.rightOK {
				b.Fields["value"] = tc.right
			}
			got, err := CompareEntityStateProjections("run", map[string]EntityStateProjection{"entity": a}, map[string]EntityStateProjection{"entity": b})
			if err != nil {
				t.Fatal(err)
			}
			if (len(got.Rows) == 0) != tc.same {
				t.Fatalf("rows=%+v", got.Rows)
			}
			if !tc.same && (got.Rows[0].FoldedPresent != tc.leftOK || got.Rows[0].StoredPresent != tc.rightOK || *got.Rows[0].Path != "value") {
				t.Fatalf("lost presence/path: %+v", got.Rows)
			}
		})
	}
	t.Run("nested_vs_atomic", func(t *testing.T) {
		a := EntityStateProjection{Fields: map[string]any{"profile": map[string]any{"name": "old"}}, Gates: map[string]any{"node.gate": map[string]any{"open": false}}}
		b := EntityStateProjection{Fields: map[string]any{"profile": map[string]any{"name": "new"}}, Gates: map[string]any{"node.gate": map[string]any{"open": true}}}
		got, err := CompareEntityStateProjections("run", map[string]EntityStateProjection{"entity": a}, map[string]EntityStateProjection{"entity": b})
		if err != nil || len(got.Rows) != 2 || *got.Rows[0].Path != "profile.name" || *got.Rows[1].Path != "node.gate" {
			t.Fatalf("got=%+v err=%v", got, err)
		}
	})
}

func TestVerifyRunProjectionEntityMembership(t *testing.T) {
	got, err := CompareEntityStateProjections("run", map[string]EntityStateProjection{"history": {}}, map[string]EntityStateProjection{"state": {}})
	if err != nil || got.EntitiesChecked != 2 || len(got.Rows) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	for _, row := range got.Rows {
		if row.Kind != "entity_presence" || row.Domain != "" || row.Path != nil || row.FoldedPresent == row.StoredPresent {
			t.Fatalf("fake domain row: %+v", row)
		}
	}
	if !reflect.DeepEqual(got.Rows[0].EntityID, "history") {
		t.Fatal("nondeterministic rows")
	}
}
