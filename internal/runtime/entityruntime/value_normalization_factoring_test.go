package entityruntime

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestEntityValueFactoringCreationAndSnapshots(t *testing.T) {
	contract := Contract{Entity: c.EntityContract{Fields: map[string]c.EntityFieldDecl{
		"count": {Type: "integer"}, "enabled": {Type: "boolean"},
		"label": {Type: "text", Initial: "initial"},
	}}}
	initial, err := InitialValues(contract)
	requireValueFactoringResult(t, initial, map[string]any{"label": "initial"}, err)
	created, err := Initialize(contract, map[string]any{"label": "", "count": 0, "enabled": false})
	requireValueFactoringResult(t, created, map[string]any{"label": "", "count": int64(0), "enabled": false}, err)
	for _, values := range []map[string]any{nil, {"count": int64(0), "enabled": false}} {
		got, err := NormalizeState(contract, values)
		want := map[string]any{}
		if values != nil {
			want = map[string]any{"count": int64(0), "enabled": false}
		}
		requireValueFactoringResult(t, got, want, err)
		if _, exists := got["label"]; exists {
			t.Fatal("snapshot reapplied a creation initializer")
		}
	}
	contract.Entity.Fields["count"] = c.EntityFieldDecl{Type: "integer", Initial: "invalid"}
	if got, err := Initialize(contract, map[string]any{"count": int64(1)}); got != nil || err == nil || err.Error() != "field count must be integer" {
		t.Fatalf("initializer precedes supplied overrides: got=%#v err=%v", got, err)
	}
}

func TestEntityValueFactoringPresenceAndErrorPhases(t *testing.T) {
	contract := Contract{Entity: c.EntityContract{Fields: map[string]c.EntityFieldDecl{"value": {Type: "json"}}}}
	for _, value := range []any{nil, map[string]any(nil), []any(nil), (*string)(nil)} {
		if got, err := NormalizeFieldValue(contract, "value", value); got != nil || err == nil || err.Error() != "field value cannot be null" {
			t.Fatalf("explicit null %T: got=%#v err=%v", value, got, err)
		}
	}
	for _, value := range []any{"", false, int64(0), []any{}, map[string]any{}} {
		got, err := NormalizeFieldValue(contract, "value", value)
		requireValueFactoringResult(t, got, value, err)
	}
	for _, field := range []string{"unknown", "unknown.child"} {
		if _, err := NormalizeFieldValue(contract, field, nil); err == nil || err.Error() != "undeclared field unknown" {
			t.Fatalf("path admission must precede value admission: %v", err)
		}
	}
	if got, err := NormalizeState(contract, map[string]any{"unknown": true}); got != nil || err == nil || err.Error() != "undeclared field unknown" {
		t.Fatalf("undeclared snapshot: got=%#v err=%v", got, err)
	}
}

func TestEntityValueFactoringScalarRepresentations(t *testing.T) {
	catalog := c.TypeCatalogDocument{
		Scalars: map[string]c.ScalarTypeDecl{"Label": {Base: "text"}},
		Enums:   map[string]c.EnumTypeDecl{"Status": {Values: []string{"open", "closed"}, Default: "open"}},
	}
	instant := time.Date(2026, 1, 2, 1, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		name, typ   string
		input, want any
		failure     string
	}{
		{"text", "text", "  exact  ", "  exact  ", ""},
		{"alias", "Label", "  exact  ", "  exact  ", ""},
		{"integer", "integer", float64(7), int64(7), ""},
		{"fraction", "integer", 7.5, nil, "must be integer"},
		{"numeric_integer", "numeric", int64(7), int64(7), ""},
		{"numeric_float", "numeric", float64(7), float64(7), ""},
		{"numeric_lexeme", "number", json.Number("7.25"), float64(7.25), ""},
		{"numeric_string", "numeric", "7.25", nil, "must be numeric"},
		{"numeric_nan", "numeric", math.NaN(), nil, "must be numeric"},
		{"boolean", "boolean", false, false, ""},
		{"boolean_string", "boolean", "false", nil, "must be boolean"},
		{"timestamp_text", "timestamp", " 2026-01-02T03:04:05+02:00 ", instant, ""},
		{"timestamp_value", "timestamp", instant.In(time.FixedZone("offset", 7200)), instant, ""},
		{"timestamp_invalid", "timestamp", "tomorrow", nil, "must be RFC3339 timestamp"},
		{"uuid", "uuid", " D9428888-122B-11E1-B85C-61CD3CBB3210 ", "D9428888-122B-11E1-B85C-61CD3CBB3210", ""},
		{"uuid_invalid", "uuid", "no-uuid", nil, "must be uuid"},
		{"enum", "Status", " open ", "open", ""},
		{"enum_invalid", "Status", "OPEN", nil, "must match declared enum value"},
		{"enum_number", "Status", int64(1), nil, "must be enum string"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			contract := Contract{Entity: c.EntityContract{Fields: map[string]c.EntityFieldDecl{"value": {Type: tc.typ}}}, Types: catalog}
			got, err := NormalizeFieldValue(contract, "value", tc.input)
			if tc.failure != "" {
				if got != nil || err == nil || err.Error() != "field value "+tc.failure {
					t.Fatalf("refusal: got=%#v err=%v", got, err)
				}
				return
			}
			requireValueFactoringResult(t, got, tc.want, err)
		})
	}
}

func TestEntityValueFactoringContainersAndDetachment(t *testing.T) {
	contract := Contract{
		Entity: c.EntityContract{Fields: map[string]c.EntityFieldDecl{
			"record": {Type: "Record"}, "list": {Type: "[Record]"},
			"numbers": {Type: "[integer]"}, "json": {Type: "json"},
		}},
		Types: c.TypeCatalogDocument{Types: map[string]c.NamedTypeDecl{"Record": {Fields: map[string]c.TypeFieldSpec{
			"name": {Type: "text"}, "note": {Type: "text", IsOptional: true},
		}}}},
	}
	for _, field := range []string{"record", "list", "json"} {
		t.Run(field, func(t *testing.T) {
			record := map[string]any{"name": "original"}
			var input, want any = record, map[string]any{"name": "original"}
			if field == "list" || field == "json" {
				input = []any{record}
				want = []any{map[string]any{"name": "original"}}
			}
			got, err := NormalizeFieldValue(contract, field, input)
			requireValueFactoringResult(t, got, want, err)
			record["name"] = "changed input"
			requireValueFactoringResult(t, got, want, nil)
			if field == "record" {
				got.(map[string]any)["name"] = "changed result"
			} else {
				got.([]any)[0].(map[string]any)["name"] = "changed result"
			}
			if record["name"] != "changed input" {
				t.Fatal("result retains caller's record")
			}
		})
	}
	for _, input := range []any{[]int{1, 2}, [2]int{1, 2}} {
		got, err := NormalizeFieldValue(contract, "numbers", input)
		requireValueFactoringResult(t, got, []any{int64(1), int64(2)}, err)
	}
	for _, input := range []map[string]any{{}, {"name": "x", "extra": true}, {"name": nil}} {
		if got, err := NormalizeFieldValue(contract, "record", input); err == nil || got != nil {
			t.Fatalf("incomplete, undeclared or null record: got=%#v err=%v", got, err)
		}
	}
}

func TestEntityValueFactoringMapKeysAndContainedConsumer(t *testing.T) {
	contract := Contract{
		Entity: c.EntityContract{Fields: map[string]c.EntityFieldDecl{"items": {Type: "map[Status]integer"}}},
		Types:  c.TypeCatalogDocument{Enums: map[string]c.EnumTypeDecl{"Status": {Values: []string{"open", "closed"}, Default: "open"}}},
	}
	got, err := NormalizeFieldValue(contract, "items", map[string]any{" open ": 1})
	requireValueFactoringResult(t, got, map[string]any{"open": int64(1)}, err)
	if got, err := NormalizeFieldValue(contract, "items", map[string]any{"open": 1, " open ": 2}); got != nil || err == nil || !strings.Contains(err.Error(), "duplicate normalized map key") {
		t.Fatalf("normalized-key collision: got=%#v err=%v", got, err)
	}
	target, err := ResolveContainedOperationTarget(contract, "entity.items", ContainedOperationSet, true, false)
	if err != nil || target.MapKeyType != "Status" || target.MapValueType != "integer" || target.TargetType != "integer" {
		t.Fatalf("MapTypeParts contained-operation consumer: target=%+v err=%v", target, err)
	}
	value, err := NormalizeContainedOperationValue(contract, target, ContainedOperationSet, float64(2))
	requireValueFactoringResult(t, value, int64(2), err)
	if _, err := NormalizeContainedOperationValue(contract, target, ContainedOperationSet, "2"); err == nil {
		t.Fatal("contained-operation consumer omitted value admission")
	}
	for _, ref := range []string{"map[]integer", "map[text]", "text"} {
		if _, _, ok := MapTypeParts(ref); ok {
			t.Fatalf("accepted incomplete map parts %q", ref)
		}
	}
}

func TestEntityValueFactoringRefinementOrderAndBounds(t *testing.T) {
	minLength, maxLength := 2, 3
	minRange, maxRange := 1.0, 2.0
	contract := Contract{Entity: c.EntityContract{Fields: map[string]c.EntityFieldDecl{
		"text":   {Type: "text", Refinements: c.SchemaRefinements{Pattern: "^[a-z]+$", Length: c.SchemaLengthRefinement{Min: &minLength, Max: &maxLength}}},
		"number": {Type: "numeric", Refinements: c.SchemaRefinements{Range: c.SchemaRangeRefinement{Min: &minRange, Max: &maxRange}}},
	}}}
	for _, tc := range []struct {
		field   string
		input   any
		failure string
	}{
		{"text", "ab", ""}, {"text", "abc", ""},
		{"text", "A", "must match pattern"}, {"text", "a", "length must be >= 2"}, {"text", "abcd", "length must be <= 3"},
		{"number", float64(1), ""}, {"number", float64(2), ""},
		{"number", float64(0), "must be >= 1"}, {"number", float64(3), "must be <= 2"},
	} {
		got, err := NormalizeFieldValue(contract, tc.field, tc.input)
		if tc.failure == "" {
			requireValueFactoringResult(t, got, tc.input, err)
		} else if got != nil || err == nil || err.Error() != "field "+tc.field+" "+tc.failure {
			t.Fatalf("refinement field=%s input=%#v got=%#v err=%v", tc.field, tc.input, got, err)
		}
	}
	if length, ok := refinementLength("\u00e9\u00e9"); !ok || length != 2 {
		t.Fatalf("length counts code points: %d %t", length, ok)
	}
	if _, ok := refinementLength(true); ok {
		t.Fatal("boolean acquired length")
	}
	if _, err := validateValueRefinements(contract, "text", "text", c.SchemaRefinements{Pattern: "["}, "ab"); err == nil || err.Error() != "field text has invalid pattern refinement" {
		t.Fatalf("defensive invalid-pattern branch: %v", err)
	}
}

func TestEntityValueFactoringEqualityPresence(t *testing.T) {
	fields := map[string]c.EntityFieldDecl{
		"left": {Type: "text", Refinements: c.SchemaRefinements{EqualTo: "right"}}, "right": {Type: "text"},
	}
	contract := Contract{Entity: c.EntityContract{Fields: fields}}
	for _, tc := range []struct {
		values  map[string]any
		failure string
	}{
		{nil, ""}, {map[string]any{"left": "x", "right": "x"}, ""},
		{map[string]any{"left": "x"}, "target is missing"},
		{map[string]any{"right": "x"}, "source is missing"},
		{map[string]any{"left": "x", "right": "y"}, "must equal entity.right"},
	} {
		got, err := NormalizeState(contract, tc.values)
		if tc.failure == "" {
			want := tc.values
			if want == nil {
				want = map[string]any{}
			}
			requireValueFactoringResult(t, got, want, err)
		} else if got != nil || err == nil || !strings.Contains(err.Error(), tc.failure) {
			t.Fatalf("root equality: got=%#v err=%v", got, err)
		}
	}
	value, err := NormalizeFieldValue(contract, "left", "x")
	requireValueFactoringResult(t, value, "x", err)
	contract = Contract{
		Entity: c.EntityContract{Fields: map[string]c.EntityFieldDecl{"record": {Type: "Pair"}}},
		Types: c.TypeCatalogDocument{Types: map[string]c.NamedTypeDecl{"Pair": {Fields: map[string]c.TypeFieldSpec{
			"left":  {Type: "text", IsOptional: true, Refinements: c.SchemaRefinements{EqualTo: "right"}},
			"right": {Type: "text", IsOptional: true},
		}}}},
	}
	got, err := NormalizeFieldValue(contract, "record", map[string]any{})
	requireValueFactoringResult(t, got, map[string]any{}, err)
	if got, err := NormalizeFieldValue(contract, "record", map[string]any{"left": "x"}); got != nil || err == nil || !strings.Contains(err.Error(), "target is missing") {
		t.Fatalf("optional structural equality: got=%#v err=%v", got, err)
	}
}

func TestEntityValueFactoringProjectionAndCloning(t *testing.T) {
	contract := Contract{Entity: c.EntityContract{Fields: map[string]c.EntityFieldDecl{
		"value": {Type: "json"}, "count": {Type: "integer"},
	}}}
	if names := FieldNames(contract); !reflect.DeepEqual(names, []string{"count", "value"}) {
		t.Fatalf("sorted declarations: %#v", names)
	}
	if _, err := FieldDecl(contract, " missing "); err == nil || err.Error() != "undeclared field missing" {
		t.Fatalf("declaration refusal: %v", err)
	}
	input := map[string]any{"value": map[string]any{"nested": []any{"original"}}, "run_id": "retained", "count": float64(2)}
	declared := DeclaredValues(contract, input)
	if _, ok := declared["run_id"]; ok {
		t.Fatal("undeclared metadata acquired entity authority")
	}
	input["value"].(map[string]any)["nested"].([]any)[0] = "changed"
	if declared["value"].(map[string]any)["nested"].([]any)[0] != "original" {
		t.Fatal("declared projection retains input")
	}
	for _, source := range []semanticview.Source{nil, semanticview.Wrap(&c.WorkflowContractBundle{RootEntities: c.EntityContractsDocument{"item": contract.Entity}})} {
		got, err := NormalizeMetadataForFlow(source, "", input)
		if err != nil || got["run_id"] != "retained" {
			t.Fatalf("metadata projection: %#v %v", got, err)
		}
		wantCount := any(float64(2))
		if source != nil {
			wantCount = int64(2)
		}
		if !reflect.DeepEqual(got["count"], wantCount) {
			t.Fatalf("metadata count: %#v", got["count"])
		}
		got["value"].(map[string]any)["nested"].([]any)[0] = "output"
		if input["value"].(map[string]any)["nested"].([]any)[0] != "changed" {
			t.Fatal("metadata projection retains input")
		}
	}
	if got := cloneMap(nil); got == nil || len(got) != 0 {
		t.Fatalf("nil clone shape: %#v", got)
	}
	if got := cloneValue([]any(nil)); !reflect.DeepEqual(got, []any{}) {
		t.Fatalf("private list clone shape: %#v", got)
	}
	if asString(7) != "" || asString(" exact ") != " exact " {
		t.Fatal("string carrier conversion changed")
	}
}

func requireValueFactoringResult(t *testing.T, got, want any, err error) {
	t.Helper()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%#v (%T) want=%#v (%T) err=%v", got, got, want, want, err)
	}
}
