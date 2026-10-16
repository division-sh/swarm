package eventschema

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func optionalNullObject(properties map[string]any, required ...string) map[string]any {
	return map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}
}

func optionalNullAlternatives() map[string]any {
	return map[string]any{"type": "object", "oneOf": []any{
		optionalNullObject(map[string]any{"label": map[string]any{"type": "string"}}),
		optionalNullObject(map[string]any{"other": map[string]any{"type": "string"}}, "other"),
	}}
}

func TestOptionalNullOneOfPreservesUniqueBranchAndInput(t *testing.T) {
	implicit := optionalNullAlternatives()
	delete(implicit, "type")
	outer := optionalNullAlternatives()
	outer["properties"] = map[string]any{"label": map[string]any{"type": "string", "minLength": 1}}
	array := map[string]any{"oneOf": []any{
		map[string]any{"type": "array", "items": optionalNullObject(map[string]any{"label": map[string]any{"type": "string"}})},
		map[string]any{"type": "array", "items": optionalNullObject(map[string]any{"other": map[string]any{"type": "string"}}, "other")},
	}}
	shared := map[string]any{
		"type": "object", "properties": map[string]any{"kind": map[string]any{"type": "string"}, "label": map[string]any{"type": "string"}},
		"required": []string{"kind"}, "additionalProperties": false,
		"oneOf": []any{
			map[string]any{"type": "object", "properties": map[string]any{"kind": map[string]any{"type": "string", "enum": []any{"first"}}}, "required": []string{"kind"}},
			map[string]any{"type": "object", "properties": map[string]any{"kind": map[string]any{"type": "string", "enum": []any{"second"}}}, "required": []string{"kind"}},
		},
	}
	for _, tc := range []struct {
		name    string
		schema  map[string]any
		payload map[string]any
		want    map[string]any
	}{
		{"root", optionalNullAlternatives(), map[string]any{"label": nil}, map[string]any{}},
		{"nested_implicit_object", optionalNullObject(map[string]any{"result": implicit}, "result"), map[string]any{"result": map[string]any{"label": nil}}, map[string]any{"result": map[string]any{}}},
		{"outer_declared_optional", outer, map[string]any{"label": nil}, map[string]any{}},
		{"shared_outer_field", shared, map[string]any{"kind": "first", "label": nil}, map[string]any{"kind": "first"}},
		{"list_record", optionalNullObject(map[string]any{"items": map[string]any{"type": "array", "items": optionalNullAlternatives()}}, "items"), map[string]any{"items": []any{map[string]any{"label": nil}}}, map[string]any{"items": []any{map[string]any{}}}},
		{"array_alternatives", optionalNullObject(map[string]any{"items": array}, "items"), map[string]any{"items": []any{map[string]any{"label": nil}}}, map[string]any{"items": []any{map[string]any{}}}},
		{"map_record", optionalNullObject(map[string]any{"entries": map[string]any{"type": "object", "additionalProperties": optionalNullAlternatives()}}, "entries"), map[string]any{"entries": map[string]any{"entry": map[string]any{"label": nil}}}, map[string]any{"entries": map[string]any{"entry": map[string]any{}}}},
		{"non_null_branch", optionalNullAlternatives(), map[string]any{"other": "present"}, map[string]any{"other": "present"}},
		{"already_omitted", optionalNullAlternatives(), map[string]any{}, map[string]any{}},
		{"execution_number_kind", optionalNullObject(map[string]any{"record": optionalNullAlternatives(), "number": map[string]any{"type": "number"}}, "record", "number"), map[string]any{"record": map[string]any{"label": nil}, "number": json.Number("8.0")}, map[string]any{"record": map[string]any{}, "number": json.Number("8.0")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inputBefore := cloneSchemaValue(tc.payload)
			schemaBefore, err := json.Marshal(tc.schema)
			if err != nil {
				t.Fatal(err)
			}
			got, err := NormalizeOptionalFieldNulls(tc.schema, tc.payload)
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("normalization=%#v, %v; want %#v", got, err, tc.want)
			}
			if err := ValidatePayloadAgainstSchema(tc.schema, got); err != nil {
				t.Fatalf("complete schema rejected normalized result: %v", err)
			}
			again, err := NormalizeOptionalFieldNulls(tc.schema, got)
			if err != nil || !reflect.DeepEqual(again, got) {
				t.Fatalf("normalization is not idempotent: %#v, %v", again, err)
			}
			schemaAfter, err := json.Marshal(tc.schema)
			if err != nil || !reflect.DeepEqual(inputBefore, tc.payload) || !bytes.Equal(schemaBefore, schemaAfter) {
				t.Fatal("normalization changed caller payload or schema")
			}
		})
	}
}

func TestOptionalNullOneOfCannotBorrowAnotherBranch(t *testing.T) {
	declaresLabel := optionalNullObject(map[string]any{"label": map[string]any{"type": "string"}})
	declaresLabel["enum"] = []any{map[string]any{"label": "keep"}}
	schema := map[string]any{"type": "object", "oneOf": []any{
		declaresLabel, optionalNullObject(map[string]any{}),
	}}
	if err := ValidatePayloadAgainstSchema(schema, map[string]any{}); err != nil {
		t.Fatalf("different branch should accept omission: %v", err)
	}
	if _, err := NormalizeOptionalFieldNulls(schema, map[string]any{"label": nil}); err == nil {
		t.Fatal("null omission borrowed validity from a branch that never declared the field")
	}
}

func TestOptionalNullOneOfRefusesDistinctCanonicalResults(t *testing.T) {
	payload := map[string]any{"value": map[string]any{"label": nil}}
	opaque := optionalNullObject(map[string]any{"value": map[string]any{}}, "value")
	opaque["enum"] = []any{payload}
	record := optionalNullObject(map[string]any{"value": optionalNullObject(map[string]any{"label": map[string]any{"type": "string"}})}, "value")
	for _, branches := range [][]any{{opaque, record}, {record, opaque}} {
		schema := map[string]any{"type": "object", "oneOf": branches}
		if err := ValidatePayloadAgainstSchema(schema, payload); err != nil {
			t.Fatalf("opaque branch must accept the original value: %v", err)
		}
		if err := ValidatePayloadAgainstSchema(schema, map[string]any{"value": map[string]any{}}); err != nil {
			t.Fatalf("record branch must accept its distinct omission result: %v", err)
		}
		if _, err := NormalizeOptionalFieldNulls(schema, payload); err == nil {
			t.Fatal("two different valid canonical results acquired a branch-order-dependent normalization")
		}
	}
}

func TestOptionalNullOneOfRefusalsRemainClosed(t *testing.T) {
	parentRequired := optionalNullAlternatives()
	parentRequired["properties"] = map[string]any{"label": map[string]any{"type": "string"}}
	parentRequired["required"] = []string{"label"}
	parentClosed := optionalNullAlternatives()
	parentClosed["additionalProperties"] = false
	ambiguous := map[string]any{"type": "object", "oneOf": []any{
		optionalNullObject(map[string]any{"label": map[string]any{"type": "string"}}),
		optionalNullObject(map[string]any{"label": map[string]any{"type": "string"}}),
	}}
	for _, tc := range []struct {
		name    string
		schema  map[string]any
		payload map[string]any
	}{
		{"required_null", optionalNullAlternatives(), map[string]any{"other": nil}},
		{"undeclared_null", optionalNullAlternatives(), map[string]any{"unknown": nil}},
		{"wrong_branch", optionalNullAlternatives(), map[string]any{"label": "present", "other": "present"}},
		{"two_candidates", ambiguous, map[string]any{"label": nil}},
		{"already_ambiguous", ambiguous, map[string]any{}},
		{"parent_required", parentRequired, map[string]any{"label": nil}},
		{"parent_closed_undeclared", parentClosed, map[string]any{"label": nil}},
		{"null_list_element", optionalNullObject(map[string]any{"items": map[string]any{"type": "array", "items": optionalNullAlternatives()}}, "items"), map[string]any{"items": []any{nil}}},
		{"null_map_value", optionalNullObject(map[string]any{"entries": map[string]any{"type": "object", "additionalProperties": optionalNullAlternatives()}}, "entries"), map[string]any{"entries": map[string]any{"entry": nil}}},
		{"empty_alternatives", map[string]any{"oneOf": []any{}}, map[string]any{}},
		{"invalid_alternatives", map[string]any{"oneOf": "invalid"}, map[string]any{}},
		{"invalid_branch", map[string]any{"oneOf": []any{nil}}, map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := cloneSchemaValue(tc.payload)
			got, err := NormalizeOptionalFieldNulls(tc.schema, tc.payload)
			if !reflect.DeepEqual(before, tc.payload) {
				t.Fatal("failed normalization changed the caller payload")
			}
			if err == nil && ValidatePayloadAgainstSchema(tc.schema, got) == nil {
				t.Fatal("invalid payload obtained admission")
			}
		})
	}
}

func TestOptionalNullOneOfBranchOrderAndNestedConstraints(t *testing.T) {
	inner := optionalNullAlternatives()
	first := optionalNullObject(map[string]any{"record": inner}, "record")
	second := optionalNullObject(map[string]any{"different": map[string]any{"type": "boolean"}}, "different")
	for _, branches := range [][]any{{first, second}, {second, first}} {
		schema := map[string]any{"type": "object", "oneOf": branches}
		payload := map[string]any{"record": map[string]any{"label": nil}}
		got, err := NormalizeOptionalFieldNulls(schema, payload)
		if err != nil || !reflect.DeepEqual(got, map[string]any{"record": map[string]any{}}) {
			t.Fatalf("branch order changed unique result: %#v, %v", got, err)
		}
		if err := ValidatePayloadAgainstSchema(schema, got); err != nil {
			t.Fatalf("nested candidate failed its full schema: %v", err)
		}
	}
}
