package contracts

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/eventschema"
)

func issue2586Object(properties map[string]any, required ...string) map[string]any {
	if required == nil {
		required = []string{}
	}
	return map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}
}

func issue2586Branches() map[string]any {
	return map[string]any{
		"type": "object",
		"oneOf": []any{
			issue2586Object(map[string]any{"label": map[string]any{"type": "string"}}),
			issue2586Object(map[string]any{"other": map[string]any{"type": "string"}}, "other"),
		},
	}
}

func TestIssue2586OptionalNullParityProbe(t *testing.T) {
	raw := issue2586Branches()
	admitted, err := AdmitToolInputSchemaMap(raw)
	if err != nil {
		t.Fatalf("schema admission: %v", err)
	}
	projected, err := admitted.Project()
	if err != nil {
		t.Fatal(err)
	}
	registry := EventSchemaRegistryFromCatalog(map[string]EventCatalogEntry{
		"probe.done": {Payload: EventPayloadSpec{
			Properties: map[string]EventFieldSpec{"result": {Type: "object", ExactSchema: &admitted}},
			Required:   []string{"result"},
		}},
	})
	withoutType := issue2586Branches()
	delete(withoutType, "type")
	for _, tc := range []struct {
		name    string
		schema  map[string]any
		payload map[string]any
		want    map[string]any
	}{
		{"direct", projected, map[string]any{"label": nil}, map[string]any{}},
		{"exact_schema_registry", registry["probe.done"].Schema, map[string]any{"result": map[string]any{"label": nil}}, map[string]any{"result": map[string]any{}}},
		{"implicit_object_branch", withoutType, map[string]any{"label": nil}, map[string]any{}},
		{"array_member_object", issue2586Object(map[string]any{"items": map[string]any{"type": "array", "items": projected}}, "items"), map[string]any{"items": []any{map[string]any{"label": nil}}}, map[string]any{"items": []any{map[string]any{}}}},
		{"map_value_object", issue2586Object(map[string]any{"values": map[string]any{"type": "object", "additionalProperties": projected}}, "values"), map[string]any{"values": map[string]any{"entry": map[string]any{"label": nil}}}, map[string]any{"values": map[string]any{"entry": map[string]any{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := eventschema.ValidatePayloadAgainstSchema(tc.schema, tc.want); err != nil {
				t.Fatalf("omitted equivalent must validate: %v", err)
			}
			before := cloneEventSchemaMap(tc.payload)
			got, err := eventschema.NormalizeOptionalFieldNulls(tc.schema, tc.payload)
			if !reflect.DeepEqual(tc.payload, before) {
				t.Fatal("normalization mutated the caller payload")
			}
			if err != nil {
				t.Fatalf("admitted optional branch field must lower null to omission: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("normalized=%#v, want %#v", got, tc.want)
			}
			if err := eventschema.ValidatePayloadAgainstSchema(tc.schema, got); err != nil {
				t.Fatalf("normalized equivalent must validate: %v", err)
			}
		})
	}
}

func TestIssue2586OptionalNullSafetyControls(t *testing.T) {
	for _, tc := range []struct {
		name    string
		schema  map[string]any
		payload map[string]any
	}{
		{"required_null", issue2586Branches(), map[string]any{"other": nil}},
		{"undeclared_null", issue2586Branches(), map[string]any{"unknown": nil}},
		{"ambiguous_branches", map[string]any{"type": "object", "oneOf": []any{issue2586Object(map[string]any{"label": map[string]any{"type": "string"}}), issue2586Object(map[string]any{"label": map[string]any{"type": "string"}})}}, map[string]any{"label": nil}},
		{"null_list_element", issue2586Object(map[string]any{"items": map[string]any{"type": "array", "items": issue2586Branches()}}, "items"), map[string]any{"items": []any{nil}}},
		{"null_map_value", issue2586Object(map[string]any{"values": map[string]any{"type": "object", "additionalProperties": issue2586Branches()}}, "values"), map[string]any{"values": map[string]any{"entry": nil}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := eventschema.NormalizeOptionalFieldNulls(tc.schema, tc.payload)
			if err == nil && eventschema.ValidatePayloadAgainstSchema(tc.schema, got) == nil {
				t.Fatal("invalid input acquired admission through null lowering")
			}
		})
	}
}
