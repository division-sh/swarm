package llm

import (
	"encoding/json"
	"fmt"
	"testing"
)

type providerSchemaCase struct {
	name   string
	schema any
	want   string
}

func TestProviderSchemaFactoringCharacterization(t *testing.T) {
	t.Run("name-and-wrapper", func(t *testing.T) {
		for _, tc := range []struct{ name, want string }{
			{" probe ", "provider tool schema probe: probe.input_schema must be a JSON object schema, got bool"},
			{" \t", "provider tool schema tool: tool.input_schema must be a JSON object schema, got bool"},
		} {
			requireProviderSchemaError(t, ValidateProviderToolSchema(tc.name, false), tc.want)
		}
	})
	groups := []struct {
		name  string
		cases []providerSchemaCase
	}{
		{"nil-schema", []providerSchemaCase{
			{"nil", nil, ""},
			{"typed-map", map[string]any(nil), " must be a JSON object schema, got map[string]interface {}"},
			{"typed-slice", []any(nil), " must be a JSON object schema, got []interface {}"},
			{"typed-pointer", (*string)(nil), " must be a JSON object schema, got *string"},
		}},
		{"object-shape", []providerSchemaCase{
			{"scalar", 42, " must be a JSON object schema, got int"},
			{"other-map", map[string]string{}, " must be a JSON object schema, got map[string]string"},
			{"nested", map[string]any{"properties": map[string]any{"a": false}}, ".properties.a must be a JSON object schema, got bool"},
		}},
		{"unsupported-type", []providerSchemaCase{
			{"unknown", map[string]any{"type": "unsupported"}, ".type has unsupported JSON Schema type \"unsupported\""},
			{"precision", map[string]any{"type": "numeric(5,2)"}, ".type has unsupported JSON Schema type \"numeric(5,2)\""},
			{"blank", map[string]any{"type": " \t"}, ".type has unsupported JSON Schema type \" \\t\""},
			{"wrong-case", map[string]any{"type": "String"}, ".type has unsupported JSON Schema type \"String\""},
			{"null", map[string]any{"type": nil}, ".type must be a string or string array, got <nil>"},
		}},
		{"type-any-array", []providerSchemaCase{
			{"nested", map[string]any{"type": []any{"string", []any{"null", []string{"number"}}}}, ".type[1] must be a string, got []interface {}"},
			{"empty", map[string]any{"type": []any{}}, ".type must be a non-empty string array"},
			{"typed-nil", map[string]any{"type": []any(nil)}, ".type must be a non-empty string array"},
			{"nested-error", map[string]any{"type": []any{"string", []any{"null", "bad"}, "later"}}, ".type[1] must be a string, got []interface {}"},
			{"bad-element", map[string]any{"type": []any{"string", 7}}, ".type[1] must be a string, got int"},
		}},
		{"type-string-array", []providerSchemaCase{
			{"valid", map[string]any{"type": []string{"string", "null"}}, ""},
			{"padded", map[string]any{"type": []string{"string", " null "}}, ".type[1] has unsupported JSON Schema type \" null \""},
			{"duplicate", map[string]any{"type": []string{"string", "string"}}, ".type[1] repeats JSON Schema type \"string\""},
			{"empty", map[string]any{"type": []string{}}, ".type must be a non-empty string array"},
			{"typed-nil", map[string]any{"type": []string(nil)}, ".type must be a non-empty string array"},
			{"first-error", map[string]any{"type": []string{"string", "bad", "later"}}, ".type[1] has unsupported JSON Schema type \"bad\""},
		}},
		{"properties-container", []providerSchemaCase{
			{"typed-nil", map[string]any{"properties": map[string]any(nil)}, ".properties must be an object, got map[string]interface {}"},
			{"nil", map[string]any{"properties": nil}, ".properties must be an object, got <nil>"},
			{"array", map[string]any{"properties": []any{}}, ".properties must be an object, got []interface {}"},
		}},
		{"property-order", []providerSchemaCase{
			{"lexical", map[string]any{"properties": map[string]any{"z": map[string]any{"type": "last"}, "a": map[string]any{"type": "first"}}}, ".properties.a.type has unsupported JSON Schema type \"first\""},
			{"nil-children", map[string]any{"properties": map[string]any{"a": nil, "b": map[string]any(nil)}}, ".properties.a must be a JSON object schema, got <nil>"},
			{"nested-lexical", map[string]any{"properties": map[string]any{"a": map[string]any{"properties": map[string]any{"z": false, "b": 1}}}}, ".properties.a.properties.b must be a JSON object schema, got int"},
		}},
		{"items", []providerSchemaCase{
			{"nil", map[string]any{"items": nil}, ".items must be a JSON object schema, got <nil>"},
			{"typed-nil", map[string]any{"items": map[string]any(nil)}, ".items must be a JSON object schema, got map[string]interface {}"},
			{"not-object", map[string]any{"items": []any{}}, ".items must be a JSON object schema, got []interface {}"},
			{"nested-type", map[string]any{"items": map[string]any{"type": "bad"}}, ".items.type has unsupported JSON Schema type \"bad\""},
		}},
		{"property-names", []providerSchemaCase{
			{"nil", map[string]any{"propertyNames": nil}, ".propertyNames must be a JSON object schema"},
			{"typed-nil", map[string]any{"propertyNames": map[string]any(nil)}, ".propertyNames must be a JSON object schema, got map[string]interface {}"},
			{"not-object", map[string]any{"propertyNames": true}, ".propertyNames must be a JSON object schema, got bool"},
			{"nested-type", map[string]any{"propertyNames": map[string]any{"type": "bad"}}, ".propertyNames.type has unsupported JSON Schema type \"bad\""},
		}},
		{"oneof-container", []providerSchemaCase{
			{"nil", map[string]any{"oneOf": nil}, ".oneOf must be a non-empty schema array"},
			{"empty", map[string]any{"oneOf": []any{}}, ".oneOf must be a non-empty schema array"},
			{"typed-nil", map[string]any{"oneOf": []any(nil)}, ".oneOf must be a non-empty schema array"},
			{"typed-map-array", map[string]any{"oneOf": []map[string]any{{}}}, ".oneOf must be a non-empty schema array"},
		}},
		{"oneof-branches", []providerSchemaCase{
			{"nil-child", map[string]any{"oneOf": []any{map[string]any{}, nil}}, ".oneOf[1] must be a JSON object schema"},
			{"typed-nil-child", map[string]any{"oneOf": []any{map[string]any(nil)}}, ".oneOf[0] must be a JSON object schema, got map[string]interface {}"},
			{"not-object", map[string]any{"oneOf": []any{false}}, ".oneOf[0] must be a JSON object schema, got bool"},
			{"first-error", map[string]any{"oneOf": []any{map[string]any{"type": "first"}, map[string]any{"type": "later"}}}, ".oneOf[0].type has unsupported JSON Schema type \"first\""},
			{"nested-error", map[string]any{"oneOf": []any{map[string]any{}, map[string]any{"items": map[string]any{"type": "bad"}}}}, ".oneOf[1].items.type has unsupported JSON Schema type \"bad\""},
		}},
		{"additional-bool", []providerSchemaCase{
			{"true", map[string]any{"additionalProperties": true}, ""},
			{"false", map[string]any{"additionalProperties": false}, ""},
		}},
		{"additional-schema", []providerSchemaCase{
			{"typed-nil", map[string]any{"additionalProperties": map[string]any(nil)}, ".additionalProperties must be a JSON object schema, got map[string]interface {}"},
			{"nested-type", map[string]any{"additionalProperties": map[string]any{"type": "bad"}}, ".additionalProperties.type has unsupported JSON Schema type \"bad\""},
			{"nested-required", map[string]any{"additionalProperties": map[string]any{"required": []any{1}}}, ".additionalProperties.required[0] must be a string, got int"},
		}},
		{"additional-shape", []providerSchemaCase{
			{"nil", map[string]any{"additionalProperties": nil}, ".additionalProperties must be boolean or object schema, got <nil>"},
			{"array", map[string]any{"additionalProperties": []any{}}, ".additionalProperties must be boolean or object schema, got []interface {}"},
			{"other-map", map[string]any{"additionalProperties": map[string]string{}}, ".additionalProperties must be boolean or object schema, got map[string]string"},
		}},
		{"required-arrays", []providerSchemaCase{
			{"strings", map[string]any{"required": []string{"", " x ", "x"}}, ""},
			{"any-strings", map[string]any{"required": []any{"", " x ", "x"}}, ""},
			{"duplicate", map[string]any{"required": []any{"x", "x"}}, ".required[1] repeats required property \"x\""},
			{"empty", map[string]any{"required": []any{}}, ""},
			{"nil-strings", map[string]any{"required": []string(nil)}, ".required must be a non-null array"},
			{"nil-any", map[string]any{"required": []any(nil)}, ".required must be a non-null array"},
		}},
		{"required-error", []providerSchemaCase{
			{"first-error", map[string]any{"required": []any{"a", 1, false}}, ".required[1] must be a string, got int"},
			{"nil-element", map[string]any{"required": []any{nil}}, ".required[0] must be a string, got <nil>"},
			{"nil", map[string]any{"required": nil}, ".required must be an array, got <nil>"},
			{"scalar", map[string]any{"required": "a"}, ".required must be an array, got string"},
		}},
		{"enum-shape", []providerSchemaCase{
			{"values", map[string]any{"enum": []any{nil, "", 1, map[string]any{}}}, ""},
			{"empty", map[string]any{"enum": []any{}}, ""},
			{"typed-nil", map[string]any{"enum": []any(nil)}, ".enum must be a non-null array"},
			{"nil", map[string]any{"enum": nil}, ".enum must be an array, got <nil>"},
			{"strings", map[string]any{"enum": []string{"a"}}, ".enum must be an array, got []string"},
			{"scalar", map[string]any{"enum": "a"}, ".enum must be an array, got string"},
		}},
		{"dialect-boundary", []providerSchemaCase{
			{"unknown-keywords", map[string]any{"format": false, "$ref": 1, "allOf": true, "anyOf": nil, "not": "ignored"}, ""},
			{"open-object-empty-enum", map[string]any{"type": "object", "additionalProperties": true, "properties": map[string]any{"a": map[string]any{"enum": []any{}}}}, ""},
		}},
	}
	for _, group := range groups {
		t.Run(group.name, func(t *testing.T) {
			for _, tc := range group.cases {
				t.Run(tc.name, func(t *testing.T) {
					before := providerSchemaJSON(t, tc.schema)
					want := tc.want
					if want != "" {
						want = "provider tool schema probe: probe.input_schema" + want
					}
					requireProviderSchemaError(t, ValidateProviderToolSchema("probe", tc.schema), want)
					if after := providerSchemaJSON(t, tc.schema); after != before {
						t.Fatalf("schema mutated: before=%s after=%s", before, after)
					}
				})
			}
		})
	}
	t.Run("supported-types", func(t *testing.T) {
		for _, name := range []string{"array", "boolean", "integer", "null", "number", "object", "string"} {
			schema := map[string]any{"type": name}
			before := providerSchemaJSON(t, schema)
			requireProviderSchemaError(t, ValidateProviderToolSchema("probe", schema), "")
			if after := providerSchemaJSON(t, schema); after != before {
				t.Fatalf("type spelling mutated: before=%s after=%s", before, after)
			}
		}
	})
	t.Run("constraint-order", testProviderSchemaConstraintOrder)
	t.Run("input-unchanged", testProviderSchemaInputUnchanged)
	t.Run("definition-order", testProviderSchemaDefinitionOrder)
}

func testProviderSchemaConstraintOrder(t *testing.T) {
	schema := map[string]any{
		"type": "bad", "properties": false, "items": false, "propertyNames": nil,
		"oneOf": []any{}, "additionalProperties": nil, "required": 1, "enum": 1,
	}
	for _, tc := range []struct{ key, want string }{
		{"type", ".type has unsupported JSON Schema type \"bad\""},
		{"properties", ".properties must be an object, got bool"},
		{"items", ".items must be a JSON object schema, got bool"},
		{"propertyNames", ".propertyNames must be a JSON object schema"},
		{"oneOf", ".oneOf must be a non-empty schema array"},
		{"additionalProperties", ".additionalProperties must be boolean or object schema, got <nil>"},
		{"required", ".required must be an array, got int"},
		{"enum", ".enum must be an array, got int"},
	} {
		requireProviderSchemaError(t, ValidateProviderToolSchema("probe", schema), "provider tool schema probe: probe.input_schema"+tc.want)
		delete(schema, tc.key)
	}
	requireProviderSchemaError(t, ValidateProviderToolSchema("probe", schema), "")
}

func testProviderSchemaInputUnchanged(t *testing.T) {
	child := map[string]any{"type": []any{"string", "null"}, "enum": []any{nil, "x"}}
	schema := map[string]any{
		"properties": map[string]any{"a": child, "b": child}, "items": child,
		"propertyNames": child, "oneOf": []any{child}, "additionalProperties": child,
		"required": []string{" a ", "a"},
	}
	for _, typ := range []any{child["type"], "bad"} {
		child["type"] = typ
		before := providerSchemaJSON(t, schema)
		for range 3 {
			err := ValidateProviderToolSchema("probe", schema)
			if _, invalid := typ.(string); invalid {
				requireProviderSchemaError(t, err, "provider tool schema probe: probe.input_schema.properties.a.type has unsupported JSON Schema type \"bad\"")
			} else {
				requireProviderSchemaError(t, err, "")
			}
		}
		if after := providerSchemaJSON(t, schema); after != before {
			t.Fatalf("shared schema mutated: before=%s after=%s", before, after)
		}
	}
}

func testProviderSchemaDefinitionOrder(t *testing.T) {
	tools := []ToolDefinition{
		{Name: " valid ", Schema: nil},
		{Name: " first ", Schema: map[string]any{"type": "first-bad"}},
		{Name: "last", Schema: false},
	}
	before := providerSchemaJSON(t, tools)
	requireProviderSchemaError(t, ValidateProviderToolDefinitions(tools), "provider tool schema first: first.input_schema.type has unsupported JSON Schema type \"first-bad\"")
	if after := providerSchemaJSON(t, tools); after != before {
		t.Fatalf("definitions mutated: before=%s after=%s", before, after)
	}
	requireProviderSchemaError(t, ValidateProviderToolDefinitions(nil), "")
	requireProviderSchemaError(t, ValidateProviderToolDefinitions(tools[:1]), "")
}

func requireProviderSchemaError(t *testing.T, err error, want string) {
	t.Helper()
	got := ""
	if err != nil {
		got = err.Error()
	}
	if got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

func providerSchemaJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(fmt.Errorf("serialize schema evidence: %w", err))
	}
	return string(raw)
}
