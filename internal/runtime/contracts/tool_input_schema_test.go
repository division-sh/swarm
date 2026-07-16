package contracts

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestValidateToolInputSchemaRejectsMalformedRecursiveSchemas(t *testing.T) {
	negative := -1
	zero := 0
	one := 1
	falseValue := false
	trueValue := true
	negativeZero := math.Copysign(0, -1)
	notFinite := math.Inf(1)
	tests := []struct {
		name   string
		schema ToolInputSchema
		want   string
	}{
		{name: "missing type", schema: ToolInputSchema{}, want: "explicit supported JSON type"},
		{name: "noncanonical type", schema: ToolInputSchema{Type: " String "}, want: "not canonical"},
		{name: "nested unsupported type", schema: ToolInputSchema{Type: "array", Items: &ToolInputSchema{Type: "money"}}, want: "$.items requires an explicit supported JSON type"},
		{name: "array missing items", schema: ToolInputSchema{Type: "array"}, want: "array requires items"},
		{name: "negative item bound", schema: ToolInputSchema{Type: "array", Items: &ToolInputSchema{Type: "string"}, MinItems: &negative}, want: "minItems must be non-negative"},
		{name: "impossible item bounds", schema: ToolInputSchema{Type: "array", Items: &ToolInputSchema{Type: "string"}, MinItems: &one, MaxItems: &zero}, want: "minItems must be <= maxItems"},
		{name: "invalid regex", schema: ToolInputSchema{Type: "string", Pattern: "["}, want: "pattern is invalid"},
		{name: "inapplicable string constraint", schema: ToolInputSchema{Type: "boolean", MinLength: &one}, want: "cannot declare string constraints"},
		{name: "negative zero", schema: ToolInputSchema{Type: "number", Minimum: &negativeZero}, want: "finite non-negative-zero"},
		{name: "nonfinite number", schema: ToolInputSchema{Type: "number", Maximum: &notFinite}, want: "finite non-negative-zero"},
		{name: "missing required property", schema: ToolInputSchema{Type: "object", Required: []string{"id"}}, want: "required property \"id\" is not declared"},
		{name: "noncanonical property", schema: ToolInputSchema{Type: "object", Properties: map[string]ToolInputSchema{" id ": {Type: "string"}}}, want: "property name \" id \" is not canonical"},
		{name: "duplicate required", schema: ToolInputSchema{Type: "object", Properties: map[string]ToolInputSchema{"id": {Type: "string"}}, Required: []string{"id", "id"}}, want: "required property \"id\" is duplicated"},
		{name: "two additional properties forms", schema: ToolInputSchema{Type: "object", AdditionalProperties: ToolAdditionalProperties{Allowed: &falseValue, Schema: &ToolInputSchema{Type: "string"}}}, want: "boolean or schema, not both"},
		{name: "additional properties malformed", schema: ToolInputSchema{Type: "object", AdditionalProperties: ToolAdditionalProperties{Schema: &ToolInputSchema{Type: "array"}}}, want: "additionalProperties array requires items"},
		{name: "enum wrong type", schema: ToolInputSchema{Type: "integer", Enum: []SchemaLiteral{schemaStringLiteral("one")}}, want: "enum[0] must be integer"},
		{name: "duplicate semantic enum", schema: ToolInputSchema{Type: "string", Enum: []SchemaLiteral{schemaStringLiteral("one"), schemaStringLiteral("one")}}, want: "duplicates another semantic value"},
		{name: "object constraints on scalar", schema: ToolInputSchema{Type: "string", AdditionalProperties: ToolAdditionalProperties{Allowed: &trueValue}}, want: "cannot declare object constraints"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateToolInputSchema(tc.schema); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateToolInputSchema error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestToolInputSchemaProjectionPreservesExactSemanticConstraints(t *testing.T) {
	schema := schemaWithEnum(t, `
type: object
additionalProperties:
  type: integer
required: [status]
properties:
  status:
    type: string
    pattern: ' approved $'
    enum: [' approved ']
  payload:
    type: array
    items: {type: number}
`)
	if err := ValidateToolInputSchema(schema); err != nil {
		t.Fatalf("ValidateToolInputSchema: %v", err)
	}
	projected := ToolInputSchemaJSONSchema(schema)
	if got := projected["required"]; !reflect.DeepEqual(got, []string{"status"}) {
		t.Fatalf("required = %#v", got)
	}
	properties := projected["properties"].(map[string]any)
	status := properties["status"].(map[string]any)
	if status["pattern"] != " approved $" || !reflect.DeepEqual(status["enum"], []any{" approved "}) {
		t.Fatalf("status schema was normalized: %#v", status)
	}
	additional, ok := projected["additionalProperties"].(map[string]any)
	if !ok || additional["type"] != "integer" {
		t.Fatalf("additionalProperties = %#v", projected["additionalProperties"])
	}
}

func TestCloneToolInputSchemaAndEventCatalogAreMutationIsolated(t *testing.T) {
	schema := schemaWithEnum(t, `
type: object
properties:
  state:
    type: object
    properties:
      code: {type: string}
    required: [code]
    enum:
      - {code: approved}
`)
	anchor := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "anchored"}
	alias := yaml.Node{Kind: yaml.AliasNode, Alias: anchor}
	state := schema.Properties["state"]
	state.Enum = append(state.Enum, SchemaLiteral{Node: alias})
	schema.Properties["state"] = state

	cloned := CloneToolInputSchema(schema)
	clonedState := cloned.Properties["state"]
	clonedState.Required[0] = "changed"
	clonedState.Enum[0].Node.Content[1].Value = "changed"
	clonedState.Enum[1].Node.Alias.Value = "changed"
	cloned.Properties["state"] = clonedState
	originalState := schema.Properties["state"]
	if originalState.Required[0] != "code" || originalState.Enum[0].Node.Content[1].Value != "approved" || originalState.Enum[1].Node.Alias.Value != "anchored" {
		t.Fatalf("schema clone mutated original: %#v", originalState)
	}

	entry := EventCatalogEntry{Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{
		"value": {ExactSchema: &schema, Citation: CriteriaCitation{AllowedClasses: []string{"source"}}},
	}}}
	entryClone := CloneEventCatalogEntry(entry)
	entryField := entryClone.Payload.Properties["value"]
	entryField.Citation.AllowedClasses[0] = "changed"
	entryField.ExactSchema.Properties["state"] = ToolInputSchema{Type: "boolean"}
	entryClone.Payload.Properties["value"] = entryField
	if entry.Payload.Properties["value"].Citation.AllowedClasses[0] != "source" || entry.Payload.Properties["value"].ExactSchema.Properties["state"].Type != "object" {
		t.Fatal("event catalog clone shared nested schema state")
	}
}

func schemaWithEnum(t *testing.T, body string) ToolInputSchema {
	t.Helper()
	var schema ToolInputSchema
	if err := yaml.Unmarshal([]byte(body), &schema); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	return schema
}

func schemaStringLiteral(value string) SchemaLiteral {
	return SchemaLiteral{Node: yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}}
}
