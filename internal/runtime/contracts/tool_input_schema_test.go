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

func TestValidateToolInputSchemaRejectsCyclesAndExcessiveDepthBeforeProjectionOrClone(t *testing.T) {
	cyclic := ToolInputSchema{Type: "array"}
	cyclic.Items = &cyclic
	if err := ValidateToolInputSchema(cyclic); err == nil || !strings.Contains(err.Error(), "schema cycle") {
		t.Fatalf("cyclic schema error = %v", err)
	}
	if _, err := ProjectToolInputSchema(cyclic); err == nil || !strings.Contains(err.Error(), "schema cycle") {
		t.Fatalf("cyclic projection error = %v", err)
	}
	if cloned := CloneToolInputSchema(cyclic); ToolInputSchemaIsZero(cloned) || ValidateToolInputSchema(cloned) == nil {
		t.Fatalf("cyclic clone = %#v, want fail-closed invalid schema", cloned)
	}

	deep := ToolInputSchema{Type: "string"}
	for depth := 0; depth <= MaxToolInputSchemaDepth; depth++ {
		item := deep
		deep = ToolInputSchema{Type: "array", Items: &item}
	}
	if err := ValidateToolInputSchema(deep); err == nil || !strings.Contains(err.Error(), "exceeds maximum schema depth") {
		t.Fatalf("deep schema error = %v", err)
	}
	if _, err := ProjectToolInputSchema(deep); err == nil || !strings.Contains(err.Error(), "exceeds maximum schema depth") {
		t.Fatalf("deep projection error = %v", err)
	}
	if cloned := CloneToolInputSchema(deep); ToolInputSchemaIsZero(cloned) || ValidateToolInputSchema(cloned) == nil {
		t.Fatalf("deep clone = %#v, want fail-closed invalid schema", cloned)
	}
}

func TestToolInputSchemaRejectsExplicitEmptyEnum(t *testing.T) {
	var schema ToolInputSchema
	err := yaml.Unmarshal([]byte("type: string\nenum: []\n"), &schema)
	if err == nil || !strings.Contains(err.Error(), "enum must contain at least one value") {
		t.Fatalf("empty enum error = %v", err)
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

func TestCloneToolSchemaEntryOwnsEveryMutableSchemaCarrier(t *testing.T) {
	schema := schemaWithEnum(t, "type: string\nenum: [approved]\n")
	entry := ToolSchemaEntry{
		InputSchema:  schema,
		OutputSchema: ToolInputSchema{Type: "object", Properties: map[string]ToolInputSchema{"state": schema}},
		HTTP:         &HTTPToolSpec{Headers: map[string]string{"X-Test": "one"}, Body: map[string]any{"nested": []any{"one"}}},
		ResponseMapping: map[string]any{
			"state": map[string]any{"from": "result.state"},
		},
		ResponseSuccess: &HTTPResponseSuccess{Kind: "field_equals", Equals: map[string]any{"ok": true}},
		Credentials:     []string{"token"},
		CompiledResult: &CompiledResultProjection{
			Fields: map[string]CompiledResultField{"state": {From: "result.state"}}, OutputSchema: schema,
		},
	}
	cloned := CloneToolSchemaEntry(entry)
	cloned.InputSchema.Enum[0].Node.Value = "changed"
	cloned.OutputSchema.Properties["state"] = ToolInputSchema{Type: "boolean"}
	cloned.HTTP.Headers["X-Test"] = "changed"
	cloned.HTTP.Body.(map[string]any)["nested"].([]any)[0] = "changed"
	cloned.ResponseMapping["state"].(map[string]any)["from"] = "changed"
	cloned.ResponseSuccess.Equals.(map[string]any)["ok"] = false
	cloned.Credentials[0] = "changed"
	cloned.CompiledResult.Fields["state"] = CompiledResultField{From: "changed"}
	cloned.CompiledResult.OutputSchema.Enum[0].Node.Value = "changed"

	if entry.InputSchema.Enum[0].Node.Value != "approved" || entry.OutputSchema.Properties["state"].Type != "string" ||
		entry.HTTP.Headers["X-Test"] != "one" || entry.HTTP.Body.(map[string]any)["nested"].([]any)[0] != "one" ||
		entry.ResponseMapping["state"].(map[string]any)["from"] != "result.state" || entry.ResponseSuccess.Equals.(map[string]any)["ok"] != true ||
		entry.Credentials[0] != "token" || entry.CompiledResult.Fields["state"].From != "result.state" ||
		entry.CompiledResult.OutputSchema.Enum[0].Node.Value != "approved" {
		t.Fatal("tool schema clone leaked a mutable carrier")
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
