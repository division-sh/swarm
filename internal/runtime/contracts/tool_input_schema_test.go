package contracts

import (
	"fmt"
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
	stringSchema := MustToolInputSchema(ToolSchemaString)

	tests := []struct {
		name  string
		build func() error
		want  string
	}{
		{name: "missing type", build: func() error { _, err := NewToolInputSchema(""); return err }, want: "explicit supported JSON type"},
		{name: "noncanonical type", build: func() error { _, err := NewToolInputSchema(ToolSchemaKind(" String ")); return err }, want: "explicit supported JSON type"},
		{name: "array missing items", build: func() error { _, err := NewToolInputSchema(ToolSchemaArray); return err }, want: "array requires items"},
		{name: "negative item bound", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaArray, ToolSchemaItems(stringSchema), ToolSchemaMinItems(negative))
			return err
		}, want: "minItems must be non-negative"},
		{name: "impossible item bounds", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaArray, ToolSchemaItems(stringSchema), ToolSchemaMinItems(one), ToolSchemaMaxItems(zero))
			return err
		}, want: "minItems must be <= maxItems"},
		{name: "invalid regex", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaString, ToolSchemaPattern("["))
			return err
		}, want: "pattern is invalid"},
		{name: "inapplicable string constraint", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaBoolean, ToolSchemaMinLength(one))
			return err
		}, want: "cannot declare string constraints"},
		{name: "format on non-string", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaInteger, ToolSchemaFormat("uuid"))
			return err
		}, want: "cannot declare string constraints"},
		{name: "equalTo outside object property", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaString, ToolSchemaEqualTo("other"))
			return err
		}, want: "only valid on a declared object property"},
		{name: "equalTo missing sibling", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{
				"owner": stringSchema,
			}), toolSchemaPropertyEqualities(map[string]string{"owner": "missing"}))
			return err
		}, want: "target \"missing\" is not declared"},
		{name: "negative zero", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaNumber, ToolSchemaMinimum(negativeZero))
			return err
		}, want: "finite non-negative-zero"},
		{name: "nonfinite number", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaNumber, ToolSchemaMaximum(notFinite))
			return err
		}, want: "finite non-negative-zero"},
		{name: "missing required property", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaObject, ToolSchemaRequired("id"))
			return err
		}, want: "required property \"id\" is not declared"},
		{name: "noncanonical property", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{" id ": stringSchema}))
			return err
		}, want: "property name \" id \" is not canonical"},
		{name: "duplicate required", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{"id": stringSchema}), ToolSchemaRequired("id", "id"))
			return err
		}, want: "required property \"id\" is duplicated"},
		{name: "two additional properties forms", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaObject, ToolSchemaAdditionalPropertiesAllowed(falseValue), ToolSchemaAdditionalPropertiesSchema(stringSchema))
			return err
		}, want: "boolean or schema, not both"},
		{name: "enum wrong type", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaInteger, ToolSchemaEnum("one"))
			return err
		}, want: "enum[0] must be integer"},
		{name: "duplicate semantic enum", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaString, ToolSchemaEnum("one", "one"))
			return err
		}, want: "duplicates another semantic value"},
		{name: "object constraints on scalar", build: func() error {
			_, err := NewToolInputSchema(ToolSchemaString, ToolSchemaAdditionalPropertiesAllowed(trueValue))
			return err
		}, want: "cannot declare object constraints"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.build(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("admission error = %v, want %q", err, tc.want)
			}
		})
	}

	var nested ToolInputSchema
	err := yaml.Unmarshal([]byte("type: array\nitems:\n  type: money\n"), &nested)
	if err == nil || !strings.Contains(err.Error(), "requires an explicit supported JSON type") {
		t.Fatalf("nested YAML admission error = %v", err)
	}
}

func TestToolInputSchemaDirectValidationEnforcesFormatAndEqualTo(t *testing.T) {
	schema := MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{
		"id":      MustToolInputSchema(ToolSchemaString, ToolSchemaFormat("uuid")),
		"created": MustToolInputSchema(ToolSchemaString, ToolSchemaFormat("date-time")),
		"owner":   MustToolInputSchema(ToolSchemaString),
	}), toolSchemaPropertyEqualities(map[string]string{"owner": "id"}), ToolSchemaRequired("id", "created", "owner"))

	valid := map[string]any{
		"id":      "2b12ef3b-2028-45ec-bbb1-cdc2f4a19b1f",
		"created": "2026-07-29T12:30:00Z",
		"owner":   "2b12ef3b-2028-45ec-bbb1-cdc2f4a19b1f",
	}
	if err := schema.Validate(valid); err != nil {
		t.Fatalf("Validate(valid): %v", err)
	}
	for _, tc := range []struct {
		name  string
		field string
		value any
		want  string
	}{
		{name: "uuid", field: "id", value: "not-a-uuid", want: "must be uuid"},
		{name: "date-time", field: "created", value: "tomorrow", want: "must be RFC3339 date-time"},
		{name: "equalTo", field: "owner", value: "7e13a328-c3e2-4dc4-984f-77b74ca99ed1", want: "must equal $.id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := make(map[string]any, len(valid))
			for key, value := range valid {
				candidate[key] = value
			}
			candidate[tc.field] = tc.value
			if err := schema.Validate(candidate); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestValidateToolInputSchemaRejectsCyclesAndExcessiveDepthBeforeProjectionOrClone(t *testing.T) {
	cyclicValue := &toolInputSchemaValue{kind: ToolSchemaArray, hasItems: true}
	cyclic := ToolInputSchema{value: cyclicValue}
	cyclicValue.items = cyclic
	if err := cyclic.ValidateDefinition(); err == nil || !strings.Contains(err.Error(), "schema cycle") {
		t.Fatalf("cyclic schema error = %v", err)
	}
	if _, err := cyclic.Project(); err == nil || !strings.Contains(err.Error(), "schema cycle") {
		t.Fatalf("cyclic projection error = %v", err)
	}

	deep := MustToolInputSchema(ToolSchemaString)
	var err error
	for depth := 0; depth <= MaxToolInputSchemaDepth; depth++ {
		deep, err = NewToolInputSchema(ToolSchemaArray, ToolSchemaItems(deep))
		if err != nil {
			break
		}
	}
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum schema depth") {
		t.Fatalf("deep schema error = %v", err)
	}
}

func TestToolInputSchemaRejectsExplicitEmptyEnum(t *testing.T) {
	var schema ToolInputSchema
	err := yaml.Unmarshal([]byte("type: string\nenum: []\n"), &schema)
	if err == nil || !strings.Contains(err.Error(), "enum must contain at least one value") {
		t.Fatalf("empty enum YAML error = %v", err)
	}
	if _, err := NewToolInputSchema(ToolSchemaString, ToolSchemaEnum()); err == nil || !strings.Contains(err.Error(), "enum must contain at least one value") {
		t.Fatalf("typed empty enum error = %v", err)
	}
}

func TestToolInputSchemaRejectsExplicitNullForEveryKeyword(t *testing.T) {
	tests := []struct {
		name    string
		keyword string
		body    string
	}{
		{name: "type", keyword: "type", body: "type: null\n"},
		{name: "description", keyword: "description", body: "type: string\ndescription: null\n"},
		{name: "properties", keyword: "properties", body: "type: object\nproperties: null\n"},
		{name: "required", keyword: "required", body: "type: object\nproperties: {value: {type: string}}\nrequired: null\n"},
		{name: "items", keyword: "items", body: "type: array\nitems: null\n"},
		{name: "enum", keyword: "enum", body: "type: string\nenum: null\n"},
		{name: "additionalProperties", keyword: "additionalProperties", body: "type: object\nadditionalProperties: null\n"},
		{name: "minimum", keyword: "minimum", body: "type: number\nminimum: null\n"},
		{name: "maximum", keyword: "maximum", body: "type: number\nmaximum: null\n"},
		{name: "pattern", keyword: "pattern", body: "type: string\npattern: null\n"},
		{name: "minLength", keyword: "minLength", body: "type: string\nminLength: null\n"},
		{name: "maxLength", keyword: "maxLength", body: "type: string\nmaxLength: null\n"},
		{name: "minItems", keyword: "minItems", body: "type: array\nitems: {type: string}\nminItems: null\n"},
		{name: "maxItems", keyword: "maxItems", body: "type: array\nitems: {type: string}\nmaxItems: null\n"},
		{name: "nested enum", keyword: "enum", body: "type: object\nproperties:\n  child:\n    type: string\n    enum: null\n"},
	}
	for _, tc := range tests {
		for _, form := range []string{"direct", "alias"} {
			t.Run(tc.name+"/"+form, func(t *testing.T) {
				var schema ToolInputSchema
				var err error
				if form == "direct" {
					err = yaml.Unmarshal([]byte(tc.body), &schema)
				} else {
					aliased := strings.Replace(tc.body, tc.keyword+": null", tc.keyword+": *nil", 1)
					body := "null_anchor: &nil null\nschema:\n  " + strings.ReplaceAll(aliased, "\n", "\n  ")
					var document struct {
						Schema ToolInputSchema `yaml:"schema"`
					}
					err = yaml.Unmarshal([]byte(body), &document)
					schema = document.Schema
				}
				want := fmt.Sprintf("tool schema field %q must not be null", tc.keyword)
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("yaml.Unmarshal error = %v, want %q; schema = %#v", err, want, schema)
				}
			})
		}
	}
}

func TestToolInputSchemaAliasAdmissionIsBoundedAndPreservesNonNullAliases(t *testing.T) {
	var document struct {
		Schema ToolInputSchema `yaml:"schema"`
	}
	if err := yaml.Unmarshal([]byte(`
child_schema: &child
  type: string
  enum: [approved]
schema:
  type: object
  properties:
    child: *child
`), &document); err != nil {
		t.Fatalf("decode non-null schema alias: %v", err)
	}
	child, ok := document.Schema.Property("child")
	values, declared := child.EnumValues()
	if !ok || child.Kind() != ToolSchemaString || !declared || len(values) != 1 {
		t.Fatalf("non-null schema alias = %#v", child)
	}

	cycleA := &yaml.Node{Kind: yaml.AliasNode}
	cycleB := &yaml.Node{Kind: yaml.AliasNode}
	cycleA.Alias = cycleB
	cycleB.Alias = cycleA
	cycleSchema := &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
		{Kind: yaml.ScalarNode, Tag: "!!str", Value: "enum"}, cycleA,
	}}
	var schema ToolInputSchema
	if err := schema.UnmarshalYAML(cycleSchema); err == nil || !strings.Contains(err.Error(), "YAML alias cycle") {
		t.Fatalf("alias-cycle error = %v", err)
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
	projected, err := schema.Project()
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
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

func TestAdmittedToolExecutionContractIsOpaqueAndMutationProof(t *testing.T) {
	stringSchema := MustToolInputSchema(ToolSchemaString, ToolSchemaEnum("approved"))
	properties := map[string]ToolInputSchema{"state": stringSchema}
	required := []string{"state"}
	schema := MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(properties), ToolSchemaRequired(required...))
	properties["state"] = MustToolInputSchema(ToolSchemaBoolean)
	required[0] = "changed"
	snapshot := schema.Properties()
	snapshot["state"] = MustToolInputSchema(ToolSchemaBoolean)
	requiredSnapshot := schema.RequiredProperties()
	requiredSnapshot[0] = "changed"
	state, _ := schema.Property("state")
	if state.Kind() != ToolSchemaString || !schema.IsRequired("state") {
		t.Fatal("schema retained caller or accessor mutation")
	}

	headers := map[string]string{"X-Test": "one"}
	body := map[string]any{"nested": []any{"one"}}
	mapping := map[string]any{"state": "{{response.body.state}}"}
	fields := map[string]CompiledResultField{"state": {From: "result.state"}}
	credentials := []string{"token"}
	entry := MustToolSchemaEntry(
		WithToolHandler(ToolHandlerHTTP),
		WithToolSchemas(schema, schema),
		WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://example.test", Headers: headers, Body: body}),
		WithToolResponseMapping(mapping),
		WithToolResponseSuccess(HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true}),
		WithToolCredentials(credentials...),
		WithToolCompiledResult(CompiledResultProjection{Fields: fields, OutputSchema: schema}),
	)
	headers["X-Test"] = "changed"
	body["nested"].([]any)[0] = "changed"
	mapping["state"] = "changed"
	fields["state"] = CompiledResultField{From: "changed"}
	credentials[0] = "changed"

	httpSpec, _ := entry.HTTP()
	responseMapping, _ := entry.ResponseMapping()
	responseSuccess, _ := entry.ResponseSuccess()
	compiled, _ := entry.CompiledResult()
	entryCredentials := entry.Credentials()
	httpSpec.Headers["X-Test"] = "changed-again"
	responseMapping["state"] = "changed-again"
	compiled.Fields["state"] = CompiledResultField{From: "changed-again"}
	entryCredentials[0] = "changed-again"

	httpSpec, _ = entry.HTTP()
	responseMapping, _ = entry.ResponseMapping()
	responseSuccess, _ = entry.ResponseSuccess()
	compiled, _ = entry.CompiledResult()
	if httpSpec.Headers["X-Test"] != "one" ||
		httpSpec.Body.(map[string]any)["nested"].([]any)[0] != "one" ||
		responseMapping["state"] != "{{response.body.state}}" ||
		responseSuccess.Equals != true ||
		entry.Credentials()[0] != "token" ||
		compiled.Fields["state"].From != "result.state" {
		t.Fatal("admitted tool execution contract leaked mutable authority")
	}
}

func TestAdmittedToolSemanticJSONCarriersRejectCycles(t *testing.T) {
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	_, err := NewToolSchemaEntry(
		WithToolSchemas(MustToolInputSchema(ToolSchemaObject), MustToolInputSchema(ToolSchemaObject)),
		WithToolHTTP(HTTPToolSpec{Method: "POST", URL: "https://example.test", Body: cyclic}),
	)
	if err == nil || !strings.Contains(err.Error(), "http.body") || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cyclic semantic carrier error = %v", err)
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
