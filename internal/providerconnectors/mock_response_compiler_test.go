package providerconnectors

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"gopkg.in/yaml.v3"
)

func TestCompileMockResponsePlanGeneratesEveryEffectiveConnectorDeterministically(t *testing.T) {
	tools := map[string]runtimecontracts.ToolSchemaEntry{}
	for _, installed := range DefaultPackRegistry().Inventory() {
		tools[installed.ToolID] = installed.Tool
	}
	if got := len(tools); got != 7 {
		t.Fatalf("shipped connector tool count = %d, want 7", got)
	}

	flowLocal := telegramConnectorTool("https://example.test")
	flowLocal.OutputSchema = runtimecontracts.ToolInputSchema{
		Type: "object",
		Properties: map[string]runtimecontracts.ToolInputSchema{
			"accepted": {Type: "boolean"},
			"count":    {Type: "integer", Minimum: float64Pointer(2), Maximum: float64Pointer(5)},
			"items":    {Type: "array", Items: &runtimecontracts.ToolInputSchema{Type: "string"}},
			"metadata": {Type: "object"},
			"name": {
				Type: "string",
				Enum: []runtimecontracts.SchemaLiteral{{Node: yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "fixture"}}},
			},
			"nothing":  {Type: "null"},
			"optional": {Type: "string"},
		},
		Required: []string{"accepted", "count", "items", "metadata", "name", "nothing"},
	}
	tools["acme.create"] = flowLocal
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: tools})

	first, err := CompileMockResponsePlan(source)
	if err != nil {
		t.Fatalf("CompileMockResponsePlan first: %v", err)
	}
	second, err := CompileMockResponsePlan(source)
	if err != nil {
		t.Fatalf("CompileMockResponsePlan second: %v", err)
	}
	if len(first.responses) != 8 || len(second.responses) != 8 {
		t.Fatalf("compiled response counts = %d, %d, want 8", len(first.responses), len(second.responses))
	}
	for toolID, firstRaw := range first.responses {
		if !bytes.Equal(firstRaw, second.responses[toolID]) {
			t.Fatalf("response %q is not byte-deterministic: first=%s second=%s", toolID, firstRaw, second.responses[toolID])
		}
		admitted, admitErr := first.Admit(toolID, tools[toolID])
		if admitErr != nil {
			t.Fatalf("Admit(%s): %v", toolID, admitErr)
		}
		if _, materializeErr := admitted.Materialize(); materializeErr != nil {
			t.Fatalf("Materialize(%s): %v", toolID, materializeErr)
		}
	}

	admitted, err := first.Admit("acme.create", flowLocal)
	if err != nil {
		t.Fatalf("Admit flow-local response: %v", err)
	}
	got, err := admitted.Materialize()
	if err != nil {
		t.Fatalf("Materialize flow-local response: %v", err)
	}
	want := map[string]any{
		"accepted": false,
		"count":    float64(2),
		"items":    []any{},
		"metadata": map[string]any{},
		"name":     "fixture",
		"nothing":  nil,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("flow-local generated response = %#v, want %#v", got, want)
	}
	if _, exists := got["optional"]; exists {
		t.Fatalf("flow-local generated response invented optional field: %#v", got)
	}
}

func TestCompileMockResponsePlanFailsClosedWithExactSchemaPath(t *testing.T) {
	var explicitEmptyEnum runtimecontracts.ToolInputSchema
	if err := yaml.Unmarshal([]byte(`
type: object
properties:
  status:
    type: string
    enum: []
required: [status]
`), &explicitEmptyEnum); err != nil {
		t.Fatalf("unmarshal explicit empty enum schema: %v", err)
	}
	if explicitEmptyEnum.Properties["status"].Enum == nil {
		t.Fatal("explicit empty enum lost authored presence")
	}
	decodeSchema := func(raw string) runtimecontracts.ToolInputSchema {
		t.Helper()
		var schema runtimecontracts.ToolInputSchema
		if err := yaml.Unmarshal([]byte(raw), &schema); err != nil {
			t.Fatalf("unmarshal enum schema: %v", err)
		}
		return schema
	}
	timestampEnum := decodeSchema(`
type: object
properties:
  value: {type: string, enum: [2026-07-16T12:00:00Z]}
required: [value]
`)
	binaryEnum := decodeSchema(`
type: object
properties:
  value: {type: string, enum: [!!binary aGVsbG8=]}
required: [value]
`)
	malformedLiteralSchema := func(node yaml.Node) runtimecontracts.ToolInputSchema {
		return runtimecontracts.ToolInputSchema{
			Type: "object",
			Properties: map[string]runtimecontracts.ToolInputSchema{
				"value": {Type: "string", Enum: []runtimecontracts.SchemaLiteral{{Node: node}}},
			},
			Required: []string{"value"},
		}
	}

	tests := []struct {
		name   string
		schema runtimecontracts.ToolInputSchema
		want   string
	}{
		{
			name:   "root must be object",
			schema: runtimecontracts.ToolInputSchema{Type: "string"},
			want:   "output_schema: provider connector mock response root must be object",
		},
		{
			name:   "required property must be declared",
			schema: runtimecontracts.ToolInputSchema{Type: "object", Required: []string{"missing"}},
			want:   "output_schema.properties.missing: required property has no declared schema",
		},
		{
			name: "unsupported nested type",
			schema: runtimecontracts.ToolInputSchema{
				Type:       "object",
				Properties: map[string]runtimecontracts.ToolInputSchema{"value": {Type: "date"}},
				Required:   []string{"value"},
			},
			want: `output_schema.properties.value: unsupported schema type "date"`,
		},
		{
			name: "contradictory bounds",
			schema: runtimecontracts.ToolInputSchema{
				Type:       "object",
				Properties: map[string]runtimecontracts.ToolInputSchema{"value": {Type: "number", Minimum: float64Pointer(5), Maximum: float64Pointer(2)}},
				Required:   []string{"value"},
			},
			want: "output_schema.properties.value: minimum 5 exceeds maximum 2",
		},
		{
			name: "integer interval has no inhabitant",
			schema: runtimecontracts.ToolInputSchema{
				Type:       "object",
				Properties: map[string]runtimecontracts.ToolInputSchema{"value": {Type: "integer", Minimum: float64Pointer(0.2), Maximum: float64Pointer(0.8)}},
				Required:   []string{"value"},
			},
			want: "output_schema.properties.value: bounds contain no integer",
		},
		{
			name:   "explicit empty enum has no inhabitant",
			schema: explicitEmptyEnum,
			want:   "output_schema.properties.status.enum: explicitly declared enum must contain at least one value",
		},
		{
			name:   "YAML timestamp enum is not coerced",
			schema: timestampEnum,
			want:   `output_schema.properties.value.enum[0]: YAML scalar tag "!!timestamp" is not a JSON value`,
		},
		{
			name:   "YAML binary enum is not coerced",
			schema: binaryEnum,
			want:   `output_schema.properties.value.enum[0]: YAML scalar tag "!!binary" is not a JSON value`,
		},
		{
			name:   "missing typed enum literal is rejected",
			schema: malformedLiteralSchema(yaml.Node{}),
			want:   "output_schema.properties.value.enum[0]: literal node is missing",
		},
		{
			name: "invalid UTF-8 enum string is rejected",
			schema: malformedLiteralSchema(yaml.Node{
				Kind:  yaml.ScalarNode,
				Tag:   "!!str",
				Value: string([]byte{0xff}),
			}),
			want: "output_schema.properties.value.enum[0]: string literal is not valid UTF-8",
		},
		{
			name: "unsafe enum integer is rejected",
			schema: malformedLiteralSchema(yaml.Node{
				Kind:  yaml.ScalarNode,
				Tag:   "!!int",
				Value: "9007199254740992",
			}),
			want: "output_schema.properties.value.enum[0]",
		},
		{
			name: "non-finite enum number is rejected",
			schema: malformedLiteralSchema(yaml.Node{
				Kind:  yaml.ScalarNode,
				Tag:   "!!float",
				Value: ".nan",
			}),
			want: "output_schema.properties.value.enum[0]",
		},
		{
			name: "non-string enum object key is rejected",
			schema: malformedLiteralSchema(yaml.Node{
				Kind: yaml.MappingNode,
				Tag:  "!!map",
				Content: []*yaml.Node{
					{Kind: yaml.ScalarNode, Tag: "!!int", Value: "1"},
					{Kind: yaml.ScalarNode, Tag: "!!str", Value: "value"},
				},
			}),
			want: "output_schema.properties.value.enum[0]: object key[0] must be a JSON string",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tool := telegramConnectorTool("https://example.test")
			tool.OutputSchema = tc.schema
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"acme.create": tool}})
			plan, err := CompileMockResponsePlan(source)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CompileMockResponsePlan plan=%#v error=%v, want containing %q", plan, err, tc.want)
			}
			if plan != nil {
				t.Fatalf("CompileMockResponsePlan returned partial plan %#v", plan)
			}
		})
	}
}

func TestCompileMockResponsePlanReturnsNoAmbientPlan(t *testing.T) {
	plan, err := CompileMockResponsePlan(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err != nil {
		t.Fatalf("CompileMockResponsePlan: %v", err)
	}
	if plan != nil {
		t.Fatalf("CompileMockResponsePlan without effective connectors = %#v, want nil", plan)
	}
}

func float64Pointer(value float64) *float64 {
	return &value
}
