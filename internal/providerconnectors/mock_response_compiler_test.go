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
		if installed.Tool.Category() != runtimecontracts.ToolCategoryProviderConnector {
			continue
		}
		tools[installed.ToolID] = installed.Tool
	}
	if got := len(tools); got != 10 {
		t.Fatalf("shipped connector tool count = %d, want 10", got)
	}

	flowLocal := withOutputSchema(t, telegramConnectorTool("https://example.test"), runtimecontracts.MustToolInputSchema(
		runtimecontracts.ToolSchemaObject,
		runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
			"accepted": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaBoolean),
			"count": runtimecontracts.MustToolInputSchema(
				runtimecontracts.ToolSchemaInteger,
				runtimecontracts.ToolSchemaMinimum(2),
				runtimecontracts.ToolSchemaMaximum(5),
			),
			"items": runtimecontracts.MustToolInputSchema(
				runtimecontracts.ToolSchemaArray,
				runtimecontracts.ToolSchemaItems(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString)),
				runtimecontracts.ToolSchemaMinItems(2),
			),
			"metadata": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
			"name": runtimecontracts.MustToolInputSchema(
				runtimecontracts.ToolSchemaString,
				runtimecontracts.ToolSchemaEnum("zeta", "alpha"),
			),
			"nothing":  runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaNull),
			"optional": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString),
		}),
		runtimecontracts.ToolSchemaRequired("accepted", "count", "items", "metadata", "name", "nothing"),
	))
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
	if len(first.responses) != 11 || len(second.responses) != 11 {
		t.Fatalf("compiled response counts = %d, %d, want 11", len(first.responses), len(second.responses))
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
	addLabelsResponse, err := first.Admit("github.add_labels_to_issue", tools["github.add_labels_to_issue"])
	if err != nil {
		t.Fatalf("Admit(github.add_labels_to_issue): %v", err)
	}
	addLabelsValue, err := addLabelsResponse.Materialize()
	if err != nil {
		t.Fatalf("Materialize(github.add_labels_to_issue): %v", err)
	}
	if labels, ok := addLabelsValue.([]any); !ok || len(labels) != 0 {
		t.Fatalf("generated GitHub add-labels response = %#v, want canonical empty array", addLabelsValue)
	}

	admitted, err := first.Admit("acme.create", flowLocal)
	if err != nil {
		t.Fatalf("Admit flow-local response: %v", err)
	}
	materialized, err := admitted.Materialize()
	if err != nil {
		t.Fatalf("Materialize flow-local response: %v", err)
	}
	got, ok := materialized.(map[string]any)
	if !ok {
		t.Fatalf("flow-local generated response = %T, want object", materialized)
	}
	want := map[string]any{
		"accepted": false,
		"count":    float64(2),
		"items":    []any{"", ""},
		"metadata": map[string]any{},
		"name":     "zeta",
		"nothing":  nil,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("flow-local generated response = %#v, want %#v", got, want)
	}
	if _, exists := got["optional"]; exists {
		t.Fatalf("flow-local generated response invented optional field: %#v", got)
	}
}

func TestCompileMockResponsePlanPreservesStructuredEnumKinds(t *testing.T) {
	var outputSchema runtimecontracts.ToolInputSchema
	if err := yaml.Unmarshal([]byte(`
type: object
properties:
  value:
    type: object
    additionalProperties: false
    properties:
      null_value: {type: 'null'}
      bool_value: {type: boolean}
      int_value: {type: integer}
      float_value: {type: number}
      text_value: {type: string}
      list_value: {type: array, items: {type: boolean}}
      object_value:
        type: object
        additionalProperties: false
        properties: {key: {type: boolean}}
        required: [key]
    required: [null_value, bool_value, int_value, float_value, text_value, list_value, object_value]
    enum:
      - null_value: !!null null
        bool_value: !!bool true
        int_value: !!int 1
        float_value: !!float 1.5
        text_value: text
        list_value: [false]
        object_value: {key: true}
required: [value]
`), &outputSchema); err != nil {
		t.Fatalf("unmarshal structured enum schema: %v", err)
	}
	tool := withOutputSchema(t, telegramConnectorTool("https://example.test"), outputSchema)
	plan, err := CompileMockResponsePlan(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{"acme.create": tool},
	}))
	if err != nil {
		t.Fatalf("CompileMockResponsePlan: %v", err)
	}
	admitted, err := plan.Admit("acme.create", tool)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	materialized, err := admitted.Materialize()
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	got, ok := materialized.(map[string]any)
	if !ok {
		t.Fatalf("generated structured response = %T, want object", materialized)
	}
	want := map[string]any{
		"value": map[string]any{
			"null_value": nil, "bool_value": true, "int_value": float64(1), "float_value": 1.5,
			"text_value": "text", "list_value": []any{false}, "object_value": map[string]any{"key": true},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("structured enum response = %#v, want %#v", got, want)
	}
}

func TestCompileMockResponsePlanFailsClosedWithExactSchemaPath(t *testing.T) {
	tests := []struct {
		name   string
		schema runtimecontracts.ToolInputSchema
		want   string
	}{
		{
			name:   "uninhabited any schema",
			schema: runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaAny),
			want:   "output_schema: $: schema has no deterministic inhabitant type",
		},
		{
			name: "integer interval has no inhabitant",
			schema: runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject, runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
				"value": runtimecontracts.MustToolInputSchema(
					runtimecontracts.ToolSchemaInteger,
					runtimecontracts.ToolSchemaMinimum(0.2),
					runtimecontracts.ToolSchemaMaximum(0.8),
				),
			}), runtimecontracts.ToolSchemaRequired("value")),
			want: "output_schema: $.properties[value]: numeric bounds contain no integer inhabitant",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tool := withOutputSchema(t, telegramConnectorTool("https://example.test"), tc.schema)
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Tools: map[string]runtimecontracts.ToolSchemaEntry{"acme.create": tool},
			})
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

func withOutputSchema(t *testing.T, tool runtimecontracts.ToolSchemaEntry, output runtimecontracts.ToolInputSchema) runtimecontracts.ToolSchemaEntry {
	t.Helper()
	updated, err := tool.WithSchemas(tool.InputSchema(), output)
	if err != nil {
		t.Fatalf("replace tool output schema: %v", err)
	}
	return updated
}
