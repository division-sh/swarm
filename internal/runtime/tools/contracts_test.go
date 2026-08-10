package tools

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"gopkg.in/yaml.v3"
)

func TestContractDefinitionsForSource_UsesProvidedSource(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"agent_message": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("platform"), runtimecontracts.WithToolDescription("source-backed agent messaging schema"), runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerPlatformBuiltin), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaDescription("source-backed agent messaging schema"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
				"to": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")),
			}), runtimecontracts.ToolSchemaRequired("to")), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
		},
	}

	defs, err := ContractDefinitionsForSource(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("ContractDefinitionsForSource: %v", err)
	}

	for _, def := range defs {
		if def.Name != "agent_message" {
			continue
		}
		if def.Description != "source-backed agent messaging schema" {
			t.Fatalf("agent_message description = %q", def.Description)
		}
		return
	}
	t.Fatal("expected source-backed agent_message definition")
}

func TestContractDefinitionsForSource_AttachesPlatformUsageHints(t *testing.T) {
	defs, err := ContractDefinitionsForSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err != nil {
		t.Fatalf("ContractDefinitionsForSource: %v", err)
	}
	for _, def := range defs {
		if def.Name != "query_entities" {
			continue
		}
		if !strings.Contains(def.Usage, "filter is CEL") {
			t.Fatalf("query_entities usage = %q, want CEL guidance", def.Usage)
		}
		if strings.Contains(def.Description, "Usage:") {
			t.Fatalf("canonical description should not be pre-concatenated: %q", def.Description)
		}
		return
	}
	t.Fatal("expected query_entities definition")
}

func TestContractDefinitionsForSource_DoesNotExposeCreateFlowInstance(t *testing.T) {
	defs, err := ContractDefinitionsForSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	if err != nil {
		t.Fatalf("ContractDefinitionsForSource: %v", err)
	}
	for _, def := range defs {
		if def.Name == "create_flow_instance" {
			t.Fatal("create_flow_instance should not be exposed as an agent tool definition")
		}
	}
}

func TestContractDefinitionsForSource_DoesNotExposeInternalOrRetiredMutationTools(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"configure_routing": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("platform"), runtimecontracts.WithToolDescription("deprecated runtime stub should stay hidden"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("platform_builtin")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
			"agent_hire":        runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("platform"), runtimecontracts.WithToolDescription("retired mutation should stay hidden"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("platform_builtin")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
			"agent_reconfigure": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("platform"), runtimecontracts.WithToolDescription("retired mutation should stay hidden"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("platform_builtin")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
		},
	}

	defs, err := ContractDefinitionsForSource(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("ContractDefinitionsForSource: %v", err)
	}
	for _, def := range defs {
		switch def.Name {
		case "configure_routing", "agent_hire", "agent_reconfigure":
			t.Fatalf("%s should not be exposed as an agent tool definition", def.Name)
		}
	}
}

func TestContractDefinitionsForSource_EmitsCanonicalJSONSchema(t *testing.T) {
	var schema runtimecontracts.ToolInputSchema
	if err := yaml.Unmarshal([]byte(`
type: object
properties:
  mode:
    type: string
    enum: [one, two]
  metadata:
    type: object
    additionalProperties:
      type: string
additionalProperties: false
required: [mode]
`), &schema); err != nil {
		t.Fatalf("unmarshal tool schema: %v", err)
	}

	bundle := &runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"agent_message": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("platform"), runtimecontracts.WithToolDescription("canonical schema test"), runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerPlatformBuiltin), runtimecontracts.WithToolSchemas(schema, runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
		},
	}

	defs, err := ContractDefinitionsForSource(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("ContractDefinitionsForSource: %v", err)
	}

	var schemaMap map[string]any
	for _, def := range defs {
		if def.Name == "agent_message" {
			var ok bool
			schemaMap, ok = def.Schema.(map[string]any)
			if !ok {
				t.Fatalf("agent_message schema type = %T", def.Schema)
			}
			break
		}
	}
	if schemaMap == nil {
		t.Fatal("expected agent_message definition")
	}
	raw := stringify(schemaMap)
	if strings.Contains(raw, "AdditionalProperties") || strings.Contains(raw, "\"Node\"") || strings.Contains(raw, "\"Type\"") {
		t.Fatalf("schema leaked Go/YAML internals: %s", raw)
	}
	if schemaMap["type"] != "object" {
		t.Fatalf("schema type = %#v", schemaMap["type"])
	}
	if schemaMap["additionalProperties"] != false {
		t.Fatalf("additionalProperties = %#v", schemaMap["additionalProperties"])
	}
	props, _ := schemaMap["properties"].(map[string]any)
	mode, _ := props["mode"].(map[string]any)
	enumVals, _ := mode["enum"].([]any)
	if len(enumVals) != 2 || enumVals[0] != "one" || enumVals[1] != "two" {
		t.Fatalf("enum = %#v", mode["enum"])
	}
	metadata, _ := props["metadata"].(map[string]any)
	nested, _ := metadata["additionalProperties"].(map[string]any)
	if nested["type"] != "string" {
		t.Fatalf("nested additionalProperties = %#v", metadata["additionalProperties"])
	}
}

func TestProviderVisibleAndRuntimeToolSchemaPreserveNestedTypedEnumParity(t *testing.T) {
	var schema runtimecontracts.ToolInputSchema
	if err := yaml.Unmarshal([]byte(`
type: object
properties:
  result:
    type: object
    properties:
      approved:
        type: boolean
        enum: [true]
      code:
        type: integer
        enum: [1, 2]
    required: [approved, code]
required: [result]
`), &schema); err != nil {
		t.Fatalf("unmarshal nested enum schema: %v", err)
	}

	providerVisible := schema.Projection()
	runtimeAdmission, err := schema.Project()
	if err != nil {
		t.Fatalf("runtime admission projection: %v", err)
	}
	providerResult := providerVisible["properties"].(map[string]any)["result"].(map[string]any)
	runtimeResult := runtimeAdmission["properties"].(map[string]any)["result"].(map[string]any)
	providerNested := providerResult["properties"].(map[string]any)
	runtimeNested := runtimeResult["properties"].(map[string]any)
	for field, want := range map[string][]any{
		"approved": {true},
		"code":     {float64(1), float64(2)},
	} {
		providerEnum := providerNested[field].(map[string]any)["enum"]
		runtimeEnum := runtimeNested[field].(map[string]any)["enum"]
		if !reflect.DeepEqual(providerEnum, want) || !reflect.DeepEqual(runtimeEnum, want) || !reflect.DeepEqual(providerEnum, runtimeEnum) {
			t.Fatalf("nested %s enum parity: provider=%#v runtime=%#v want=%#v", field, providerEnum, runtimeEnum, want)
		}
	}

	accepted := map[string]any{"result": map[string]any{"approved": true, "code": float64(1)}}
	for name, projected := range map[string]map[string]any{"provider_visible": providerVisible, "runtime_admission": runtimeAdmission} {
		if err := eventschema.ValidatePayloadAgainstSchema(projected, accepted); err != nil {
			t.Fatalf("%s rejected typed nested enum payload: %v", name, err)
		}
		rejected := map[string]any{"result": map[string]any{"approved": false, "code": float64(1)}}
		if err := eventschema.ValidatePayloadAgainstSchema(projected, rejected); err == nil || !strings.Contains(err.Error(), "$.result.approved has invalid enum value false") {
			t.Fatalf("%s enum rejection = %v", name, err)
		}
	}
}

func TestExecutionToolProjectionConsumesExactCanonicalSchemaOwner(t *testing.T) {
	var schema runtimecontracts.ToolInputSchema
	if err := yaml.Unmarshal([]byte(`
type: object
properties:
  state:
    type: string
    pattern: ' approved $'
    enum: [' approved ']
  metadata:
    type: object
    additionalProperties:
      type: integer
required: [state]
additionalProperties: false
`), &schema); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	want, err := schema.Project()
	if err != nil {
		t.Fatalf("schema Project: %v", err)
	}
	execution, included := executionToolFromAdmitted("test.exact", runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolSchemas(schema, runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.test"}),
	))
	if !included {
		t.Fatalf("executionToolFromAdmitted = (%#v, %v)", execution, included)
	}
	gotHash, err := canonicaljson.Hash(execution.InputSchema())
	if err != nil {
		t.Fatalf("hash execution schema: %v", err)
	}
	wantHash, err := canonicaljson.Hash(want)
	if err != nil {
		t.Fatalf("hash canonical owner schema: %v", err)
	}
	if gotHash != wantHash {
		t.Fatalf("execution schema hash = %s, want exact owner hash %s", gotHash, wantHash)
	}

	if _, err := runtimecontracts.NewToolInputSchema(runtimecontracts.ToolSchemaArray); err == nil || !strings.Contains(err.Error(), "requires items") {
		t.Fatalf("malformed admitted schema error = %v", err)
	}
}

func TestRuntimeExecutionViewIsDerivedAndAuthorityFree(t *testing.T) {
	headers := map[string]string{"X-Test": "owner"}
	body := map[string]any{"nested": []any{"owner"}}
	mapping := map[string]any{"value": "{{response.body.value}}"}
	input := runtimecontracts.MustToolInputSchema(
		runtimecontracts.ToolSchemaObject,
		runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
			"value": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString),
		}),
		runtimecontracts.ToolSchemaRequired("value"),
	)
	entry := runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolSchemas(input, input),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
			Method: "POST", URL: "https://example.test", Headers: headers, Body: body,
		}),
		runtimecontracts.WithToolResponseMapping(mapping),
	)
	ownerHash, err := entry.CanonicalHash()
	if err != nil {
		t.Fatalf("owner hash: %v", err)
	}
	execution, included := executionToolFromAdmitted("test.execution", entry)
	if !included {
		t.Fatalf("executionToolFromAdmitted = (%#v, %v)", execution, included)
	}

	headers["X-Test"] = "caller mutation"
	body["nested"].([]any)[0] = "caller mutation"
	mapping["value"] = "caller mutation"
	execution.InputSchema()["properties"].(map[string]any)["value"] = map[string]any{"type": "boolean"}
	httpSnapshot, ok := execution.HTTP()
	if !ok {
		t.Fatal("derived execution view lost HTTP semantics")
	}
	httpSnapshot.Headers["X-Test"] = "readback mutation"
	httpSnapshot.Body.(map[string]any)["nested"].([]any)[0] = "readback mutation"
	execution.ResponseMapping()["value"] = "readback mutation"

	httpSnapshot, _ = execution.HTTP()
	if execution.Handler() != runtimecontracts.ToolHandlerHTTP ||
		httpSnapshot.Headers["X-Test"] != "owner" ||
		httpSnapshot.Body.(map[string]any)["nested"].([]any)[0] != "owner" ||
		execution.ResponseMapping()["value"] != "{{response.body.value}}" {
		t.Fatalf("derived execution view leaked mutation authority: %#v", execution)
	}
	afterHash, err := entry.CanonicalHash()
	if err != nil {
		t.Fatalf("owner hash after readback mutation: %v", err)
	}
	if afterHash != ownerHash {
		t.Fatalf("derived execution view changed admitted owner hash: before=%s after=%s", ownerHash, afterHash)
	}
}

func stringify(v any) string {
	out, _ := yaml.Marshal(v)
	return string(out)
}
