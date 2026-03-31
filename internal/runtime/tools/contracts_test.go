package tools

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

func TestContractDefinitionsForSource_UsesProvidedSource(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"agent_message": {
				Category:    "platform",
				Description: "source-backed agent messaging schema",
				InputSchema: runtimecontracts.ToolInputSchema{
					Type:        "object",
					Description: "source-backed agent messaging schema",
					Required:    []string{"to"},
					Properties: map[string]runtimecontracts.ToolInputSchema{
						"to": {
							Type: "string",
						},
					},
				},
			},
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

func TestContractDefinitionsForSource_DoesNotExposeConfigureRouting(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"configure_routing": {
				HandlerType: "platform_builtin",
				Category:    "platform",
				Description: "deprecated runtime stub should stay hidden",
			},
		},
	}

	defs, err := ContractDefinitionsForSource(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("ContractDefinitionsForSource: %v", err)
	}
	for _, def := range defs {
		if def.Name == "configure_routing" {
			t.Fatal("configure_routing should not be exposed as an agent tool definition")
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
			"agent_message": {
				Category:    "platform",
				Description: "canonical schema test",
				InputSchema: schema,
			},
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

func stringify(v any) string {
	out, _ := yaml.Marshal(v)
	return string(out)
}
