package tools

import (
	"testing"

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
