package semanticview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func TestResolveAgentRegistryEntryRoleFallbackUsesFlowID(t *testing.T) {
	const owner = "test://semanticview/support/agents/flow-responder"
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			FlowPath: "support",
		},
		Path:   "support",
		Schema: runtimecontracts.FlowSchemaDocument{Mode: "singleton"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"flow-responder": {
				ID:   "authored-responder",
				Role: "responder",
			},
		},
		AgentURIs: map[string]string{"flow-responder": owner},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{owner: {Kind: "agent", FlowID: "support", LocalID: "flow-responder", Full: owner}},
			ByURI:  map[string]runtimecontracts.ContractURIRef{owner: {Kind: "agent", FlowID: "support", LocalID: "flow-responder", Full: owner}},
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &flow,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"support": &flow,
			},
		},
	}

	logicalID, entry, ok := ResolveAgentRegistryEntry(Wrap(bundle), models.AgentConfig{
		ExecutionMode: "live",
		ID:            "materialized-responder",
		Role:          "responder",
		FlowID:        "support",
	})
	if !ok {
		t.Fatal("ResolveAgentRegistryEntry did not resolve the flow-owned role fallback")
	}
	if logicalID != "flow-responder" || entry.ID != "authored-responder" {
		t.Fatalf("resolved agent = %q/%q, want flow-responder/authored-responder", logicalID, entry.ID)
	}
}
