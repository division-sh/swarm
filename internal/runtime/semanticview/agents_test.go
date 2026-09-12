package semanticview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func TestResolveAgentRegistryEntryRejectsRoleInferencePreservesExactName(t *testing.T) {
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

	source := Wrap(bundle)
	for _, id := range []string{"materialized-responder", "", "flow-responder"} {
		for _, role := range []string{"responder", "RESPONDER", "unrelated"} {
			actor := models.AgentConfig{ID: id, Role: role, FlowID: "support"}
			if _, _, ok := ResolveAgentRegistryEntry(source, actor); ok {
				t.Errorf("unknown or retired name %q borrowed role %q", id, role)
			}
			if _, ok := ResolveAgentContractProjection(source, actor); ok {
				t.Errorf("unknown or retired name %q received contract projection", id)
			}
		}
	}
	logicalID, entry, ok := ResolveAgentRegistryEntry(source, models.AgentConfig{
		ExecutionMode: "live",
		ID:            "authored-responder",
		Role:          "unrelated",
		FlowID:        "support",
	})
	if !ok {
		t.Fatal("ResolveAgentRegistryEntry did not resolve the exact flow-owned public name")
	}
	if logicalID != "flow-responder" || entry.ID != "authored-responder" {
		t.Fatalf("resolved agent = %q/%q, want flow-responder/authored-responder", logicalID, entry.ID)
	}
}
