package semanticview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func TestResolveAgentRegistryEntryRoleFallbackUsesFlowID(t *testing.T) {
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "support",
			Mode: "singleton",
		},
		Path: "support",
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"flow-responder": {
				ID:   "authored-responder",
				Role: "responder",
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
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
