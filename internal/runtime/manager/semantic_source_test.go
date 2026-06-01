package manager

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestDefaultManagerAgentID_UsesInjectedSemanticSource(t *testing.T) {
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "ops", Flow: "ops"},
		Path:  "ops",
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {Role: "worker", ManagerFallback: "control-injected"},
		},
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"ops": &flow,
			},
		},
	})
	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{SemanticSource: source})

	got := am.defaultManagerAgentID(runtimeactors.AgentConfig{ID: "worker-1", Role: "worker"})
	if got != "control-injected" {
		t.Fatalf("defaultManagerAgentID = %q, want control-injected", got)
	}
}

func TestDefaultManagerAgentID_DoesNotUseAmbientWorkflowSemanticSource(t *testing.T) {
	am := NewAgentManager(nil, nil)
	got := am.defaultManagerAgentID(runtimeactors.AgentConfig{ID: "worker-1", Role: "worker"})
	if got != "" {
		t.Fatalf("defaultManagerAgentID = %q, want empty without injected semantic source", got)
	}
}
