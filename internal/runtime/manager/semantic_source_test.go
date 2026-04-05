package manager

import (
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

type managerSemanticSourceTestModule struct {
	source semanticview.Source
}

func (m managerSemanticSourceTestModule) SemanticSource() semanticview.Source {
	return m.source
}

func (managerSemanticSourceTestModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return nil
}

func (managerSemanticSourceTestModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return nil
}

func (managerSemanticSourceTestModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return nil
}

func (managerSemanticSourceTestModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return nil
}

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
	previous := runtimepipeline.DefaultWorkflowModuleOrNil()
	runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule {
		flow := runtimecontracts.FlowContractView{
			Paths: runtimecontracts.FlowContractPaths{ID: "ops", Flow: "ops"},
			Path:  "ops",
			Agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {Role: "worker", ManagerFallback: "control-ambient"},
			},
		}
		return managerSemanticSourceTestModule{
			source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				FlowTree: runtimecontracts.FlowTree{
					Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}},
					ByID: map[string]*runtimecontracts.FlowContractView{
						"ops": &flow,
					},
				},
			}),
		}
	})
	t.Cleanup(func() {
		if previous == nil {
			runtimepipeline.SetDefaultWorkflowModuleFactory(nil)
			return
		}
		runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule { return previous })
	})

	am := NewAgentManager(nil, nil)
	got := am.defaultManagerAgentID(runtimeactors.AgentConfig{ID: "worker-1", Role: "worker"})
	if got != "" {
		t.Fatalf("defaultManagerAgentID = %q, want empty without injected semantic source", got)
	}
}
