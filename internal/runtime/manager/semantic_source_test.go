package manager

import (
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestDefaultManagerAgentID_UsesInjectedSemanticSource(t *testing.T) {
	root := filepath.Join(t.TempDir(), "manager-source")
	for _, flow := range []string{".", "ops"} {
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow, "schema.yaml"), "stages: []\n")
	}
	writeFlowActivationFixtureFile(t, filepath.Join(root, "ops", "agents.yaml"), "worker:\n  role: worker\n  intent: {inline: handle work}\n  manager_fallback: control-injected\n")
	repo := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	am := newTestAgentManagerWithOptions(t, nil, nil, AgentManagerOptions{SemanticSource: source})

	got := am.defaultManagerAgentID(runtimeactors.AgentConfig{ExecutionMode: "live", ID: "worker", Role: "unrelated", FlowID: "ops"})
	if got != "control-injected" {
		t.Fatalf("defaultManagerAgentID = %q, want control-injected", got)
	}
	for _, id := range []string{"worker-1", ""} {
		actor := runtimeactors.AgentConfig{ID: id, Role: "worker", FlowID: "ops"}
		if got := am.defaultManagerAgentID(actor); got != "" {
			t.Errorf("unknown actor %q borrowed manager %q", id, got)
		}
		actor.ManagerFallback = "explicit-manager"
		if got := am.defaultManagerAgentID(actor); got != "explicit-manager" {
			t.Errorf("independent explicit configuration lost: %q", got)
		}
	}
}

func TestDefaultManagerAgentID_DoesNotUseAmbientWorkflowSemanticSource(t *testing.T) {
	am := newTestAgentManager(t, nil, nil)
	got := am.defaultManagerAgentID(runtimeactors.AgentConfig{ExecutionMode: "live", ID: "worker-1", Role: "worker"})
	if got != "" {
		t.Fatalf("defaultManagerAgentID = %q, want empty without injected semantic source", got)
	}
}
