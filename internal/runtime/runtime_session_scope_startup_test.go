package runtime

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRuntimeStart_PackageBackedFlowOwnedStaticAgentsCarryCanonicalFlowPath(t *testing.T) {
	source := loadPackageBackedRuntimeSessionScopeSource(t)
	assertRuntimeStartCarriesFlowPath(t, source)
}

func TestRuntimeStart_SoleParentFlowPackageAgentsCarryCanonicalFlowPath(t *testing.T) {
	source := loadSoleParentFlowRuntimeSessionScopeSource(t)
	assertRuntimeStartCarriesFlowPath(t, source)
}

func assertRuntimeStartCarriesFlowPath(t *testing.T, source semanticview.Source) {
	t.Helper()
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{}, Stores: Stores{}, Options: RuntimeOptions{
		SelfCheck:      false,
		LLMRuntime:     noopLLMRuntime{},
		WorkflowModule: semanticOnlyWorkflowRuntime{source: source},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Shutdown()
	})

	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cfg, ok := rt.Manager.GetAgentConfig("backend-{vertical_id}")
	if !ok {
		t.Fatal("expected package-backed static flow agent config")
	}
	if cfg.FlowPath != "support" {
		t.Fatalf("FlowPath = %q, want support", cfg.FlowPath)
	}
	if cfg.Mode != "support" {
		t.Fatalf("Mode = %q, want support", cfg.Mode)
	}
}

func loadPackageBackedRuntimeSessionScopeSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := canonicalrouting.CopyRuntimeSessionScope(t, canonicalrouting.RuntimeSessionScopePackageBacked)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func loadSoleParentFlowRuntimeSessionScopeSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := canonicalrouting.CopyRuntimeSessionScope(t, canonicalrouting.RuntimeSessionScopeSoleParent)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}
