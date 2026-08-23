package runtime

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRuntimeStart_PackageBackedFlowOwnedStaticAgentsCarryCanonicalMemoryIdentity(t *testing.T) {
	source := loadPackageBackedRuntimeAgentMemorySource(t)
	assertRuntimeStartCarriesMemoryIdentity(t, source)
}

func TestRuntimeStart_StructurallyNestedProjectAgentsCarryCanonicalMemoryIdentity(t *testing.T) {
	source := loadNestedProjectRuntimeAgentMemorySource(t)
	assertRuntimeStartCarriesMemoryIdentity(t, source)
}

func assertRuntimeStartCarriesMemoryIdentity(t *testing.T, source semanticview.Source) {
	t.Helper()
	rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: &config.Config{}, Options: RuntimeOptions{
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

	if err := rt.Start(testAuthorActivityContext(context.Background())); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cfg, err := rt.Manager.ResolveAgentConfig("backend", "support")
	if err != nil {
		t.Fatalf("resolve package-backed static flow agent config: %v", err)
	}
	if cfg.FlowPath != "support" {
		t.Fatalf("FlowPath = %q, want support", cfg.FlowPath)
	}
	if cfg.FlowID != "support" {
		t.Fatalf("FlowID = %q, want support", cfg.FlowID)
	}
	if cfg.Memory != agentmemory.Authored(true) {
		t.Fatalf("Memory = %#v, want authored true", cfg.Memory)
	}
}

func loadPackageBackedRuntimeAgentMemorySource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := canonicalrouting.CopyRuntimeAgentMemory(t, canonicalrouting.RuntimeAgentMemoryPackageBacked)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func loadNestedProjectRuntimeAgentMemorySource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := canonicalrouting.CopyRuntimeAgentMemory(t, canonicalrouting.RuntimeAgentMemoryNestedProject)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}
