package runtime_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestNewRuntime_FullyMockedBundleBootsWithoutCredential(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "anthropic"
	t.Setenv("ANTHROPIC_API_KEY", "")

	processOwner := worklifetime.NewProcess()
	t.Cleanup(processOwner.Retire)
	store := storetest.StartSQLiteRuntimeStore(t)
	rt, err := swarmruntime.NewRuntime(testAuthorActivityContext(context.Background()), swarmruntime.RuntimeDeps{
		Config:                   cfg,
		EffectsStore:             store,
		CompletionStore:          store,
		CompletionHeartbeatStore: store,
		LiveSessionAcquirer:      store,
		Options: swarmruntime.RuntimeOptions{
			SelfCheck:         false,
			WorkflowModule:    newRuntimeTestWorkflowModule(t, fullyMockedBootAgentMemorySource(t)),
			BundleSourceFact:  authorActivityTestBundleSourceFact,
			RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
			ProcessWorkOwner:  processOwner,
		},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })
}

func TestNewRuntime_FullyMockedBundleBootsClaudeCLIWithoutCLIBinary(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	t.Setenv("SWARM_CLAUDE_USE_MCP", "")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")

	processOwner := worklifetime.NewProcess()
	t.Cleanup(processOwner.Retire)
	store := storetest.StartSQLiteRuntimeStore(t)
	rt, err := swarmruntime.NewRuntime(testAuthorActivityContext(context.Background()), swarmruntime.RuntimeDeps{
		Config:                   cfg,
		EffectsStore:             store,
		CompletionStore:          store,
		CompletionHeartbeatStore: store,
		LiveSessionAcquirer:      store,
		Options: swarmruntime.RuntimeOptions{
			SelfCheck:         false,
			WorkflowModule:    newRuntimeTestWorkflowModule(t, fullyMockedBootAgentMemorySource(t)),
			BundleSourceFact:  authorActivityTestBundleSourceFact,
			RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
			ProcessWorkOwner:  processOwner,
		},
	})
	if err != nil {
		t.Fatalf("NewRuntime with claude_cli backend: %v", err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })
}

func fullyMockedBootAgentMemorySource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := canonicalrouting.CopyRuntimeAgentMemory(t, canonicalrouting.RuntimeAgentMemoryPackageBacked)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	ids := make([]string, 0, len(bundle.Agents))
	for id := range bundle.Agents {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		t.Fatal("agent memory fixture unexpectedly declares zero agents")
	}
	for id, entry := range bundle.Agents {
		entry.Mock = mockperformance.Performance{
			Kind:   "python",
			Module: "mocks/" + id + ".py",
			Source: []byte("def handle(input):\n    return {}\n"),
			Digest: "sha256:" + strings.Repeat("a", 64),
		}
		bundle.Agents[id] = entry
	}
	return semanticview.Wrap(bundle)
}
