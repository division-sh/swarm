package runtime_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packadmission"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestNewRuntime_FullyMockedBundleBootsWithoutCredential(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{ExecutionPosture: executionposture.MockOnly}}
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
	cfg := &config.Config{Runtime: config.RuntimeConfig{ExecutionPosture: executionposture.MockOnly}}
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

func TestNewRuntime_FullyMockedBundleBootsWithoutUnreachableConnectorCredential(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{ExecutionPosture: executionposture.MockOnly}}
	cfg.LLM.Backend = "claude_cli"
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

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
			WorkflowModule:    newRuntimeTestWorkflowModule(t, fullyMockedBootAgentMemoryWithConnectorSource(t)),
			Credentials:       credentialStore,
			BundleSourceFact:  authorActivityTestBundleSourceFact,
			RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
			ProcessWorkOwner:  processOwner,
		},
	})
	if err != nil {
		t.Fatalf("NewRuntime without unreachable connector credential: %v", err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })
}

func fullyMockedBootAgentMemorySource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := canonicalrouting.CopyRuntimeAgentMemory(t, canonicalrouting.RuntimeAgentMemoryPackageBacked)
	// This boot proof needs one declaration owner; package composition is
	// covered by the contracts loader parity tests.
	if err := os.Remove(filepath.Join(root, "flows", "support", "package.yaml")); err != nil {
		t.Fatalf("remove overlapping nested package declaration: %v", err)
	}
	agentsPath := filepath.Join(root, "flows", "support", "agents.yaml")
	agents, err := os.ReadFile(agentsPath)
	if err != nil {
		t.Fatalf("read package-backed agents: %v", err)
	}
	agents = append(agents, []byte("  mock:\n    kind: python\n    module: mocks/backend.py\n")...)
	if err := os.WriteFile(agentsPath, agents, 0o644); err != nil {
		t.Fatalf("write package-backed mocked agents: %v", err)
	}
	mockPath := filepath.Join(root, "mocks", "backend.py")
	if err := os.MkdirAll(filepath.Dir(mockPath), 0o755); err != nil {
		t.Fatalf("create mock module directory: %v", err)
	}
	if err := os.WriteFile(mockPath, []byte("def handle(input):\n    return {'text': 'mock'}\n"), 0o644); err != nil {
		t.Fatalf("write mock module: %v", err)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(
		repoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
		runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory},
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	if len(bundle.Agents) == 0 {
		t.Fatal("agent memory fixture unexpectedly declares zero agents")
	}
	return semanticview.Wrap(bundle)
}

func fullyMockedBootAgentMemoryWithConnectorSource(t *testing.T) semanticview.Source {
	t.Helper()
	objectSchema := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
	connector := runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolCategory("provider_connector"),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassNonIdempotentWrite))),
		runtimecontracts.WithToolSchemas(objectSchema, objectSchema),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://provider.example/messages"}),
		runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"}),
		runtimecontracts.WithToolCredentials("provider_credential"),
	)
	source, err := semanticview.WithRuntimeTools(fullyMockedBootAgentMemorySource(t), map[string]runtimecontracts.ToolSchemaEntry{"provider.send": connector})
	if err != nil {
		t.Fatalf("WithRuntimeTools: %v", err)
	}
	return source
}
