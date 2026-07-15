package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestRuntimeStart_AgentFreeCLITestDoesNotRequireClaudeStartupEnv(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	t.Setenv("SWARM_CLAUDE_USE_MCP", "")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")
	module := loadAgentFreeRuntimeWorkflowModule(t)
	if got := len(module.SemanticSource().AgentEntries()); got != 0 {
		t.Fatalf("agent-free fixture has %d semantic agents", got)
	}

	rt, err := newScopedTestRuntime(testAuthorActivityContext(context.Background()), RuntimeDeps{Config: cfg, Stores: Stores{}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })

	if err := rt.Start(testAuthorActivityContext(context.Background())); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestNewRuntimeRejectsRetiredLLMRuntimeMode(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"

	_, err := newScopedTestRuntime(testAuthorActivityContext(context.Background()), RuntimeDeps{Config: cfg, Stores: Stores{}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: loadAgentFreeRuntimeWorkflowModule(t),
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err == nil || !strings.Contains(err.Error(), "llm.runtime_mode is retired") {
		t.Fatalf("NewRuntime error = %v, want retired runtime mode rejection", err)
	}
}

func TestNewRuntime_AgentPresentRequiresSelectedBackendCredential(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "anthropic"
	t.Setenv("ANTHROPIC_API_KEY", "")
	module := semanticOnlyWorkflowRuntime{source: loadPackageBackedRuntimeAgentMemorySource(t)}

	_, err := newScopedTestRuntime(testAuthorActivityContext(context.Background()), RuntimeDeps{Config: cfg, Stores: Stores{}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
	}})

	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != runtimefailures.ClassAuthenticationNeeded || failure.Failure.Detail.Code != "provider_credential_missing" {
		t.Fatalf("NewRuntime error = %v, want selected backend credential failure", err)
	}
}

func TestNewRuntime_AgentPresentCLITestStillRequiresClaudeStartupEnv(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	t.Setenv("SWARM_CLAUDE_USE_MCP", "")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")
	module := semanticOnlyWorkflowRuntime{source: loadPackageBackedRuntimeAgentMemorySource(t)}
	if got := len(module.SemanticSource().AgentEntries()); got == 0 {
		t.Fatal("agent-present fixture unexpectedly has zero semantic agents")
	}

	_, err := newScopedTestRuntime(testAuthorActivityContext(context.Background()), RuntimeDeps{Config: cfg, Stores: Stores{}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err == nil {
		t.Fatal("expected agent-present cli_test runtime to require Claude startup env")
	}
	if !strings.Contains(err.Error(), "claude runtime startup validation failed") {
		t.Fatalf("NewRuntime error = %v, want claude startup validation failure", err)
	}
}

func TestRuntimeStart_ActiveManagerAgentRequiresFullClaudeStartupBinding(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")

	rt, err := newScopedTestRuntime(testAuthorActivityContext(context.Background()), RuntimeDeps{Config: cfg, Stores: Stores{}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: loadAgentFreeRuntimeWorkflowModule(t),
		LLMRuntime:     noopLLMRuntime{},
		WorkspaceLifecycle: claudeStartupWorkspaceStub{
			target: &workspace.Target{Container: "swarm-agent-recovered-agent", Workdir: "/workspace"},
		},
		EnableToolGateway:  true,
		ToolGatewayBinding: testToolGatewayBinding("http://127.0.0.1:8081", "", "gateway-token"),
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })
	if err := rt.Manager.SpawnAgent(runtimeactors.AgentConfig{ExecutionMode: "live", ID: "recovered-agent", Role: "recovered", Model: "regular", Config: json.RawMessage(`{"system_prompt":"Recovered agent"}`)}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	err = rt.Start(testAuthorActivityContext(context.Background()))
	if err == nil || !strings.Contains(err.Error(), "claude runtime startup validation failed") || !strings.Contains(err.Error(), "tool gateway binding workspace endpoint") {
		t.Fatalf("Start error = %v, want startup validation failure for missing workspace gateway binding", err)
	}
}

func TestNewRuntimeToolGatewayRequiresBindingToken(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"

	_, err := newScopedTestRuntime(testAuthorActivityContext(context.Background()), RuntimeDeps{Config: cfg, Stores: Stores{}, Options: RuntimeOptions{
		SelfCheck:          false,
		WorkflowModule:     loadAgentFreeRuntimeWorkflowModule(t),
		LLMRuntime:         noopLLMRuntime{},
		EnableToolGateway:  true,
		ToolGatewayBinding: testToolGatewayBinding("http://127.0.0.1:8081", "http://host.docker.internal:8081", ""),
	}})
	if err == nil || !strings.Contains(err.Error(), "tool gateway binding token is required") {
		t.Fatalf("NewRuntime error = %v, want missing binding token rejection", err)
	}
}

func loadAgentFreeRuntimeWorkflowModule(t *testing.T) semanticOnlyWorkflowRuntime {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()
	writeAgentFreeRuntimeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: agent-free-runtime
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeAgentFreeRuntimeFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: agent-free-runtime
initial_state: idle
states:
  - idle
terminal_states:
  - idle
`)
	writeAgentFreeRuntimeFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeAgentFreeRuntimeFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeAgentFreeRuntimeFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeAgentFreeRuntimeFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticOnlyWorkflowRuntime{source: semanticview.Wrap(bundle)}
}

func writeAgentFreeRuntimeFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
