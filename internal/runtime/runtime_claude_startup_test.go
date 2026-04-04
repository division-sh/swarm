package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"swarm/internal/config"
	runtimeactors "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/toolcapabilities"
	llm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimemcp "swarm/internal/runtime/mcp"
	"swarm/internal/runtime/semanticview"
	workspace "swarm/internal/runtime/workspace"
)

type claudeStartupWorkspaceStub struct {
	target *workspace.Target
	err    error
}

func (s claudeStartupWorkspaceStub) ResolveWorkspace(context.Context, runtimeactors.AgentConfig) (*workspace.Target, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.target, nil
}

func (s claudeStartupWorkspaceStub) ValidateSource(context.Context, semanticview.Source) error {
	return nil
}
func (s claudeStartupWorkspaceStub) EnsurePrereqs(context.Context) error                 { return nil }
func (s claudeStartupWorkspaceStub) EnsureSystemWorkspaces(context.Context) error        { return nil }
func (s claudeStartupWorkspaceStub) EnsureEntityWorkspace(context.Context, string) error { return nil }
func (s claudeStartupWorkspaceStub) StopEntityWorkspace(context.Context, string) error   { return nil }

func TestValidateClaudeStartupConfig_RequiresWorkspaceAndGateway(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	err := validateClaudeStartupConfig(cfg, RuntimeOptions{})
	if err == nil || !strings.Contains(err.Error(), "workspace lifecycle") {
		t.Fatalf("expected workspace lifecycle error, got %v", err)
	}
}

func TestValidateClaudeManagedAgentWorkspaces_RequiresResolvedContainerTargets(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	err := validateClaudeManagedAgentWorkspaces(context.Background(), cfg, claudeStartupWorkspaceStub{}, manager)
	if err == nil || !strings.Contains(err.Error(), "resolved no container workspace target") {
		t.Fatalf("expected workspace target error, got %v", err)
	}
}

func TestValidateClaudeManagedAgentWorkspaces_PropagatesResolverErrors(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	err := validateClaudeManagedAgentWorkspaces(context.Background(), cfg, claudeStartupWorkspaceStub{err: errors.New("docker unavailable")}, manager)
	if err == nil || !strings.Contains(err.Error(), "docker unavailable") {
		t.Fatalf("expected resolver error, got %v", err)
	}
}

func TestValidateClaudeManagedAgentWorkspaces_AcceptsContainerTargets(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	err := validateClaudeManagedAgentWorkspaces(context.Background(), cfg, claudeStartupWorkspaceStub{
		target: &workspace.Target{Container: "swarm-agent-campaign-coordinator", Workdir: "/workspace"},
	}, manager)
	if err != nil {
		t.Fatalf("validateClaudeManagedAgentWorkspaces: %v", err)
	}
}

type startupProbeToolExecutor struct{}

func (startupProbeToolExecutor) Execute(context.Context, string, any) (any, error) {
	return map[string]any{"ok": true}, nil
}
func (startupProbeToolExecutor) ToolDefinitions() []llm.ToolDefinition {
	return []llm.ToolDefinition{{Name: "query_entities"}}
}

func (startupProbeToolExecutor) ToolDefinitionsForActor(runtimeactors.AgentConfig) []llm.ToolDefinition {
	return []llm.ToolDefinition{{Name: "query_entities"}}
}

func (startupProbeToolExecutor) ToolCapabilitiesForActor(_ runtimeactors.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		caps = append(caps, toolcapabilities.Capability{
			Name:               name,
			Visible:            true,
			Callable:           true,
			ContextRequirement: toolcapabilities.ContextRequirementActorContext,
		})
	}
	return toolcapabilities.NewSet(caps)
}

type emptyStartupProbeToolExecutor struct{}

func (emptyStartupProbeToolExecutor) Execute(context.Context, string, any) (any, error) {
	return map[string]any{"ok": true}, nil
}

func (emptyStartupProbeToolExecutor) ToolDefinitions() []llm.ToolDefinition { return nil }

func (emptyStartupProbeToolExecutor) ToolDefinitionsForActor(runtimeactors.AgentConfig) []llm.ToolDefinition {
	return nil
}

func (emptyStartupProbeToolExecutor) ToolCapabilitiesForActor(_ runtimeactors.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		caps = append(caps, toolcapabilities.Capability{Name: name})
	}
	return toolcapabilities.NewSet(caps)
}

func TestValidateClaudeMCPToolsForManagedAgents_AcceptsVisibleTools(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{
		ID:     "campaign-coordinator",
		Role:   "campaign_coordinator",
		Config: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	gateway := runtimemcp.NewGateway(startupProbeToolExecutor{}, "gateway-token", RuntimeMCPGatewayHooks(nil, manager.GetAgentConfig))

	if err := validateClaudeMCPToolsForManagedAgents(cfg, gateway, "gateway-token", manager); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsWhenRequiredEmitToolsMissing(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{
		ID:     "market-research-agent",
		Role:   "market_research",
		Config: json.RawMessage(`{"emit_events":["discovery/category.assessed","discovery/market_research.scan_complete"]}`),
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	gateway := runtimemcp.NewGateway(startupProbeToolExecutor{}, "gateway-token", RuntimeMCPGatewayHooks(nil, manager.GetAgentConfig))

	err := validateClaudeMCPToolsForManagedAgents(cfg, gateway, "gateway-token", manager)
	if err == nil || !strings.Contains(err.Error(), "missing required emit tools") {
		t.Fatalf("expected missing emit tools error, got %v", err)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsOnEmptyToolList(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	gateway := runtimemcp.NewGateway(emptyStartupProbeToolExecutor{}, "gateway-token", runtimemcp.GatewayHooks{})

	err := validateClaudeMCPToolsForManagedAgents(cfg, gateway, "gateway-token", manager)
	if err == nil || !strings.Contains(err.Error(), "returned no tools") {
		t.Fatalf("expected empty tools error, got %v", err)
	}
}
