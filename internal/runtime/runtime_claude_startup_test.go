package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"net/url"
	"slices"
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

type startupProbeToolExecutor struct {
	defs     []llm.ToolDefinition
	caps     map[string]toolcapabilities.Capability
	executed []string
	execErrs map[string]error
}

func (s *startupProbeToolExecutor) Execute(_ context.Context, name string, _ any) (any, error) {
	name = strings.TrimSpace(name)
	s.executed = append(s.executed, name)
	if err, ok := s.execErrs[name]; ok {
		return nil, err
	}
	return map[string]any{"ok": true, "tool": name}, nil
}

func (s *startupProbeToolExecutor) ToolDefinitions() []llm.ToolDefinition {
	return append([]llm.ToolDefinition(nil), s.defs...)
}

func (s *startupProbeToolExecutor) ToolDefinitionsForActor(runtimeactors.AgentConfig) []llm.ToolDefinition {
	return append([]llm.ToolDefinition(nil), s.defs...)
}

func (s *startupProbeToolExecutor) ToolCapabilitiesForActor(_ runtimeactors.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		capability, ok := s.caps[strings.TrimSpace(name)]
		if !ok {
			capability = toolcapabilities.Capability{
				Name:               strings.TrimSpace(name),
				Visible:            true,
				Callable:           true,
				ContextRequirement: toolcapabilities.ContextRequirementActorContext,
			}
		}
		caps = append(caps, capability)
	}
	return toolcapabilities.NewSet(caps)
}

func startupProbeDefs() []llm.ToolDefinition {
	return []llm.ToolDefinition{
		{
			Name:        "hidden_tool",
			Description: "Hidden from filtered MCP catalog",
			Schema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"query": map[string]any{"type": "string"}},
				"required":   []any{"query"},
			},
		},
		{
			Name:        "query_entities",
			Description: "Query entities",
			Schema: map[string]any{
				"type":       "object",
				"properties": map[string]any{"query": map[string]any{"type": "string"}},
				"required":   []any{"query"},
			},
		},
		{
			Name:        "health_check",
			Description: "No-input reachability smoke",
			Schema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
	}
}

func startupProbeCaps() map[string]toolcapabilities.Capability {
	return map[string]toolcapabilities.Capability{
		"hidden_tool": {
			Name:               "hidden_tool",
			Visible:            false,
			Callable:           true,
			ContextRequirement: toolcapabilities.ContextRequirementActorContext,
		},
		"query_entities": {
			Name:               "query_entities",
			Visible:            true,
			Callable:           true,
			ContextRequirement: toolcapabilities.ContextRequirementActorContext,
		},
		"health_check": {
			Name:               "health_check",
			Visible:            true,
			Callable:           true,
			ContextRequirement: toolcapabilities.ContextRequirementActorContext,
		},
	}
}

func setupStartupProbeTransport(t *testing.T, manager *runtimemanager.AgentManager, exec *startupProbeToolExecutor, gatewayToken string) *runtimemcp.TurnContextRegistry {
	t.Helper()
	turns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	gateway := runtimemcp.NewGateway(exec, gatewayToken, RuntimeMCPGatewayHooks(nil, manager.GetAgentConfig, nil, nil, turns))
	server := httptest.NewServer(gateway.Handler())
	t.Cleanup(server.Close)
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", server.URL)
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", gatewayToken)
	return turns
}

func TestValidateClaudeMCPToolsForManagedAgents_UsesRealFilteredTransport(t *testing.T) {
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
	exec := &startupProbeToolExecutor{
		defs: startupProbeDefs(),
		caps: startupProbeCaps(),
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")

	if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, turns, exec, manager); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
	if !slices.Equal(exec.executed, []string{"health_check"}) {
		t.Fatalf("executed = %#v, want health_check tools/call smoke", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_RewritesContainerOnlyGatewayAliasForHostProbe(t *testing.T) {
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
	exec := &startupProbeToolExecutor{
		defs: startupProbeDefs(),
		caps: startupProbeCaps(),
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")

	serverURL, err := url.Parse(strings.TrimSpace(llm.RuntimeMCPGatewayURLForHostExecution()))
	if err != nil {
		t.Fatalf("parse host gateway url: %v", err)
	}
	serverURL.Host = "host.docker.internal:" + serverURL.Port()
	t.Setenv("SWARM_TOOL_GATEWAY_URL", serverURL.String())

	if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, turns, exec, manager); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
	if !slices.Equal(exec.executed, []string{"health_check"}) {
		t.Fatalf("executed = %#v, want health_check tools/call smoke", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_AcceptsExplicitValidationOnlyProbeOutcome(t *testing.T) {
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
	exec := &startupProbeToolExecutor{
		defs: startupProbeDefs(),
		caps: startupProbeCaps(),
		execErrs: map[string]error{
			"health_check": WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_health_check.input", false, nil, "probe input is invalid"),
		},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")

	if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, turns, exec, manager); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
	if !slices.Equal(exec.executed, []string{"health_check"}) {
		t.Fatalf("executed = %#v, want health_check tools/call smoke", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsClosedOnConfiguredGatewayTokenMismatch(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{
		ID:   "market-research-agent",
		Role: "market_research",
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: startupProbeDefs(),
		caps: startupProbeCaps(),
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "wrong-token")

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "invalid token") {
		t.Fatalf("expected invalid token error, got %v", err)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsOnEmptyVisibleToolSurface(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: []llm.ToolDefinition{{
			Name:   "query_entities",
			Schema: map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}, "required": []any{"query"}},
		}},
		caps: map[string]toolcapabilities.Capability{
			"query_entities": {
				Name:               "query_entities",
				Visible:            false,
				Callable:           true,
				ContextRequirement: toolcapabilities.ContextRequirementActorContext,
			},
		},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "found no visible tools") {
		t.Fatalf("expected empty visible tools error, got %v", err)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsClosedWhenVisibleToolsRequireInput(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	for _, tc := range []struct {
		name string
	}{
		{
			name: "structured_invalid_tool_input",
		},
		{
			name: "plain_required_field_message",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := runtimemanager.NewAgentManager(nil, nil)
			if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
				t.Fatalf("SpawnAgent: %v", err)
			}
			exec := &startupProbeToolExecutor{
				defs: []llm.ToolDefinition{{
					Name:   "query_entities",
					Schema: map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}, "required": []any{"query"}},
				}},
				caps: map[string]toolcapabilities.Capability{
					"query_entities": {
						Name:               "query_entities",
						Visible:            true,
						Callable:           true,
						ContextRequirement: toolcapabilities.ContextRequirementActorContext,
					},
				},
			}
			turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")

			err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, turns, exec, manager)
			if err == nil || !strings.Contains(err.Error(), "found no probe-safe callable non-emit tool") {
				t.Fatalf("expected no probe-safe callable tool error, got %v", err)
			}
			if len(exec.executed) != 0 {
				t.Fatalf("executed = %#v, want no tools/call smoke for required-input-only surface", exec.executed)
			}
		})
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsClosedWhenVisibleToolsAreEmitOnly(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: []llm.ToolDefinition{{
			Name:   "emit_category_assessed",
			Schema: map[string]any{"type": "object", "properties": map[string]any{}},
		}},
		caps: map[string]toolcapabilities.Capability{
			"emit_category_assessed": {
				Name:               "emit_category_assessed",
				Kind:               toolcapabilities.KindEmit,
				Visible:            true,
				Callable:           true,
				ContextRequirement: toolcapabilities.ContextRequirementActorContext,
			},
		},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "found no probe-safe callable non-emit tool") {
		t.Fatalf("expected no probe-safe callable tool error, got %v", err)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("executed = %#v, want no tools/call smoke for emit-only surface", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsClosedOnUnexpectedCallableProbeError(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: startupProbeDefs(),
		caps: startupProbeCaps(),
		execErrs: map[string]error{
			"health_check": errors.New("unexpected tool failure"),
		},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "unexpected tool failure") {
		t.Fatalf("expected unexpected callable probe error, got %v", err)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsClosedOnGenericPhraseNonValidationProbeError(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "cli_test"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: startupProbeDefs(),
		caps: startupProbeCaps(),
		execErrs: map[string]error{
			"health_check": errors.New("schema validation failed: execution path must be enabled before use"),
		},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "execution path must be enabled before use") {
		t.Fatalf("expected generic phrase non-validation probe error, got %v", err)
	}
}
