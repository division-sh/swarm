package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"swarm/internal/config"
	runtimecontracts "swarm/internal/runtime/contracts"
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

func claudeStartupAgentFreeSource() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
}

func claudeStartupAgentSource(ids ...string) semanticview.Source {
	if len(ids) == 0 {
		ids = []string{"campaign-coordinator"}
	}
	agents := make(map[string]runtimecontracts.AgentRegistryEntry, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		agents[id] = runtimecontracts.AgentRegistryEntry{ID: id, Role: id}
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Agents: agents})
}

func TestValidateClaudeStartupConfig_RequiresWorkspaceAndGateway(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	err := validateClaudeStartupConfig(cfg, RuntimeOptions{}, claudeStartupAgentSource())
	if err == nil || !strings.Contains(err.Error(), "workspace lifecycle") {
		t.Fatalf("expected workspace lifecycle error, got %v", err)
	}
}

func TestValidateClaudeStartupConfig_SkipsClaudeEnvForAgentFreeSource(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	t.Setenv("SWARM_CLAUDE_USE_MCP", "")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")

	if err := validateClaudeStartupConfig(cfg, RuntimeOptions{}, claudeStartupAgentFreeSource()); err != nil {
		t.Fatalf("validateClaudeStartupConfig: %v", err)
	}
}

func TestValidateClaudeStartupConfigForActiveAgents_RequiresFullCLIEnvForRecoveredManagerAgent(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "recovered-agent", Role: "recovered"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	opts := RuntimeOptions{
		WorkspaceLifecycle: claudeStartupWorkspaceStub{
			target: &workspace.Target{Container: "swarm-agent-recovered-agent", Workdir: "/workspace"},
		},
		EnableToolGateway: true,
		ToolGatewayToken:  "gateway-token",
	}
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:8081")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")

	err := validateClaudeStartupConfigForActiveAgents(cfg, opts, claudeStartupAgentFreeSource(), manager)
	if err == nil || !strings.Contains(err.Error(), "SWARM_TOOL_GATEWAY_CONTAINER_URL") {
		t.Fatalf("expected container gateway URL error, got %v", err)
	}

	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:8081")
	err = validateClaudeStartupConfigForActiveAgents(cfg, opts, claudeStartupAgentFreeSource(), manager)
	if err == nil || !strings.Contains(err.Error(), "CLAUDE_CODE_OAUTH_TOKEN") {
		t.Fatalf("expected oauth token error, got %v", err)
	}
}

func TestValidateClaudeManagedAgentWorkspaces_RequiresResolvedContainerTargets(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	err := validateClaudeManagedAgentWorkspaces(context.Background(), cfg, claudeStartupAgentSource("campaign-coordinator"), claudeStartupWorkspaceStub{}, manager)
	if err == nil || !strings.Contains(err.Error(), "resolved no container workspace target") {
		t.Fatalf("expected workspace target error, got %v", err)
	}
}

func TestValidateClaudeManagedAgentWorkspaces_PropagatesResolverErrors(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	err := validateClaudeManagedAgentWorkspaces(context.Background(), cfg, claudeStartupAgentSource("campaign-coordinator"), claudeStartupWorkspaceStub{err: errors.New("docker unavailable")}, manager)
	if err == nil || !strings.Contains(err.Error(), "docker unavailable") {
		t.Fatalf("expected resolver error, got %v", err)
	}
}

func TestValidateClaudeManagedAgentWorkspaces_AcceptsContainerTargets(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	err := validateClaudeManagedAgentWorkspaces(context.Background(), cfg, claudeStartupAgentSource("campaign-coordinator"), claudeStartupWorkspaceStub{
		target: &workspace.Target{Container: "swarm-agent-campaign-coordinator", Workdir: "/workspace"},
	}, manager)
	if err != nil {
		t.Fatalf("validateClaudeManagedAgentWorkspaces: %v", err)
	}
}

func TestValidateClaudeManagedAgentWorkspaces_SkipsWorkspaceForAgentFreeSource(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"

	if err := validateClaudeManagedAgentWorkspaces(context.Background(), cfg, claudeStartupAgentFreeSource(), nil, nil); err != nil {
		t.Fatalf("validateClaudeManagedAgentWorkspaces: %v", err)
	}
}

func TestValidateClaudeManagedAgentWorkspaces_RequiresWorkspaceForRecoveredManagerAgent(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "recovered-agent", Role: "recovered"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	err := validateClaudeManagedAgentWorkspaces(context.Background(), cfg, claudeStartupAgentFreeSource(), nil, manager)
	if err == nil || !strings.Contains(err.Error(), "workspace lifecycle") {
		t.Fatalf("expected workspace lifecycle error, got %v", err)
	}
}

type startupProbeToolExecutor struct {
	defs             []llm.ToolDefinition
	contextDefs      []llm.ToolDefinition
	caps             map[string]toolcapabilities.Capability
	contextCaps      map[string]toolcapabilities.Capability
	contextDefCalls  int
	contextCapsCalls int
	executed         []string
	execErrs         map[string]error
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
	return startupProbeCapabilitySet(names, s.caps)
}

func (s *startupProbeToolExecutor) ToolDefinitionsForActorInContext(context.Context, runtimeactors.AgentConfig) []llm.ToolDefinition {
	s.contextDefCalls++
	if s.contextDefs != nil {
		return append([]llm.ToolDefinition(nil), s.contextDefs...)
	}
	return append([]llm.ToolDefinition(nil), s.defs...)
}

func (s *startupProbeToolExecutor) ToolCapabilitiesForActorInContext(_ context.Context, _ runtimeactors.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	s.contextCapsCalls++
	if s.contextCaps != nil {
		return startupProbeCapabilitySet(names, s.contextCaps)
	}
	return startupProbeCapabilitySet(names, s.caps)
}

func startupProbeCapabilitySet(names []string, source map[string]toolcapabilities.Capability) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, name := range names {
		capability, ok := source[strings.TrimSpace(name)]
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

type startupVisibleSurfaceProbeStub struct {
	resp  *llm.Response
	err   error
	calls []string
}

func (s *startupVisibleSurfaceProbeStub) ProbeStartupVisibleToolSurface(_ context.Context, actor runtimeactors.AgentConfig, _ string, _ []llm.ToolDefinition) (*llm.Response, error) {
	s.calls = append(s.calls, strings.TrimSpace(actor.ID))
	if s.err != nil {
		return nil, s.err
	}
	if s.resp == nil {
		return &llm.Response{}, nil
	}
	return s.resp, nil
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
			StartupProbeMode:   toolcapabilities.StartupProbeModeCallEmptyObject,
		},
	}
}

func setupStartupProbeTransport(t *testing.T, manager *runtimemanager.AgentManager, exec *startupProbeToolExecutor, gatewayToken string) *runtimemcp.TurnContextRegistry {
	t.Helper()
	turns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	gateway := runtimemcp.NewGateway(exec, gatewayToken, RuntimeMCPGatewayHooks(nil, nil, manager.GetAgentConfig, nil, nil, turns))
	server := httptest.NewServer(gateway.Handler())
	t.Cleanup(server.Close)
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", server.URL)
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", gatewayToken)
	return turns
}

func TestValidateClaudeMCPToolsForManagedAgents_SkipsGatewayEnvForAgentFreeSource(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentFreeSource(), nil, nil, nil, nil); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_RequiresGatewayEnvForAgentSource(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
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
	turns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource("campaign-coordinator"), nil, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "SWARM_TOOL_GATEWAY_URL") {
		t.Fatalf("expected gateway URL error, got %v", err)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_RequiresGatewayEnvForRecoveredManagerAgent(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{
		ID:     "recovered-agent",
		Role:   "recovered",
		Config: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: startupProbeDefs(),
		caps: startupProbeCaps(),
	}
	turns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentFreeSource(), nil, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "SWARM_TOOL_GATEWAY_URL") {
		t.Fatalf("expected gateway URL error, got %v", err)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_UsesRealFilteredTransport(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
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
	probe := &startupVisibleSurfaceProbeStub{}

	if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
	if !slices.Equal(probe.calls, []string{"campaign-coordinator"}) {
		t.Fatalf("probe calls = %#v, want CLI startup proof before MCP runtime proof", probe.calls)
	}
	if !slices.Equal(exec.executed, []string{"health_check"}) {
		t.Fatalf("executed = %#v, want health_check tools/call smoke", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_RequiresCLIStartupProbeForMCPOnlySurface(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
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

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), nil, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "claude cli startup probe is required") {
		t.Fatalf("expected required CLI startup probe error, got %v", err)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("executed = %#v, want no MCP tools/call before CLI startup proof", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsClosedWhenMCPOnlyCLIStartupProbeFails(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
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
	probe := &startupVisibleSurfaceProbeStub{err: errors.New("claude: command not found")}

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "claude cli startup probe failed") || !strings.Contains(err.Error(), "claude: command not found") {
		t.Fatalf("expected CLI startup probe failure, got %v", err)
	}
	if !slices.Equal(probe.calls, []string{"campaign-coordinator"}) {
		t.Fatalf("probe calls = %#v, want CLI startup probe for MCP-only surface", probe.calls)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("executed = %#v, want no MCP tools/call after CLI startup proof failure", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_SeparatesStaticInventoryFromNoCurrentTurnSurface(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{
		ID:     "analysis-agent",
		Role:   "analysis",
		Config: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: []llm.ToolDefinition{
			{Name: "emit_vertical_derived", Schema: map[string]any{"type": "object", "properties": map[string]any{}}},
			{Name: "read_vertical", Schema: map[string]any{"type": "object", "properties": map[string]any{}}},
			{Name: "save_vertical_scores", Schema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "object"}}}},
			{Name: "update_vertical_scores_signal_strength", Schema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "number"}}}},
		},
		contextDefs: []llm.ToolDefinition{
			{Name: "emit_vertical_derived", Schema: map[string]any{"type": "object", "properties": map[string]any{}}},
		},
		caps: map[string]toolcapabilities.Capability{
			"emit_vertical_derived":                  {Name: "emit_vertical_derived", Kind: toolcapabilities.KindEmit, Visible: true, Callable: true},
			"read_vertical":                          {Name: "read_vertical", Visible: true, Callable: true, ContextRequirement: toolcapabilities.ContextRequirementTurnContext},
			"save_vertical_scores":                   {Name: "save_vertical_scores", Visible: true, Callable: true, ContextRequirement: toolcapabilities.ContextRequirementTurnContext},
			"update_vertical_scores_signal_strength": {Name: "update_vertical_scores_signal_strength", Visible: true, Callable: true, ContextRequirement: toolcapabilities.ContextRequirementTurnContext},
		},
		contextCaps: map[string]toolcapabilities.Capability{
			"emit_vertical_derived": {Name: "emit_vertical_derived", Kind: toolcapabilities.KindEmit, Visible: true, Callable: true},
		},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")
	probe := &startupVisibleSurfaceProbeStub{}

	if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
	if !slices.Equal(probe.calls, []string{"analysis-agent"}) {
		t.Fatalf("probe calls = %#v, want CLI startup proof before MCP visibility proof", probe.calls)
	}
	if exec.contextDefCalls == 0 || exec.contextCapsCalls == 0 {
		t.Fatalf("expected startup MCP proof to consume context-aware surface, defCalls=%d capCalls=%d", exec.contextDefCalls, exec.contextCapsCalls)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("executed = %#v, want visibility-only proof for no-current generated entity tools", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_AllowsEmptyConcreteSurfaceWhenStaticInventoryIsGeneratedCurrentEntityOnly(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{
		ID:   "validation-agent",
		Role: "validation",
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: []llm.ToolDefinition{
			{Name: "read_validation_case", Schema: map[string]any{"type": "object", "properties": map[string]any{}}},
			{Name: "save_validation_case_mvp_spec", Schema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "object"}}}},
			{Name: "update_validation_case_mvp_spec_summary", Schema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}}},
		},
		contextDefs: []llm.ToolDefinition{},
		caps: map[string]toolcapabilities.Capability{
			"read_validation_case":                    {Name: "read_validation_case", Visible: true, Callable: true, ContextRequirement: toolcapabilities.ContextRequirementTurnContext},
			"save_validation_case_mvp_spec":           {Name: "save_validation_case_mvp_spec", Visible: true, Callable: true, ContextRequirement: toolcapabilities.ContextRequirementTurnContext},
			"update_validation_case_mvp_spec_summary": {Name: "update_validation_case_mvp_spec_summary", Visible: true, Callable: true, ContextRequirement: toolcapabilities.ContextRequirementTurnContext},
		},
		contextCaps: map[string]toolcapabilities.Capability{},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")
	probe := &startupVisibleSurfaceProbeStub{}

	if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
	if !slices.Equal(probe.calls, []string{"validation-agent"}) {
		t.Fatalf("probe calls = %#v, want CLI startup proof for empty concrete MCP surface", probe.calls)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("executed = %#v, want no callable startup probe for empty concrete surface", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_AcceptsExplicitValidationOnlyProbeOutcome(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
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
	probe := &startupVisibleSurfaceProbeStub{}

	if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
	if !slices.Equal(probe.calls, []string{"campaign-coordinator"}) {
		t.Fatalf("probe calls = %#v, want CLI startup proof before validation-only MCP call", probe.calls)
	}
	if !slices.Equal(exec.executed, []string{"health_check"}) {
		t.Fatalf("executed = %#v, want health_check tools/call smoke", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsClosedOnConfiguredGatewayTokenMismatch(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
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
	probe := &startupVisibleSurfaceProbeStub{}

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "invalid token") {
		t.Fatalf("expected invalid token error, got %v", err)
	}
	if !slices.Equal(probe.calls, []string{"market-research-agent"}) {
		t.Fatalf("probe calls = %#v, want CLI startup proof before MCP auth proof", probe.calls)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsOnEmptyVisibleToolSurface(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
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
	probe := &startupVisibleSurfaceProbeStub{}

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "found no visible tools") {
		t.Fatalf("expected empty visible tools error, got %v", err)
	}
	if !slices.Equal(probe.calls, []string{"campaign-coordinator"}) {
		t.Fatalf("probe calls = %#v, want CLI startup proof before empty visible-surface failure", probe.calls)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_UsesLiveVisibleSurfaceForNativeBuiltinOnlySurface(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{
		ID:   "campaign-coordinator",
		Role: "campaign_coordinator",
		NativeTools: runtimeactors.NativeToolConfig{
			FileIO:    true,
			Bash:      true,
			WebSearch: true,
		},
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: []llm.ToolDefinition{
			{Name: "read_file", Schema: map[string]any{"type": "object"}},
			{Name: "write_file", Schema: map[string]any{"type": "object"}},
			{Name: "bash", Schema: map[string]any{"type": "object"}},
			{Name: "web_search", Schema: map[string]any{"type": "object"}},
		},
		caps: map[string]toolcapabilities.Capability{
			"read_file":  {Name: "read_file", Visible: true, Callable: true},
			"write_file": {Name: "write_file", Visible: true, Callable: true},
			"bash":       {Name: "bash", Visible: true, Callable: true},
			"web_search": {Name: "web_search", Visible: true, Callable: true},
		},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")
	probe := &startupVisibleSurfaceProbeStub{
		resp: &llm.Response{
			VisibleTools: []string{"Bash", "Read", "Write", "Edit", "WebFetch", "WebSearch"},
		},
	}

	if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
	if !slices.Equal(probe.calls, []string{"campaign-coordinator"}) {
		t.Fatalf("probe calls = %#v, want startup visible-surface probe", probe.calls)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("executed = %#v, want no MCP startup probe for native-only surface", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_ComparesProviderNativeSurfaceOnly(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{
		ID:   "trend-research-agent",
		Role: "trend_research",
		NativeTools: runtimeactors.NativeToolConfig{
			WebSearch: true,
		},
		Config: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: []llm.ToolDefinition{
			{Name: "query_entities", Schema: map[string]any{"type": "object", "properties": map[string]any{}}},
			{Name: "emit_trend_identified", Schema: map[string]any{"type": "object", "properties": map[string]any{}}},
		},
		caps: map[string]toolcapabilities.Capability{
			"query_entities":        {Name: "query_entities", Visible: true, Callable: true, ContextRequirement: toolcapabilities.ContextRequirementActorContext},
			"emit_trend_identified": {Name: "emit_trend_identified", Kind: toolcapabilities.KindEmit, Visible: true, Callable: true},
		},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")
	probe := &startupVisibleSurfaceProbeStub{
		resp: &llm.Response{
			VisibleTools: []string{"WebSearch"},
		},
	}

	if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
	if !slices.Equal(probe.calls, []string{"trend-research-agent"}) {
		t.Fatalf("probe calls = %#v, want startup visible-surface probe", probe.calls)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("executed = %#v, want MCP visibility-only proof when no explicit safe startup call exists", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsClosedOnUnexpectedProviderNativeTool(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{
		ID:   "trend-research-agent",
		Role: "trend_research",
		NativeTools: runtimeactors.NativeToolConfig{
			WebSearch: true,
		},
		Config: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: []llm.ToolDefinition{
			{Name: "query_entities", Schema: map[string]any{"type": "object", "properties": map[string]any{}}},
			{Name: "emit_trend_identified", Schema: map[string]any{"type": "object", "properties": map[string]any{}}},
		},
		caps: map[string]toolcapabilities.Capability{
			"query_entities":        {Name: "query_entities", Visible: true, Callable: true, ContextRequirement: toolcapabilities.ContextRequirementActorContext},
			"emit_trend_identified": {Name: "emit_trend_identified", Kind: toolcapabilities.KindEmit, Visible: true, Callable: true},
		},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")
	probe := &startupVisibleSurfaceProbeStub{
		resp: &llm.Response{
			VisibleTools: []string{"WebSearch", "Bash"},
		},
	}

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "unexpected visible tool surface") {
		t.Fatalf("expected provider-native visible-surface mismatch, got %v", err)
	}
	if !slices.Equal(probe.calls, []string{"trend-research-agent"}) {
		t.Fatalf("probe calls = %#v, want startup visible-surface probe", probe.calls)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("executed = %#v, want no MCP startup probe after provider-native mismatch", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsClosedWhenNativeBuiltinVisibleSurfaceMismatches(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{
		ID:   "campaign-coordinator",
		Role: "campaign_coordinator",
		NativeTools: runtimeactors.NativeToolConfig{
			FileIO: true,
		},
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: []llm.ToolDefinition{
			{Name: "read_file", Schema: map[string]any{"type": "object"}},
			{Name: "write_file", Schema: map[string]any{"type": "object"}},
		},
		caps: map[string]toolcapabilities.Capability{
			"read_file":  {Name: "read_file", Visible: true, Callable: true},
			"write_file": {Name: "write_file", Visible: true, Callable: true},
		},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")
	probe := &startupVisibleSurfaceProbeStub{
		resp: &llm.Response{
			VisibleTools: []string{"Read"},
		},
	}

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "unexpected visible tool surface") {
		t.Fatalf("expected visible tool surface mismatch, got %v", err)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("executed = %#v, want no MCP startup probe for mismatched native-only surface", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_UsesVisibilityOnlyWhenNoExplicitSafeStartupCallExists(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
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
			probe := &startupVisibleSurfaceProbeStub{}

			if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager); err != nil {
				t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
			}
			if !slices.Equal(probe.calls, []string{"campaign-coordinator"}) {
				t.Fatalf("probe calls = %#v, want CLI startup proof before MCP visibility-only proof", probe.calls)
			}
			if len(exec.executed) != 0 {
				t.Fatalf("executed = %#v, want visibility-only startup proof for required-input-only surface", exec.executed)
			}
		})
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_DoesNotCallSchemaOnlyEmptyObjectTool(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "lifecycle-coordinator", Role: "lifecycle_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: []llm.ToolDefinition{{
			Name:   "query_entities",
			Schema: map[string]any{"type": "object", "properties": map[string]any{}},
		}},
		caps: map[string]toolcapabilities.Capability{
			"query_entities": {
				Name:               "query_entities",
				Visible:            true,
				Callable:           true,
				ContextRequirement: toolcapabilities.ContextRequirementActorContext,
			},
		},
		execErrs: map[string]error{
			"query_entities": errors.New("flow-owned entity contract is not available for actor lifecycle-coordinator"),
		},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")
	probe := &startupVisibleSurfaceProbeStub{}

	if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
	if !slices.Equal(probe.calls, []string{"lifecycle-coordinator"}) {
		t.Fatalf("probe calls = %#v, want CLI startup proof before schema-only MCP proof", probe.calls)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("executed = %#v, want query_entities visibility-only despite empty-object schema", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_UsesVisibilityOnlyWhenVisibleToolsAreEmitOnly(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
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
	probe := &startupVisibleSurfaceProbeStub{}

	if err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager); err != nil {
		t.Fatalf("validateClaudeMCPToolsForManagedAgents: %v", err)
	}
	if !slices.Equal(probe.calls, []string{"campaign-coordinator"}) {
		t.Fatalf("probe calls = %#v, want CLI startup proof before emit-only MCP proof", probe.calls)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("executed = %#v, want visibility-only startup proof for emit-only surface", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsClosedWhenExplicitProbeSafeToolRequiresInput(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	manager := runtimemanager.NewAgentManager(nil, nil)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	exec := &startupProbeToolExecutor{
		defs: []llm.ToolDefinition{{
			Name:   "health_check",
			Schema: map[string]any{"type": "object", "properties": map[string]any{"probe": map[string]any{"type": "string"}}, "required": []any{"probe"}},
		}},
		caps: map[string]toolcapabilities.Capability{
			"health_check": {
				Name:             "health_check",
				Visible:          true,
				Callable:         true,
				StartupProbeMode: toolcapabilities.StartupProbeModeCallEmptyObject,
			},
		},
	}
	turns := setupStartupProbeTransport(t, manager, exec, "gateway-token")
	probe := &startupVisibleSurfaceProbeStub{}

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "call_empty_object") {
		t.Fatalf("expected explicit probe-safe schema mismatch error, got %v", err)
	}
	if !slices.Equal(probe.calls, []string{"campaign-coordinator"}) {
		t.Fatalf("probe calls = %#v, want CLI startup proof before explicit startup probe policy failure", probe.calls)
	}
	if len(exec.executed) != 0 {
		t.Fatalf("executed = %#v, want no tools/call smoke for invalid explicit startup probe policy", exec.executed)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsClosedOnUnexpectedCallableProbeError(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
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
	probe := &startupVisibleSurfaceProbeStub{}

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "unexpected tool failure") {
		t.Fatalf("expected unexpected callable probe error, got %v", err)
	}
	if !slices.Equal(probe.calls, []string{"campaign-coordinator"}) {
		t.Fatalf("probe calls = %#v, want CLI startup proof before callable probe failure", probe.calls)
	}
}

func TestValidateClaudeMCPToolsForManagedAgents_FailsClosedOnGenericPhraseNonValidationProbeError(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
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
	probe := &startupVisibleSurfaceProbeStub{}

	err := validateClaudeMCPToolsForManagedAgents(context.Background(), cfg, claudeStartupAgentSource(), probe, turns, exec, manager)
	if err == nil || !strings.Contains(err.Error(), "execution path must be enabled before use") {
		t.Fatalf("expected generic phrase non-validation probe error, got %v", err)
	}
	if !slices.Equal(probe.calls, []string{"campaign-coordinator"}) {
		t.Fatalf("probe calls = %#v, want CLI startup proof before generic callable probe failure", probe.calls)
	}
}
