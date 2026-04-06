package llm

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"swarm/internal/config"
	models "swarm/internal/runtime/core/actors"
)

type mcpTurnContextStoreStub struct {
	register   func(context.Context, time.Duration, []string) string
	unregister func(string)
}

func (s mcpTurnContextStoreStub) RegisterTurnContextWithTTL(ctx context.Context, ttl time.Duration) string {
	return s.RegisterTurnContextWithAllowedTools(ctx, ttl, nil)
}

func (s mcpTurnContextStoreStub) RegisterTurnContextWithAllowedTools(ctx context.Context, ttl time.Duration, allowedTools []string) string {
	if s.register == nil {
		return ""
	}
	return s.register(ctx, ttl, allowedTools)
}

func (s mcpTurnContextStoreStub) UnregisterTurnContext(token string) {
	if s.unregister != nil {
		s.unregister(token)
	}
}

func TestShouldUseMCPBridge_DefaultsOn(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "")
	if !shouldUseMCPBridge() {
		t.Fatal("expected MCP bridge to default on")
	}
}

func TestShouldUseMCPBridge_CanDisable(t *testing.T) {
	for _, raw := range []string{"0", "false", "no"} {
		t.Setenv("SWARM_CLAUDE_USE_MCP", raw)
		if shouldUseMCPBridge() {
			t.Fatalf("expected %q to disable MCP bridge", raw)
		}
	}
}

func TestClaudeDisallowedBuiltinToolsArgForActor_DefaultsToAllKnownBuiltins(t *testing.T) {
	got := claudeDisallowedBuiltinToolsArgForActor(models.AgentConfig{})
	if got == "" {
		t.Fatal("expected builtin tools to be blocked by default")
	}
	gotNames := strings.Split(got, ",")
	for _, name := range []string{"Bash", "Read", "WebSearch", "ToolSearch", "AskUserQuestion"} {
		if !slices.Contains(gotNames, name) {
			t.Fatalf("expected %q in disallowed tools %q", name, got)
		}
	}
}

func TestClaudeDisallowedBuiltinToolsArgForActor_MapsNativeCapabilities(t *testing.T) {
	got := claudeDisallowedBuiltinToolsArgForActor(models.AgentConfig{
		NativeTools: models.NativeToolConfig{
			Bash:      true,
			WebSearch: true,
			FileIO:    true,
		},
	})
	gotNames := strings.Split(got, ",")
	for _, name := range []string{"Bash", "Read", "Write", "Edit", "WebSearch"} {
		if slices.Contains(gotNames, name) {
			t.Fatalf("did not expect allowed tool %q in disallowed list %q", name, got)
		}
	}
	for _, name := range []string{"ToolSearch", "AskUserQuestion", "Glob"} {
		if !slices.Contains(gotNames, name) {
			t.Fatalf("expected undeclared tool %q in disallowed list %q", name, got)
		}
	}
}

func TestClaudeAllowedToolsArgForActor_IncludesContractAndNativeTools(t *testing.T) {
	got := claudeAllowedToolsArgForActor(models.AgentConfig{
		NativeTools: models.NativeToolConfig{FileIO: true},
	}, []ToolDefinition{
		{Name: "emit_category_assessed"},
		{Name: "emit_market_research_scan_complete"},
		{Name: "query_entities"},
	})
	gotNames := strings.Split(got, ",")
	for _, name := range []string{
		"emit_category_assessed",
		"emit_market_research_scan_complete",
		"query_entities",
		"Read",
		"Write",
		"Edit",
		"ExitPlanMode",
	} {
		if !slices.Contains(gotNames, name) {
			t.Fatalf("expected %q in allowed tools %q", name, got)
		}
	}
	for _, name := range []string{"Bash", "WebSearch", "ToolSearch"} {
		if slices.Contains(gotNames, name) {
			t.Fatalf("did not expect %q in allowed tools %q", name, got)
		}
	}
}

func TestBuildMCPConfigArg_UsesContextTokenWithoutLegacyCorrelationPropagation(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:18082")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")

	r := &ClaudeCLIRuntime{
		cfg: &config.Config{},
		mcpTurns: mcpTurnContextStoreStub{
			register: func(_ context.Context, _ time.Duration, allowedTools []string) string {
				if !slices.Equal(allowedTools, []string{"emit_category_assessed", "query_entities"}) {
					t.Fatalf("allowedTools = %#v", allowedTools)
				}
				return "ctx-token-123"
			},
			unregister: func(string) {},
		},
	}
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:   "market-research-agent",
		Role: "market_research",
		Mode: "discovery",
	})
	s := &Session{
		AgentID: "market-research-agent",
		Tools: []ToolDefinition{
			{Name: "emit_category_assessed"},
			{Name: "query_entities"},
		},
	}

	cfgJSON, contextToken, enabled, err := r.buildMCPConfigArg(ctx, s)
	if err != nil {
		t.Fatalf("buildMCPConfigArg: %v", err)
	}
	if !enabled {
		t.Fatal("expected MCP config to be enabled")
	}
	if contextToken != "ctx-token-123" {
		t.Fatalf("context token = %q, want ctx-token-123", contextToken)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(cfgJSON), &payload); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	servers := payload["mcpServers"].(map[string]any)
	runtimeTools := servers["runtime-tools"].(map[string]any)
	headers := runtimeTools["headers"].(map[string]any)
	if got := headers["X-SWARM-Trace-Id"]; got != nil {
		t.Fatalf("unexpected trace header = %#v", got)
	}
	if got := headers[mcpContextTokenHeader]; got != contextToken {
		t.Fatalf("context header = %#v, want %q", got, contextToken)
	}
	for _, key := range []string{
		mcpActorIDHeader,
		mcpActorRoleHeader,
		mcpActorModeHeader,
		mcpEntityIDHeader,
		mcpAllowedToolsHeader,
	} {
		if got := headers[key]; got != nil {
			t.Fatalf("unexpected gateway identity header %q = %#v", key, got)
		}
	}
	urlRaw := runtimeTools["url"].(string)
	legacyTraceQuery := "trace" + "_id="
	if strings.Contains(urlRaw, legacyTraceQuery) {
		t.Fatalf("url %q should not propagate legacy trace query params", urlRaw)
	}
	for _, key := range []string{
		mcpActorIDQuery,
		mcpActorRoleQuery,
		mcpActorModeQuery,
		mcpEntityIDQuery,
		mcpAllowedToolsQuery,
	} {
		if strings.Contains(urlRaw, key+"=") {
			t.Fatalf("url %q should not propagate %s", urlRaw, key)
		}
	}
}

func TestBuildMCPConfigArg_FailsClosedWithoutTurnContextStore(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:18082")

	r := &ClaudeCLIRuntime{cfg: &config.Config{}}
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:   "market-research-agent",
		Role: "market_research",
	})
	s := &Session{
		AgentID: "market-research-agent",
		Tools:   []ToolDefinition{{Name: "query_entities"}},
	}

	_, _, _, err := r.buildMCPConfigArg(ctx, s)
	if err == nil || !strings.Contains(err.Error(), "mcp turn context store is required") {
		t.Fatalf("buildMCPConfigArg err = %v, want missing store error", err)
	}
}
