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
	got := claudeDisallowedBuiltinToolsArgForActor(models.AgentConfig{}, nil)
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
	}, nil)
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

func TestClaudeAllowedToolsArgForActor_IncludesPostCompositionAndNativeTools(t *testing.T) {
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
		"mcp__runtime-tools__emit_category_assessed",
		"mcp__runtime-tools__emit_market_research_scan_complete",
		"mcp__runtime-tools__query_entities",
		"Read",
		"Write",
		"Edit",
		"ExitPlanMode",
	} {
		if !slices.Contains(gotNames, name) {
			t.Fatalf("expected %q in allowed tools %q", name, got)
		}
	}
	for _, name := range []string{"Bash", "WebSearch", "ToolSearch", "query_entities"} {
		if slices.Contains(gotNames, name) {
			t.Fatalf("did not expect %q in allowed tools %q", name, got)
		}
	}
}

func TestClaudeToolSurface_PrefersFallbackRuntimeToolsOverNativeBuiltins(t *testing.T) {
	actor := models.AgentConfig{
		NativeTools: models.NativeToolConfig{
			FileIO: true,
			Bash:   true,
		},
	}
	tools := []ToolDefinition{
		{Name: "read_file"},
		{Name: "write_file"},
		{Name: "bash"},
		{Name: "emit_category_assessed"},
	}

	allowed := claudeAllowedToolsArgForActor(actor, tools)
	allowedNames := strings.Split(allowed, ",")
	for _, name := range []string{
		"mcp__runtime-tools__read_file",
		"mcp__runtime-tools__write_file",
		"mcp__runtime-tools__bash",
		"mcp__runtime-tools__emit_category_assessed",
		"emit_category_assessed",
		"ExitPlanMode",
	} {
		if !slices.Contains(allowedNames, name) {
			t.Fatalf("expected %q in allowed tools %q", name, allowed)
		}
	}
	for _, name := range []string{"Read", "Write", "Edit", "Bash", "read_file", "write_file", "bash"} {
		if slices.Contains(allowedNames, name) {
			t.Fatalf("did not expect native builtin %q in allowed tools %q", name, allowed)
		}
	}

	disallowed := claudeDisallowedBuiltinToolsArgForActor(actor, tools)
	disallowedNames := strings.Split(disallowed, ",")
	for _, name := range []string{"Read", "Write", "Edit", "Bash"} {
		if !slices.Contains(disallowedNames, name) {
			t.Fatalf("expected fallback-backed builtin %q in disallowed tools %q", name, disallowed)
		}
	}

	prompt := augmentCLISystemPrompt("You are here.", actor, tools)
	if !strings.Contains(prompt, "Call Swarm runtime tools by these exact names") {
		t.Fatalf("expected runtime tool prompt section, got %q", prompt)
	}
	if !strings.Contains(prompt, "emit_category_assessed") {
		t.Fatalf("expected emit fallback tool in prompt, got %q", prompt)
	}
	for _, name := range []string{"read_file", "write_file", "bash"} {
		if strings.Contains(prompt, "\n- "+name+"\n") {
			t.Fatalf("did not expect native capability tool %q in prompt, got %q", name, prompt)
		}
	}
	for _, name := range []string{"mcp__runtime-tools__read_file", "mcp__runtime-tools__write_file", "mcp__runtime-tools__bash"} {
		if !strings.Contains(prompt, name) {
			t.Fatalf("expected provider-visible MCP tool %q in prompt, got %q", name, prompt)
		}
	}
	if strings.Contains(prompt, "Claude CLI native tools available in this turn") {
		t.Fatalf("did not expect native builtin prompt section when fallback runtime tools own the surface, got %q", prompt)
	}
}

func TestCLIExecutionToolSurface_CanonicalizesProviderBuiltins(t *testing.T) {
	surface := cliExecutionToolSurfaceForActor(models.AgentConfig{
		NativeTools: models.NativeToolConfig{
			FileIO:    true,
			WebSearch: true,
		},
	}, []ToolDefinition{
		{Name: "emit_category_assessed"},
	})

	if !slices.Equal(surface.CanonicalVisibleTools, []string{"emit_category_assessed", "read_file", "web_search", "write_file"}) {
		t.Fatalf("canonical visible tools = %#v", surface.CanonicalVisibleTools)
	}
	if !slices.Equal(surface.ProviderBuiltinTools, []string{"Edit", "Read", "WebSearch", "Write"}) {
		t.Fatalf("provider builtin tools = %#v", surface.ProviderBuiltinTools)
	}
	if !slices.Equal(surface.ProviderMCPTools, []string{"mcp__runtime-tools__emit_category_assessed"}) {
		t.Fatalf("provider mcp tools = %#v", surface.ProviderMCPTools)
	}
	if !slices.Equal(surface.LocalFallbackTools, []string{"emit_category_assessed"}) {
		t.Fatalf("local fallback tools = %#v", surface.LocalFallbackTools)
	}
	if !slices.Equal(surface.PromptRuntimeTools, []string{"emit_category_assessed"}) {
		t.Fatalf("prompt runtime tools = %#v", surface.PromptRuntimeTools)
	}
}

func TestCLIExecutionToolSurface_FileIOMixedFallbacksStayExecutable(t *testing.T) {
	surface := cliExecutionToolSurfaceForActor(models.AgentConfig{
		NativeTools: models.NativeToolConfig{
			FileIO: true,
		},
	}, []ToolDefinition{
		{Name: "read_file"},
		{Name: "emit_category_assessed"},
	})

	if !slices.Equal(surface.CanonicalVisibleTools, []string{"emit_category_assessed", "read_file", "write_file"}) {
		t.Fatalf("canonical visible tools = %#v", surface.CanonicalVisibleTools)
	}
	if !slices.Equal(surface.ProviderBuiltinTools, []string{"Edit", "Write"}) {
		t.Fatalf("provider builtin tools = %#v", surface.ProviderBuiltinTools)
	}
	if !slices.Equal(surface.ProviderMCPTools, []string{"mcp__runtime-tools__emit_category_assessed", "mcp__runtime-tools__read_file"}) {
		t.Fatalf("provider mcp tools = %#v", surface.ProviderMCPTools)
	}
	if !slices.Equal(surface.LocalFallbackTools, []string{"emit_category_assessed"}) {
		t.Fatalf("local fallback tools = %#v", surface.LocalFallbackTools)
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
	urlRaw := runtimeTools["url"].(string)
	legacyTraceQuery := "trace" + "_id="
	if strings.Contains(urlRaw, legacyTraceQuery) {
		t.Fatalf("url %q should not propagate legacy trace query params", urlRaw)
	}
	if strings.Contains(urlRaw, "ctx_token=") {
		t.Fatalf("url %q should not propagate context token query", urlRaw)
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

func TestValidateCLIResponseToolCallsForTurn_FailsClosedForNonEmitToolOutsideObservedSurface(t *testing.T) {
	actor := models.AgentConfig{
		ID:          "market-research-agent",
		NativeTools: models.NativeToolConfig{FileIO: true},
	}
	err := validateCLIResponseToolCallsForTurn(actor, []ToolDefinition{
		{Name: "emit_category_assessed"},
		{Name: "read_file"},
	}, &Response{
		MCPServers: map[string]string{
			"runtime-tools": "failed",
		},
		ToolCalls: []ToolCall{
			{Name: "read_file", Arguments: map[string]any{"path": "/workspace/corpus.json"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "read_file") {
		t.Fatalf("validateCLIResponseToolCallsForTurn err = %v, want read_file visibility failure", err)
	}
}

func TestValidateCLIResponseToolCallsForTurn_AllowsObservedMCPToolAndEmitFallback(t *testing.T) {
	actor := models.AgentConfig{
		ID:          "market-research-agent",
		NativeTools: models.NativeToolConfig{FileIO: true},
	}
	err := validateCLIResponseToolCallsForTurn(actor, []ToolDefinition{
		{Name: "emit_category_assessed"},
		{Name: "read_file"},
	}, &Response{
		MCPServers: map[string]string{
			"runtime-tools": "connected",
		},
		VisibleTools: []string{
			"emit_category_assessed",
			"read_file",
		},
		ToolCalls: []ToolCall{
			{Name: "read_file", Arguments: map[string]any{"path": "/workspace/corpus.json"}},
			{Name: "emit_category_assessed", Arguments: map[string]any{"category": "x"}},
		},
	})
	if err != nil {
		t.Fatalf("validateCLIResponseToolCallsForTurn: %v", err)
	}
}

func TestValidateCLIResponseToolCallsForTurn_AllowsPlannedNonEmitSurfaceWhenObservedMetadataIsAbsent(t *testing.T) {
	actor := models.AgentConfig{
		ID: "market-research-agent",
	}
	err := validateCLIResponseToolCallsForTurn(actor, []ToolDefinition{
		{Name: "query_entities"},
	}, &Response{
		ToolCalls: []ToolCall{
			{Name: "query_entities", Arguments: map[string]any{"entity_type": "company"}},
		},
	})
	if err != nil {
		t.Fatalf("validateCLIResponseToolCallsForTurn: %v", err)
	}
}
