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
	runtimecorrelation "swarm/internal/runtime/correlation"
)

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
	raw, err := json.Marshal(map[string]any{
		"native_tools": map[string]any{
			"bash":       true,
			"web_search": true,
			"file_io":    true,
		},
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	got := claudeDisallowedBuiltinToolsArgForActor(models.AgentConfig{Config: raw})
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
	raw, err := json.Marshal(map[string]any{
		"native_tools": map[string]any{
			"file_io": true,
		},
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	got := claudeAllowedToolsArgForActor(models.AgentConfig{Config: raw}, []ToolDefinition{
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

func TestBuildMCPConfigArg_UsesCorrelationTraceID(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:18082")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "gateway-token")

	r := &ClaudeCLIRuntime{cfg: &config.Config{}}
	prevRegister := mcpTurnContextRegister
	prevUnregister := mcpTurnContextUnregister
	mcpTurnContextRegister = func(context.Context, time.Duration) string { return "ctx-token-123" }
	mcpTurnContextUnregister = func(string) {}
	defer func() {
		mcpTurnContextRegister = prevRegister
		mcpTurnContextUnregister = prevUnregister
	}()
	ctx := runtimecorrelation.WithTraceID(context.Background(), "trace-root-123")
	ctx = models.WithActor(ctx, models.AgentConfig{
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
	if got := headers[mcpTraceIDHeader]; got != "trace-root-123" {
		t.Fatalf("trace header = %#v, want trace-root-123", got)
	}
	if got := headers[mcpContextTokenHeader]; got != contextToken {
		t.Fatalf("context header = %#v, want %q", got, contextToken)
	}
	urlRaw := runtimeTools["url"].(string)
	if !strings.Contains(urlRaw, "trace_id=trace-root-123") {
		t.Fatalf("url %q missing propagated trace_id", urlRaw)
	}
}
