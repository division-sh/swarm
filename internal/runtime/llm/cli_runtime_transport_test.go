package llm

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
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
	for _, name := range []string{"Bash", "Read", "Write", "Edit", "WebFetch", "WebSearch"} {
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

func TestClaudeToolSurface_UsesProviderNativeBuiltinsWithoutFallbackInjection(t *testing.T) {
	actor := models.AgentConfig{
		NativeTools: models.NativeToolConfig{
			FileIO:    true,
			Bash:      true,
			WebSearch: true,
		},
	}
	tools := []ToolDefinition{
		{Name: "read_file"},
		{Name: "write_file"},
		{Name: "bash"},
		{Name: "web_search"},
		{Name: "emit_category_assessed"},
	}

	allowed := claudeAllowedToolsArgForActor(actor, tools)
	allowedNames := strings.Split(allowed, ",")
	for _, name := range []string{
		"mcp__runtime-tools__emit_category_assessed",
		"emit_category_assessed",
		"Bash",
		"WebFetch",
		"WebSearch",
		"Read",
		"Write",
		"Edit",
		"ExitPlanMode",
	} {
		if !slices.Contains(allowedNames, name) {
			t.Fatalf("expected %q in allowed tools %q", name, allowed)
		}
	}
	for _, name := range []string{
		"mcp__runtime-tools__read_file",
		"mcp__runtime-tools__write_file",
		"mcp__runtime-tools__bash",
		"mcp__runtime-tools__web_search",
		"read_file",
		"write_file",
		"bash",
		"web_search",
	} {
		if slices.Contains(allowedNames, name) {
			t.Fatalf("did not expect native builtin %q in allowed tools %q", name, allowed)
		}
	}

	disallowed := claudeDisallowedBuiltinToolsArgForActor(actor, tools)
	disallowedNames := strings.Split(disallowed, ",")
	for _, name := range []string{"Read", "Write", "Edit", "Bash", "WebFetch", "WebSearch"} {
		if slices.Contains(disallowedNames, name) {
			t.Fatalf("did not expect provider-native builtin %q in disallowed tools %q", name, disallowed)
		}
	}

	prompt := augmentCLISystemPrompt("You are here.", actor, tools)
	if !strings.Contains(prompt, "Call Swarm runtime tools by these exact names") {
		t.Fatalf("expected runtime tool prompt section, got %q", prompt)
	}
	if !strings.Contains(prompt, "emit_category_assessed") {
		t.Fatalf("expected emit fallback tool in prompt, got %q", prompt)
	}
	for _, name := range []string{"read_file", "write_file", "bash", "web_search"} {
		if strings.Contains(prompt, "\n- "+name+"\n") {
			t.Fatalf("did not expect native capability tool %q in prompt, got %q", name, prompt)
		}
	}
	for _, name := range []string{"mcp__runtime-tools__read_file", "mcp__runtime-tools__write_file", "mcp__runtime-tools__bash", "mcp__runtime-tools__web_search"} {
		if strings.Contains(prompt, name) {
			t.Fatalf("did not expect fallback MCP tool %q in prompt, got %q", name, prompt)
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
	if !slices.Equal(surface.ProviderBuiltinTools, []string{"Edit", "Read", "WebFetch", "WebSearch", "Write"}) {
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
	if !slices.Equal(surface.RuntimeToolNames, []string{"emit_category_assessed"}) {
		t.Fatalf("runtime tool names = %#v", surface.RuntimeToolNames)
	}
}

func TestCLIExecutionToolSurface_DoesNotInjectRetiredLegacyEntityTools(t *testing.T) {
	actor := models.AgentConfig{ID: "validation-orchestrator", Role: "validation_orchestrator"}
	tools := []ToolDefinition{
		{Name: "read_validation_case"},
		{Name: "read_validation_case_business_brief"},
		{Name: "save_validation_case_business_brief"},
		{Name: "update_validation_case_business_brief_summary"},
		{Name: "emit_validation_package_ready"},
	}

	surface := cliExecutionToolSurfaceForActor(actor, tools)
	allowed := strings.Split(claudeAllowedToolsArgForActor(actor, tools), ",")
	summary := AgentVisibleToolSummaryLinesForActor(actor, tools)
	legacyNames := []string{
		"create_entity",
		"get_entity",
		"get_subject_status",
		"query_entities",
		"query_metrics",
		"save_entity_field",
		"search_entities",
	}
	for _, name := range legacyNames {
		if slices.Contains(surface.RuntimeToolNames, name) || slices.Contains(surface.CanonicalVisibleTools, name) {
			t.Fatalf("legacy entity tool %q appeared in CLI runtime surface: %#v", name, surface)
		}
		if slices.Contains(allowed, "mcp__runtime-tools__"+name) || slices.Contains(allowed, name) {
			t.Fatalf("legacy entity tool %q appeared in Claude allowed tools: %#v", name, allowed)
		}
		for _, line := range summary {
			if strings.Contains(line, name) {
				t.Fatalf("legacy entity tool %q appeared in prompt tool summary line %q", name, line)
			}
		}
	}
	for _, name := range []string{"read_validation_case", "save_validation_case_business_brief", "emit_validation_package_ready"} {
		if !slices.Contains(surface.RuntimeToolNames, name) {
			t.Fatalf("generated/runtime tool %q missing from CLI runtime surface: %#v", name, surface.RuntimeToolNames)
		}
		if !slices.Contains(allowed, "mcp__runtime-tools__"+name) {
			t.Fatalf("generated/runtime tool %q missing from Claude allowed tools: %#v", name, allowed)
		}
	}
	if len(AgentVisibleToolSurfaceForActor(actor, tools).WritableEntityPaths) != 0 {
		t.Fatalf("save_entity_field writable paths should not be advertised for generated save tools")
	}
}

func TestCLIExecutionToolSurface_FileIONativeSurfaceRemovesFallbackRuntimeTools(t *testing.T) {
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
	if !slices.Equal(surface.ProviderBuiltinTools, []string{"Edit", "Read", "Write"}) {
		t.Fatalf("provider builtin tools = %#v", surface.ProviderBuiltinTools)
	}
	if !slices.Equal(surface.ProviderMCPTools, []string{"mcp__runtime-tools__emit_category_assessed"}) {
		t.Fatalf("provider mcp tools = %#v", surface.ProviderMCPTools)
	}
	if !slices.Equal(surface.LocalFallbackTools, []string{"emit_category_assessed"}) {
		t.Fatalf("local fallback tools = %#v", surface.LocalFallbackTools)
	}
	if !slices.Equal(surface.RuntimeToolNames, []string{"emit_category_assessed"}) {
		t.Fatalf("runtime tool names = %#v", surface.RuntimeToolNames)
	}
}

func TestBuildMCPConfigArg_UsesContextTokenWithoutLegacyCorrelationPropagation(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")

	r := &ClaudeCLIRuntime{
		cfg:         &config.Config{},
		toolGateway: testToolGatewayBinding("http://127.0.0.1:18082", "http://host.docker.internal:18082", "gateway-token"),
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
		ID:     "market-research-agent",
		Role:   "market_research",
		FlowID: "discovery",
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
	if got := headers["Authorization"]; got != "Bearer gateway-token" {
		t.Fatalf("authorization header = %#v, want binding bearer token", got)
	}
	if got := headers[mcpContextTokenHeader]; got != contextToken {
		t.Fatalf("context header = %#v, want %q", got, contextToken)
	}
	urlRaw := runtimeTools["url"].(string)
	if urlRaw != "http://host.docker.internal:18082/mcp" {
		t.Fatalf("url = %q, want explicit container MCP endpoint", urlRaw)
	}
	legacyTraceQuery := "trace" + "_id="
	if strings.Contains(urlRaw, legacyTraceQuery) {
		t.Fatalf("url %q should not propagate legacy trace query params", urlRaw)
	}
	if strings.Contains(urlRaw, "ctx_token=") {
		t.Fatalf("url %q should not propagate context token query", urlRaw)
	}
}

func TestBuildMCPHTTPBinding_DisablesBridgeForNativeBuiltinOnlySurface(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	registered := false
	binding, enabled, err := BuildMCPHTTPBinding(
		models.WithActor(context.Background(), models.AgentConfig{
			ID: "analysis-agent",
			NativeTools: models.NativeToolConfig{
				FileIO:    true,
				Bash:      true,
				WebSearch: true,
			},
		}),
		&config.Config{},
		mcpTurnContextStoreStub{
			register: func(_ context.Context, _ time.Duration, _ []string) string {
				registered = true
				return "ctx-token"
			},
		},
		&Session{
			AgentID: "analysis-agent",
			Tools: []ToolDefinition{
				{Name: "read_file"},
				{Name: "write_file"},
				{Name: "bash"},
				{Name: "web_search"},
			},
		},
		testToolGatewayBinding("http://127.0.0.1:18082", "http://host.docker.internal:18082", "gateway-token"),
		MCPGatewayWorkspaceEndpoint,
	)
	if err != nil {
		t.Fatalf("BuildMCPHTTPBinding: %v", err)
	}
	if enabled {
		t.Fatalf("expected native-builtin-only surface to disable MCP bridge, got %#v", binding)
	}
	if registered {
		t.Fatal("did not expect turn context registration for native-builtin-only surface")
	}
}

func TestBuildMCPHTTPBindingRequiresConstructionProvenanceBeforeRegistration(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	manual := toolgateway.Binding{
		Transport:         toolgateway.TransportHTTP,
		HostEndpoint:      "http://127.0.0.1:18082",
		WorkspaceEndpoint: "http://host.docker.internal:18082",
		Token:             "copied-token",
		LifecycleOwner:    toolgateway.LifecycleOwnerServeBoot,
		Source:            toolgateway.SourceBoundMCPListener,
	}
	registered := false
	_, enabled, err := BuildMCPHTTPBinding(
		models.WithActor(context.Background(), models.AgentConfig{ID: "analysis-agent"}),
		&config.Config{},
		mcpTurnContextStoreStub{register: func(context.Context, time.Duration, []string) string {
			registered = true
			return "ctx-token"
		}},
		&Session{AgentID: "analysis-agent", Tools: []ToolDefinition{{Name: "query_entities"}}},
		manual,
		MCPGatewayWorkspaceEndpoint,
	)
	if err == nil || !strings.Contains(err.Error(), "runtime-owned") {
		t.Fatalf("BuildMCPHTTPBinding error = %v, want provenance rejection", err)
	}
	if enabled || registered {
		t.Fatalf("enabled=%t registered=%t, want rejection before registration", enabled, registered)
	}
}

func TestBuildMCPHTTPBindingRequiresFreshNonEmptyContextToken(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	binding, enabled, err := BuildMCPHTTPBinding(
		models.WithActor(context.Background(), models.AgentConfig{ID: "analysis-agent"}),
		&config.Config{},
		mcpTurnContextStoreStub{register: func(context.Context, time.Duration, []string) string { return "" }},
		&Session{AgentID: "analysis-agent", Tools: []ToolDefinition{{Name: "query_entities"}}},
		testToolGatewayBinding("http://127.0.0.1:18082", "http://host.docker.internal:18082", "gateway-token"),
		MCPGatewayWorkspaceEndpoint,
	)
	if err == nil || !strings.Contains(err.Error(), "empty token") {
		t.Fatalf("BuildMCPHTTPBinding error = %v, want empty context rejection", err)
	}
	if enabled || binding.IsRuntimeOwned() {
		t.Fatalf("binding = %#v enabled=%t, want no trusted binding", binding, enabled)
	}
}

func TestBuildMCPHTTPBindingProvenanceInvalidatesOnCopiedFieldMutation(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	binding, enabled, err := BuildMCPHTTPBinding(
		models.WithActor(context.Background(), models.AgentConfig{ID: "analysis-agent"}),
		&config.Config{},
		mcpTurnContextStoreStub{register: func(context.Context, time.Duration, []string) string { return "ctx-token" }},
		&Session{AgentID: "analysis-agent", Tools: []ToolDefinition{{Name: "query_entities"}}},
		testToolGatewayBinding("http://127.0.0.1:18082", "http://host.docker.internal:18082", "gateway-token"),
		MCPGatewayWorkspaceEndpoint,
	)
	if err != nil || !enabled || !binding.IsRuntimeOwned() {
		t.Fatalf("BuildMCPHTTPBinding = %#v, %t, %v", binding, enabled, err)
	}
	binding.URL = "http://127.0.0.1:9999/mcp"
	if binding.IsRuntimeOwned() {
		t.Fatal("mutated local-looking URL retained startup provenance")
	}
}

func TestBuildMCPConfigArg_PreservesNonLoopbackGatewayURLForContainerExecution(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")

	r := &ClaudeCLIRuntime{
		cfg:         &config.Config{},
		toolGateway: testToolGatewayBinding("http://127.0.0.1:8090", "http://orchestrator:8090", "gateway-token"),
		mcpTurns: mcpTurnContextStoreStub{
			register:   func(_ context.Context, _ time.Duration, _ []string) string { return "ctx-token-456" },
			unregister: func(string) {},
		},
	}
	ctx := models.WithActor(context.Background(), models.AgentConfig{
		ID:     "market-research-agent",
		Role:   "market_research",
		FlowID: "discovery",
	})
	s := &Session{
		AgentID: "market-research-agent",
		Tools:   []ToolDefinition{{Name: "query_entities"}},
	}

	cfgJSON, _, enabled, err := r.buildMCPConfigArg(ctx, s)
	if err != nil {
		t.Fatalf("buildMCPConfigArg: %v", err)
	}
	if !enabled {
		t.Fatal("expected MCP config to be enabled")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(cfgJSON), &payload); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	servers := payload["mcpServers"].(map[string]any)
	runtimeTools := servers["runtime-tools"].(map[string]any)
	if got := runtimeTools["url"]; got != "http://orchestrator:8090/mcp" {
		t.Fatalf("url = %#v, want orchestrator MCP endpoint preserved", got)
	}
}

func TestBuildMCPConfigArg_FailsClosedWithoutTurnContextStore(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")

	r := &ClaudeCLIRuntime{
		cfg:         &config.Config{},
		toolGateway: testToolGatewayBinding("http://127.0.0.1:18082", "http://host.docker.internal:18082", "gateway-token"),
	}
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
