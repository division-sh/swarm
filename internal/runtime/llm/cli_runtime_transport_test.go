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
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/google/uuid"
)

type mcpTurnContextStoreStub struct {
	register        func(context.Context, time.Duration, []string) string
	registerSurface func(context.Context, time.Duration, managedcapabilities.Surface) string
	resolve         func(string) (managedcapabilities.Surface, bool)
	unregister      func(string)
}

func (s mcpTurnContextStoreStub) RegisterTurnContextWithCapabilitySurface(ctx context.Context, ttl time.Duration, surface managedcapabilities.Surface) string {
	if s.registerSurface != nil {
		return s.registerSurface(ctx, ttl, surface)
	}
	if s.register == nil {
		return ""
	}
	var planned []string
	for _, tool := range surface.Tools {
		if !tool.Capability.Visible || !tool.Capability.Callable {
			continue
		}
		for _, binding := range tool.Bindings {
			if binding.Kind == managedcapabilities.BindingMCPTool {
				planned = append(planned, tool.Name)
				break
			}
		}
	}
	slices.Sort(planned)
	return s.register(ctx, ttl, planned)
}

func (s mcpTurnContextStoreStub) ResolveManagedCapabilitySurface(token string) (managedcapabilities.Surface, bool) {
	if s.resolve != nil {
		return s.resolve(token)
	}
	return managedcapabilities.Surface{}, false
}

func (s mcpTurnContextStoreStub) RegisterConversationForkSandboxTurnContext(ctx context.Context, ttl time.Duration, tools []string) string {
	if s.register == nil {
		return ""
	}
	return s.register(ctx, ttl, tools)
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

func TestClaudeToolArgumentsProjectExactManagedSurface(t *testing.T) {
	actor := models.AgentConfig{ID: "analysis-agent", NativeTools: models.NativeToolConfig{FileIO: true, Bash: true, WebSearch: true}}
	actor.Identity = testAgentIdentity(actor.ID, "")
	tools := []ToolDefinition{{Name: "query_metrics"}, {Name: "emit_category_assessed"}}
	ctx, surface := testManagedCLISurfaceContext(t, actor, tools)
	projection, err := projectClaudeInvocationTools(ctx, actor, tools)
	if err != nil {
		t.Fatalf("projectClaudeInvocationTools: %v", err)
	}
	allowedNames := projection.PermissionAdmission
	for _, name := range []string{"Bash", "Edit", "Read", "WebFetch", "WebSearch", "Write", "mcp__runtime-tools__emit_category_assessed", "mcp__runtime-tools__query_metrics", "ExitPlanMode"} {
		if !slices.Contains(allowedNames, name) {
			t.Fatalf("planned binding %q missing from allowed tools %#v", name, allowedNames)
		}
	}
	for _, name := range []string{"read_file", "write_file", "bash", "web_search", "query_metrics"} {
		if slices.Contains(allowedNames, name) {
			t.Fatalf("canonical name %q leaked as a second provider binding in %#v", name, allowedNames)
		}
	}
	if !slices.Equal(projection.BuiltinSelection, []string{"Bash", "Edit", "ExitPlanMode", "Read", "WebFetch", "WebSearch", "Write"}) {
		t.Fatalf("builtin selection = %#v", projection.BuiltinSelection)
	}
	for _, name := range projection.BuiltinSelection {
		if strings.HasPrefix(name, "mcp__") {
			t.Fatalf("MCP tool %q leaked into builtin selection", name)
		}
	}
	if got := surface.PlannedBindingNames(managedcapabilities.BindingMCPTool); !slices.Equal(got, []string{"mcp__runtime-tools__emit_category_assessed", "mcp__runtime-tools__query_metrics"}) {
		t.Fatalf("planned MCP gateway bindings = %#v", got)
	}
}

func TestClaudeInvocationToolProjectionCoversEveryNativeFamily(t *testing.T) {
	for _, test := range []struct {
		name     string
		native   models.NativeToolConfig
		expected []string
	}{
		{name: "zero", expected: []string{"ExitPlanMode"}},
		{name: "bash", native: models.NativeToolConfig{Bash: true}, expected: []string{"Bash", "ExitPlanMode"}},
		{name: "web", native: models.NativeToolConfig{WebSearch: true}, expected: []string{"ExitPlanMode", "WebFetch", "WebSearch"}},
		{name: "file", native: models.NativeToolConfig{FileIO: true}, expected: []string{"Edit", "ExitPlanMode", "Read", "Write"}},
		{
			name: "all",
			native: models.NativeToolConfig{
				Bash:      true,
				WebSearch: true,
				FileIO:    true,
			},
			expected: []string{"Bash", "Edit", "ExitPlanMode", "Read", "WebFetch", "WebSearch", "Write"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			actor := models.AgentConfig{ID: "native-agent", NativeTools: test.native}
			actor.Identity = testAgentIdentity(actor.ID, "")
			tools := []ToolDefinition{{Name: "emit_done"}}
			ctx, _ := testManagedCLISurfaceContext(t, actor, tools)
			projection, err := projectClaudeInvocationTools(ctx, actor, tools)
			if err != nil {
				t.Fatalf("projectClaudeInvocationTools: %v", err)
			}
			if !slices.Equal(projection.BuiltinSelection, test.expected) {
				t.Fatalf("builtin selection = %#v, want %#v", projection.BuiltinSelection, test.expected)
			}
			if slices.Contains(projection.BuiltinSelection, "mcp__runtime-tools__emit_done") {
				t.Fatalf("MCP tool leaked into builtin selection: %#v", projection.BuiltinSelection)
			}
			if !slices.Contains(projection.PermissionAdmission, "mcp__runtime-tools__emit_done") {
				t.Fatalf("MCP tool missing from permission admission: %#v", projection.PermissionAdmission)
			}
		})
	}
}

func TestClaudeInvocationToolProjectionRequiresOneExactOwner(t *testing.T) {
	actor := models.AgentConfig{ID: "fork-agent"}
	tools := []ToolDefinition{{Name: "emit_event"}}
	managedCtx, _ := testManagedCLISurfaceContext(t, actor, tools)
	forkCtx := testConversationForkInvocationContext(tools)

	for _, test := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "missing", ctx: context.Background()},
		{name: "fork_authority_without_policy", ctx: runtimeeffects.WithAuthority(context.Background(), testConversationForkAuthority())},
		{name: "policy_without_fork_authority", ctx: WithConversationForkSandboxInvocationPolicy(context.Background(), toolNames(tools))},
		{name: "managed_and_fork_policy", ctx: WithConversationForkSandboxInvocationPolicy(managedCtx, toolNames(tools))},
		{name: "fork_policy_tool_mismatch", ctx: WithConversationForkSandboxInvocationPolicy(runtimeeffects.WithAuthority(context.Background(), testConversationForkAuthority()), []string{"different_tool"})},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := projectClaudeInvocationTools(test.ctx, actor, tools); err == nil {
				t.Fatal("invalid Claude invocation authority was accepted")
			}
		})
	}

	if _, err := projectClaudeInvocationTools(forkCtx, actor, tools); err != nil {
		t.Fatalf("valid fork authority rejected: %v", err)
	}
}

func TestClaudeInvocationToolProjectionDoesNotReconstructForkBuiltinsFromSourceActor(t *testing.T) {
	actor := models.AgentConfig{
		ID: "source-agent",
		NativeTools: models.NativeToolConfig{
			Bash:      true,
			WebSearch: true,
			FileIO:    true,
		},
	}
	tools := []ToolDefinition{{Name: "fork_snapshot_read_entities"}, {Name: "emit_event"}}
	projection, err := projectClaudeInvocationTools(testConversationForkInvocationContext(tools), actor, tools)
	if err != nil {
		t.Fatalf("projectClaudeInvocationTools: %v", err)
	}
	if !slices.Equal(projection.BuiltinSelection, []string{"ExitPlanMode"}) {
		t.Fatalf("fork builtin selection = %#v, want only explicit control", projection.BuiltinSelection)
	}
	for _, leaked := range []string{"Bash", "Edit", "Read", "WebFetch", "WebSearch", "Write"} {
		if slices.Contains(projection.PermissionAdmission, leaked) {
			t.Fatalf("source actor builtin %q leaked into fork permission admission %#v", leaked, projection.PermissionAdmission)
		}
	}
}

func TestClaudeInvocationToolProjectionRejectsUnknownForkBuiltinEvidence(t *testing.T) {
	actor := models.AgentConfig{ID: "source-agent"}
	tools := []ToolDefinition{{Name: "fork_snapshot_read_entities"}}
	projection, err := projectClaudeInvocationTools(testConversationForkInvocationContext(tools), actor, tools)
	if err != nil {
		t.Fatalf("projectClaudeInvocationTools: %v", err)
	}
	if err := validateClaudeInvocationProviderBuiltins(projection, &Response{
		ProviderVisibleTools: []string{"ExitPlanMode", "FutureUnplannedBuiltin"},
	}); err == nil || !strings.Contains(err.Error(), "FutureUnplannedBuiltin") {
		t.Fatalf("unknown provider builtin validation error = %v", err)
	}
}

func testConversationForkInvocationContext(tools []ToolDefinition) context.Context {
	ctx := runtimeeffects.WithAuthority(context.Background(), testConversationForkAuthority())
	return WithConversationForkSandboxInvocationPolicy(ctx, toolNames(tools))
}

func testConversationForkAuthority() runtimeeffects.Authority {
	forkTurnID := uuid.NewString()
	return runtimeeffects.Authority{
		Kind:            runtimeeffects.AuthorityConversationForkChat,
		ID:              forkTurnID,
		ExecutionOwner:  "forkchat-test-owner",
		LeaseExpiresAt:  time.Now().UTC().Add(time.Minute),
		FenceGeneration: 1,
		ExecutionMode:   runtimeeffects.ExecutionModeLive,
		ForkChat: runtimeeffects.ConversationForkChatAuthority{
			ForkTurnID:          forkTurnID,
			ForkID:              uuid.NewString(),
			SourceRunID:         uuid.NewString(),
			BundleHash:          "bundle-v1:sha256:" + strings.Repeat("a", 64),
			ActorTokenID:        "actor-token",
			RequestOccurrenceID: uuid.NewString(),
			RequestHash:         "request-hash",
		},
	}
}

func TestManagedCapabilitySurfaceDoesNotInjectRetiredLegacyEntityTools(t *testing.T) {
	actor := models.AgentConfig{ID: "validation-orchestrator", Role: "validation_orchestrator"}
	actor.Identity = testAgentIdentity(actor.ID, "")
	tools := []ToolDefinition{
		{Name: "read_validation_case"},
		{Name: "read_validation_case_business_brief"},
		{Name: "save_validation_case_business_brief"},
		{Name: "update_validation_case_business_brief_summary"},
		{Name: "emit_validation_package_ready"},
	}

	ctx, surface := testManagedCLISurfaceContext(t, actor, tools)
	projection, err := projectClaudeInvocationTools(ctx, actor, tools)
	if err != nil {
		t.Fatalf("projectClaudeInvocationTools: %v", err)
	}
	allowed := projection.PermissionAdmission
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
		if _, present := surface.Capability(name); present {
			t.Fatalf("legacy entity tool %q appeared in CLI runtime surface: %#v", name, surface)
		}
		if slices.Contains(allowed, "mcp__runtime-tools__"+name) || slices.Contains(allowed, name) {
			t.Fatalf("legacy entity tool %q appeared in Claude allowed tools: %#v", name, allowed)
		}
	}
	for _, name := range []string{"read_validation_case", "save_validation_case_business_brief", "emit_validation_package_ready"} {
		if _, present := surface.Capability(name); !present {
			t.Fatalf("generated/runtime tool %q missing from managed surface", name)
		}
		if !slices.Contains(allowed, "mcp__runtime-tools__"+name) && !slices.Contains(allowed, name) {
			t.Fatalf("generated/runtime tool %q missing from Claude allowed tools: %#v", name, allowed)
		}
	}
}

func testManagedCLISurfaceContext(t *testing.T, actor models.AgentConfig, tools []ToolDefinition) (context.Context, managedcapabilities.Surface) {
	t.Helper()
	if strings.TrimSpace(actor.ID) == "" {
		actor.ID = "test-agent"
	}
	if actor.Identity.IsZero() {
		actor.Identity = testAgentIdentity(actor.ID, actor.FlowPath)
		actor.FlowPath = actor.Identity.FlowInstance()
	}
	caps := make([]toolcapabilities.Capability, 0, len(tools))
	for _, tool := range tools {
		caps = append(caps, toolcapabilities.Capability{Name: tool.Name, Visible: true, Callable: true})
	}
	ctx := models.WithActor(context.Background(), actor)
	surface, err := managedCapabilityPlanForTest(ctx, &ClaudeCLIRuntime{}, "test", tools, toolcapabilities.NewSet(caps), managedcapabilities.Authority{
		Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(), ExecutionKind: managedcapabilities.ExecutionNormalAgent,
		ExecutionAuthorityID: uuid.NewString(), StartupOwnerID: "test-owner", StartupGeneration: 1,
	})
	if err != nil {
		t.Fatalf("managedCapabilityPlan: %v", err)
	}
	return managedcapabilities.WithContext(ctx, surface), surface
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
	actor := models.AgentConfig{
		ID:     "market-research-agent",
		Role:   "market_research",
		FlowID: "discovery",
	}
	s := &Session{
		AgentID: "market-research-agent",
		Tools: []ToolDefinition{
			{Name: "emit_category_assessed"},
			{Name: "query_entities"},
		},
	}
	ctx, _ := testManagedCLISurfaceContext(t, actor, s.Tools)

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
	actor := models.AgentConfig{
		ID: "analysis-agent",
		NativeTools: models.NativeToolConfig{
			FileIO:    true,
			Bash:      true,
			WebSearch: true,
		},
	}
	session := &Session{
		AgentID: "analysis-agent",
		Tools: []ToolDefinition{
			{Name: "read_file"},
			{Name: "write_file"},
			{Name: "bash"},
			{Name: "web_search"},
		},
	}
	ctx, _ := testManagedCLISurfaceContext(t, actor, session.Tools)
	binding, enabled, err := BuildMCPHTTPBinding(
		ctx,
		&config.Config{},
		mcpTurnContextStoreStub{
			register: func(_ context.Context, _ time.Duration, _ []string) string {
				registered = true
				return "ctx-token"
			},
		},
		session,
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
	actor := models.AgentConfig{ID: "analysis-agent"}
	session := &Session{AgentID: "analysis-agent", Tools: []ToolDefinition{{Name: "query_entities"}}}
	ctx, _ := testManagedCLISurfaceContext(t, actor, session.Tools)
	_, enabled, err := BuildMCPHTTPBinding(
		ctx,
		&config.Config{},
		mcpTurnContextStoreStub{register: func(context.Context, time.Duration, []string) string {
			registered = true
			return "ctx-token"
		}},
		session,
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
	actor := models.AgentConfig{ID: "analysis-agent"}
	session := &Session{AgentID: "analysis-agent", Tools: []ToolDefinition{{Name: "query_entities"}}}
	ctx, _ := testManagedCLISurfaceContext(t, actor, session.Tools)
	binding, enabled, err := BuildMCPHTTPBinding(
		ctx,
		&config.Config{},
		mcpTurnContextStoreStub{register: func(context.Context, time.Duration, []string) string { return "" }},
		session,
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
	actor := models.AgentConfig{ID: "analysis-agent"}
	session := &Session{AgentID: "analysis-agent", Tools: []ToolDefinition{{Name: "query_entities"}}}
	ctx, _ := testManagedCLISurfaceContext(t, actor, session.Tools)
	binding, enabled, err := BuildMCPHTTPBinding(
		ctx,
		&config.Config{},
		mcpTurnContextStoreStub{register: func(context.Context, time.Duration, []string) string { return "ctx-token" }},
		session,
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
	actor := models.AgentConfig{
		ID:     "market-research-agent",
		Role:   "market_research",
		FlowID: "discovery",
	}
	s := &Session{
		AgentID: "market-research-agent",
		Tools:   []ToolDefinition{{Name: "query_entities"}},
	}
	ctx, _ := testManagedCLISurfaceContext(t, actor, s.Tools)

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
	actor := models.AgentConfig{
		ID:   "market-research-agent",
		Role: "market_research",
	}
	s := &Session{
		AgentID: "market-research-agent",
		Tools:   []ToolDefinition{{Name: "query_entities"}},
	}
	ctx, _ := testManagedCLISurfaceContext(t, actor, s.Tools)

	_, _, _, err := r.buildMCPConfigArg(ctx, s)
	if err == nil || !strings.Contains(err.Error(), "mcp turn context store is required") {
		t.Fatalf("buildMCPConfigArg err = %v, want missing store error", err)
	}
}

func TestValidateCLIResponseToolCallsForTurn_FailsClosedForNonEmitToolOutsideObservedSurface(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		NativeTools:   models.NativeToolConfig{FileIO: true},
	}
	err := validateCLIResponseToolCallsForTurn(context.Background(), actor, []ToolDefinition{
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

func TestValidateCLIResponseToolCallsForTurn_ManagedTurnRejectsMissingCapabilitySurface(t *testing.T) {
	identity := testMemoryIdentity("market-research-agent", "market/instance-1")
	actor := models.AgentConfig{ID: "market-research-agent", Identity: identity, FlowPath: identity.FlowInstance()}
	admission, err := managedexecution.New(managedexecution.KindNormalRuntime, "runtime-owner", 1, "", "actors", "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", nil)
	if err != nil {
		t.Fatalf("build managed execution admission: %v", err)
	}
	ctx := managedexecution.WithAdmission(context.Background(), admission)
	ctx = runtimeeffects.WithLifecycleToken(ctx, runtimeeffects.LifecycleToken{Identity: identity, RuntimeEpoch: 1, AgentID: actor.ID, Generation: 1})
	err = validateCLIResponseToolCallsForTurn(ctx, actor, []ToolDefinition{{Name: "query_entities"}}, &Response{
		ToolCalls: []ToolCall{{Name: "query_entities"}},
	})
	if err == nil || !strings.Contains(err.Error(), "missing its exact capability surface") {
		t.Fatalf("managed missing-surface validation err = %v", err)
	}
}

func TestValidateCLIResponseToolCallsForTurn_AllowsObservedMCPToolAndEmitFallback(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		NativeTools:   models.NativeToolConfig{FileIO: true},
	}
	err := validateCLIResponseToolCallsForTurn(context.Background(), actor, []ToolDefinition{
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

func TestValidateCLIResponseToolCallsForTurn_ForkSandboxAllowsPlannedToolWhenObservedMetadataIsAbsent(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
	}
	err := validateCLIResponseToolCallsForTurn(context.Background(), actor, []ToolDefinition{
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
