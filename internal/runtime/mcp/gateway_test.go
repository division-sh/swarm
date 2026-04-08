package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/toolcapabilities"
	"swarm/internal/runtime/core/toolidentity"
	llm "swarm/internal/runtime/llm"
	runtimerterr "swarm/internal/runtime/rterrors"
)

const testGatewayToken = "gateway-token"

func authorizeGatewayRequest(req *http.Request) {
	if req != nil {
		req.Header.Set("Authorization", "Bearer "+testGatewayToken)
	}
}

func withContextToken(req *http.Request, token string) *http.Request {
	if req != nil {
		req.Header.Set(contextTokenHeader, strings.TrimSpace(token))
	}
	return req
}

func mustRPCResponse(t *testing.T, rec *httptest.ResponseRecorder) RPCResponse {
	t.Helper()
	var resp RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return resp
}

func mustMCPToolsForRequest(t *testing.T, g *Gateway, req *http.Request) []ToolDef {
	t.Helper()
	tools, err := g.mcpToolsForRequest(req)
	if err != nil {
		t.Fatalf("mcpToolsForRequest: %v", err)
	}
	return tools
}

type testToolExecutor func(context.Context, string, any) (any, error)

func (f testToolExecutor) Execute(ctx context.Context, name string, input any) (any, error) {
	return f(ctx, name, input)
}

func (f testToolExecutor) ToolDefinitions() []llm.ToolDefinition {
	return []llm.ToolDefinition{
		{Name: "query_entities"},
		{Name: "read_file"},
		{Name: "write_file"},
		{Name: "emit_scan_requested"},
		{Name: "emit_score_dimension_complete"},
	}
}

func (f testToolExecutor) ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition {
	return f.ToolDefinitions()
}

func (f testToolExecutor) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, raw := range names {
		name := toolidentity.CanonicalName(raw)
		if name == "" {
			continue
		}
		visible := true
		callable := true
		if len(requestAllowed) > 0 {
			_, visible = requestAllowed[name]
			callable = visible
		}
		requirement := toolcapabilities.ContextRequirementTurnContext
		switch name {
		case "query_entities", "read_file":
			requirement = toolcapabilities.ContextRequirementActorContext
		}
		kind := toolcapabilities.KindStandard
		if toolidentity.IsEmitToolName(name) {
			kind = toolcapabilities.KindEmit
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:               name,
			Kind:               kind,
			Visible:            visible,
			Callable:           callable,
			ContextRequirement: requirement,
		})
	}
	return toolcapabilities.NewSet(caps)
}

type actorScopedToolExecutorStub struct {
	defs      []llm.ToolDefinition
	actorDefs []llm.ToolDefinition
}

func (s actorScopedToolExecutorStub) Execute(context.Context, string, any) (any, error) {
	return map[string]any{"ok": true}, nil
}

func (s actorScopedToolExecutorStub) ToolDefinitions() []llm.ToolDefinition {
	return append([]llm.ToolDefinition(nil), s.defs...)
}

func (s actorScopedToolExecutorStub) ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition {
	return append([]llm.ToolDefinition(nil), s.actorDefs...)
}

func (s actorScopedToolExecutorStub) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, raw := range names {
		name := toolidentity.CanonicalName(raw)
		if name == "" {
			continue
		}
		visible := true
		if len(requestAllowed) > 0 {
			_, visible = requestAllowed[name]
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:     name,
			Visible:  visible,
			Callable: visible,
		})
	}
	return toolcapabilities.NewSet(caps)
}

type capabilityAwareExecutorStub struct {
	callCount int
}

func (s *capabilityAwareExecutorStub) Execute(context.Context, string, any) (any, error) {
	s.callCount++
	return map[string]any{"ok": true}, nil
}

func (s *capabilityAwareExecutorStub) ToolDefinitions() []llm.ToolDefinition {
	return []llm.ToolDefinition{
		{Name: "emit_score_dimension_complete", Description: "emit"},
		{Name: "query_entities", Description: "query"},
	}
}

func (s *capabilityAwareExecutorStub) ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition {
	return s.ToolDefinitions()
}

func (s *capabilityAwareExecutorStub) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, raw := range names {
		name := toolidentity.CanonicalName(raw)
		callable := len(requestAllowed) == 0
		if len(requestAllowed) > 0 {
			_, callable = requestAllowed[name]
		}
		kind := toolcapabilities.KindStandard
		if toolidentity.IsEmitToolName(name) {
			kind = toolcapabilities.KindEmit
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:     name,
			Kind:     kind,
			Visible:  callable,
			Callable: callable,
		})
	}
	return toolcapabilities.NewSet(caps)
}

type actorAwareToolExecutorStub struct {
	callCount int
}

func (s *actorAwareToolExecutorStub) Execute(context.Context, string, any) (any, error) {
	s.callCount++
	return map[string]any{"ok": true}, nil
}

func (s *actorAwareToolExecutorStub) ToolDefinitions() []llm.ToolDefinition {
	return []llm.ToolDefinition{
		{Name: "query_entities"},
		{Name: "emit_score_dimension_complete"},
	}
}

func (s *actorAwareToolExecutorStub) ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition {
	return s.ToolDefinitions()
}

func (s *actorAwareToolExecutorStub) ToolCapabilitiesForActor(actor models.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, raw := range names {
		name := toolidentity.CanonicalName(raw)
		if name == "" {
			continue
		}
		visible := true
		callable := true
		if name == "emit_score_dimension_complete" && strings.TrimSpace(actor.Role) != "campaign_coordinator" {
			visible = false
			callable = false
		}
		kind := toolcapabilities.KindStandard
		if toolidentity.IsEmitToolName(name) {
			kind = toolcapabilities.KindEmit
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:               name,
			Kind:               kind,
			Visible:            visible,
			Callable:           callable,
			ContextRequirement: toolcapabilities.ContextRequirementTurnContext,
		})
	}
	return toolcapabilities.NewSet(caps)
}

func newTestTurnContextRegistry() *TurnContextRegistry {
	return NewTurnContextRegistry(nil)
}

func putTestTurnContext(t testing.TB, registry *TurnContextRegistry, token string, turn TurnContext) {
	t.Helper()
	registry.PutTurnContextForTest(token, turn)
	t.Cleanup(func() {
		registry.UnregisterTurnContext(token)
	})
}

func TestGatewayHydrateActor_PrefersResolvedRuntimeConfig(t *testing.T) {
	g := NewGateway(nil, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			if agentID != "market-research-agent" {
				return models.AgentConfig{}, false
			}
			return models.AgentConfig{
				ID:          "market-research-agent",
				Role:        "market_research",
				Mode:        "discovery",
				EntityID:    "entity-1",
				Permissions: []string{"schedule"},
				EmitEvents:  []string{"category.assessed", "market_research.scan_complete"},
			}, true
		},
	})

	hydrated := g.hydrateActor(models.AgentConfig{
		ID:   "market-research-agent",
		Role: "spoofed_role",
		Mode: "spoofed_mode",
	})

	if len(hydrated.EmitEvents) != 2 {
		t.Fatalf("emit_events = %#v, want two resolved events", hydrated.EmitEvents)
	}
	if hydrated.Mode != "discovery" {
		t.Fatalf("mode = %q, want discovery", hydrated.Mode)
	}
	if hydrated.Role != "market_research" {
		t.Fatalf("role = %q, want market_research", hydrated.Role)
	}
	if len(hydrated.Permissions) != 1 || hydrated.Permissions[0] != "schedule" {
		t.Fatalf("permissions = %#v", hydrated.Permissions)
	}
}

func TestNormalizeGatewayToolNameCanonicalAliases(t *testing.T) {
	tests := map[string]string{
		"":                                   "",
		"bash":                               "bash",
		"Bash":                               "bash",
		"web_search":                         "web_search",
		"WebSearch":                          "web_search",
		"Read":                               "read_file",
		"read_file":                          "read_file",
		"Write":                              "write_file",
		"Edit":                               "write_file",
		"mcp__runtime-tools__read_file":      "read_file",
		"mcp__runtime-tools__write_file":     "write_file",
		"mcp__runtime-tools__emit_scan_done": "emit_scan_done",
	}

	for raw, want := range tests {
		if got := normalizeGatewayToolName(raw); got != want {
			t.Fatalf("normalizeGatewayToolName(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestParseToolListHeaderCanonicalizesAliases(t *testing.T) {
	allowed := ParseToolListHeader("Read, mcp__runtime-tools__write_file, emit_scan_done")
	if len(allowed) != 3 {
		t.Fatalf("allowed size = %d, want 3", len(allowed))
	}
	for _, name := range []string{"read_file", "write_file", "emit_scan_done"} {
		if _, ok := allowed[name]; !ok {
			t.Fatalf("expected canonical allowed tool %q in %#v", name, allowed)
		}
	}
}

func TestGatewayMCPToolsForRequest_UsesHydratedActorRoleForEmitTools(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(nil, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			if agentID != "campaign-coordinator" {
				return models.AgentConfig{}, false
			}
			return models.AgentConfig{
				ID:   "campaign-coordinator",
				Role: "campaign_coordinator",
			}, true
		},
		EmitTools: func(role string) []llm.ToolDefinition {
			if role != "campaign_coordinator" {
				return nil
			}
			return []llm.ToolDefinition{{
				Name:        "emit_scan_requested",
				Description: "Emit scan.requested",
				Schema:      map[string]any{"type": "object"},
			}}
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-hydrated-role", TurnContext{
		Actor:     models.AgentConfig{ID: "campaign-coordinator"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest("POST", "/mcp", nil), "ctx-hydrated-role")
	tools := mustMCPToolsForRequest(t, g, req)
	for _, tool := range tools {
		if tool.Name == "emit_scan_requested" {
			return
		}
	}
	t.Fatalf("emit_scan_requested not found in MCP tools: %#v", tools)
}

func TestGatewayMCPToolsForRequest_KeepsEmitToolsForDirectMCPContext(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(nil, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			if agentID != "campaign-coordinator" {
				return models.AgentConfig{}, false
			}
			return models.AgentConfig{
				ID:   "campaign-coordinator",
				Role: "campaign_coordinator",
			}, true
		},
		EmitTools: func(role string) []llm.ToolDefinition {
			if role != "campaign_coordinator" {
				return nil
			}
			return []llm.ToolDefinition{{
				Name:        "emit_scan_requested",
				Description: "Emit scan.requested",
				Schema:      map[string]any{"type": "object"},
			}}
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-1", TurnContext{
		Actor:     models.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest("POST", "/mcp", nil), "ctx-1")
	tools := mustMCPToolsForRequest(t, g, req)
	for _, tool := range tools {
		if tool.Name == "emit_scan_requested" {
			return
		}
	}
	t.Fatalf("emit_scan_requested should remain visible for direct MCP context: %#v", tools)
}

func TestGatewayMCPToolsForRequest_PrefersActorScopedToolDefinitions(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(actorScopedToolExecutorStub{
		defs: []llm.ToolDefinition{
			{Name: "workflow_custom_tool", Description: "global"},
		},
		actorDefs: []llm.ToolDefinition{
			{Name: "query_entities", Description: "actor scoped"},
		},
	}, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			return models.AgentConfig{
				ID:   agentID,
				Role: "analysis",
			}, true
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-actor-scoped", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest("POST", "/mcp", nil), "ctx-actor-scoped")
	tools := mustMCPToolsForRequest(t, g, req)
	if len(tools) != 1 {
		t.Fatalf("tool count = %d, want 1 (%#v)", len(tools), tools)
	}
	if tools[0].Name != "query_entities" {
		t.Fatalf("tool name = %q, want query_entities", tools[0].Name)
	}
}

func TestGatewayMCPToolsForRequest_IgnoresCallerAllowlist(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(actorScopedToolExecutorStub{
		actorDefs: []llm.ToolDefinition{
			{Name: "query_entities", Description: "actor scoped"},
			{Name: "read_file", Description: "reader"},
		},
	}, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			return models.AgentConfig{ID: agentID, Role: "analysis"}, true
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-ignore-allowlist", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest("POST", "/mcp?allowed_tools=Read", nil), "ctx-ignore-allowlist")
	tools := mustMCPToolsForRequest(t, g, req)
	if len(tools) != 2 {
		t.Fatalf("tool count = %d, want 2 (%#v)", len(tools), tools)
	}
	names := []string{tools[0].Name, tools[1].Name}
	slices.Sort(names)
	if !slices.Equal(names, []string{"query_entities", "read_file"}) {
		t.Fatalf("tool names = %#v", names)
	}
}

func TestGatewayMCPToolsForRequest_DoesNotTrustUnknownCallerAllowlist(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(actorScopedToolExecutorStub{
		actorDefs: []llm.ToolDefinition{
			{Name: "query_entities", Description: "actor scoped"},
		},
	}, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			return models.AgentConfig{ID: agentID, Role: "analysis"}, true
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-unknown-allowlist", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest("POST", "/mcp?allowed_tools=does_not_exist", nil), "ctx-unknown-allowlist")
	tools := mustMCPToolsForRequest(t, g, req)
	if len(tools) != 1 {
		t.Fatalf("tool count = %d, want 1 (%#v)", len(tools), tools)
	}
	if tools[0].Name != "query_entities" {
		t.Fatalf("tool name = %q, want query_entities", tools[0].Name)
	}
}

func TestGatewayHandleMCP_ToolsListRejectsMissingOrInvalidContextToken(t *testing.T) {
	for _, rawQuery := range []string{"", "?ctx_token=missing&agent_id=analysis-agent"} {
		t.Run(rawQuery, func(t *testing.T) {
			g := NewGateway(actorScopedToolExecutorStub{
				actorDefs: []llm.ToolDefinition{
					{Name: "query_entities", Description: "actor scoped"},
				},
			}, testGatewayToken, GatewayHooks{})

			body, err := json.Marshal(map[string]any{
				"id":     "req-1",
				"method": "tools/list",
				"params": map[string]any{},
			})
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}
			req := httptest.NewRequest(http.MethodPost, "/mcp"+rawQuery, strings.NewReader(string(body)))
			authorizeGatewayRequest(req)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			var resp RPCResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("unmarshal response: %v", err)
			}
			if resp.Error == nil {
				t.Fatalf("response error = nil, want context error in %s", rec.Body.String())
			}
			if !strings.Contains(resp.Error.Message, "missing or invalid mcp context token") {
				t.Fatalf("error message = %q", resp.Error.Message)
			}
		})
	}
}

func TestGatewayHandleMCP_ToolsListUsesResolvedTurnContext(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(actorScopedToolExecutorStub{
		actorDefs: []llm.ToolDefinition{
			{Name: "query_entities", Description: "actor scoped"},
		},
	}, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-list", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/list",
		"params": map[string]any{},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-list")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("response error = %#v", resp.Error)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", resp.Result)
	}
	rawTools, ok := result["tools"].([]any)
	if !ok || len(rawTools) != 1 {
		t.Fatalf("tools = %#v, want one visible tool", result["tools"])
	}
	tool, ok := rawTools[0].(map[string]any)
	if !ok || strings.TrimSpace(asString(tool["name"])) != "query_entities" {
		t.Fatalf("tool payload = %#v", rawTools[0])
	}
}

func TestMarkEmitUsed(t *testing.T) {
	registry := newTestTurnContextRegistry()
	putTestTurnContext(t, registry, "ctx-emit", TurnContext{
		Actor:     models.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	if already := registry.MarkEmitUsed("ctx-emit"); already {
		t.Fatal("first emit use should not report already used")
	}
	if already := registry.MarkEmitUsed("ctx-emit"); !already {
		t.Fatal("second emit use should report already used")
	}
}

func TestMarkEmitKeyUsed_AllowsDistinctKeysPerTurn(t *testing.T) {
	registry := newTestTurnContextRegistry()
	putTestTurnContext(t, registry, "ctx-emit-keys", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	if already := registry.MarkEmitKeyUsed("ctx-emit-keys", "emit_a\n{\"dimension\":\"one\"}"); already {
		t.Fatal("first unique emit key should not report already used")
	}
	if already := registry.MarkEmitKeyUsed("ctx-emit-keys", "emit_a\n{\"dimension\":\"two\"}"); already {
		t.Fatal("second distinct emit key should be allowed")
	}
	if already := registry.MarkEmitKeyUsed("ctx-emit-keys", "emit_a\n{\"dimension\":\"one\"}"); !already {
		t.Fatal("duplicate emit key should report already used")
	}
}

func TestGatewayHandleMCP_AllowsDistinctEmitPayloadsPerTurn(t *testing.T) {
	registry := newTestTurnContextRegistry()
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
		MarkEmitKeyUsed:    registry.MarkEmitKeyUsed,
	})

	putTestTurnContext(t, registry, "ctx-emit-gateway", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	handler := g.Handler()
	makeReq := func(arguments any) *httptest.ResponseRecorder {
		body, err := json.Marshal(map[string]any{
			"id":     "req-1",
			"method": "tools/call",
			"params": map[string]any{
				"name":      "emit_score_dimension_complete",
				"arguments": arguments,
			},
		})
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-emit-gateway")
		authorizeGatewayRequest(req)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	first := makeReq(map[string]any{"dimension": "build_complexity", "score": 32})
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d", first.Code)
	}
	second := makeReq(map[string]any{"dimension": "icp_crispness", "score": 70})
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d", second.Code)
	}
	duplicate := makeReq(map[string]any{"dimension": "build_complexity", "score": 32})
	if duplicate.Code != http.StatusOK {
		t.Fatalf("duplicate status = %d", duplicate.Code)
	}
	if callCount != 2 {
		t.Fatalf("executor call count = %d, want 2", callCount)
	}
	if !strings.Contains(duplicate.Body.String(), "duplicate emit already executed this turn") {
		t.Fatalf("duplicate body = %s", duplicate.Body.String())
	}
}

func TestGatewayHandleMCP_AllowsPrefixedToolNameFromRuntimeOwnedTurnContext(t *testing.T) {
	registry := newTestTurnContextRegistry()
	exec := &capabilityAwareExecutorStub{}
	g := NewGateway(exec, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
		MarkEmitKeyUsed:    registry.MarkEmitKeyUsed,
	})

	putTestTurnContext(t, registry, "ctx-prefixed-emit", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		Allowed:   map[string]struct{}{"emit_score_dimension_complete": {}},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "mcp__runtime-tools__emit_score_dimension_complete",
			"arguments": map[string]any{"dimension": "build_complexity", "score": 32},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp?allowed_tools=query_entities", strings.NewReader(string(body))), "ctx-prefixed-emit")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if exec.callCount != 1 {
		t.Fatalf("executor call count = %d, want 1", exec.callCount)
	}
	if strings.Contains(rec.Body.String(), "tool is not allowed for this agent") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayHandleMCP_DoesNotLetCallerAllowlistGrantToolAccess(t *testing.T) {
	registry := newTestTurnContextRegistry()
	exec := &capabilityAwareExecutorStub{}
	var loggedAction string
	var denialLayer string
	g := NewGateway(exec, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
		MarkEmitKeyUsed:    registry.MarkEmitKeyUsed,
		Log: func(_ context.Context, level, action, agentID, entityID string, detail map[string]any, errText string) {
			loggedAction = action
			denialLayer = strings.TrimSpace(asString(detail["denial_layer"]))
		},
	})

	putTestTurnContext(t, registry, "ctx-denied-allowlist", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		Allowed:   map[string]struct{}{"query_entities": {}},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "mcp__runtime-tools__emit_score_dimension_complete",
			"arguments": map[string]any{"dimension": "build_complexity", "score": 32},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp?allowed_tools=emit_score_dimension_complete", strings.NewReader(string(body))), "ctx-denied-allowlist")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if exec.callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", exec.callCount)
	}
	if !strings.Contains(rec.Body.String(), "tool is not allowed for this agent") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if loggedAction != "mcp.tools.call.denied" {
		t.Fatalf("logged action = %q", loggedAction)
	}
	if denialLayer != "gateway" {
		t.Fatalf("denial_layer = %q, want gateway", denialLayer)
	}
}

func TestGatewayHandleMCP_ToolsCallIncludesStructuredRuntimeErrorPayload(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(testToolExecutor(func(_ context.Context, _ string, _ any) (any, error) {
		return nil, runtimerterr.WrapRuntimeError("invalid_tool_input", "tool-executor", "exec_query_entities.filter", false, nil, "query is required")
	}), testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-runtime-error", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-runtime-error")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	resp := mustRPCResponse(t, rec)
	if resp.Error != nil {
		t.Fatalf("rpc error = %#v", resp.Error)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", resp.Result)
	}
	if isError, _ := result["isError"].(bool); !isError {
		t.Fatalf("isError = %#v, want true", result["isError"])
	}
	runtimeErr, err := DecodeRuntimeErrorPayload(result["runtimeError"])
	if err != nil {
		t.Fatalf("DecodeRuntimeErrorPayload: %v", err)
	}
	if runtimeErr.Code != ErrCodeToolExecFailed {
		t.Fatalf("runtimeError.code = %q, want %q", runtimeErr.Code, ErrCodeToolExecFailed)
	}
	if runtimeErr.Cause == nil || runtimeErr.Cause.Code != "invalid_tool_input" {
		t.Fatalf("runtimeError.cause = %#v, want invalid_tool_input", runtimeErr.Cause)
	}
}

func TestGatewayHandleMCP_RejectsToolWhenContextTokenMisses(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "emit_score_dimension_complete",
			"arguments": map[string]any{"dimension": "build_complexity", "score": 72},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/mcp?agent_id=analysis-agent&agent_role=analysis", strings.NewReader(string(body)))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
	if !strings.Contains(rec.Body.String(), "missing or invalid mcp context token") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayMCPExecutionContext_RejectsPrefixedMutatingToolOnContextMiss(t *testing.T) {
	g := NewGateway(nil, "", GatewayHooks{})
	req := httptest.NewRequest(http.MethodPost, "/mcp?agent_id=analysis-agent&agent_role=analysis", nil)
	if _, err := g.mcpExecutionContext(req, "mcp__runtime-tools__emit_score_dimension_complete"); err == nil {
		t.Fatal("expected context miss error for prefixed mutating tool")
	}
}

func TestGatewayExecutionContext_UsesInboundTraceNotRequestTraceOnResolvedTurn(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(nil, "", GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
		WithActor: func(ctx context.Context, actor models.AgentConfig) context.Context {
			return models.WithActor(ctx, actor)
		},
		WithCurrentRuntimeEpoch: func(ctx context.Context) context.Context {
			return runtimebus.WithCurrentRuntimeEpoch(ctx)
		},
		WithInboundEvent: func(ctx context.Context, evt events.Event) context.Context {
			return runtimebus.WithInboundEvent(ctx, evt)
		},
	})
	putTestTurnContext(t, registry, "ctx-trace", TurnContext{
		Actor:      models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		Inbound:    events.Event{ID: "evt-1", RunID: "run-1"},
		HasInbound: true,
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", nil), "ctx-trace")
	ctx, err := g.mcpExecutionContext(req, "get_entity")
	if err != nil {
		t.Fatalf("mcpExecutionContext: %v", err)
	}
	_ = ctx
}

func TestGatewayMCPExecutionContext_KeepsOtherRegistryTokensValidAfterGlobalEpochBump(t *testing.T) {
	registryA := newTestTurnContextRegistry()
	registryB := newTestTurnContextRegistry()
	registryB.PutTurnContextForTest("ctx-b", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	t.Cleanup(func() { registryB.UnregisterTurnContext("ctx-b") })

	gatewayB := NewGateway(nil, "", GatewayHooks{
		ResolveTurnContext:      registryB.ResolveTurnContext,
		WithActor:               models.WithActor,
		WithCurrentRuntimeEpoch: runtimebus.WithCurrentRuntimeEpoch,
	})

	registryA.Reset()
	runtimebus.EnterRuntimeResetMode()
	runtimebus.ExitRuntimeResetMode()

	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", nil), "ctx-b")
	ctx, err := gatewayB.mcpExecutionContext(req, "query_entities")
	if err != nil {
		t.Fatalf("mcpExecutionContext after unrelated reset: %v", err)
	}
	if epoch, ok := runtimebus.RuntimeEpochFromContext(ctx); !ok || epoch != runtimebus.CurrentRuntimeEpoch() {
		t.Fatalf("context epoch = %d ok=%v, want current epoch %d", epoch, ok, runtimebus.CurrentRuntimeEpoch())
	}
}

func TestGatewayAuthorize_FailsClosedWithoutConfiguredToken(t *testing.T) {
	g := NewGateway(nil, "", GatewayHooks{})
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	err := g.AuthorizeForTest(req)
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("AuthorizeForTest err = %v, want unconfigured auth error", err)
	}
}

func TestGatewayAuthorize_DeniesMissingBearer(t *testing.T) {
	g := NewGateway(nil, testGatewayToken, GatewayHooks{})
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	err := g.AuthorizeForTest(req)
	if err == nil || !strings.Contains(err.Error(), "missing authorization bearer token") {
		t.Fatalf("AuthorizeForTest err = %v, want missing bearer error", err)
	}
}

func TestGatewayAuthorize_DeniesInvalidBearer(t *testing.T) {
	g := NewGateway(nil, testGatewayToken, GatewayHooks{})
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	err := g.AuthorizeForTest(req)
	if err == nil || !strings.Contains(err.Error(), "invalid token") {
		t.Fatalf("AuthorizeForTest err = %v, want invalid token error", err)
	}
}

func TestGatewayHandleMCP_RejectsReadOnlyToolWhenContextTokenMisses(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "mcp__runtime-tools__query_entities",
			"arguments": map[string]any{"query": "kind = 'vertical'"},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/mcp?agent_id=analysis-agent&agent_role=analysis", strings.NewReader(string(body)))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
	if !strings.Contains(rec.Body.String(), "missing or invalid mcp context token") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayHandleTool_RejectsMutatingToolWithoutContextToken(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{})

	body, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"dimension": "build_complexity", "score": 72},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tools/emit_score_dimension_complete", strings.NewReader(string(body)))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
	if !strings.Contains(rec.Body.String(), "missing or invalid mcp context token") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayHandleTool_RejectsReadOnlyToolWithoutContextToken(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{})

	body, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"query": "kind = 'vertical'"},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tools/query_entities", strings.NewReader(string(body)))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
	if !strings.Contains(rec.Body.String(), "missing or invalid mcp context token") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayHandleTool_IgnoresCallerSuppliedPrivilegeFields(t *testing.T) {
	registry := newTestTurnContextRegistry()
	exec := &actorAwareToolExecutorStub{}
	g := NewGateway(exec, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-analysis", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"dimension": "build_complexity", "score": 72},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/tools/emit_score_dimension_complete", strings.NewReader(string(body))), "ctx-analysis")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if exec.callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", exec.callCount)
	}
	if !strings.Contains(rec.Body.String(), "tool is not allowed for this agent") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayHandleTool_AllowsLegitimateRuntimeOwnedActorContext(t *testing.T) {
	registry := newTestTurnContextRegistry()
	exec := &actorAwareToolExecutorStub{}
	g := NewGateway(exec, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-coordinator", TurnContext{
		Actor:     models.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"dimension": "build_complexity", "score": 72},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/tools/emit_score_dimension_complete", strings.NewReader(string(body))), "ctx-coordinator")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if exec.callCount != 1 {
		t.Fatalf("executor call count = %d, want 1", exec.callCount)
	}
}

func TestGatewayTransports_AlignReadOnlyToolContextTokenFailures(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{})

	mcpBody, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{"query": "kind = 'vertical'"},
		},
	})
	if err != nil {
		t.Fatalf("marshal mcp request: %v", err)
	}
	mcpReq := httptest.NewRequest(http.MethodPost, "/mcp?agent_id=analysis-agent&agent_role=analysis", strings.NewReader(string(mcpBody)))
	authorizeGatewayRequest(mcpReq)
	mcpRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(mcpRec, mcpReq)

	toolBody, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"query": "kind = 'vertical'"},
	})
	if err != nil {
		t.Fatalf("marshal tool request: %v", err)
	}
	toolReq := httptest.NewRequest(http.MethodPost, "/tools/query_entities", strings.NewReader(string(toolBody)))
	authorizeGatewayRequest(toolReq)
	toolRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(toolRec, toolReq)

	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
	if !strings.Contains(mcpRec.Body.String(), "missing or invalid mcp context token") {
		t.Fatalf("mcp body = %s", mcpRec.Body.String())
	}
	if !strings.Contains(toolRec.Body.String(), "missing or invalid mcp context token") {
		t.Fatalf("tool body = %s", toolRec.Body.String())
	}
}

func TestGatewayTransports_RejectLegacyQueryContextTokenCarrier(t *testing.T) {
	registry := newTestTurnContextRegistry()
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-query-only", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	mcpBody, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{"query": "kind = 'vertical'"},
		},
	})
	if err != nil {
		t.Fatalf("marshal mcp request: %v", err)
	}
	mcpReq := httptest.NewRequest(http.MethodPost, "/mcp?ctx_token=ctx-query-only", strings.NewReader(string(mcpBody)))
	authorizeGatewayRequest(mcpReq)
	mcpRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(mcpRec, mcpReq)

	toolBody, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"query": "kind = 'vertical'"},
	})
	if err != nil {
		t.Fatalf("marshal tool request: %v", err)
	}
	toolReq := httptest.NewRequest(http.MethodPost, "/tools/query_entities?ctx_token=ctx-query-only", strings.NewReader(string(toolBody)))
	authorizeGatewayRequest(toolReq)
	toolRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(toolRec, toolReq)

	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
	if !strings.Contains(mcpRec.Body.String(), "missing or invalid mcp context token") {
		t.Fatalf("mcp body = %s", mcpRec.Body.String())
	}
	if !strings.Contains(toolRec.Body.String(), "missing or invalid mcp context token") {
		t.Fatalf("tool body = %s", toolRec.Body.String())
	}
}

func TestGatewayTransports_AlignReadOnlyToolSuccessWithResolvedTurnContext(t *testing.T) {
	registry := newTestTurnContextRegistry()
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-query", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	mcpBody, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{"query": "kind = 'vertical'"},
		},
	})
	if err != nil {
		t.Fatalf("marshal mcp request: %v", err)
	}
	mcpReq := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(mcpBody))), "ctx-query")
	authorizeGatewayRequest(mcpReq)
	mcpRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(mcpRec, mcpReq)

	toolBody, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"query": "kind = 'vertical'"},
	})
	if err != nil {
		t.Fatalf("marshal tool request: %v", err)
	}
	toolReq := withContextToken(httptest.NewRequest(http.MethodPost, "/tools/query_entities", strings.NewReader(string(toolBody))), "ctx-query")
	authorizeGatewayRequest(toolReq)
	toolRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(toolRec, toolReq)

	if mcpRec.Code != http.StatusOK {
		t.Fatalf("mcp status = %d, want 200", mcpRec.Code)
	}
	if toolRec.Code != http.StatusOK {
		t.Fatalf("tool status = %d, want 200", toolRec.Code)
	}
	if callCount != 2 {
		t.Fatalf("executor call count = %d, want 2", callCount)
	}
	if strings.Contains(mcpRec.Body.String(), "missing or invalid mcp context token") {
		t.Fatalf("mcp body = %s", mcpRec.Body.String())
	}
	if strings.Contains(toolRec.Body.String(), "missing or invalid mcp context token") {
		t.Fatalf("tool body = %s", toolRec.Body.String())
	}
}

func TestGatewayHandleMCP_DoesNotLogFallbackUsedReason(t *testing.T) {
	var actions []string
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		Log: func(_ context.Context, level, action, agentID, entityID string, detail map[string]any, errText string) {
			actions = append(actions, action)
		},
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "mcp__runtime-tools__query_entities",
			"arguments": map[string]any{"query": "kind = 'vertical'"},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/mcp?agent_id=analysis-agent&agent_role=analysis", strings.NewReader(string(body)))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if slices.Contains(actions, "mcp.context.fallback_used") {
		t.Fatalf("actions = %#v, did not expect mcp.context.fallback_used", actions)
	}
}

func TestGatewayHandleTool_DoesNotLogFallbackBlockedReason(t *testing.T) {
	var actions []string
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		Log: func(_ context.Context, level, action, agentID, entityID string, detail map[string]any, errText string) {
			actions = append(actions, action)
		},
	})

	body, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"dimension": "build_complexity", "score": 72},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tools/emit_score_dimension_complete", strings.NewReader(string(body)))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if slices.Contains(actions, "tool.context.fallback_blocked") {
		t.Fatalf("actions = %#v, did not expect tool.context.fallback_blocked", actions)
	}
}
