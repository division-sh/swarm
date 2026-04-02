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
	models "swarm/internal/runtime/core/actors"
	runtimebus "swarm/internal/runtime/bus"
	llm "swarm/internal/runtime/llm"
)

type testToolExecutor func(context.Context, string, any) (any, error)

func (f testToolExecutor) Execute(ctx context.Context, name string, input any) (any, error) {
	return f(ctx, name, input)
}

func TestGatewayHydrateActorMergesResolvedConfig(t *testing.T) {
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
				Config:      []byte(`{"emit_events":["category.assessed","market_research.scan_complete"]}`),
			}, true
		},
	})

	hydrated := g.hydrateActor(models.AgentConfig{
		ID:   "market-research-agent",
		Role: "market_research",
	})

	if string(hydrated.Config) == "" {
		t.Fatal("expected hydrated actor config")
	}
	if hydrated.Mode != "discovery" {
		t.Fatalf("mode = %q, want discovery", hydrated.Mode)
	}
	if len(hydrated.Permissions) != 1 || hydrated.Permissions[0] != "schedule" {
		t.Fatalf("permissions = %#v", hydrated.Permissions)
	}
}

func TestGatewayMCPToolsForRequest_UsesHydratedActorRoleForEmitTools(t *testing.T) {
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
	})

	req := httptest.NewRequest("POST", "/mcp?agent_id=campaign-coordinator", nil)
	tools := g.mcpToolsForRequest(req)
	for _, tool := range tools {
		if tool.Name == "emit_scan_requested" {
			return
		}
	}
	t.Fatalf("emit_scan_requested not found in MCP tools: %#v", tools)
}

func TestGatewayMCPToolsForRequest_KeepsEmitToolsForDirectMCPContext(t *testing.T) {
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
	})

	PutTurnContextForTest("ctx-1", TurnContext{
		Actor:     models.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	t.Cleanup(func() { UnregisterTurnContext("ctx-1") })

	req := httptest.NewRequest("POST", "/mcp?agent_id=campaign-coordinator&ctx_token=ctx-1", nil)
	tools := g.mcpToolsForRequest(req)
	for _, tool := range tools {
		if tool.Name == "emit_scan_requested" {
			return
		}
	}
	t.Fatalf("emit_scan_requested should remain visible for direct MCP context: %#v", tools)
}

func TestMarkEmitUsed(t *testing.T) {
	PutTurnContextForTest("ctx-emit", TurnContext{
		Actor:     models.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	t.Cleanup(func() { UnregisterTurnContext("ctx-emit") })

	if already := MarkEmitUsed("ctx-emit"); already {
		t.Fatal("first emit use should not report already used")
	}
	if already := MarkEmitUsed("ctx-emit"); !already {
		t.Fatal("second emit use should report already used")
	}
}

func TestMarkEmitKeyUsed_AllowsDistinctKeysPerTurn(t *testing.T) {
	PutTurnContextForTest("ctx-emit-keys", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	t.Cleanup(func() { UnregisterTurnContext("ctx-emit-keys") })

	if already := MarkEmitKeyUsed("ctx-emit-keys", "emit_a\n{\"dimension\":\"one\"}"); already {
		t.Fatal("first unique emit key should not report already used")
	}
	if already := MarkEmitKeyUsed("ctx-emit-keys", "emit_a\n{\"dimension\":\"two\"}"); already {
		t.Fatal("second distinct emit key should be allowed")
	}
	if already := MarkEmitKeyUsed("ctx-emit-keys", "emit_a\n{\"dimension\":\"one\"}"); !already {
		t.Fatal("duplicate emit key should report already used")
	}
}

func TestGatewayHandleMCP_AllowsDistinctEmitPayloadsPerTurn(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), "", GatewayHooks{ResolveTurnContext: ResolveTurnContext})

	PutTurnContextForTest("ctx-emit-gateway", TurnContext{
		Actor:     models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	t.Cleanup(func() { UnregisterTurnContext("ctx-emit-gateway") })

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
		req := httptest.NewRequest(http.MethodPost, "/mcp?ctx_token=ctx-emit-gateway", strings.NewReader(string(body)))
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

func TestGatewayHandleMCP_RejectsMutatingToolWhenContextTokenMisses(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), "", GatewayHooks{ResolveTurnContext: ResolveTurnContext})

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
	req := httptest.NewRequest(http.MethodPost, "/mcp?ctx_token=missing&agent_id=analysis-agent&agent_role=analysis", strings.NewReader(string(body)))
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
	g := NewGateway(nil, "", GatewayHooks{ResolveTurnContext: ResolveTurnContext})
	req := httptest.NewRequest(http.MethodPost, "/mcp?ctx_token=missing&agent_id=analysis-agent&agent_role=analysis", nil)
	if _, err := g.mcpExecutionContext(req, "mcp__runtime-tools__emit_score_dimension_complete"); err == nil {
		t.Fatal("expected context miss error for prefixed mutating tool")
	}
}

func TestGatewayExecutionContext_UsesInboundTraceNotRequestTraceOnResolvedTurn(t *testing.T) {
	g := NewGateway(nil, "", GatewayHooks{
		ResolveTurnContext: ResolveTurnContext,
		WithActor: func(ctx context.Context, actor models.AgentConfig) context.Context {
			return models.WithActor(ctx, actor)
		},
		WithRuntimeEpoch: func(ctx context.Context, epoch int64) context.Context {
			return ctx
		},
		WithInboundEvent: func(ctx context.Context, evt events.Event) context.Context {
			return runtimebus.WithInboundEvent(ctx, evt)
		},
	})
	PutTurnContextForTest("ctx-trace", TurnContext{
		Actor:      models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		Inbound:    events.Event{ID: "evt-1", RunID: "run-1"},
		HasInbound: true,
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  time.Now().UTC().Add(time.Hour),
	})
	t.Cleanup(func() { UnregisterTurnContext("ctx-trace") })

	req := httptest.NewRequest(http.MethodPost, "/mcp?ctx_token=ctx-trace", nil)
	ctx, err := g.mcpExecutionContext(req, "get_entity")
	if err != nil {
		t.Fatalf("mcpExecutionContext: %v", err)
	}
	_ = ctx
}

func TestGatewayHandleMCP_AllowsReadOnlyToolWhenContextTokenMisses(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), "", GatewayHooks{ResolveTurnContext: ResolveTurnContext})

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
	req := httptest.NewRequest(http.MethodPost, "/mcp?ctx_token=missing&agent_id=analysis-agent&agent_role=analysis", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if callCount != 1 {
		t.Fatalf("executor call count = %d, want 1", callCount)
	}
	if strings.Contains(rec.Body.String(), "missing or invalid mcp context token") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayHandleTool_RejectsMutatingToolWithoutContextToken(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), "", GatewayHooks{ResolveTurnContext: ResolveTurnContext})

	body, err := json.Marshal(ToolGatewayRequest{
		Actor: models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		Input: map[string]any{"dimension": "build_complexity", "score": 72},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tools/emit_score_dimension_complete", strings.NewReader(string(body)))
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

func TestGatewayHandleTool_AllowsReadOnlyToolWithoutContextToken(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), "", GatewayHooks{ResolveTurnContext: ResolveTurnContext})

	body, err := json.Marshal(ToolGatewayRequest{
		Actor: models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		Input: map[string]any{"query": "kind = 'vertical'"},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tools/query_entities", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if callCount != 1 {
		t.Fatalf("executor call count = %d, want 1", callCount)
	}
}

func TestGatewayHandleMCP_LogsFallbackUsedReason(t *testing.T) {
	var actions []string
	var reasons []string
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), "", GatewayHooks{
		ResolveTurnContext: ResolveTurnContext,
		Log: func(_ context.Context, level, action, agentID, entityID string, detail map[string]any, errText string) {
			actions = append(actions, action)
			if reason, _ := detail["reason"].(string); strings.TrimSpace(reason) != "" {
				reasons = append(reasons, strings.TrimSpace(reason))
			}
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
	req := httptest.NewRequest(http.MethodPost, "/mcp?ctx_token=missing&agent_id=analysis-agent&agent_role=analysis", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !slices.Contains(actions, "mcp.context.fallback_used") {
		t.Fatalf("actions = %#v, want mcp.context.fallback_used", actions)
	}
	if !slices.Contains(reasons, "token_not_found") {
		t.Fatalf("reasons = %#v, want token_not_found", reasons)
	}
}

func TestGatewayHandleTool_LogsFallbackBlockedReason(t *testing.T) {
	var actions []string
	var reasons []string
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), "", GatewayHooks{
		ResolveTurnContext: ResolveTurnContext,
		Log: func(_ context.Context, level, action, agentID, entityID string, detail map[string]any, errText string) {
			actions = append(actions, action)
			if reason, _ := detail["reason"].(string); strings.TrimSpace(reason) != "" {
				reasons = append(reasons, strings.TrimSpace(reason))
			}
		},
	})

	body, err := json.Marshal(ToolGatewayRequest{
		Actor: models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		Input: map[string]any{"dimension": "build_complexity", "score": 72},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tools/emit_score_dimension_complete", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !slices.Contains(actions, "tool.context.fallback_blocked") {
		t.Fatalf("actions = %#v, want tool.context.fallback_blocked", actions)
	}
	if !slices.Contains(reasons, "token_missing") {
		t.Fatalf("reasons = %#v, want token_missing", reasons)
	}
}
