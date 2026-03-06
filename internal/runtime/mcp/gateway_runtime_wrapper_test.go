package mcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
	rt "empireai/internal/runtime"
	runtimebus "empireai/internal/runtime/bus"
	llm "empireai/internal/runtime/llm"
	"empireai/internal/runtime/mcp"
	runtimetools "empireai/internal/runtime/tools"
)

type actorKey struct{}

type toolGatewayExecStub struct {
	lastName  string
	lastInput any
	lastActor string
	err       error
}

func (s *toolGatewayExecStub) Execute(ctx context.Context, name string, input any) (any, error) {
	if actor, ok := ctx.Value(actorKey{}).(models.AgentConfig); ok {
		s.lastActor = actor.ID
	}
	s.lastName = name
	s.lastInput = input
	if s.err != nil {
		return nil, s.err
	}
	return map[string]any{"ok": true}, nil
}

func (s *toolGatewayExecStub) ToolDefinitions() []llm.ToolDefinition {
	return []llm.ToolDefinition{{Name: "sql_execute", Description: "sql tool"}, {Name: "agent_message", Description: "msg tool"}}
}

func newGateway(executor llm.ToolExecutor, authToken string, paused func() bool) *mcp.Gateway {
	if paused == nil {
		paused = func() bool { return false }
	}
	return mcp.NewGateway(executor, authToken, mcp.GatewayHooks{
		RuntimeIngressPaused: paused,
		FormatError: func(err error) string {
			if err == nil {
				return ""
			}
			return err.Error()
		},
		NewRuntimeError: func(code, operation string, retryable bool, cause error, format string, args ...any) error {
			return rt.NewRuntimeError(code, "mcp-gateway", operation, retryable, format, args...)
		},
		RetryableFromError: func(err error) (bool, bool) {
			if runtimeErr, ok := rt.AsRuntimeError(err); ok {
				return runtimeErr.Retryable, true
			}
			return false, false
		},
		WithActor: func(ctx context.Context, actor models.AgentConfig) context.Context {
			return context.WithValue(ctx, actorKey{}, actor)
		},
		ActorFromContext: func(ctx context.Context) (models.AgentConfig, bool) {
			actor, ok := ctx.Value(actorKey{}).(models.AgentConfig)
			return actor, ok
		},
		WithRuntimeEpoch:          runtimebus.WithRuntimeEpoch,
		WithCurrentRuntimeEpoch:   runtimebus.WithCurrentRuntimeEpoch,
		IsCurrentRuntimeEpoch:     runtimebus.IsCurrentRuntimeEpoch,
		WithInboundEvent:          runtimebus.WithInboundEvent,
		WithEmittedEventsRecorder: runtimebus.WithEmittedEventsRecorder,
		ResolveTurnContext:        mcp.ResolveTurnContext,
		EmitTools:                 func(role string) []llm.ToolDefinition { return runtimetools.GenerateEmitToolsForRole(role, nil) },
		EmitSchemaForTool: func(name string) (string, any, bool) {
			if !strings.HasPrefix(name, "emit_") {
				return "", nil, false
			}
			eventType := strings.ReplaceAll(strings.TrimPrefix(name, "emit_"), "_", ".")
			schema := runtimetools.EventSchemaSnapshot()
			if s, ok := schema[eventType]; ok {
				return strings.TrimSpace(s.Description), s.Schema, true
			}
			return "", nil, false
		},
	})
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if v == nil {
		return ""
	}
	return strings.TrimSpace(strings.ReplaceAll(strings.TrimSpace(strings.TrimPrefix(strings.TrimSuffix(strings.TrimSpace(strings.Repeat("", 0)), ""), "")), "", ""))
}

func contains(in []string, target string) bool {
	for _, item := range in {
		if item == target {
			return true
		}
	}
	return false
}

func decodeToolNames(t *testing.T, body []byte) []map[string]any {
	t.Helper()
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	result, _ := resp["result"].(map[string]any)
	items, _ := result["tools"].([]any)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		m, _ := item.(map[string]any)
		out = append(out, m)
	}
	return out
}

func TestGatewayExecute(t *testing.T) {
	stub := &toolGatewayExecStub{}
	gw := newGateway(stub, "", nil)
	body := map[string]any{"actor": map[string]any{"id": "agent-1", "role": "opco-ceo", "vertical_id": "v1", "mode": "operating"}, "input": map[string]any{"q": "SELECT 1"}}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/tools/sql_execute", bytes.NewReader(raw))
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if stub.lastName != "sql_execute" || stub.lastActor != "agent-1" {
		t.Fatalf("unexpected execution context: name=%q actor=%q", stub.lastName, stub.lastActor)
	}
}

func TestGatewayAuthRequired(t *testing.T) {
	gw := newGateway(&toolGatewayExecStub{}, "secret", nil)
	req := httptest.NewRequest(http.MethodPost, "/tools/agent_message", bytes.NewReader([]byte(`{"agent_id":"a1"}`)))
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestGatewayMCPToolsListHonorsAllowlist(t *testing.T) {
	gw := newGateway(&toolGatewayExecStub{}, "", nil)
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)))
	req.Header.Set("X-Empire-Allowed-Tools", "emit_scan_requested,sql_execute")
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	tools := decodeToolNames(t, rr.Body.Bytes())
	if len(tools) != 2 {
		t.Fatalf("expected 2 tools, got %#v", tools)
	}
	names := []string{}
	var emitScan map[string]any
	for _, item := range tools {
		name := strings.TrimSpace(item["name"].(string))
		names = append(names, name)
		if name == "emit_scan_requested" {
			emitScan = item
		}
	}
	if !contains(names, "emit_scan_requested") || !contains(names, "sql_execute") {
		t.Fatalf("unexpected tools: %#v", names)
	}
	schema, _ := emitScan["inputSchema"].(map[string]any)
	requiredAny, _ := schema["required"].([]any)
	required := make([]string, 0, len(requiredAny))
	for _, v := range requiredAny {
		required = append(required, strings.TrimSpace(v.(string)))
	}
	for _, field := range []string{"mode", "geography", "campaign_context"} {
		if !contains(required, field) {
			t.Fatalf("missing required field %s in %#v", field, required)
		}
	}
}

func TestGatewayMCPContextTokenFlow(t *testing.T) {
	stub := &toolGatewayExecStub{}
	gw := newGateway(stub, "", nil)
	token := "tok-ctx"
	mcp.PutTurnContextForTest(token, mcp.TurnContext{Actor: models.AgentConfig{ID: "a-mcp", Role: "market-research-agent", Mode: "factory", VerticalID: "v1"}, Inbound: events.Event{ID: "evt-1", Type: events.EventType("market_research.scan_assigned")}, HasInbound: true, Recorder: runtimebus.NewEmittedEventsRecorder(), Epoch: runtimebus.CurrentRuntimeEpoch()})
	defer mcp.UnregisterTurnContext(token)
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"sql_execute","arguments":{"query":"SELECT 1"}}}`)))
	req.Header.Set("X-Empire-Allowed-Tools", "sql_execute")
	req.Header.Set("X-Empire-Context-Token", token)
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || stub.lastActor != "a-mcp" || stub.lastName != "sql_execute" {
		t.Fatalf("unexpected result code=%d actor=%q tool=%q body=%s", rr.Code, stub.lastActor, stub.lastName, rr.Body.String())
	}
}

func TestGatewayMCPMissingContextTokenRejected(t *testing.T) {
	t.Setenv("EMPIREAI_MCP_REQUIRE_CONTEXT_TOKEN", "")
	gw := newGateway(&toolGatewayExecStub{}, "", nil)
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"sql_execute","arguments":{"query":"SELECT 1"}}}`)))
	req.Header.Set("X-Empire-Allowed-Tools", "sql_execute")
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	result, _ := resp["result"].(map[string]any)
	content, _ := result["content"].([]any)
	first, _ := content[0].(map[string]any)
	text := strings.TrimSpace(first["text"].(string))
	if !strings.Contains(text, "code="+mcp.ErrCodeContextMissing) {
		t.Fatalf("unexpected text %q", text)
	}
}

func TestGatewayMCPUnknownTokenNotFoundCode(t *testing.T) {
	t.Setenv("EMPIREAI_MCP_CONTEXT_FALLBACK_ON_MISS", "false")
	gw := newGateway(&toolGatewayExecStub{}, "", nil)
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"sql_execute","arguments":{"query":"SELECT 1"}}}`)))
	req.Header.Set("X-Empire-Allowed-Tools", "sql_execute")
	req.Header.Set("X-Empire-Context-Token", "missing-token")
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	result, _ := resp["result"].(map[string]any)
	content, _ := result["content"].([]any)
	first, _ := content[0].(map[string]any)
	text := strings.TrimSpace(first["text"].(string))
	if !strings.Contains(text, "code="+mcp.ErrCodeContextNotFound) {
		t.Fatalf("unexpected text %q", text)
	}
}

func TestGatewayMCPToolCallExecErrorPreservesRetryability(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		expect string
	}{
		{name: "guardrail", err: rt.NewRuntimeError("emit_transition_guardrail_violation", "tool-executor", "emit.validate_transition", false, "emit transition rejected"), expect: "retryable=false"},
		{name: "generic", err: context.DeadlineExceeded, expect: "retryable=true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &toolGatewayExecStub{err: tc.err}
			gw := newGateway(stub, "", nil)
			token := "tok-err-" + tc.name
			mcp.PutTurnContextForTest(token, mcp.TurnContext{Actor: models.AgentConfig{ID: "a-err", Role: "empire-coordinator", Mode: "holding"}, Inbound: events.Event{ID: "evt-err", Type: events.EventType("system.started")}, HasInbound: true, Recorder: runtimebus.NewEmittedEventsRecorder(), Epoch: runtimebus.CurrentRuntimeEpoch()})
			defer mcp.UnregisterTurnContext(token)
			req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"sql_execute","arguments":{"query":"SELECT 1"}}}`)))
			req.Header.Set("X-Empire-Allowed-Tools", "sql_execute")
			req.Header.Set("X-Empire-Context-Token", token)
			rr := httptest.NewRecorder()
			gw.Handler().ServeHTTP(rr, req)
			var resp map[string]any
			_ = json.Unmarshal(rr.Body.Bytes(), &resp)
			result, _ := resp["result"].(map[string]any)
			content, _ := result["content"].([]any)
			first, _ := content[0].(map[string]any)
			text := strings.TrimSpace(first["text"].(string))
			if !strings.Contains(text, tc.expect) || !strings.Contains(text, "code="+mcp.ErrCodeToolExecFailed) {
				t.Fatalf("unexpected text %q", text)
			}
		})
	}
}

func TestGatewayRejectsWhenRuntimeIngressPaused(t *testing.T) {
	paused := true
	gw := newGateway(&toolGatewayExecStub{}, "", func() bool { return paused })
	req := httptest.NewRequest(http.MethodPost, "/tools/sql_execute", bytes.NewReader([]byte(`{"actor":{"id":"agent-1","mode":"holding"},"input":{"query":"SELECT 1"}}`)))
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
}

func TestGatewayHelpers_AuthorizeAndMCPError(t *testing.T) {
	g := newGateway(nil, "secret-token", nil)
	req := httptest.NewRequest(http.MethodPost, "/tools/sql_execute", nil)
	if err := g.AuthorizeForTest(req); err == nil {
		t.Fatal("expected missing authorization error")
	}
	req.Header.Set("Authorization", "Bearer wrong")
	if err := g.AuthorizeForTest(req); err == nil {
		t.Fatal("expected invalid token error")
	}
	req.Header.Set("Authorization", "Bearer secret-token")
	if err := g.AuthorizeForTest(req); err != nil {
		t.Fatalf("expected authorize success, got %v", err)
	}
	w := httptest.NewRecorder()
	mcp.WriteRPCError(w, "id-1", -32600, "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp mcp.RPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil || resp.Error.Code != -32600 || resp.Error.Message == "" {
		t.Fatalf("unexpected response %+v", resp)
	}
	if got := mcp.ToolResultText(nil); got != "ok" {
		t.Fatalf("unexpected result text %q", got)
	}
}
