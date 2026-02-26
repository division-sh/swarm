package runtime

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
)

type toolGatewayExecStub struct {
	lastName  string
	lastInput any
	lastActor string
}

func (s *toolGatewayExecStub) Execute(ctx context.Context, name string, input any) (any, error) {
	actor, _ := ActorFromContext(ctx)
	s.lastActor = actor.ID
	s.lastName = name
	s.lastInput = input
	return map[string]any{"ok": true}, nil
}

func (s *toolGatewayExecStub) ToolDefinitions() []ToolDefinition {
	return []ToolDefinition{
		{Name: "sql_execute", Description: "sql tool"},
		{Name: "agent_message", Description: "msg tool"},
	}
}

func TestToolGatewayExecute(t *testing.T) {
	stub := &toolGatewayExecStub{}
	gw := NewToolGateway(stub, "")
	body := map[string]any{
		"actor": map[string]any{
			"id":          "agent-1",
			"role":        "opco-ceo",
			"vertical_id": "v1",
			"mode":        "operating",
		},
		"input": map[string]any{"q": "SELECT 1"},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/tools/sql_execute", bytes.NewReader(raw))
	rr := httptest.NewRecorder()

	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if stub.lastName != "sql_execute" {
		t.Fatalf("unexpected tool name: %s", stub.lastName)
	}
	if stub.lastActor != "agent-1" {
		t.Fatalf("expected actor agent-1, got %s", stub.lastActor)
	}
}

func TestToolGatewayAuthRequired(t *testing.T) {
	stub := &toolGatewayExecStub{}
	gw := NewToolGateway(stub, "secret")
	req := httptest.NewRequest(http.MethodPost, "/tools/agent_message", bytes.NewReader([]byte(`{"agent_id":"a1"}`)))
	rr := httptest.NewRecorder()

	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestToolGatewayMCPToolsListHonorsAllowlist(t *testing.T) {
	stub := &toolGatewayExecStub{}
	gw := NewToolGateway(stub, "")
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)))
	req.Header.Set("X-Empire-Allowed-Tools", "emit_scan_requested,sql_execute")
	rr := httptest.NewRecorder()

	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	result, _ := resp["result"].(map[string]any)
	tools, _ := result["tools"].([]any)
	if len(tools) != 2 {
		t.Fatalf("expected 2 tools, got %#v", result)
	}
	names := []string{}
	var emitScan map[string]any
	for _, item := range tools {
		m, _ := item.(map[string]any)
		name := strings.TrimSpace(asString(m["name"]))
		names = append(names, name)
		if name == "emit_scan_requested" {
			emitScan = m
		}
	}
	if !(slicesContains(names, "emit_scan_requested") && slicesContains(names, "sql_execute")) {
		t.Fatalf("unexpected tool names: %#v", names)
	}
	if emitScan == nil {
		t.Fatalf("emit_scan_requested not found in tools list: %#v", tools)
	}
	schema, _ := emitScan["inputSchema"].(map[string]any)
	if schema == nil {
		t.Fatalf("emit_scan_requested missing inputSchema: %#v", emitScan)
	}
	requiredAny, _ := schema["required"].([]any)
	required := make([]string, 0, len(requiredAny))
	for _, v := range requiredAny {
		required = append(required, strings.TrimSpace(asString(v)))
	}
	if !slicesContains(required, "mode") {
		t.Fatalf("expected emit_scan_requested schema required to contain mode, got %#v", required)
	}
}

func TestToolGatewayMCPToolsListEmitPortfolioDigestSchema(t *testing.T) {
	stub := &toolGatewayExecStub{}
	gw := NewToolGateway(stub, "")
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":11,"method":"tools/list"}`)))
	req.Header.Set("X-Empire-Allowed-Tools", "emit_portfolio_digest_compiled")
	req.Header.Set("X-Empire-Agent-Role", "empire-coordinator")
	rr := httptest.NewRecorder()

	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	result, _ := resp["result"].(map[string]any)
	tools, _ := result["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("expected 1 tool, got %#v", result)
	}
	tool, _ := tools[0].(map[string]any)
	if strings.TrimSpace(asString(tool["name"])) != "emit_portfolio_digest_compiled" {
		t.Fatalf("unexpected tool: %#v", tool)
	}
	schema, _ := tool["inputSchema"].(map[string]any)
	if schema == nil {
		t.Fatalf("missing input schema: %#v", tool)
	}
	requiredAny, _ := schema["required"].([]any)
	required := make([]string, 0, len(requiredAny))
	for _, v := range requiredAny {
		required = append(required, strings.TrimSpace(asString(v)))
	}
	if !slicesContains(required, "digest_text") {
		t.Fatalf("expected digest_text required, got %#v", required)
	}
}

func TestToolGatewayMCPToolCallUsesScopedContextToken(t *testing.T) {
	stub := &toolGatewayExecStub{}
	gw := NewToolGateway(stub, "")
	rec := NewEmittedEventsRecorder()
	token := "tok-ctx"
	globalMCPTurnRegistry.put(token, mcpTurnContext{
		Actor: models.AgentConfig{
			ID:         "a-mcp",
			Role:       "market-research-agent",
			Mode:       "factory",
			VerticalID: "v1",
		},
		Inbound:    events.Event{ID: "evt-1", Type: events.EventType("market_research.scan_assigned")},
		HasInbound: true,
		Recorder:   rec,
		Epoch:      CurrentRuntimeEpoch(),
	})
	defer globalMCPTurnRegistry.delete(token)

	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{
		"jsonrpc":"2.0",
		"id":2,
		"method":"tools/call",
		"params":{"name":"sql_execute","arguments":{"query":"SELECT 1"}}
	}`)))
	req.Header.Set("X-Empire-Allowed-Tools", "sql_execute")
	req.Header.Set("X-Empire-Context-Token", token)
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if stub.lastName != "sql_execute" {
		t.Fatalf("unexpected tool name: %s", stub.lastName)
	}
	if stub.lastActor != "a-mcp" {
		t.Fatalf("expected actor from scoped context token, got %q", stub.lastActor)
	}
}

func TestToolGatewayMCPToolCallRejectsMissingContextTokenByDefault(t *testing.T) {
	t.Setenv("EMPIREAI_MCP_REQUIRE_CONTEXT_TOKEN", "")
	stub := &toolGatewayExecStub{}
	gw := NewToolGateway(stub, "")
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{
		"jsonrpc":"2.0",
		"id":3,
		"method":"tools/call",
		"params":{"name":"sql_execute","arguments":{"query":"SELECT 1"}}
	}`)))
	req.Header.Set("X-Empire-Allowed-Tools", "sql_execute")
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 rpc envelope, got %d", rr.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	result, _ := resp["result"].(map[string]any)
	if result == nil {
		t.Fatalf("expected json-rpc result error payload, got %#v", resp)
	}
	isErr, _ := result["isError"].(bool)
	if !isErr {
		t.Fatalf("expected mcp tool error for missing context token, got %#v", result)
	}
}

func TestToolGatewayRejectsWhenRuntimeIngressPaused(t *testing.T) {
	stub := &toolGatewayExecStub{}
	gw := NewToolGateway(stub, "")
	PauseRuntimeIngress()
	defer ResumeRuntimeIngress()

	req := httptest.NewRequest(http.MethodPost, "/tools/sql_execute", bytes.NewReader([]byte(`{
		"actor":{"id":"agent-1","mode":"holding"},
		"input":{"query":"SELECT 1"}
	}`)))
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 during reset ingress pause, got %d", rr.Code)
	}
}

func slicesContains(in []string, target string) bool {
	for _, item := range in {
		if item == target {
			return true
		}
	}
	return false
}
