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
	err       error
}

func (s *toolGatewayExecStub) Execute(ctx context.Context, name string, input any) (any, error) {
	actor, _ := ActorFromContext(ctx)
	s.lastActor = actor.ID
	s.lastName = name
	s.lastInput = input
	if s.err != nil {
		return nil, s.err
	}
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
	for _, field := range []string{"mode", "geography", "campaign_context"} {
		if !slicesContains(required, field) {
			t.Fatalf("expected emit_scan_requested schema required to contain %s, got %#v", field, required)
		}
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
	if !slicesContains(required, "message") {
		t.Fatalf("expected message required, got %#v", required)
	}
}

func TestToolGatewayMCPToolsListMailboxSendRequiresType(t *testing.T) {
	exec := NewRuntimeToolExecutor(nil, nil, nil)
	gw := NewToolGateway(exec, "")
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":12,"method":"tools/list"}`)))
	req.Header.Set("X-Empire-Allowed-Tools", "mailbox_send")
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
	if strings.TrimSpace(asString(tool["name"])) != "mailbox_send" {
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
	if !slicesContains(required, "type") {
		t.Fatalf("expected type required, got %#v", required)
	}
	if ap, ok := schema["additionalProperties"].(bool); !ok || ap {
		t.Fatalf("expected additionalProperties=false, got %#v", schema["additionalProperties"])
	}
}

func TestToolGatewayMCPToolsListCoreRuntimeToolsExposeRequiredFields(t *testing.T) {
	exec := NewRuntimeToolExecutor(nil, nil, nil)
	gw := NewToolGateway(exec, "")
	cases := []struct {
		tool     string
		required []string
	}{
		{tool: "agent_fire", required: []string{"agent_id"}},
		{tool: "agent_reconfigure", required: []string{"agent_id"}},
		{tool: "configure_routing", required: []string{"event_pattern", "subscriber_id"}},
		{tool: "human_task_request", required: []string{"category", "description"}},
		{tool: "human_task_decide", required: []string{"task_id", "decision"}},
		{tool: "sql_execute", required: []string{"query"}},
		{tool: "systemd_control", required: []string{"action", "unit"}},
		{tool: "certbot_execute", required: []string{"domain"}},
		{tool: "instagram_handle_check", required: []string{"handle"}},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":13,"method":"tools/list"}`)))
		req.Header.Set("X-Empire-Allowed-Tools", tc.tool)
		rr := httptest.NewRecorder()
		gw.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d", tc.tool, rr.Code)
		}
		var resp map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("%s: decode response: %v", tc.tool, err)
		}
		result, _ := resp["result"].(map[string]any)
		tools, _ := result["tools"].([]any)
		if len(tools) != 1 {
			t.Fatalf("%s: expected 1 tool, got %#v", tc.tool, result)
		}
		tool, _ := tools[0].(map[string]any)
		if strings.TrimSpace(asString(tool["name"])) != tc.tool {
			t.Fatalf("%s: unexpected tool payload %#v", tc.tool, tool)
		}
		schema, _ := tool["inputSchema"].(map[string]any)
		if schema == nil {
			t.Fatalf("%s: missing input schema", tc.tool)
		}
		requiredAny, _ := schema["required"].([]any)
		required := make([]string, 0, len(requiredAny))
		for _, v := range requiredAny {
			required = append(required, strings.TrimSpace(asString(v)))
		}
		for _, field := range tc.required {
			if !slicesContains(required, field) {
				t.Fatalf("%s: expected required field %s, got %#v", tc.tool, field, required)
			}
		}
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
	content, _ := result["content"].([]any)
	if len(content) == 0 {
		t.Fatalf("expected content payload, got %#v", result)
	}
	first, _ := content[0].(map[string]any)
	text := strings.TrimSpace(asString(first["text"]))
	if !strings.Contains(text, "code="+ErrCodeMCPContextMissing) {
		t.Fatalf("expected structured mcp missing-context code, got %q", text)
	}
	if !strings.Contains(text, "retryable=false") {
		t.Fatalf("expected non-retryable missing-context error, got %q", text)
	}
}

func TestToolGatewayMCPToolCallUsesQueryContextTokenFallback(t *testing.T) {
	stub := &toolGatewayExecStub{}
	gw := NewToolGateway(stub, "")
	token := "tok-query"
	globalMCPTurnRegistry.put(token, mcpTurnContext{
		Actor: models.AgentConfig{
			ID:         "a-query",
			Role:       "market-research-agent",
			Mode:       "factory",
			VerticalID: "v-query",
		},
		Inbound:    events.Event{ID: "evt-q", Type: events.EventType("market_research.scan_assigned")},
		HasInbound: true,
		Recorder:   NewEmittedEventsRecorder(),
		Epoch:      CurrentRuntimeEpoch(),
	})
	defer globalMCPTurnRegistry.delete(token)

	req := httptest.NewRequest(http.MethodPost, "/mcp?empire_ctx_token="+token+"&empire_allowed_tools=sql_execute", bytes.NewReader([]byte(`{
		"jsonrpc":"2.0",
		"id":4,
		"method":"tools/call",
		"params":{"name":"sql_execute","arguments":{"query":"SELECT 1"}}
	}`)))
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if stub.lastName != "sql_execute" {
		t.Fatalf("unexpected tool name: %q", stub.lastName)
	}
	if stub.lastActor != "a-query" {
		t.Fatalf("expected actor from query-scoped context token, got %q", stub.lastActor)
	}
}

func TestToolGatewayMCPToolsListUsesQueryAllowedToolsFallback(t *testing.T) {
	stub := &toolGatewayExecStub{}
	gw := NewToolGateway(stub, "")

	req := httptest.NewRequest(http.MethodPost, "/mcp?empire_allowed_tools=emit_scan_requested", bytes.NewReader([]byte(`{
		"jsonrpc":"2.0",
		"id":5,
		"method":"tools/list"
	}`)))
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	result, _ := resp["result"].(map[string]any)
	tools, _ := result["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("expected exactly one tool from query allowlist, got %#v", result)
	}
	tool, _ := tools[0].(map[string]any)
	if strings.TrimSpace(asString(tool["name"])) != "emit_scan_requested" {
		t.Fatalf("unexpected tool name: %#v", tool)
	}
}

func TestToolGatewayMCPToolCallUnknownTokenReportsNotFoundCode(t *testing.T) {
	t.Setenv("EMPIREAI_MCP_CONTEXT_FALLBACK_ON_MISS", "false")
	stub := &toolGatewayExecStub{}
	gw := NewToolGateway(stub, "")
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{
		"jsonrpc":"2.0",
		"id":6,
		"method":"tools/call",
		"params":{"name":"sql_execute","arguments":{"query":"SELECT 1"}}
	}`)))
	req.Header.Set("X-Empire-Allowed-Tools", "sql_execute")
	req.Header.Set("X-Empire-Context-Token", "missing-token")
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
	content, _ := result["content"].([]any)
	first, _ := content[0].(map[string]any)
	text := strings.TrimSpace(asString(first["text"]))
	if !strings.Contains(text, "code="+ErrCodeMCPContextNotFound) {
		t.Fatalf("expected not-found code in response, got %q", text)
	}
	if !strings.Contains(text, "retryable=false") {
		t.Fatalf("expected non-retryable context-not-found error, got %q", text)
	}
}

func TestToolGatewayMCPToolCallUnknownTokenFallsBackToActorContextByDefault(t *testing.T) {
	stub := &toolGatewayExecStub{}
	gw := NewToolGateway(stub, "")
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{
		"jsonrpc":"2.0",
		"id":7,
		"method":"tools/call",
		"params":{"name":"sql_execute","arguments":{"query":"SELECT 1"}}
	}`)))
	req.Header.Set("X-Empire-Allowed-Tools", "sql_execute")
	req.Header.Set("X-Empire-Context-Token", "missing-token")
	req.Header.Set("X-Empire-Agent-Id", "analysis-agent")
	req.Header.Set("X-Empire-Agent-Role", "analysis-agent")
	req.Header.Set("X-Empire-Agent-Mode", "factory")
	req.Header.Set("X-Empire-Vertical-Id", "v-test")
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 rpc envelope, got %d", rr.Code)
	}
	if stub.lastName != "sql_execute" {
		t.Fatalf("unexpected tool name: %q", stub.lastName)
	}
	if stub.lastActor != "analysis-agent" {
		t.Fatalf("expected fallback actor context, got %q", stub.lastActor)
	}
}

func TestToolGatewayMCPToolCallExecErrorPreservesRetryability(t *testing.T) {
	cases := []struct {
		name              string
		err               error
		expectRetryable   string
		expectRuntimeCode string
	}{
		{
			name:              "guardrail_non_retryable",
			err:               NewRuntimeError("emit_transition_guardrail_violation", "tool-executor", "emit.validate_transition", false, "emit transition rejected"),
			expectRetryable:   "retryable=false",
			expectRuntimeCode: "code=" + ErrCodeMCPToolExecFailed,
		},
		{
			name:              "generic_error_defaults_retryable",
			err:               context.DeadlineExceeded,
			expectRetryable:   "retryable=true",
			expectRuntimeCode: "code=" + ErrCodeMCPToolExecFailed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &toolGatewayExecStub{err: tc.err}
			gw := NewToolGateway(stub, "")
			token := "tok-err-" + tc.name
			globalMCPTurnRegistry.put(token, mcpTurnContext{
				Actor: models.AgentConfig{
					ID:         "a-err",
					Role:       "empire-coordinator",
					Mode:       "holding",
					VerticalID: "",
				},
				Inbound:    events.Event{ID: "evt-err", Type: events.EventType("system.started")},
				HasInbound: true,
				Recorder:   NewEmittedEventsRecorder(),
				Epoch:      CurrentRuntimeEpoch(),
			})
			defer globalMCPTurnRegistry.delete(token)

			req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(`{
				"jsonrpc":"2.0",
				"id":8,
				"method":"tools/call",
				"params":{"name":"sql_execute","arguments":{"query":"SELECT 1"}}
			}`)))
			req.Header.Set("X-Empire-Allowed-Tools", "sql_execute")
			req.Header.Set("X-Empire-Context-Token", token)
			rr := httptest.NewRecorder()
			gw.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("expected 200 rpc envelope, got %d body=%s", rr.Code, rr.Body.String())
			}

			var resp map[string]any
			if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			result, _ := resp["result"].(map[string]any)
			isErr, _ := result["isError"].(bool)
			if !isErr {
				t.Fatalf("expected tool exec error payload, got %#v", result)
			}
			content, _ := result["content"].([]any)
			if len(content) == 0 {
				t.Fatalf("expected content payload, got %#v", result)
			}
			first, _ := content[0].(map[string]any)
			text := strings.TrimSpace(asString(first["text"]))
			if !strings.Contains(text, tc.expectRuntimeCode) {
				t.Fatalf("expected %q in %q", tc.expectRuntimeCode, text)
			}
			if !strings.Contains(text, tc.expectRetryable) {
				t.Fatalf("expected %q in %q", tc.expectRetryable, text)
			}
		})
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
