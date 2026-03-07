package llm

import (
	"context"
	"encoding/json"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimeactor "empireai/internal/runtime/actorctx"
	runtimebus "empireai/internal/runtime/bus"
	"empireai/internal/runtime/sessions"
	workspace "empireai/internal/runtime/workspace"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
)

func TestClaudeCLIRuntimeBuildCommandInWorkspace(t *testing.T) {
	t.Setenv("EMPIREAI_DOCKER_BIN", "docker-test")
	t.Setenv("EMPIREAI_TOOL_GATEWAY_URL", "http://orchestrator:8090")
	t.Setenv("EMPIREAI_TOOL_GATEWAY_TOKEN", "secret")
	cfg := &config.Config{}
	cfg.LLM.ClaudeCLI.Command = "claude"
	cfg.LLM.ClaudeCLI.Timeout = time.Second
	rt := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "owner", nil, nil, nil, nil)

	cmd := rt.buildCommand(context.Background(), []string{"-p", "hello"}, &workspace.Target{
		Container: "empireai-v1",
		Workdir:   "/workspace",
	})
	expected := []string{
		"docker-test", "exec", "-i",
		"-e", "EMPIREAI_TOOL_GATEWAY_URL=http://orchestrator:8090",
		"-e", "EMPIREAI_TOOL_GATEWAY_TOKEN=secret",
		"-w", "/workspace", "empireai-v1", "claude", "-p", "hello",
	}
	if !reflect.DeepEqual(cmd.Args, expected) {
		t.Fatalf("unexpected command args: got=%v want=%v", cmd.Args, expected)
	}
}

func TestClaudeCLIRuntimeBuildCommandHostFallback(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.ClaudeCLI.Command = "claude"
	cfg.LLM.ClaudeCLI.Timeout = time.Second
	rt := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "owner", nil, nil, nil, nil)

	cmd := rt.buildCommand(context.Background(), []string{"-p", "hello"}, nil)
	expected := []string{"claude", "-p", "hello"}
	if !reflect.DeepEqual(cmd.Args, expected) {
		t.Fatalf("unexpected command args: got=%v want=%v", cmd.Args, expected)
	}
}

func TestParseCLIResponseToolCalls(t *testing.T) {
	raw := []byte(`{
		"content": [
			{"type":"text","text":"Working..."},
			{"type":"tool_use","name":"agent_message","input":{"event_type":"x.y","payload":{"ok":true}}}
		]
	}`)
	resp := parseCLIResponse(raw)
	if resp == nil {
		t.Fatal("expected response")
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(resp.ToolCalls))
	}
	if resp.ToolCalls[0].Name != "agent_message" {
		t.Fatalf("unexpected tool name: %s", resp.ToolCalls[0].Name)
	}
	if resp.Message.Content != "Working..." {
		t.Fatalf("unexpected content: %q", resp.Message.Content)
	}
}

func TestParseCLIResponseOpenAIStyleToolCalls(t *testing.T) {
	raw := []byte(`{
		"tool_calls":[
			{"name":"sql_execute","arguments":{"query":"SELECT 1"}}
		],
		"result":"ok"
	}`)
	resp := parseCLIResponse(raw)
	if resp == nil {
		t.Fatal("expected response")
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "sql_execute" {
		t.Fatalf("unexpected tool calls: %+v", resp.ToolCalls)
	}
	var args map[string]any
	b, _ := json.Marshal(resp.ToolCalls[0].Arguments)
	_ = json.Unmarshal(b, &args)
	if args["query"] != "SELECT 1" {
		t.Fatalf("unexpected tool args: %+v", args)
	}
}

func TestParseCLIResponse_DoesNotInferTaggedEmitCallsFromText(t *testing.T) {
	raw := []byte(`I am done.
<emit_market_research_scan_complete>
{"scan_id":"s1","categories_assessed":8,"high_signal_count":3,"geography":"argentina"}
</emit_market_research_scan_complete>`)
	resp := parseCLIResponse(raw)
	if len(resp.ToolCalls) != 0 {
		t.Fatalf("expected no inferred tool calls from text, got %+v", resp.ToolCalls)
	}
}

func TestParseCLIResponse_DoesNotInferEmitEnvelopeFromCodeFence(t *testing.T) {
	raw := []byte("```json\n{\"emit_events\":[{\"type\":\"scan.requested\",\"task_id\":\"t1\",\"vertical_id\":\"v1\",\"payload\":{\"mode\":\"saas_gap\"}}]}\n```")
	resp := parseCLIResponse(raw)
	if len(resp.ToolCalls) != 0 {
		t.Fatalf("expected no inferred tool calls from text envelope, got %+v", resp.ToolCalls)
	}
}

func TestNormalizeMCPServerURL(t *testing.T) {
	if got := normalizeMCPServerURL("http://orchestrator:8090"); got != "http://orchestrator:8090/mcp" {
		t.Fatalf("unexpected mcp url: %q", got)
	}
	if got := normalizeMCPServerURL("http://orchestrator:8090/"); got != "http://orchestrator:8090/mcp" {
		t.Fatalf("unexpected mcp url with slash: %q", got)
	}
	if got := normalizeMCPServerURL("http://orchestrator:8090/custom"); got != "http://orchestrator:8090/custom" {
		t.Fatalf("expected explicit custom path to be preserved, got %q", got)
	}
}

func TestBuildMCPConfigArg_IncludesScopedHeaders(t *testing.T) {
	t.Setenv("EMPIREAI_CLAUDE_USE_MCP", "true")
	t.Setenv("EMPIREAI_TOOL_GATEWAY_URL", "http://orchestrator:8090")
	t.Setenv("EMPIREAI_TOOL_GATEWAY_TOKEN", "tok")
	SetMCPTurnContextHooks(
		func(context.Context, time.Duration) string { return "test-token" },
		func(string) {},
	)

	cfg := &config.Config{}
	cfg.LLM.ClaudeCLI.Command = "claude"
	cfg.LLM.ClaudeCLI.Timeout = time.Second
	rt := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "owner", nil, nil, nil, nil)

	rec := runtimebus.NewEmittedEventsRecorder()
	ctx := runtimeactor.WithActor(context.Background(), models.AgentConfig{
		ID:         "a1",
		Role:       "market-research-agent",
		Mode:       "factory",
		VerticalID: "v1",
	})
	ctx = runtimebus.WithInboundEvent(ctx, events.Event{ID: "e1", Type: events.EventType("market_research.scan_assigned")})
	ctx = runtimebus.WithEmittedEventsRecorder(ctx, rec)

	s := &Session{
		AgentID: "a1",
		Tools: []ToolDefinition{
			{Name: "emit_category_assessed"},
			{Name: "emit_market_research_scan_complete"},
			{Name: "sql_execute"},
		},
	}
	raw, token, enabled, err := rt.buildMCPConfigArg(ctx, s)
	if err != nil {
		t.Fatalf("buildMCPConfigArg error: %v", err)
	}
	if !enabled {
		t.Fatal("expected mcp bridge enabled")
	}
	if strings.TrimSpace(token) == "" {
		t.Fatal("expected context token")
	}

	var cfgObj map[string]any
	if err := json.Unmarshal([]byte(raw), &cfgObj); err != nil {
		t.Fatalf("unmarshal mcp config: %v", err)
	}
	servers, _ := cfgObj["mcpServers"].(map[string]any)
	srvAny, ok := servers["empire-runtime"]
	if !ok {
		t.Fatalf("missing empire-runtime server in config: %#v", cfgObj)
	}
	srv, _ := srvAny.(map[string]any)
	if srv["type"] != "http" {
		t.Fatalf("expected http transport, got %#v", srv["type"])
	}
	rawURL, _ := srv["url"].(string)
	if !strings.HasPrefix(rawURL, "http://orchestrator:8090/mcp?") {
		t.Fatalf("unexpected mcp url: %#v", srv["url"])
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse mcp url: %v", err)
	}
	if got := strings.TrimSpace(u.Query().Get("empire_ctx_token")); got != strings.TrimSpace(token) {
		t.Fatalf("expected context token in query, got %q", got)
	}
	if got := strings.TrimSpace(u.Query().Get("empire_agent_id")); got != "a1" {
		t.Fatalf("expected agent id query, got %q", got)
	}
	if got := strings.TrimSpace(u.Query().Get("empire_allowed_tools")); got == "" || !strings.Contains(got, "emit_category_assessed") {
		t.Fatalf("expected allowed tools in query, got %q", got)
	}
	headers, _ := srv["headers"].(map[string]any)
	if headers["X-Empire-Agent-Id"] != "a1" {
		t.Fatalf("missing agent header: %#v", headers)
	}
	if headers["X-Empire-Agent-Role"] != "market-research-agent" {
		t.Fatalf("missing role header: %#v", headers)
	}
	if headers["X-Empire-Agent-Mode"] != "factory" {
		t.Fatalf("missing mode header: %#v", headers)
	}
	if headers["X-Empire-Vertical-Id"] != "v1" {
		t.Fatalf("missing vertical header: %#v", headers)
	}
	allowed, _ := headers["X-Empire-Allowed-Tools"].(string)
	if !strings.Contains(allowed, "emit_category_assessed") || !strings.Contains(allowed, "sql_execute") {
		t.Fatalf("missing allowed tools header: %#v", headers)
	}
	if headers["Authorization"] != "Bearer tok" {
		t.Fatalf("missing auth header: %#v", headers)
	}
}
