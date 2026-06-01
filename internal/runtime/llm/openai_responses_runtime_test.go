package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"swarm/internal/config"
	runtimeactors "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/sessions"
)

func TestOpenAIResponsesRuntimeConversationToolBudgetAndPersistence(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key")

	var requests []openAIResponsesRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("path = %s, want /responses", r.URL.Path)
		}
		if got := r.Header.Get("authorization"); got != "Bearer test-key" {
			t.Fatalf("authorization = %q, want bearer test-key", got)
		}
		var req openAIResponsesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		requests = append(requests, req)
		w.Header().Set("content-type", "application/json")
		switch len(requests) {
		case 1:
			if req.Model != "gpt-5.4-nano" {
				t.Fatalf("first request model = %q, want cheap model", req.Model)
			}
			if !strings.Contains(req.Instructions, "system prompt") {
				t.Fatalf("instructions = %q, want system prompt", req.Instructions)
			}
			if len(req.Tools) != 1 || req.Tools[0].Type != "function" || req.Tools[0].Name != "lookup" {
				t.Fatalf("tools = %#v, want lookup function tool", req.Tools)
			}
			if !openAIResponsesRequestHasMessage(req.Input, "user", "check status") {
				t.Fatalf("first request input missing user message: %#v", req.Input)
			}
			_, _ = w.Write([]byte(`{
				"id":"resp_1",
				"model":"gpt-5.4-nano",
				"output":[{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"query\":\"status\"}"}],
				"usage":{"input_tokens":11,"output_tokens":7,"total_tokens":18}
			}`))
		case 2:
			if !openAIResponsesRequestHasFunctionCall(req.Input, "call_1") {
				t.Fatalf("second request input missing prior function_call call_1: %#v", req.Input)
			}
			if !openAIResponsesRequestHasFunctionOutput(req.Input, "call_1") {
				t.Fatalf("second request input missing function_call_output call_1: %#v", req.Input)
			}
			_, _ = w.Write([]byte(`{
				"id":"resp_2",
				"model":"gpt-5.4-nano",
				"output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],
				"usage":{"input_tokens":13,"output_tokens":5,"total_tokens":18}
			}`))
		default:
			t.Fatalf("unexpected request %d", len(requests))
		}
	}))
	defer server.Close()

	turns := &turnCapture{}
	conversations := &captureConversationStore{}
	budget := &budgetCapture{}
	cfg := openAIResponsesTestConfig(server.URL)
	runtime, err := RuntimeFactory{
		Cfg:           cfg,
		Sessions:      sessions.NewInMemoryRegistry(time.Second),
		Turns:         turns,
		Conversations: conversations,
		Budget:        budget,
		LockOwner:     "worker-1",
	}.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if _, err := RequireProviderContract("openai_responses", runtime); err != nil {
		t.Fatalf("RequireProviderContract: %v", err)
	}

	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID:       "agent-1",
		Model:    "cheap",
		EntityID: "entity-1",
		FlowPath: "support/inst-1",
	})
	ctx = sessions.WithScope(ctx, sessions.RuntimeModeSession.String(), sessions.SessionScopeFlow.String(), "support/inst-1")
	conv := NewConversation("agent-1", "task-1", "system prompt", []ToolDefinition{{
		Name:        "lookup",
		Description: "Lookup status",
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{"type": "string"},
			},
			"required": []any{"query"},
		},
	}}, SessionScoped, 5, runtime)
	conv.SetToolExecutor(openAIToolExecutor{})

	resp, err := conv.Step(ctx, "check status")
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if resp.Message.Content != "done" {
		t.Fatalf("response content = %q, want done", resp.Message.Content)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(requests))
	}
	if len(turns.records) != 2 {
		t.Fatalf("turn records = %d, want 2", len(turns.records))
	}
	if turns.records[0].RuntimeMode != sessions.RuntimeModeSession.String() || !turns.records[0].ParseOK {
		t.Fatalf("first turn record = %#v", turns.records[0])
	}
	if len(turns.records[0].ToolCalls) != 1 || turns.records[0].ToolCalls[0].ID != "call_1" {
		t.Fatalf("first turn tool calls = %#v, want call_1", turns.records[0].ToolCalls)
	}
	if !strings.Contains(string(turns.records[0].ResponseRaw), `"resp_1"`) {
		t.Fatalf("first turn raw response = %s, want raw Responses payload", string(turns.records[0].ResponseRaw))
	}
	if conversations.record.SessionID == "" || conversations.record.Mode != sessions.RuntimeModeSession.String() {
		t.Fatalf("conversation record = %#v, want session snapshot", conversations.record)
	}
	if budget.runtimeMode != "openai_responses" || !budget.exact {
		t.Fatalf("budget runtime=%q exact=%v, want openai_responses exact", budget.runtimeMode, budget.exact)
	}
	if budget.usage.InputTokens != 13 || budget.usage.OutputTokens != 5 || budget.usage.Model != "gpt-5.4-nano" {
		t.Fatalf("budget usage = %#v, want final response usage", budget.usage)
	}
	if budget.meta["backend_profile"] != "openai_responses" || budget.meta["provider"] != "openai" || budget.meta["transport"] != "api" || budget.meta["resolved_model"] != "gpt-5.4-nano" {
		t.Fatalf("budget meta = %#v, want backend/provider/transport/model facts", budget.meta)
	}
}

func TestOpenAIResponsesRuntimeFailsClosedWhenUsageMissing(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_1","model":"gpt-5.4","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}`))
	}))
	defer server.Close()

	turns := &turnCapture{}
	runtime := NewOpenAIResponsesRuntime(openAIResponsesTestConfig(server.URL), sessions.NewInMemoryRegistry(time.Second), "worker-1", turns, nil, nil, nil)
	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{ID: "agent-1", Model: "regular"})
	ctx = sessions.WithScope(ctx, sessions.RuntimeModeTask.String(), "", "task-1")
	session, err := runtime.StartSession(ctx, "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	_, err = runtime.ContinueSession(ctx, session, Message{Role: "user", Content: "hello"})
	if err == nil || !strings.Contains(err.Error(), "missing usage") {
		t.Fatalf("ContinueSession error = %v, want missing usage", err)
	}
	if len(turns.records) != 1 || turns.records[0].ParseOK {
		t.Fatalf("turn records = %#v, want parse failure", turns.records)
	}
}

func TestOpenAIResponsesRuntimeFailsClosedWhenCredentialMissing(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests++
	}))
	defer server.Close()

	runtime := NewOpenAIResponsesRuntime(openAIResponsesTestConfig(server.URL), sessions.NewInMemoryRegistry(time.Second), "worker-1", nil, nil, nil, nil)
	ctx := sessions.WithScope(context.Background(), sessions.RuntimeModeTask.String(), "", "task-1")
	session, err := runtime.StartSession(ctx, "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	_, err = runtime.ContinueSession(ctx, session, Message{Role: "user", Content: "hello"})
	if err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY") {
		t.Fatalf("ContinueSession error = %v, want missing OPENAI_API_KEY", err)
	}
	if requests != 0 {
		t.Fatalf("requests = %d, want fail closed before HTTP request", requests)
	}
}

func TestOpenAIResponsesURLUsesConfiguredBaseURL(t *testing.T) {
	tests := []struct {
		name string
		base string
		want string
	}{
		{name: "official version base", base: "https://api.openai.com/v1", want: "https://api.openai.com/v1/responses"},
		{name: "local fixture root", base: "https://fixture.test", want: "https://fixture.test/responses"},
		{name: "trim slash", base: "https://fixture.test/v1/", want: "https://fixture.test/v1/responses"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := openAIResponsesURL(tt.base); got != tt.want {
				t.Fatalf("openAIResponsesURL(%q) = %q, want %q", tt.base, got, tt.want)
			}
		})
	}
}

func TestOpenAIResponsesSSEParserNormalizesTextAndFunctionCalls(t *testing.T) {
	raw := strings.Join([]string{
		`data: {"type":"response.output_text.delta","delta":"hel"}`,
		``,
		`data: {"type":"response.output_text.delta","delta":"lo"}`,
		``,
		`data: {"type":"response.function_call_arguments.done","item_id":"fc_1","call_id":"call_1","name":"lookup","arguments":"{\"query\":\"status\"}"}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n")
	resp, err := parseOpenAIResponsesSSE([]byte(raw))
	if err != nil {
		t.Fatalf("parseOpenAIResponsesSSE: %v", err)
	}
	if resp.Message.Content != "hello" {
		t.Fatalf("content = %q, want hello", resp.Message.Content)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].ID != "call_1" || resp.ToolCalls[0].Name != "lookup" {
		t.Fatalf("tool calls = %#v, want lookup call_1", resp.ToolCalls)
	}
}

func TestOpenAIResponsesSSEParserNormalizesFunctionCallLifecycleThroughCompleted(t *testing.T) {
	raw := strings.Join([]string{
		`data: {"type":"response.output_item.added","output_index":0,"item":{"id":"fc_1","type":"function_call","status":"in_progress","call_id":"call_1","name":"lookup","arguments":""}}`,
		``,
		`data: {"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":0,"delta":"{\"query\""}`,
		``,
		`data: {"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":0,"delta":":\"status\"}"}`,
		``,
		`data: {"type":"response.function_call_arguments.done","item_id":"fc_1","output_index":0,"arguments":"{\"query\":\"status\"}"}`,
		``,
		`data: {"type":"response.output_item.done","output_index":0,"item":{"id":"fc_1","type":"function_call","status":"completed","call_id":"call_1","name":"lookup","arguments":""}}`,
		``,
		`data: {"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.4","output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"need lookup"}]},{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":""}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n")
	resp, err := parseOpenAIResponsesSSE([]byte(raw))
	if err != nil {
		t.Fatalf("parseOpenAIResponsesSSE: %v", err)
	}
	if resp.Message.Content != "need lookup" {
		t.Fatalf("content = %q, want completed response text", resp.Message.Content)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].ID != "call_1" || resp.ToolCalls[0].Name != "lookup" {
		t.Fatalf("tool calls = %#v, want lookup call_1", resp.ToolCalls)
	}
	args, ok := resp.ToolCalls[0].Arguments.(map[string]any)
	if !ok || args["query"] != "status" {
		t.Fatalf("tool call arguments = %#v, want streamed query status", resp.ToolCalls[0].Arguments)
	}
}

func openAIResponsesTestConfig(baseURL string) *config.Config {
	return &config.Config{
		LLM: config.LLMConfig{
			Backend: "openai_responses",
			Session: config.LLMSessionConfig{
				LockTTL:               time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			OpenAIResponses: config.OpenAIResponsesConfig{
				BaseURL: baseURL,
			},
		},
	}
}

func openAIResponsesRequestHasMessage(items []any, role, content string) bool {
	for _, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if obj["role"] == role && strings.Contains(fmtString(obj["content"]), content) {
			return true
		}
	}
	return false
}

func openAIResponsesRequestHasFunctionCall(items []any, id string) bool {
	for _, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if obj["type"] == "function_call" && obj["call_id"] == id {
			return true
		}
	}
	return false
}

func openAIResponsesRequestHasFunctionOutput(items []any, id string) bool {
	for _, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if obj["type"] == "function_call_output" && obj["call_id"] == id {
			return true
		}
	}
	return false
}

func fmtString(v any) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(strings.Trim(fmt.Sprintf("%v", v), `"`)), `\n`, "\n"), `\t`, "\t"))
}
