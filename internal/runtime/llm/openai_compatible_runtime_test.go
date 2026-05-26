package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"swarm/internal/config"
	runtimeactors "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/toolcapabilities"
	"swarm/internal/runtime/sessions"
)

func TestOpenAICompatibleRuntimeConversationToolBudgetAndPersistence(t *testing.T) {
	t.Setenv("OPENAI_COMPATIBLE_API_KEY", "test-key")

	var requests []openAICompatibleRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %s, want /v1/chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("authorization"); got != "Bearer test-key" {
			t.Fatalf("authorization = %q, want bearer test-key", got)
		}
		var req openAICompatibleRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		requests = append(requests, req)
		w.Header().Set("content-type", "application/json")
		switch len(requests) {
		case 1:
			if req.Model != "gpt-compatible-mini" {
				t.Fatalf("first request model = %q, want low-cost model", req.Model)
			}
			if len(req.Tools) != 1 || req.Tools[0].Type != "function" || req.Tools[0].Function.Name != "lookup" {
				t.Fatalf("tools = %#v, want lookup function tool", req.Tools)
			}
			_, _ = w.Write([]byte(`{
				"model":"gpt-compatible-mini",
				"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"query\":\"status\"}"}}]}}],
				"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}
			}`))
		case 2:
			if !requestHasToolResult(req.Messages, "call_1") {
				t.Fatalf("second request messages missing tool result for call_1: %#v", req.Messages)
			}
			if !requestHasAssistantToolCall(req.Messages, "call_1") {
				t.Fatalf("second request messages missing assistant tool_call call_1: %#v", req.Messages)
			}
			_, _ = w.Write([]byte(`{
				"model":"gpt-compatible-mini",
				"choices":[{"message":{"role":"assistant","content":"done"}}],
				"usage":{"prompt_tokens":13,"completion_tokens":5,"total_tokens":18}
			}`))
		default:
			t.Fatalf("unexpected request %d", len(requests))
		}
	}))
	defer server.Close()

	turns := &turnCapture{}
	conversations := &captureConversationStore{}
	budget := &budgetCapture{}
	cfg := openAICompatibleTestConfig(server.URL)
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
	if _, err := RequireProviderContract("openai_compatible", runtime); err != nil {
		t.Fatalf("RequireProviderContract: %v", err)
	}

	ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
		ID:        "agent-1",
		ModelTier: "low_cost",
		EntityID:  "entity-1",
	})
	ctx = sessions.WithScope(ctx, sessions.RuntimeModeSession.String(), sessions.SessionScopeGlobal.String(), "global")
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
	if conversations.record.SessionID == "" || conversations.record.Mode != sessions.RuntimeModeSession.String() {
		t.Fatalf("conversation record = %#v, want session snapshot", conversations.record)
	}
	if budget.runtimeMode != "openai_compatible" || !budget.exact {
		t.Fatalf("budget runtime=%q exact=%v, want openai_compatible exact", budget.runtimeMode, budget.exact)
	}
	if budget.usage.InputTokens != 13 || budget.usage.OutputTokens != 5 || budget.usage.Model != "gpt-compatible-mini" {
		t.Fatalf("budget usage = %#v, want final response usage", budget.usage)
	}
}

func TestOpenAICompatibleRuntimeFailsClosedWhenUsageMissing(t *testing.T) {
	t.Setenv("OPENAI_COMPATIBLE_API_KEY", "test-key")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"model":"gpt-compatible","choices":[{"message":{"role":"assistant","content":"done"}}]}`))
	}))
	defer server.Close()

	turns := &turnCapture{}
	runtime := NewOpenAICompatibleRuntime(openAICompatibleTestConfig(server.URL), sessions.NewInMemoryRegistry(time.Second), "worker-1", turns, nil, nil, nil)
	ctx := sessions.WithScope(context.Background(), sessions.RuntimeModeTask.String(), "", "task-1")
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

func TestOpenAICompatibleRuntimeFailsClosedWhenCredentialMissing(t *testing.T) {
	t.Setenv("OPENAI_COMPATIBLE_API_KEY", "")
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests++
	}))
	defer server.Close()

	runtime := NewOpenAICompatibleRuntime(openAICompatibleTestConfig(server.URL), sessions.NewInMemoryRegistry(time.Second), "worker-1", nil, nil, nil, nil)
	ctx := sessions.WithScope(context.Background(), sessions.RuntimeModeTask.String(), "", "task-1")
	session, err := runtime.StartSession(ctx, "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	_, err = runtime.ContinueSession(ctx, session, Message{Role: "user", Content: "hello"})
	if err == nil || !strings.Contains(err.Error(), "OPENAI_COMPATIBLE_API_KEY") {
		t.Fatalf("ContinueSession error = %v, want missing OPENAI_COMPATIBLE_API_KEY", err)
	}
	if requests != 0 {
		t.Fatalf("requests = %d, want fail closed before HTTP request", requests)
	}
}

func TestOpenAICompatibleChatCompletionsURLNormalizesVersionSegment(t *testing.T) {
	tests := []struct {
		name string
		base string
		want string
	}{
		{name: "server root", base: "https://example.test", want: "https://example.test/v1/chat/completions"},
		{name: "api version base", base: "https://example.test/v1", want: "https://example.test/v1/chat/completions"},
		{name: "trim slash", base: "https://example.test/v1/", want: "https://example.test/v1/chat/completions"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := openAICompatibleChatCompletionsURL(tt.base); got != tt.want {
				t.Fatalf("openAICompatibleChatCompletionsURL(%q) = %q, want %q", tt.base, got, tt.want)
			}
		})
	}
}

func TestOpenAICompatibleToolResultMessagesPreservesFallbackWithoutToolCallID(t *testing.T) {
	content := `[{"name":"lookup","tool_call_id":"call_1","ok":true,"result":{"status":"ok"}},{"name":"__runtime_guardrail__","ok":false,"error":"tool output too large"}]`
	msgs := openAICompatibleToolResultMessages(content)
	if len(msgs) != 2 {
		t.Fatalf("messages = %#v, want tool message plus fallback", msgs)
	}
	if msgs[0].Role != "tool" || msgs[0].ToolCallID != "call_1" {
		t.Fatalf("first message = %#v, want call_1 tool result", msgs[0])
	}
	if msgs[1].Role != "user" || !strings.Contains(msgs[1].Content, "__runtime_guardrail__") {
		t.Fatalf("second message = %#v, want preserved guardrail fallback", msgs[1])
	}
}

func openAICompatibleTestConfig(baseURL string) *config.Config {
	return &config.Config{
		LLM: config.LLMConfig{
			Backend: "openai_compatible",
			Session: config.LLMSessionConfig{
				LockTTL:               time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			OpenAICompatible: config.OpenAICompatibleConfig{
				BaseURL:      baseURL,
				DefaultModel: "gpt-compatible",
				LowCostModel: "gpt-compatible-mini",
			},
		},
	}
}

func requestHasToolResult(messages []openAICompatibleMessage, id string) bool {
	for _, msg := range messages {
		if msg.Role == "tool" && msg.ToolCallID == id {
			return true
		}
	}
	return false
}

func requestHasAssistantToolCall(messages []openAICompatibleMessage, id string) bool {
	for _, msg := range messages {
		if msg.Role != "assistant" {
			continue
		}
		for _, call := range msg.ToolCalls {
			if call.ID == id {
				return true
			}
		}
	}
	return false
}

type openAIToolExecutor struct{}

func (openAIToolExecutor) Execute(context.Context, string, any) (any, error) {
	return map[string]any{"answer": "ok"}, nil
}

func (openAIToolExecutor) ToolCapabilitiesForActor(runtimeactors.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set {
	return toolcapabilities.NewSet([]toolcapabilities.Capability{{
		Name:     "lookup",
		Visible:  true,
		Callable: true,
	}})
}

type budgetCapture struct {
	runtimeMode string
	usage       UsageTokens
	exact       bool
}

func (b *budgetCapture) LockExecutionScope(string) func() { return func() {} }
func (b *budgetCapture) IsEntityEmergency(string) bool    { return false }
func (b *budgetCapture) IsEntityThrottle(string) bool     { return false }
func (b *budgetCapture) IsEmergency(string) bool          { return false }
func (b *budgetCapture) IsThrottle(string) bool           { return false }
func (b *budgetCapture) RecordEntityLLMUsage(_ context.Context, _ string, _ string, runtimeMode string, usage UsageTokens, exact bool, _ any) error {
	b.runtimeMode = runtimeMode
	b.usage = usage
	b.exact = exact
	return nil
}
func (b *budgetCapture) RecordLLMUsage(ctx context.Context, entityID string, agentID string, runtimeMode string, usage UsageTokens, exact bool, meta any) error {
	return b.RecordEntityLLMUsage(ctx, entityID, agentID, runtimeMode, usage, exact, meta)
}
