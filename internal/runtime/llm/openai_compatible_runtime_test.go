package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

func TestOpenAICompatibleRuntimeConversationToolBudgetAndPersistence(t *testing.T) {
	t.Setenv("OPENAI_COMPATIBLE_API_KEY", "stale-test-key")

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

	conversations := &captureConversationStore{}
	harness := effecttest.New()
	harness.Token.AgentID = "agent-1"
	cfg := openAICompatibleTestConfig(server.URL)
	runtime, err := RuntimeFactory{
		Cfg:                  cfg,
		Sessions:             sessions.NewInMemoryRegistry(time.Second),
		Conversations:        conversations,
		LockOwner:            "worker-1",
		Credentials:          testProviderCredentialResolver(t, "OPENAI_COMPATIBLE_API_KEY", "test-key").Store,
		CompletionController: runtimeeffects.NewCompletionController(harness, harness),
	}.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if _, err := RequireProviderContract("openai_compatible", runtime); err != nil {
		t.Fatalf("RequireProviderContract: %v", err)
	}

	ctx := runtimeactors.WithActor(harness.Context("openai-compatible-tool-loop"), runtimeactors.AgentConfig{
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
	settlements := harness.CompletionSettlementsForAdapter("openai_compatible")
	if len(settlements) != 2 {
		t.Fatalf("completion settlements = %d, want 2", len(settlements))
	}
	if settlements[0].AgentTurn == nil || settlements[0].AgentTurn.RuntimeMode != sessions.RuntimeModeSession.String() || !settlements[0].AgentTurn.ParseOK || !strings.Contains(string(settlements[0].AgentTurn.ToolCalls), `"call_1"`) {
		t.Fatalf("first completion turn = %#v, want session-mode call_1", settlements[0].AgentTurn)
	}
	if conversations.record.SessionID == "" || conversations.record.Mode != sessions.RuntimeModeSession.String() {
		t.Fatalf("conversation record = %#v, want session snapshot", conversations.record)
	}
	if settlements[1].Usage.InputTokens == nil || *settlements[1].Usage.InputTokens != 13 || settlements[1].Usage.OutputTokens == nil || *settlements[1].Usage.OutputTokens != 5 || settlements[1].Usage.ResolvedModel != "gpt-compatible-mini" {
		t.Fatalf("final completion usage = %#v", settlements[1].Usage)
	}
}

func TestOpenAICompatibleRuntimeFailsClosedWhenUsageMissing(t *testing.T) {
	t.Setenv("OPENAI_COMPATIBLE_API_KEY", "stale-test-key")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"model":"gpt-compatible","choices":[{"message":{"role":"assistant","content":"done"}}]}`))
	}))
	defer server.Close()

	harness := effecttest.New()
	harness.Token.AgentID = "agent-1"
	runtime := NewOpenAICompatibleRuntime(openAICompatibleTestConfig(server.URL), sessions.NewInMemoryRegistry(time.Second), "worker-1", nil, nil)
	runtime.completionController = runtimeeffects.NewCompletionController(harness, harness)
	runtime.credentials = testProviderCredentialResolver(t, "OPENAI_COMPATIBLE_API_KEY", "test-key")
	ctx := runtimeactors.WithActor(harness.Context("openai-compatible-missing-usage"), runtimeactors.AgentConfig{ID: "agent-1", Model: "regular"})
	ctx = sessions.WithScope(ctx, sessions.RuntimeModeTask.String(), "", "task-1")
	session, err := runtime.StartSession(ctx, "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	_, err = runtime.ContinueSession(ctx, session, Message{Role: "user", Content: "hello"})
	if err == nil || !strings.Contains(err.Error(), "missing usage") {
		t.Fatalf("ContinueSession error = %v, want missing usage", err)
	}
	settlements := harness.CompletionSettlementsForAdapter("openai_compatible")
	if len(settlements) != 1 || settlements[0].AgentTurn == nil || settlements[0].AgentTurn.ParseOK || settlements[0].Usage.Exactness != runtimeeffects.CompletionUsageUnavailable {
		state, _ := harness.StateForAdapter("openai_compatible")
		failure, _ := runtimefailures.As(err)
		t.Fatalf("error=%v detail=%#v state=%s settlements=%#v, want one unavailable completion failure", err, failure.Failure.Detail, state, settlements)
	}
}

func TestAnthropicAPIRuntimeFailsClosedWhenUsageMissingForBudgetAccounting(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"model":"claude-3-5-sonnet","content":[{"type":"text","text":"done"}]}`))
	}))
	defer server.Close()

	harness := effecttest.New()
	harness.Token.AgentID = "agent-1"
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(time.Second), "worker-1", nil, nil)
	runtime.completionController = runtimeeffects.NewCompletionController(harness, harness)
	runtime.apiURL = server.URL
	runtime.apiKey = "test-key"

	ctx := runtimeactors.WithActor(harness.Context("anthropic-missing-usage"), runtimeactors.AgentConfig{ID: "agent-1", Model: "regular"})
	ctx = sessions.WithScope(ctx, sessions.RuntimeModeTask.String(), "", "task-1")
	session, err := runtime.StartSession(ctx, "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	_, err = runtime.ContinueSession(ctx, session, Message{Role: "user", Content: "hello"})
	if err == nil || !strings.Contains(err.Error(), "missing usage") {
		t.Fatalf("ContinueSession error = %v, want missing usage", err)
	}
	settlements := harness.CompletionSettlementsForAdapter("anthropic_api")
	if len(settlements) != 1 || settlements[0].AgentTurn == nil || settlements[0].AgentTurn.ParseOK || settlements[0].AgentTurn.Failure == nil {
		state, _ := harness.StateForAdapter("anthropic_api")
		failure, _ := runtimefailures.As(err)
		t.Fatalf("error=%v detail=%#v state=%s settlements=%#v, want one immutable missing-usage failure completion", err, failure.Failure.Detail, state, settlements)
	}
}

func TestAnthropicUsageExtractionRequiresBothUsageFields(t *testing.T) {
	usage, ok := extractUsageTokensFromJSON([]byte(`{"model":"claude-3-5-sonnet","usage":{"input_tokens":3,"output_tokens":4}}`))
	if !ok {
		t.Fatal("extractUsageTokensFromJSON returned ok=false for valid usage")
	}
	if usage.Model != "claude-3-5-sonnet" || usage.InputTokens != 3 || usage.OutputTokens != 4 {
		t.Fatalf("usage = %#v, want model/input/output", usage)
	}
	for _, raw := range [][]byte{
		nil,
		[]byte(`{}`),
		[]byte(`{"model":"claude","usage":{"input_tokens":1}}`),
		[]byte(`not-json`),
	} {
		if got, ok := extractUsageTokensFromJSON(raw); ok {
			t.Fatalf("extractUsageTokensFromJSON(%q) = %#v, true; want false", raw, got)
		}
	}
}

func TestOpenAICompatibleRuntimeFailsClosedWhenCredentialMissing(t *testing.T) {
	t.Setenv("OPENAI_COMPATIBLE_API_KEY", "")
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests++
	}))
	defer server.Close()

	runtime := NewOpenAICompatibleRuntime(openAICompatibleTestConfig(server.URL), sessions.NewInMemoryRegistry(time.Second), "worker-1", nil, nil)
	ctx := sessions.WithScope(unmanagedLLMTestContext(), sessions.RuntimeModeTask.String(), "", "task-1")
	session, err := runtime.StartSession(ctx, "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	_, err = runtime.ContinueSession(ctx, session, Message{Role: "user", Content: "hello"})
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != runtimefailures.ClassAuthenticationNeeded || failure.Failure.Detail.Code != "provider_credential_missing" {
		t.Fatalf("ContinueSession failure = %#v, want authentication required", failure)
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
