package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/models"
	runtimeactor "empireai/internal/runtime/actorctx"
	"empireai/internal/runtime/sessions"
)

type apiTurnCapture struct {
	calls int
	last  AgentTurnRecord
}

func (t *apiTurnCapture) AppendAgentTurn(_ context.Context, rec AgentTurnRecord) error {
	t.calls++
	t.last = rec
	return nil
}

type apiConvoCapture struct {
	calls int
	last  ConversationRecord
}

func (c *apiConvoCapture) UpsertConversation(_ context.Context, rec ConversationRecord) error {
	c.calls++
	c.last = rec
	return nil
}
func (c *apiConvoCapture) LoadActiveConversation(_ context.Context, _ string, _ string, _ string) (ConversationRecord, bool, error) {
	return ConversationRecord{}, false, nil
}

func TestAnthropicAPIRuntime_ContinueSession_SendsRequestParsesUsageAndIncrementsTurn(t *testing.T) {
	var seenReq map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if got := r.Header.Get("x-api-key"); got != "test-key" {
			t.Fatalf("expected x-api-key test-key, got %q", got)
		}
		if got := r.Header.Get("anthropic-version"); got == "" {
			t.Fatalf("expected anthropic-version header")
		}
		_ = json.NewDecoder(r.Body).Decode(&seenReq)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{
		  "model":"claude-test",
		  "usage":{"input_tokens":12,"output_tokens":34},
		  "content":[
		    {"type":"text","text":"hello"},
		    {"type":"tool_use","name":"echo","input":{"text":"ok"}}
		  ]
		}`))
	}))
	defer srv.Close()

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session: config.LLMSessionConfig{
				LockTTL:               5 * time.Second,
				RotateAfterTurns:      100,
				RotateOnParseFailures: 2,
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "claude-sonnet-test",
				MaxRetries:   1,
				RetryBackoff: 1 * time.Millisecond,
			},
		},
	}

	sessions := sessions.NewInMemoryRegistry(5 * time.Second)
	turns := &apiTurnCapture{}
	convos := &apiConvoCapture{}

	r := NewAnthropicAPIRuntime(cfg, sessions, "lock-owner-1", turns, convos, nil)
	r.apiURL = srv.URL
	r.apiKey = "test-key"
	r.httpClient = srv.Client()

	s, err := r.StartSession(context.Background(), "agent-1", "sys", []ToolDefinition{{Name: "echo", Description: "echo"}})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s.ID == "" {
		t.Fatal("expected non-empty session id")
	}

	ctx := runtimeactor.WithActor(context.Background(), models.AgentConfig{ID: "agent-1", Type: "worker", Role: "pm-agent", Mode: "operating", VerticalID: "v1"})
	resp, err := r.ContinueSession(ctx, s, Message{Role: "user", Content: "hi"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp.Message.Content != "hello" {
		t.Fatalf("expected text 'hello', got %q", resp.Message.Content)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Name != "echo" {
		t.Fatalf("expected one tool call echo, got %+v", resp.ToolCalls)
	}
	if seenReq["model"] != "claude-sonnet-test" {
		t.Fatalf("expected model claude-sonnet-test, got %#v", seenReq["model"])
	}

	if turns.calls != 1 || !turns.last.ParseOK {
		t.Fatalf("expected 1 persisted turn parseOK, got calls=%d parseOK=%v", turns.calls, turns.last.ParseOK)
	}
	if convos.calls != 1 || convos.last.AgentID != "agent-1" {
		t.Fatalf("expected 1 persisted conversation, got calls=%d last=%+v", convos.calls, convos.last)
	}

	// Explicit snapshot persistence should also write.
	if err := r.PersistConversationSnapshot(context.Background(), s); err != nil {
		t.Fatalf("PersistConversationSnapshot: %v", err)
	}
	if convos.calls < 2 {
		t.Fatalf("expected snapshot to persist conversation, calls=%d", convos.calls)
	}

	// InMemorySessionRegistry should have had its turn incremented.
	rec, ok := sessions.Snapshot("agent-1")
	if !ok {
		t.Fatal("expected session snapshot")
	}
	if rec.TurnCount != 1 {
		t.Fatalf("expected turn_count=1, got %d", rec.TurnCount)
	}
}

func TestExtractUsageTokensFromJSON(t *testing.T) {
	u := extractUsageTokensFromJSON([]byte(`{"model":"m","usage":{"input_tokens":1,"output_tokens":2}}`))
	if u.Model != "m" || u.InputTokens != 1 || u.OutputTokens != 2 {
		t.Fatalf("unexpected usage: %+v", u)
	}
}
