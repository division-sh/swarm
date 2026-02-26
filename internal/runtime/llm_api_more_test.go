package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/models"
)

type convStubAPI struct {
	loadOK    bool
	loadRec   ConversationRecord
	upsertErr error
}

func (c *convStubAPI) UpsertConversation(context.Context, ConversationRecord) error {
	return c.upsertErr
}
func (c *convStubAPI) LoadActiveConversation(context.Context, string, string, string) (ConversationRecord, bool, error) {
	return c.loadRec, c.loadOK, nil
}

type turnStubAPI struct{ err error }

func (t *turnStubAPI) AppendAgentTurn(context.Context, AgentTurnRecord) error { return t.err }

func TestAnthropicAPIRuntime_StartContinue_MissingKey_And_Success(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session: config.LLMSessionConfig{
				LockTTL:               1 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "claude-test",
				HaikuModel:   "claude-haiku-test",
				MaxRetries:   1,
				RetryBackoff: 1 * time.Millisecond,
			},
		},
	}
	sessions := NewInMemorySessionRegistry(1 * time.Second)
	convs := &convStubAPI{loadOK: true, loadRec: ConversationRecord{Messages: []Message{{Role: "user", Content: "prior"}}, TurnCount: 1}, upsertErr: os.ErrInvalid}
	turns := &turnStubAPI{err: os.ErrInvalid}

	// Fake server that returns a minimal anthropicResponse.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Validate headers exist to exercise sendRequest header set.
		if strings.TrimSpace(r.Header.Get("x-api-key")) == "" {
			http.Error(w, `{"error":{"message":"missing key"}}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{
			"model":"claude-test",
			"usage":{"input_tokens":10,"output_tokens":5},
			"content":[{"type":"text","text":"ok"}]
		}`))
	}))
	defer ts.Close()

	r := NewAnthropicAPIRuntime(cfg, sessions, "lock", turns, convs, nil)
	r.apiURL = ts.URL
	r.httpClient = ts.Client()

	s, err := r.StartSession(context.Background(), "a1", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s.TurnCount != 1 {
		t.Fatalf("expected loaded turn_count=1, got %d", s.TurnCount)
	}

	// Missing key error.
	r.apiKey = ""
	if _, err := r.ContinueSession(context.Background(), s, Message{Role: "user", Content: "x"}); err == nil {
		t.Fatalf("expected missing ANTHROPIC_API_KEY error")
	}

	// Success.
	r.apiKey = "k"
	ctx := WithActor(context.Background(), models.AgentConfig{ID: "a1", Type: "haiku", VerticalID: "v1"})
	resp, err := r.ContinueSession(ctx, s, Message{Role: "user", Content: "go"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp == nil || strings.TrimSpace(resp.Message.Content) != "ok" {
		t.Fatalf("unexpected resp: %#v", resp)
	}
}

func TestAnthropicAPIRuntime_NilSession_Error(t *testing.T) {
	r := NewAnthropicAPIRuntime(&config.Config{LLM: config.LLMConfig{RuntimeMode: "api", Session: config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3}, ClaudeAPI: config.ClaudeAPIConfig{DefaultModel: "m"}}}, NewInMemorySessionRegistry(time.Second), "x", nil, nil, nil)
	if _, err := r.ContinueSession(context.Background(), nil, Message{Role: "user", Content: "x"}); err == nil {
		t.Fatalf("expected nil session error")
	}
}

func TestExtractUsageTokensFromJSON_More(t *testing.T) {
	if u := extractUsageTokensFromJSON(nil); u.InputTokens != 0 || u.OutputTokens != 0 {
		t.Fatalf("expected empty usage")
	}
	// Invalid JSON should return empty usage.
	if u := extractUsageTokensFromJSON([]byte("{")); u.InputTokens != 0 || u.OutputTokens != 0 || u.Model != "" {
		t.Fatalf("expected empty usage on decode error, got %#v", u)
	}
	raw := []byte(`{"model":"m","usage":{"input_tokens":1,"output_tokens":2}}`)
	u := extractUsageTokensFromJSON(raw)
	if u.Model != "m" || u.InputTokens != 1 || u.OutputTokens != 2 {
		t.Fatalf("unexpected usage: %#v", u)
	}
}
