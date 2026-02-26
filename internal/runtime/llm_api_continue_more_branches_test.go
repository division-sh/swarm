package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/models"
)

func TestAnthropicAPIRuntime_ContinueSession_BudgetEmergencyStops(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session:     config.LLMSessionConfig{LockTTL: time.Second},
			ClaudeAPI:   config.ClaudeAPIConfig{DefaultModel: "m", MaxRetries: 1},
		},
	}
	b := &BudgetTracker{lastState: map[string]string{"vertical|v1": "emergency"}}
	r := NewAnthropicAPIRuntime(cfg, NewInMemorySessionRegistry(time.Second), "o", nil, nil, b)

	s, err := r.StartSession(context.Background(), "a1", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	ctx := WithActor(context.Background(), models.AgentConfig{ID: "a1", VerticalID: "v1"})
	if _, err := r.ContinueSession(ctx, s, Message{Role: "user", Content: "x"}); err == nil || !strings.Contains(err.Error(), "budget emergency") {
		t.Fatalf("expected budget emergency error, got %v", err)
	}
}

func TestAnthropicAPIRuntime_ContinueSession_ParseFailure_PersistsAndRotates(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session: config.LLMSessionConfig{
				LockTTL:               time.Second,
				RotateAfterTurns:      100,
				RotateOnParseFailures: 1,
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "m",
				MaxRetries:   1,
				RetryBackoff: 1 * time.Millisecond,
			},
		},
	}
	sessions := NewInMemorySessionRegistry(time.Second)
	turns := &apiTurnCapture{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
	}))
	defer srv.Close()

	r := NewAnthropicAPIRuntime(cfg, sessions, "owner", turns, nil, nil)
	r.apiURL = srv.URL
	r.httpClient = srv.Client()
	r.apiKey = "k"

	s, err := r.StartSession(context.Background(), "a1", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	oldID := s.ID
	ctx := WithActor(context.Background(), models.AgentConfig{ID: "a1", VerticalID: "v1"})
	if _, err := r.ContinueSession(ctx, s, Message{Role: "user", Content: "x"}); err == nil || !strings.Contains(err.Error(), "anthropic status 500") {
		t.Fatalf("expected anthropic status error, got %v", err)
	}
	if turns.calls != 1 || turns.last.ParseOK {
		t.Fatalf("expected persisted parse failure, calls=%d parseOK=%v", turns.calls, turns.last.ParseOK)
	}
	// Rotation resets parse failures and changes session id.
	if s.ID == oldID {
		t.Fatalf("expected session id to rotate after parse failures, old=%q new=%q", oldID, s.ID)
	}
	if s.ParseFailures != 0 || s.TurnCount != 0 || len(s.Messages) == 0 {
		t.Fatalf("expected reset state, session=%+v", s)
	}
}

