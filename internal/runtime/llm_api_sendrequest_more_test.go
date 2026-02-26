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

func TestAnthropicAPIRuntime_SendRequest_ErrorBranches(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session: config.LLMSessionConfig{
				LockTTL:               time.Second,
				RotateAfterTurns:      100,
				RotateOnParseFailures: 2,
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "m",
				MaxRetries:   1,
			},
		},
	}
	r := NewAnthropicAPIRuntime(cfg, NewInMemorySessionRegistry(time.Second), "o", nil, nil, nil)
	r.apiKey = "k"

	t.Run("decode error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(200)
			_, _ = w.Write([]byte("not-json"))
		}))
		defer srv.Close()
		r.apiURL = srv.URL
		r.httpClient = srv.Client()

		_, _, err := r.sendRequest(context.Background(), []byte(`{}`))
		if err == nil || !strings.Contains(err.Error(), "decode anthropic response") {
			t.Fatalf("expected decode error, got %v", err)
		}
	})

	t.Run("status error with message", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"message":"bad"}}`))
		}))
		defer srv.Close()
		r.apiURL = srv.URL
		r.httpClient = srv.Client()

		_, _, err := r.sendRequest(context.Background(), []byte(`{}`))
		if err == nil || !strings.Contains(err.Error(), "anthropic status 400") || !strings.Contains(err.Error(), "bad") {
			t.Fatalf("expected status error, got %v", err)
		}
	})

	t.Run("status error falls back to body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":""}}`))
		}))
		defer srv.Close()
		r.apiURL = srv.URL
		r.httpClient = srv.Client()

		_, _, err := r.sendRequest(context.Background(), []byte(`{}`))
		if err == nil || !strings.Contains(err.Error(), "anthropic status 500") {
			t.Fatalf("expected status error, got %v", err)
		}
	})

	t.Run("200 but error payload", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"error":{"message":"nope"}}`))
		}))
		defer srv.Close()
		r.apiURL = srv.URL
		r.httpClient = srv.Client()

		_, _, err := r.sendRequest(context.Background(), []byte(`{}`))
		if err == nil || !strings.Contains(err.Error(), "anthropic error") {
			t.Fatalf("expected anthropic error, got %v", err)
		}
	})
}

func TestAnthropicHelpers_ToAnthropicMessage_AndBuildRequestErrors(t *testing.T) {
	if _, ok := toAnthropicMessage(Message{Role: "user", Content: "  "}); ok {
		t.Fatal("expected empty content to be skipped")
	}
	// Assistant role should be preserved.
	am, ok := toAnthropicMessage(Message{Role: "assistant", Content: "hi"})
	if !ok || am.Role != "assistant" {
		t.Fatalf("expected assistant role, got ok=%v msg=%+v", ok, am)
	}
	// Unknown roles normalize to user.
	um, ok := toAnthropicMessage(Message{Role: "board_directive", Content: "x"})
	if !ok || um.Role != "user" {
		t.Fatalf("expected user role for unknown, got ok=%v msg=%+v", ok, um)
	}
	m, ok := toAnthropicMessage(Message{Role: "tool", Content: "x"})
	if !ok || m.Role != "user" || !strings.Contains(m.Content.(string), "Tool result:") {
		t.Fatalf("unexpected tool message: ok=%v msg=%+v", ok, m)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session:     config.LLMSessionConfig{LockTTL: time.Second},
			ClaudeAPI:   config.ClaudeAPIConfig{DefaultModel: ""},
		},
	}
	r := NewAnthropicAPIRuntime(cfg, NewInMemorySessionRegistry(time.Second), "o", nil, nil, nil)

	// No messages due to empty content.
	_, err := r.buildRequest(context.Background(), &Session{AgentID: "a", SystemPrompt: "s", Messages: []Message{{Role: "user", Content: " "}}}, Message{Role: "user", Content: ""})
	if err == nil {
		t.Fatal("expected messages required error")
	}

	// Model required error when default model is empty.
	ctx := WithActor(context.Background(), models.AgentConfig{ID: "a", Role: "pm-agent", VerticalID: "v1"})
	_, err = r.buildRequest(ctx, &Session{AgentID: "a", SystemPrompt: "s", Messages: []Message{{Role: "user", Content: "hi"}}}, Message{Role: "user", Content: "x"})
	if err == nil || !strings.Contains(err.Error(), "default_model is required") {
		t.Fatalf("expected model required error, got %v", err)
	}
}
