package llm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/models"
	runtimeactor "empireai/internal/runtime/actorctx"
	"empireai/internal/runtime/sessions"
	runtimetestkit "empireai/internal/runtime/testkit"
)

type budgetGuardStub struct {
	emergency bool
	throttle  bool
}

func (b *budgetGuardStub) LockExecutionScope(string) func() { return func() {} }
func (b *budgetGuardStub) IsEmergency(string) bool          { return b != nil && b.emergency }
func (b *budgetGuardStub) IsThrottle(string) bool           { return b != nil && b.throttle }
func (b *budgetGuardStub) RecordLLMUsage(context.Context, string, string, string, UsageTokens, bool, any) error {
	return nil
}

func TestAnthropicAPIRuntime_ContinueSession_BudgetEmergencyStops(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session:     config.LLMSessionConfig{LockTTL: time.Second},
			ClaudeAPI:   config.ClaudeAPIConfig{DefaultModel: "m", MaxRetries: 1},
		},
	}
	b := &budgetGuardStub{emergency: true}
	r := NewAnthropicAPIRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "o", nil, nil, b)

	s, err := r.StartSession(context.Background(), "a1", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	ctx := runtimeactor.WithActor(context.Background(), models.AgentConfig{ID: "a1", VerticalID: "v1"})
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
	sessions := sessions.NewInMemoryRegistry(time.Second)
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
	ctx := runtimeactor.WithActor(context.Background(), models.AgentConfig{ID: "a1", VerticalID: "v1"})
	if _, err := r.ContinueSession(ctx, s, Message{Role: "user", Content: "x"}); err == nil || !strings.Contains(err.Error(), "anthropic status 500") {
		t.Fatalf("expected anthropic status error, got %v", err)
	}
	if turns.calls != 1 || turns.last.ParseOK {
		t.Fatalf("expected persisted parse failure, calls=%d parseOK=%v", turns.calls, turns.last.ParseOK)
	}

	if s.ID == oldID {
		t.Fatalf("expected session id to rotate after parse failures, old=%q new=%q", oldID, s.ID)
	}
	if s.ParseFailures != 0 || s.TurnCount != 0 || len(s.Messages) == 0 {
		t.Fatalf("expected reset state, session=%+v", s)
	}
}

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
	sessions := sessions.NewInMemoryRegistry(1 * time.Second)
	convs := &convStubAPI{loadOK: true, loadRec: ConversationRecord{Messages: []Message{{Role: "user", Content: "prior"}}, TurnCount: 1}, upsertErr: os.ErrInvalid}
	turns := &turnStubAPI{err: os.ErrInvalid}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

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

	r.apiKey = ""
	if _, err := r.ContinueSession(context.Background(), s, Message{Role: "user", Content: "x"}); err == nil {
		t.Fatalf("expected missing ANTHROPIC_API_KEY error")
	}

	r.apiKey = "k"
	ctx := runtimeactor.WithActor(context.Background(), models.AgentConfig{ID: "a1", Type: "haiku", VerticalID: "v1"})
	resp, err := r.ContinueSession(ctx, s, Message{Role: "user", Content: "go"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp == nil || strings.TrimSpace(resp.Message.Content) != "ok" {
		t.Fatalf("unexpected resp: %#v", resp)
	}
}

func TestAnthropicAPIRuntime_NilSession_Error(t *testing.T) {
	r := NewAnthropicAPIRuntime(&config.Config{LLM: config.LLMConfig{RuntimeMode: "api", Session: config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3}, ClaudeAPI: config.ClaudeAPIConfig{DefaultModel: "m"}}}, sessions.NewInMemoryRegistry(time.Second), "x", nil, nil, nil)
	if _, err := r.ContinueSession(context.Background(), nil, Message{Role: "user", Content: "x"}); err == nil {
		t.Fatalf("expected nil session error")
	}
}

func TestExtractUsageTokensFromJSON_More(t *testing.T) {
	if u := extractUsageTokensFromJSON(nil); u.InputTokens != 0 || u.OutputTokens != 0 {
		t.Fatalf("expected empty usage")
	}

	if u := extractUsageTokensFromJSON([]byte("{")); u.InputTokens != 0 || u.OutputTokens != 0 || u.Model != "" {
		t.Fatalf("expected empty usage on decode error, got %#v", u)
	}
	raw := []byte(`{"model":"m","usage":{"input_tokens":1,"output_tokens":2}}`)
	u := extractUsageTokensFromJSON(raw)
	if u.Model != "m" || u.InputTokens != 1 || u.OutputTokens != 2 {
		t.Fatalf("unexpected usage: %#v", u)
	}
}

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
	r := NewAnthropicAPIRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "o", nil, nil, nil)
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

	am, ok := toAnthropicMessage(Message{Role: "assistant", Content: "hi"})
	if !ok || am.Role != "assistant" {
		t.Fatalf("expected assistant role, got ok=%v msg=%+v", ok, am)
	}

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
	r := NewAnthropicAPIRuntime(cfg, sessions.NewInMemoryRegistry(time.Second), "o", nil, nil, nil)

	_, err := r.buildRequest(context.Background(), &Session{AgentID: "a", SystemPrompt: "s", Messages: []Message{{Role: "user", Content: " "}}}, Message{Role: "user", Content: ""})
	if err == nil {
		t.Fatal("expected messages required error")
	}

	ctx := runtimeactor.WithActor(context.Background(), models.AgentConfig{ID: "a", Role: "pm-agent", VerticalID: "v1"})
	_, err = r.buildRequest(ctx, &Session{AgentID: "a", SystemPrompt: "s", Messages: []Message{{Role: "user", Content: "hi"}}}, Message{Role: "user", Content: "x"})
	if err == nil || !strings.Contains(err.Error(), "default_model is required") {
		t.Fatalf("expected model required error, got %v", err)
	}
}

func TestClaudeCLIRuntime_Run_ErrorOutputBranches(t *testing.T) {
	dir := t.TempDir()

	quietFail := filepath.Join(dir, "quiet_fail.sh")
	if err := os.WriteFile(quietFail, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("write quiet fail: %v", err)
	}

	noisyFail := filepath.Join(dir, "noisy_fail.sh")
	if err := os.WriteFile(noisyFail, []byte("#!/bin/sh\necho bad 1>&2\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("write noisy fail: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session:     config.LLMSessionConfig{LockTTL: 5 * time.Second},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      quietFail,
				Timeout:      2 * time.Second,
				OutputFormat: "json",
			},
		},
	}
	rt := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(5*time.Second), "owner", nil, nil, nil, nil)

	_, err := rt.run(context.Background(), []string{"-p", "x"}, nil)
	if err == nil || !strings.Contains(err.Error(), "claude cli run failed") {
		t.Fatalf("expected run error, got %v", err)
	}
	if strings.Contains(err.Error(), "stderr=") {
		t.Fatalf("expected quiet failure to avoid stderr= wrapper, got %v", err)
	}

	rt.cfg.LLM.ClaudeCLI.Command = noisyFail
	_, err = rt.run(context.Background(), []string{"-p", "x"}, nil)
	if err == nil || !strings.Contains(err.Error(), "stderr=bad") {
		t.Fatalf("expected stderr to be included, got %v", err)
	}
}

func TestClaudeCLIRuntime_Run_TimeoutErrorIsExplicit(t *testing.T) {
	dir := t.TempDir()
	sleeper := filepath.Join(dir, "sleepy.sh")
	if err := os.WriteFile(sleeper, []byte("#!/bin/sh\nsleep 2\n"), 0o755); err != nil {
		t.Fatalf("write sleeper: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session:     config.LLMSessionConfig{LockTTL: 5 * time.Second},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      sleeper,
				Timeout:      150 * time.Millisecond,
				OutputFormat: "json",
			},
		},
	}
	rt := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(5*time.Second), "owner", nil, nil, nil, nil)
	_, err := rt.run(context.Background(), []string{"-p", "x"}, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "timeout after") {
		t.Fatalf("expected explicit timeout error, got: %v", err)
	}
}

func TestSummarizeCLIErrorOutput_Truncates(t *testing.T) {
	raw := strings.Repeat("x", 400)
	out := summarizeCLIErrorOutput(raw)
	if len(out) > 245 {
		t.Fatalf("expected truncation, got len=%d", len(out))
	}
	if !strings.HasSuffix(out, "...") {
		t.Fatalf("expected ..., got %q", out)
	}
}

func TestParseCLIResponse_ToolCallsForms(t *testing.T) {
	resp := parseCLIResponse([]byte(`{
		"result":"ok",
		"session_id":"sess-123",
		"tool_calls":[{"name":"t1","arguments":{"a":1}}, {"name":"","arguments":{"b":2}}],
		"content":[{"type":"tool_use","name":"t2","arguments":{"x":2}}]
	}`))
	if strings.TrimSpace(resp.Message.Content) != "ok" {
		t.Fatalf("expected ok content, got %q", resp.Message.Content)
	}
	if resp.SessionID != "sess-123" {
		t.Fatalf("expected session id to be parsed, got %q", resp.SessionID)
	}
	if len(resp.ToolCalls) != 2 {
		t.Fatalf("expected 2 tool calls, got %+v", resp.ToolCalls)
	}
	if resp.ToolCalls[0].Name != "t2" && resp.ToolCalls[0].Name != "t1" {
		t.Fatalf("unexpected tool call order: %+v", resp.ToolCalls)
	}

	plain := parseCLIResponse([]byte("hello"))
	if plain.Message.Content != "hello" {
		t.Fatalf("expected plain content passthrough, got %q", plain.Message.Content)
	}
}

func TestClaudeCLIHelpers_ToolNamesAndUnsupportedFlags(t *testing.T) {
	if got := toolNamesCSV([]ToolDefinition{
		{Name: "agent_message"},
		{Name: "sql_execute"},
		{Name: "agent_message"},
		{Name: " "},
	}); got != "agent_message,sql_execute" {
		t.Fatalf("unexpected tool csv: %q", got)
	}
	if !isUnsupportedCLIFlagError(assertErr("unknown option --system-prompt")) {
		t.Fatal("expected unsupported --system-prompt flag detection")
	}
	if !isUnsupportedCLIFlagError(assertErr("unrecognized option '--tools'")) {
		t.Fatal("expected unsupported --tools flag detection")
	}
	if isUnsupportedCLIFlagError(assertErr("unknown option --foo")) {
		t.Fatal("unexpected detection for unrelated flag")
	}
}

func TestClaudeCLIRuntime_EffectiveCLITimeout_FactoryFloorAndEnvOverride(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session:     config.LLMSessionConfig{LockTTL: 5 * time.Second},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "true",
				Timeout:      30 * time.Second,
				OutputFormat: "json",
			},
		},
	}
	rt := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(5*time.Second), "owner", nil, nil, nil, nil)

	factoryCtx := runtimeactor.WithActor(context.Background(), models.AgentConfig{ID: "factory-1", Mode: "factory"})
	if got := rt.effectiveCLITimeout(factoryCtx); got != 300*time.Second {
		t.Fatalf("expected factory timeout floor 300s, got %s", got)
	}

	t.Setenv("EMPIREAI_CLAUDE_TIMEOUT_SECONDS", "45")
	if got := rt.effectiveCLITimeout(factoryCtx); got != 45*time.Second {
		t.Fatalf("expected env timeout override 45s, got %s", got)
	}
}

type turnStub struct{ err error }

func (t *turnStub) AppendAgentTurn(context.Context, AgentTurnRecord) error { return t.err }

type turnCaptureStub struct {
	last AgentTurnRecord
}

func (t *turnCaptureStub) AppendAgentTurn(_ context.Context, rec AgentTurnRecord) error {
	t.last = rec
	return nil
}

type convStub struct {
	loadRec   ConversationRecord
	loadOK    bool
	loadErr   error
	upsertErr error
}

func (c *convStub) UpsertConversation(context.Context, ConversationRecord) error { return c.upsertErr }

func (c *convStub) LoadActiveConversation(context.Context, string, string, string) (ConversationRecord, bool, error) {
	return c.loadRec, c.loadOK, c.loadErr
}

func TestClaudeCLIRuntime_StartContinue_And_AuthError(t *testing.T) {

	okScript := runtimetestkit.TempScript(t, "ok.sh", "#!/bin/sh\n# ignore stdin/args\ncat >/dev/null || true\nprintf '{\"content\":[{\"type\":\"text\",\"text\":\"hi\"},{\"type\":\"tool_use\",\"name\":\"agent_message\",\"input\":{\"x\":1}}]}'\n")

	authScript := runtimetestkit.TempScript(t, "auth.sh", "#!/bin/sh\necho 'not logged in' 1>&2\nexit 1\n")

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               1 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      okScript,
				OutputFormat: "json",
				Timeout:      2 * time.Second,
				Retries:      1,
			},
		},
	}
	sessions := sessions.NewInMemoryRegistry(1 * time.Second)
	turns := &turnStub{err: os.ErrInvalid}
	convs := &convStub{loadOK: true, loadRec: ConversationRecord{Messages: []Message{{Role: "user", Content: "prior"}}, TurnCount: 1}, upsertErr: os.ErrInvalid}

	r := NewClaudeCLIRuntime(cfg, sessions, "lock", turns, nil, nil, convs)
	s, err := r.StartSession(context.Background(), "a1", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s.TurnCount != 1 {
		t.Fatalf("expected loaded turn_count=1, got %d", s.TurnCount)
	}

	ctx := runtimeactor.WithActor(context.Background(), models.AgentConfig{ID: "a1", Type: "worker", VerticalID: "v1"})
	resp, err := r.ContinueSession(ctx, s, Message{Role: "user", Content: "go"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp == nil || resp.Message.Content != "hi" || len(resp.ToolCalls) != 1 {
		t.Fatalf("unexpected resp: %#v", resp)
	}

	cfg2 := *cfg
	cfg2.LLM.ClaudeCLI.Command = authScript
	r2 := NewClaudeCLIRuntime(&cfg2, sessions, "lock", nil, nil, nil, nil)
	s2, err := r2.StartSession(context.Background(), "a2", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession2: %v", err)
	}
	if _, err := r2.ContinueSession(context.Background(), s2, Message{Role: "user", Content: "x"}); err == nil {
		t.Fatalf("expected auth required error")
	}
}

func TestClaudeCLIRuntime_NilSession_Error(t *testing.T) {
	r := NewClaudeCLIRuntime(&config.Config{LLM: config.LLMConfig{RuntimeMode: "cli_test", Session: config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3}, ClaudeCLI: config.ClaudeCLIConfig{Command: "true", OutputFormat: "json"}}}, sessions.NewInMemoryRegistry(time.Second), "x", nil, nil, nil, nil)
	if _, err := r.ContinueSession(context.Background(), nil, Message{Role: "user", Content: "x"}); err == nil {
		t.Fatalf("expected nil session error")
	}

	_, _, _, _ = runtime.Caller(0)
}

func TestClaudeCLIRuntime_FirstTurn_WithTools_UsesStdinPrompt(t *testing.T) {
	stdinRequired := runtimetestkit.TempScript(t, "stdin_required.sh", "#!/bin/sh\nin=\"$(cat)\"\nif [ -z \"$in\" ]; then\n  echo 'Error: Input must be provided either through stdin or as a prompt argument when using --print' 1>&2\n  exit 1\nfi\nprintf '{\"content\":[{\"type\":\"text\",\"text\":\"ok\"}]}'\n")

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               1 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      stdinRequired,
				OutputFormat: "json",
				Timeout:      2 * time.Second,
				Retries:      1,
			},
		},
	}
	r := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(1*time.Second), "lock", nil, nil, nil, nil)
	s, err := r.StartSession(context.Background(), "a-tools", "sys", []ToolDefinition{{Name: "agent_message"}})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	resp, err := r.ContinueSession(context.Background(), s, Message{Role: "user", Content: "hello"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp == nil || resp.Message.Content != "ok" {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestClaudeCLIRuntime_PromptArgFallback_OnPrintInputError(t *testing.T) {

	promptArgOnly := runtimetestkit.TempScript(t, "prompt_arg_only.sh", "#!/bin/sh\nextra=\"\"\nwhile [ \"$#\" -gt 0 ]; do\n  case \"$1\" in\n    -p)\n      shift\n      ;;\n    --session-id|--output-format|--system-prompt|--tools|--mcp-config|-r)\n      if [ \"$#\" -lt 2 ]; then break; fi\n      shift 2\n      ;;\n    --strict-mcp-config)\n      shift\n      ;;\n    --)\n      shift\n      if [ \"$#\" -gt 0 ]; then\n        extra=\"$1\"\n      fi\n      break\n      ;;\n    *)\n      extra=\"$1\"\n      shift\n      ;;\n  esac\ndone\nif [ -z \"$extra\" ]; then\n  echo 'Error: Input must be provided either through stdin or as a prompt argument when using --print' 1>&2\n  exit 1\nfi\nprintf '{\"content\":[{\"type\":\"text\",\"text\":\"ok-from-arg\"}]}'\n")

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               1 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      promptArgOnly,
				OutputFormat: "json",
				Timeout:      2 * time.Second,
				Retries:      1,
			},
		},
	}
	turns := &turnCaptureStub{}
	r := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(1*time.Second), "lock", turns, nil, nil, nil)
	s, err := r.StartSession(context.Background(), "a-fallback", "sys", []ToolDefinition{{Name: "agent_message"}})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	resp, err := r.ContinueSession(context.Background(), s, Message{Role: "user", Content: "hello from event"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp == nil || resp.Message.Content != "ok-from-arg" {
		t.Fatalf("unexpected response: %#v", resp)
	}
	if string(turns.last.RequestPayload) == "" || !strings.Contains(string(turns.last.RequestPayload), `"prompt_arg_fallback_used":true`) {
		t.Fatalf("expected prompt_arg_fallback_used=true in turn payload, got: %s", string(turns.last.RequestPayload))
	}
}

func TestClaudeCLIRuntime_EmptyPromptFailsFast(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               1 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "true",
				OutputFormat: "json",
				Timeout:      2 * time.Second,
				Retries:      1,
			},
		},
	}
	r := NewClaudeCLIRuntime(cfg, sessions.NewInMemoryRegistry(1*time.Second), "lock", nil, nil, nil, nil)
	s, err := r.StartSession(context.Background(), "a-empty", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if _, err := r.ContinueSession(context.Background(), s, Message{Role: "user", Content: "   "}); err == nil || !strings.Contains(err.Error(), "empty prompt input") {
		t.Fatalf("expected empty prompt error, got: %v", err)
	}
}
