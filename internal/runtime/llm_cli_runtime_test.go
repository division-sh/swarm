package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/models"
)

type cliConvoCapture struct {
	calls int
	last  ConversationRecord
}

func (c *cliConvoCapture) UpsertConversation(_ context.Context, rec ConversationRecord) error {
	c.calls++
	c.last = rec
	return nil
}

func (c *cliConvoCapture) LoadActiveConversation(_ context.Context, _ string, _ string, _ string) (ConversationRecord, bool, error) {
	return ConversationRecord{}, false, nil
}

type wsStub struct {
	calls int
	ret   *WorkspaceTarget
}

func (w *wsStub) ResolveWorkspace(_ context.Context, _ models.AgentConfig) (*WorkspaceTarget, error) {
	w.calls++
	return w.ret, nil
}

func TestClaudeCLIRuntime_StartAndContinueSession_RunsCommandAndPersistsTurns(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "claude_stub.sh")
	// Ignore args, always output stable JSON.
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' '{\"content\":[{\"type\":\"text\",\"text\":\"hi\"}]}'\n"), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               5 * time.Second,
				RotateAfterTurns:      100,
				RotateOnParseFailures: 2,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      script,
				Timeout:      2 * time.Second,
				OutputFormat: "json",
				Retries:      1,
			},
		},
	}

	sessions := NewInMemorySessionRegistry(5 * time.Second)
	turns := &turnCapture{}
	convos := &cliConvoCapture{}
	ws := &wsStub{}

	rt := NewClaudeCLIRuntime(cfg, sessions, "lock-owner-1", turns, nil, ws, convos)
	rt.SetWorkspaceResolver(ws)

	s, err := rt.StartSession(context.Background(), "agent-1", "sys", []ToolDefinition{{Name: "t1", Description: "d"}})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s.ID == "" || s.RuntimeMode != "cli_test" {
		t.Fatalf("unexpected session: %+v", s)
	}

	ctx := WithActor(context.Background(), models.AgentConfig{ID: "agent-1", Type: "worker", Role: "pm-agent", Mode: "operating", VerticalID: "v1"})

	// First turn uses --session-id and buildInitialPrompt.
	resp1, err := rt.ContinueSession(ctx, s, Message{Role: "user", Content: "hello"})
	if err != nil {
		t.Fatalf("ContinueSession 1: %v", err)
	}
	if strings.TrimSpace(resp1.Message.Content) != "hi" {
		t.Fatalf("unexpected response 1: %q", resp1.Message.Content)
	}

	// Second turn uses -r sessionid.
	resp2, err := rt.ContinueSession(ctx, s, Message{Role: "user", Content: "world"})
	if err != nil {
		t.Fatalf("ContinueSession 2: %v", err)
	}
	if strings.TrimSpace(resp2.Message.Content) != "hi" {
		t.Fatalf("unexpected response 2: %q", resp2.Message.Content)
	}

	if ws.calls != 2 {
		t.Fatalf("expected workspace resolver called twice, got %d", ws.calls)
	}
	if len(turns.records) != 2 {
		t.Fatalf("expected 2 persisted turns, got %d", len(turns.records))
	}
	if convos.calls != 2 {
		t.Fatalf("expected conversation upsert twice, got %d", convos.calls)
	}
	if err := rt.PersistConversationSnapshot(context.Background(), s); err != nil {
		t.Fatalf("PersistConversationSnapshot: %v", err)
	}
	if convos.calls < 3 {
		t.Fatalf("expected snapshot to upsert, calls=%d", convos.calls)
	}

	// Inspect persisted args to ensure correct CLI call shape on turn 0 vs turn > 0.
	var p1 map[string]any
	_ = json.Unmarshal(turns.records[0].RequestPayload, &p1)
	args1, _ := p1["args"].([]any)
	joined1 := strings.ToLower(strings.Join(anyToStrings(args1), " "))
	if !strings.Contains(joined1, "--session-id") {
		t.Fatalf("expected first turn args to include --session-id, got: %s", joined1)
	}
	var p2 map[string]any
	_ = json.Unmarshal(turns.records[1].RequestPayload, &p2)
	args2, _ := p2["args"].([]any)
	joined2 := strings.ToLower(strings.Join(anyToStrings(args2), " "))
	if !strings.Contains(joined2, " -r ") && !strings.Contains(joined2, "-r") {
		t.Fatalf("expected second turn args to include -r, got: %s", joined2)
	}
}

func TestClaudeCLIRuntime_ContinueSession_AdoptsCLIReportedSessionID(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "claude_stub_sessionid.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' '{\"content\":[{\"type\":\"text\",\"text\":\"hi\"}],\"session_id\":\"claude-session-xyz\"}'\n"), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               5 * time.Second,
				RotateAfterTurns:      100,
				RotateOnParseFailures: 2,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      script,
				Timeout:      2 * time.Second,
				OutputFormat: "json",
			},
		},
	}
	sessions := NewInMemorySessionRegistry(5 * time.Second)
	rt := NewClaudeCLIRuntime(cfg, sessions, "owner", &turnCapture{}, nil, nil, nil)

	s, err := rt.StartSession(context.Background(), "agent-sid", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	orig := s.ID
	if _, err := rt.ContinueSession(context.Background(), s, Message{Role: "user", Content: "hello"}); err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if s.ID != "claude-session-xyz" {
		t.Fatalf("expected adopted session id, got %q", s.ID)
	}
	if s.ID == orig {
		t.Fatalf("expected session id to change from bootstrap id %q", orig)
	}
	if rec, ok := sessions.Snapshot("agent-sid"); !ok || rec.SessionID != "claude-session-xyz" {
		t.Fatalf("expected registry session id sync, rec=%+v ok=%v", rec, ok)
	}
}

func TestClaudeCLIRuntime_BuildCommand_DockerExecShape(t *testing.T) {
	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               5 * time.Second,
				RotateAfterTurns:      100,
				RotateOnParseFailures: 2,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "claude",
				Timeout:      2 * time.Second,
				OutputFormat: "json",
			},
		},
	}
	rt := NewClaudeCLIRuntime(cfg, NewInMemorySessionRegistry(5*time.Second), "o", nil, nil, nil, nil)
	cmd := rt.buildCommand(context.Background(), []string{"-p", "x"}, &WorkspaceTarget{Container: "c1", Workdir: "/w"})
	if len(cmd.Args) == 0 || !strings.Contains(strings.Join(cmd.Args, " "), "docker exec") {
		t.Fatalf("expected docker exec command, got args=%v", cmd.Args)
	}
}

func TestClaudeCLIHelpers(t *testing.T) {
	if !isSessionInUseError(assertErr("Session ID already in use")) {
		t.Fatal("expected session in use error detection")
	}
	if !isSessionNotFoundError(assertErr("No conversation found with session ID: abc")) {
		t.Fatal("expected session not found error detection")
	}
	if !shouldRotateSessionOnCLIError(assertErr("No conversation found with session ID: abc")) {
		t.Fatal("expected recoverable rotation on session not found error")
	}
	if rotateSessionRetryReason(assertErr("No conversation found with session ID: abc")) != "session not found" {
		t.Fatal("expected session-not-found rotate reason")
	}
	if isSessionInUseError(nil) {
		t.Fatal("expected false for nil error")
	}
	if estimateTokensFromBytes(nil) != 0 {
		t.Fatal("expected 0 tokens for empty")
	}
	if estimateTokensFromBytes([]byte("abcd")) != 1 {
		t.Fatal("expected 1 token for 4 bytes")
	}
	p := buildInitialPrompt(&Session{SystemPrompt: "s", Tools: []ToolDefinition{{Name: "t", Description: "d"}}}, "m")
	if !strings.Contains(p, "System:") || !strings.Contains(p, "Tools:") {
		t.Fatalf("unexpected initial prompt: %q", p)
	}

	usage := estimateCLIUsageTokens(Message{Role: "user", Content: "x"}, &Response{Raw: []byte(`{"ok":true}`)}, models.AgentConfig{ID: "opco-ceo-v1", Role: "opco-ceo"})
	if usage.InputTokens < 2000 || usage.OutputTokens < 200 {
		t.Fatalf("expected role floors applied, got %+v", usage)
	}
}

func TestClaudeCLIRuntime_ContinueSession_RotatesOnSessionNotFound(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "state.txt")
	script := filepath.Join(dir, "claude_session_recover.sh")
	body := `#!/bin/sh
STATE="` + state + `"
if [ ! -f "$STATE" ]; then
  echo 1 > "$STATE"
  echo "No conversation found with session ID: stale-session" 1>&2
  exit 1
fi
printf '%s' '{"content":[{"type":"text","text":"recovered"}]}'
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               5 * time.Second,
				RotateAfterTurns:      100,
				RotateOnParseFailures: 2,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      script,
				Timeout:      2 * time.Second,
				OutputFormat: "json",
				Retries:      1,
			},
		},
	}

	sessions := NewInMemorySessionRegistry(5 * time.Second)
	turns := &turnCapture{}
	rt := NewClaudeCLIRuntime(cfg, sessions, "owner", turns, nil, nil, nil)

	s, err := rt.StartSession(context.Background(), "agent-recover", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	oldID := s.ID

	resp, err := rt.ContinueSession(context.Background(), s, Message{Role: "user", Content: "hello"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if strings.TrimSpace(resp.Message.Content) != "recovered" {
		t.Fatalf("unexpected response: %q", resp.Message.Content)
	}
	if strings.TrimSpace(s.ID) == strings.TrimSpace(oldID) {
		t.Fatalf("expected session id rotation, old=%s new=%s", oldID, s.ID)
	}
	if len(turns.records) != 1 || !turns.records[0].ParseOK {
		t.Fatalf("expected one successful persisted turn after recovery, got %+v", turns.records)
	}
}

func TestClaudeCLIRuntime_ContinueSession_AuthRequiredError(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "claude_auth_fail.sh")
	out := `{"type":"result","subtype":"success","is_error":true,"result":"Not logged in · Please run /login"}`
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' '"+out+"'\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               5 * time.Second,
				RotateAfterTurns:      100,
				RotateOnParseFailures: 2,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      script,
				Timeout:      2 * time.Second,
				OutputFormat: "json",
			},
		},
	}
	rt := NewClaudeCLIRuntime(cfg, NewInMemorySessionRegistry(5*time.Second), "owner", &turnCapture{}, nil, nil, nil)
	s, err := rt.StartSession(context.Background(), "agent-auth", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	_, err = rt.ContinueSession(context.Background(), s, Message{Role: "user", Content: "hello"})
	if err == nil {
		t.Fatal("expected auth error")
	}
	if !errors.Is(err, ErrClaudeAuthRequired) {
		t.Fatalf("expected ErrClaudeAuthRequired, got %v", err)
	}
}

func TestClaudeCLIRuntime_ContinueSession_WorkspaceRequiresOAuthToken(t *testing.T) {
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")

	dir := t.TempDir()
	script := filepath.Join(dir, "claude_ok.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' '{\"content\":[{\"type\":\"text\",\"text\":\"ok\"}]}'\n"), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               5 * time.Second,
				RotateAfterTurns:      100,
				RotateOnParseFailures: 2,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      script,
				Timeout:      2 * time.Second,
				OutputFormat: "json",
			},
		},
	}
	ws := &wsStub{ret: &WorkspaceTarget{Container: "w1", Workdir: "/workspace"}}
	rt := NewClaudeCLIRuntime(cfg, NewInMemorySessionRegistry(5*time.Second), "owner", &turnCapture{}, nil, ws, nil)
	rt.SetWorkspaceResolver(ws)

	s, err := rt.StartSession(context.Background(), "agent-auth", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	ctx := WithActor(context.Background(), models.AgentConfig{ID: "agent-auth", Mode: "holding"})
	_, err = rt.ContinueSession(ctx, s, Message{Role: "user", Content: "hello"})
	if err == nil {
		t.Fatal("expected auth error")
	}
	if !errors.Is(err, ErrClaudeAuthRequired) {
		t.Fatalf("expected ErrClaudeAuthRequired, got %v", err)
	}
}

func anyToStrings(in []any) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

type errString string

func (e errString) Error() string { return string(e) }

func assertErr(s string) error { return errString(s) }
