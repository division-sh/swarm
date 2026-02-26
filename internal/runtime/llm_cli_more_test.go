package runtime

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/models"
)

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

func writeScript(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func TestClaudeCLIRuntime_StartContinue_And_AuthError(t *testing.T) {
	// A script that always succeeds and prints a Claude-like JSON response.
	okScript := writeScript(t, "ok.sh", "#!/bin/sh\n# ignore stdin/args\ncat >/dev/null || true\nprintf '{\"content\":[{\"type\":\"text\",\"text\":\"hi\"},{\"type\":\"tool_use\",\"name\":\"agent_message\",\"input\":{\"x\":1}}]}'\n")
	// A script that fails with auth-like output.
	authScript := writeScript(t, "auth.sh", "#!/bin/sh\necho 'not logged in' 1>&2\nexit 1\n")

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
	sessions := NewInMemorySessionRegistry(1 * time.Second)
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

	ctx := WithActor(context.Background(), models.AgentConfig{ID: "a1", Type: "worker", VerticalID: "v1"})
	resp, err := r.ContinueSession(ctx, s, Message{Role: "user", Content: "go"})
	if err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if resp == nil || resp.Message.Content != "hi" || len(resp.ToolCalls) != 1 {
		t.Fatalf("unexpected resp: %#v", resp)
	}

	// Auth error path.
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
	r := NewClaudeCLIRuntime(&config.Config{LLM: config.LLMConfig{RuntimeMode: "cli_test", Session: config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3}, ClaudeCLI: config.ClaudeCLIConfig{Command: "true", OutputFormat: "json"}}}, NewInMemorySessionRegistry(time.Second), "x", nil, nil, nil, nil)
	if _, err := r.ContinueSession(context.Background(), nil, Message{Role: "user", Content: "x"}); err == nil {
		t.Fatalf("expected nil session error")
	}
	// Touch runtime import for writeScript caller.
	_, _, _, _ = runtime.Caller(0)
}

func TestClaudeCLIRuntime_FirstTurn_WithTools_UsesStdinPrompt(t *testing.T) {
	stdinRequired := writeScript(t, "stdin_required.sh", "#!/bin/sh\nin=\"$(cat)\"\nif [ -z \"$in\" ]; then\n  echo 'Error: Input must be provided either through stdin or as a prompt argument when using --print' 1>&2\n  exit 1\nfi\nprintf '{\"content\":[{\"type\":\"text\",\"text\":\"ok\"}]}'\n")

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
	r := NewClaudeCLIRuntime(cfg, NewInMemorySessionRegistry(1*time.Second), "lock", nil, nil, nil, nil)
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
	// Simulates a CLI build that rejects stdin transport and only succeeds when
	// the prompt is passed as a positional argument.
	promptArgOnly := writeScript(t, "prompt_arg_only.sh", "#!/bin/sh\nextra=\"\"\nwhile [ \"$#\" -gt 0 ]; do\n  case \"$1\" in\n    -p)\n      shift\n      ;;\n    --session-id|--output-format|--system-prompt|--tools|--mcp-config|-r)\n      if [ \"$#\" -lt 2 ]; then break; fi\n      shift 2\n      ;;\n    --strict-mcp-config)\n      shift\n      ;;\n    --)\n      shift\n      if [ \"$#\" -gt 0 ]; then\n        extra=\"$1\"\n      fi\n      break\n      ;;\n    *)\n      extra=\"$1\"\n      shift\n      ;;\n  esac\ndone\nif [ -z \"$extra\" ]; then\n  echo 'Error: Input must be provided either through stdin or as a prompt argument when using --print' 1>&2\n  exit 1\nfi\nprintf '{\"content\":[{\"type\":\"text\",\"text\":\"ok-from-arg\"}]}'\n")

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
	r := NewClaudeCLIRuntime(cfg, NewInMemorySessionRegistry(1*time.Second), "lock", turns, nil, nil, nil)
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
	r := NewClaudeCLIRuntime(cfg, NewInMemorySessionRegistry(1*time.Second), "lock", nil, nil, nil, nil)
	s, err := r.StartSession(context.Background(), "a-empty", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if _, err := r.ContinueSession(context.Background(), s, Message{Role: "user", Content: "   "}); err == nil || !strings.Contains(err.Error(), "empty prompt input") {
		t.Fatalf("expected empty prompt error, got: %v", err)
	}
}
