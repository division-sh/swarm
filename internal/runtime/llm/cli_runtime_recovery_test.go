package llm

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/runtime/sessions"
)

type rotateCaptureRegistry struct {
	sessionID     string
	rotateSummary string
}

func (r *rotateCaptureRegistry) Acquire(_ context.Context, agentID, runtimeMode, lockOwner, scopeKey string) (*sessions.Lease, error) {
	if strings.TrimSpace(r.sessionID) == "" {
		r.sessionID = "sid-old"
	}
	return &sessions.Lease{
		SessionID:   r.sessionID,
		AgentID:     agentID,
		RuntimeMode: runtimeMode,
		LockOwner:   lockOwner,
		ScopeKey:    scopeKey,
		ExpiresAt:   time.Now().Add(5 * time.Second),
	}, nil
}

func (r *rotateCaptureRegistry) Release(_ context.Context, _ *sessions.Lease) error { return nil }

func (r *rotateCaptureRegistry) Rotate(_ context.Context, agentID, runtimeMode, lockOwner, summary, scopeKey string) (*sessions.Lease, error) {
	r.rotateSummary = summary
	r.sessionID = "sid-rotated"
	return &sessions.Lease{
		SessionID:   r.sessionID,
		AgentID:     agentID,
		RuntimeMode: runtimeMode,
		LockOwner:   lockOwner,
		ScopeKey:    scopeKey,
		ExpiresAt:   time.Now().Add(5 * time.Second),
	}, nil
}

func (r *rotateCaptureRegistry) IncrementTurn(_ context.Context, _, _, _, _ string) error { return nil }

func TestClaudeCLIRuntime_RecoveryRotation_StoresCheckpointReasonAndSummary(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "state.txt")
	script := filepath.Join(dir, "recover.sh")
	body := `#!/bin/sh
STATE="` + state + `"
if [ ! -f "$STATE" ]; then
  echo 1 > "$STATE"
  echo "No conversation found with session ID: stale-session" 1>&2
  exit 1
fi
printf '%s' '{"content":[{"type":"text","text":"ok"}]}'
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

	reg := &rotateCaptureRegistry{}
	rt := NewClaudeCLIRuntime(cfg, reg, "owner", &turnCapture{}, nil, nil, nil)
	s, err := rt.StartSession(context.Background(), "agent-1", "sys", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if _, err := rt.ContinueSession(context.Background(), s, Message{Role: "user", Content: "hello"}); err != nil {
		t.Fatalf("ContinueSession: %v", err)
	}
	if !strings.Contains(reg.rotateSummary, "rotation_reason=session not found") {
		t.Fatalf("expected rotation reason in checkpoint summary, got %q", reg.rotateSummary)
	}
	if !strings.Contains(reg.rotateSummary, "No prior turns.") {
		t.Fatalf("expected session summary in checkpoint summary, got %q", reg.rotateSummary)
	}
}
