package manager_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"empireai/internal/config"
	llm "empireai/internal/runtime/llm"
	rt "empireai/internal/runtime"
	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/runtime/sessions"
	"empireai/internal/testutil"
)

func TestResetRuntimeState_PostgresSessions_AfterCLIRunRotatesAndUnlocks(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	agentID := "reset-smoke-agent"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ($1, 'llm', $1, 'factory', 'active', '{"system_prompt":"reset smoke"}'::jsonb, now(), now())
	`, agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	cliScript := filepath.Join(t.TempDir(), "claude-ok.sh")
	if err := os.WriteFile(cliScript, []byte("#!/bin/sh\nprintf '{\"content\":[{\"type\":\"text\",\"text\":\"ok\"}]}'\n"), 0o755); err != nil {
		t.Fatalf("write cli stub: %v", err)
	}

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               15 * time.Second,
				RotateAfterTurns:      50,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:              cliScript,
				Timeout:              3 * time.Second,
				OutputFormat:         "json",
				Retries:              1,
				NoSessionPersistence: false,
			},
		},
	}

	sessions := sessions.NewPostgresRegistry(db, 15*time.Second)
	cli := rt.NewClaudeCLIRuntime(cfg, sessions, "reset-integration-owner", nil, nil, nil, nil)

	s, err := cli.StartSession(ctx, agentID, "reset smoke prompt", nil)
	if err != nil {
		t.Fatalf("start cli session: %v", err)
	}
	if _, err := cli.ContinueSession(ctx, s, llm.Message{Role: "user", Content: "run smoke step"}); err != nil {
		t.Fatalf("continue cli session: %v", err)
	}

	var beforeStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM agent_sessions
		WHERE agent_id = $1 AND runtime_mode = 'cli_test'
		ORDER BY created_at DESC
		LIMIT 1
	`, agentID).Scan(&beforeStatus); err != nil {
		t.Fatalf("load session status before reset: %v", err)
	}
	if beforeStatus != "active" {
		t.Fatalf("expected active session before reset, got %q", beforeStatus)
	}

	mgr := runtimemanager.NewAgentManager(rt.NewEventBus(rt.InMemoryEventStore{}), nil)
	mgr.SetSessionRegistry(sessions, "cli_test")
	if err := mgr.ResetRuntimeState(); err != nil {
		t.Fatalf("reset runtime state: %v", err)
	}

	var afterStatus string
	var lockOwner sql.NullString
	var lockExpires sql.NullTime
	if err := db.QueryRowContext(ctx, `
		SELECT status, lock_owner, lock_expires_at
		FROM agent_sessions
		WHERE agent_id = $1 AND runtime_mode = 'cli_test'
		ORDER BY created_at DESC
		LIMIT 1
	`, agentID).Scan(&afterStatus, &lockOwner, &lockExpires); err != nil {
		t.Fatalf("load session after reset: %v", err)
	}
	if afterStatus != "rotated" {
		t.Fatalf("expected rotated session after reset, got %q", afterStatus)
	}
	if lockOwner.Valid && lockOwner.String != "" {
		t.Fatalf("expected lock_owner cleared, got %q", lockOwner.String)
	}
	if lockExpires.Valid {
		t.Fatalf("expected lock_expires_at cleared, got %v", lockExpires.Time)
	}
}
