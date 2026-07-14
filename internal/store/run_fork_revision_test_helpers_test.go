package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
)

func captureRunForkTestRevision(t *testing.T, db *sql.DB, runID string, families ...runforkrevision.Family) int64 {
	t.Helper()
	if len(families) == 0 {
		families = runforkrevision.AllFamilies()
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin run fork test revision: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	revision, err := runforkrevision.Capture(context.Background(), tx, runID, families...)
	if err != nil {
		t.Fatalf("capture run fork test revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit run fork test revision: %v", err)
	}
	return revision
}

func seedRunForkSessionProjection(t *testing.T, db *sql.DB, runID, agentID, sessionID, status string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, model, llm_backend, conversation_mode, status, created_at)
		VALUES ($1, 'worker', 'standard', 'mock', 'session', 'active', $2)
	`, agentID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run-fork session agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, scope_key, scope, conversation, turn_count,
			runtime_mode, runtime_state, status, termination_reason, terminated_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3, 'global', 'global', '[]'::jsonb, 0,
			'session', '{}'::jsonb, $4,
			CASE WHEN $4::text = 'terminated' THEN 'normal' ELSE NULL END,
			CASE WHEN $4::text = 'terminated' THEN $5::timestamptz ELSE NULL::timestamptz END, $5, $5
		)
	`, sessionID, runID, agentID, status, at.Add(-time.Second)); err != nil {
		t.Fatalf("seed run-fork session projection: %v", err)
	}
}

func mutateRunForkSessionExcludedColumns(t *testing.T, db *sql.DB, runID, sessionID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin excluded session mutation: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions
		SET conversation = '[{"role":"assistant","content":"still working"}]'::jsonb,
		    turn_count = turn_count + 1,
		    runtime_state = runtime_state || jsonb_build_object(
		        'provider_session_id', 'provider-excluded',
		        'watchdog', jsonb_build_object('state', 'healthy_long_running')
		    ),
		    lease_holder = 'excluded-owner',
		    lease_expires_at = $3,
		    updated_at = $3
		WHERE run_id = $1::uuid AND session_id = $2::uuid
	`, runID, sessionID, at); err != nil {
		t.Fatalf("update excluded session columns: %v", err)
	}
	if _, err := runforkrevision.CaptureCurrentTransaction(ctx, tx); err != nil {
		t.Fatalf("capture excluded session mutation: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit excluded session mutation: %v", err)
	}
}

func exerciseRunForkSessionExcludedWriters(t *testing.T, store *PostgresStore, agentID, sessionID string) {
	t.Helper()
	ctx := runtimeeffects.WithDifferentOwner(context.Background(), runtimeeffects.OwnerBuildTestInfrastructure)
	lease, _, err := store.AcquireLiveSession(ctx, agentID, runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "revision-writer", "global")
	if err != nil {
		t.Fatalf("acquire session lease: %v", err)
	}
	if lease.SessionID != sessionID {
		t.Fatalf("acquired session = %s, want %s", lease.SessionID, sessionID)
	}
	if err := store.IncrementTurn(ctx, agentID, runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, sessionID, "global"); err != nil {
		t.Fatalf("increment session turn: %v", err)
	}
	if err := store.AdoptSessionID(ctx, agentID, runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "revision-writer", "provider-revision-writer", "global"); err != nil {
		t.Fatalf("adopt provider session: %v", err)
	}
	if err := store.UpdateLiveSessionWatchdog(ctx, runtimellm.ConversationWatchdogUpdate{
		SessionID: sessionID, AgentID: agentID, SessionScope: "global", ScopeKey: "global", Mode: "session",
		Watchdog: &runtimellm.ConversationWatchdog{
			State: "healthy_long_running", BlockingLayer: "session_execution", Action: "turn_long_running",
			Outcome: "observed", LastOutputAt: "2026-07-14T12:00:00Z", RecordedAt: "2026-07-14T12:00:30Z",
		},
	}); err != nil {
		t.Fatalf("update session watchdog: %v", err)
	}
	if err := store.Release(ctx, lease); err != nil {
		t.Fatalf("release session lease: %v", err)
	}
}
