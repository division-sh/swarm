package runtimepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

const runForkRevisionFlowInstance = "revision-flow"

func captureRunForkTestRevision(t *testing.T, db *sql.DB, runID string, families ...runforkrevision.Family) int64 {
	t.Helper()
	if len(families) == 0 {
		families = runforkrevision.AllFamilies()
	}
	tx, err := db.BeginTx(testAuthorActivityContext(), nil)
	if err != nil {
		t.Fatalf("begin run fork test revision: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	revision, err := finalizePostgresRunForkTestRevision(testAuthorActivityContext(), tx, runID, families...)
	if err != nil {
		t.Fatalf("capture run fork test revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit run fork test revision: %v", err)
	}
	return revision
}

func finalizePostgresRunForkTestRevision(ctx context.Context, tx *sql.Tx, runID string, families ...runforkrevision.Family) (int64, error) {
	effects := runforkrevision.NewEffects()
	if err := effects.Add(runID, families...); err != nil {
		return 0, err
	}
	results, err := runforkrevision.FinalizePostgres(ctx, tx, effects)
	if err != nil {
		return 0, err
	}
	return results[runID].Revision, nil
}

func finalizeAllPostgresRunForkTestRevisions(ctx context.Context, tx *sql.Tx) (map[string]int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(run_id AS TEXT) FROM runs ORDER BY run_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	effects := runforkrevision.NewEffects()
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return nil, err
		}
		if err := effects.Add(runID, runforkrevision.AllFamilies()...); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	results, err := runforkrevision.FinalizePostgres(ctx, tx, effects)
	if err != nil {
		return nil, err
	}
	revisions := make(map[string]int64, len(results))
	for runID, result := range results {
		revisions[runID] = result.Revision
	}
	return revisions, nil
}

func seedRunForkSessionProjection(t *testing.T, db *sql.DB, runID, agentID, sessionID, status string, at time.Time) {
	t.Helper()
	ctx := testAuthorActivityContext()
	identity := testAgentIdentity(t, agentID, runForkRevisionFlowInstance)
	fields, err := identity.StorageFields()
	if err != nil {
		t.Fatal(err)
	}
	seedTestAgentRow(t, ctx, db, true, identity, "active")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
			conversation, turn_count, runtime_state, status, termination_reason, terminated_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored', '[]'::jsonb, 0,
			'{}'::jsonb, $10,
			CASE WHEN $10::text = 'terminated' THEN 'normal' ELSE NULL END,
			CASE WHEN $10::text = 'terminated' THEN $11::timestamptz ELSE NULL::timestamptz END, $11, $11
		)
	`, sessionID, runID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, status, at.Add(-time.Second)); err != nil {
		t.Fatalf("seed run-fork session projection: %v", err)
	}
}

func mutateRunForkSessionExcludedColumns(t *testing.T, db *sql.DB, runID, sessionID string, at time.Time) {
	t.Helper()
	ctx := testAuthorActivityContext()
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
	if _, err := finalizePostgresRunForkTestRevision(ctx, tx, runID, runforkrevision.FamilyAgentSessions); err != nil {
		t.Fatalf("finalize excluded session mutation revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit excluded session mutation: %v", err)
	}
}

func exerciseRunForkSessionExcludedWriters(t *testing.T, store *PostgresStore, runID, agentID, sessionID string) {
	t.Helper()
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	identity := testAgentMemoryIdentity(t, runID, agentID, runForkRevisionFlowInstance)
	lease, _, err := store.AcquireLiveSession(ctx, identity, "revision-writer")
	if err != nil {
		t.Fatalf("acquire session lease: %v", err)
	}
	if lease.SessionID != sessionID {
		t.Fatalf("acquired session = %s, want %s", lease.SessionID, sessionID)
	}
	if err := store.IncrementTurn(ctx, identity, sessionID); err != nil {
		t.Fatalf("increment session turn: %v", err)
	}
	if err := store.AdoptSessionID(ctx, identity, "revision-writer", "provider-revision-writer"); err != nil {
		t.Fatalf("adopt provider session: %v", err)
	}
	if err := store.UpdateLiveSessionWatchdog(ctx, runtimellm.ConversationWatchdogUpdate{
		SessionID: sessionID, AgentID: agentID, Identity: identity,
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
