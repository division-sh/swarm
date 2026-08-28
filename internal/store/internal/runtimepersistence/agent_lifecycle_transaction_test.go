package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type lifecycleSubordinateStore interface {
	agentfixture.Store
	runtimesessions.Registry
}

func TestLifecycleSubordinateTransactionSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveLifecycleSubordinateTransaction(t, store, store.backend.ConstructionHandle(), true)
}

func TestLifecycleSubordinateTransactionPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveLifecycleSubordinateTransaction(t, admitTestPostgresStore(t, db), db, false)
}

func seedLifecycleRun(t *testing.T, db *sql.DB, sqlite bool, runID string) {
	t.Helper()
	if sqlite {
		requireRunningSQLiteRunForTest(t, testAuthorActivityContext(), db, runID, time.Now().UTC())
	} else {
		requireRunningPostgresRunForTest(t, testAuthorActivityContext(), db, runID, time.Now().UTC())
	}
}

func proveLifecycleSubordinateTransaction(t *testing.T, store lifecycleSubordinateStore, db *sql.DB, sqlite bool) {
	t.Helper()
	ctx := testAuthorActivityContext()
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	agentID := "subordinate-transaction-agent"
	identity := testAgentIdentity(t, agentID, "global")
	rec := runtimemanager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
			ID: agentID, Identity: identity, Role: "worker", Type: "sonnet", Model: "regular", FlowID: "global",
			ExecutionMode: runtimeeffects.ExecutionModeLive,
			Memory:        agentmemory.Authored(true), FlowPath: "global",
			Config: []byte(`{}`),
		}),
		Status: "active", HiredBy: "test", StartedAt: now,
	}
	spawned, err := agentfixture.CommitStatic(t, ctx, store, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: "subordinate-spawn",
		Identity: identity, AgentID: agentID, Trigger: "spawn", TargetEpoch: 71, TargetGeneration: 1,
		TargetPhase: runtimemanager.AgentLifecycleRegistered, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStopped, Agent: &rec, Now: now,
	})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	started, err := agentfixture.CommitStatic(t, ctx, store, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "start", RequestHash: "subordinate-start",
		Identity: identity, AgentID: agentID, Trigger: "start", ExpectedEpoch: spawned.RuntimeEpoch,
		ExpectedGeneration: spawned.Generation, ExpectedPhase: spawned.Phase,
		TargetEpoch: spawned.RuntimeEpoch, TargetGeneration: spawned.Generation + 1,
		TargetPhase: runtimemanager.AgentLifecycleRunning, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStandard, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	staleToken := runtimeeffects.LifecycleToken{RuntimeEpoch: started.RuntimeEpoch, Identity: identity, AgentID: agentID, Generation: started.Generation}
	staleCtx := runtimeeffects.WithLifecycleToken(ctx, staleToken)
	activeRunID := uuid.NewString()
	suspendedRunID := uuid.NewString()
	seedLifecycleRun(t, db, sqlite, activeRunID)
	seedLifecycleRun(t, db, sqlite, suspendedRunID)
	activeIdentity := testAgentMemoryIdentity(t, activeRunID, agentID, "global")
	active, err := store.Acquire(staleCtx, activeIdentity, "worker-1")
	if err != nil {
		t.Fatalf("acquire active session: %v", err)
	}
	if err := store.Release(staleCtx, active); err != nil {
		t.Fatalf("release active session: %v", err)
	}
	suspendedID := uuid.NewString()
	identityFields, err := identity.StorageFields()
	if err != nil {
		t.Fatalf("suspended session identity: %v", err)
	}
	if sqlite {
		_, err = db.ExecContext(ctx, `
			INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source,
				agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
				memory_enabled, memory_source,
				conversation, turn_count, runtime_state, status, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored', ?, 7, ?, 'suspended', ?, ?)
		`, suspendedID, suspendedRunID, identityFields.AgentID, identityFields.NameOwner, identityFields.NameSource,
			identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID, identityFields.FlowInstancePath,
			`[{"role":"user","content":"old suspended"}]`, `{"provider_session_id":"old-suspended"}`, now, now)
		_, _ = db.ExecContext(ctx, `UPDATE agent_sessions SET conversation=?, turn_count=5, runtime_state=? WHERE session_id=?`, `[{"role":"user","content":"old active"}]`, `{"provider_session_id":"old-active"}`, active.SessionID)
	} else {
		_, err = db.ExecContext(ctx, `
			INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source,
				agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
				memory_enabled, memory_source,
				conversation, turn_count, runtime_state, status, created_at, updated_at
			) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored', $10::jsonb, 7, $11::jsonb, 'suspended', $12, $12)
		`, suspendedID, suspendedRunID, identityFields.AgentID, identityFields.NameOwner, identityFields.NameSource,
			identityFields.RoutePresence, identityFields.FlowScopeKey, identityFields.FlowInstanceID, identityFields.FlowInstancePath,
			`[{"role":"user","content":"old suspended"}]`, `{"provider_session_id":"old-suspended"}`, now)
		_, _ = db.ExecContext(ctx, `UPDATE agent_sessions SET conversation=$1::jsonb, turn_count=5, runtime_state=$2::jsonb WHERE session_id=$3::uuid`, `[{"role":"user","content":"old active"}]`, `{"provider_session_id":"old-active"}`, active.SessionID)
	}
	if err != nil {
		t.Fatalf("seed suspended session: %v", err)
	}

	operationID := uuid.NewString()
	rotate := runtimemanager.AgentLifecycleTransition{
		OperationID: operationID, OperationKind: "restart", RequestHash: "restart-with-complete-set-rotation",
		Identity: identity, AgentID: agentID, Trigger: "restart", ExpectedEpoch: started.RuntimeEpoch,
		ExpectedGeneration: started.Generation, ExpectedPhase: started.Phase,
		TargetEpoch: started.RuntimeEpoch, TargetGeneration: started.Generation + 1,
		TargetPhase: runtimemanager.AgentLifecycleRunning, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStandard,
		Subordinate: runtimesessions.LifecycleMutationPlan{
			Action:            runtimesessions.LifecycleMutationRotateCurrentSet,
			TerminationReason: runtimesessions.TerminationReasonNormal,
			TerminationDetail: "restart", CheckpointSummary: "clean restart",
		},
		Now: now.Add(2 * time.Second),
	}
	rotated, err := agentfixture.CommitStatic(t, ctx, store, rotate)
	if err != nil {
		t.Fatalf("rotate complete set: %v", err)
	}
	if len(rotated.Subordinate.Sessions) != 2 {
		t.Fatalf("rotated set = %#v, want two sessions", rotated.Subordinate)
	}
	for _, mutation := range rotated.Subordinate.Sessions {
		want := runtimesessions.LifecycleSuccessorSessionID(operationID, mutation.PreviousSessionID)
		if mutation.SuccessorSessionID != want {
			t.Fatalf("successor for %s = %s, want %s", mutation.PreviousSessionID, mutation.SuccessorSessionID, want)
		}
		conversation, runtimeState, turnCount, status := loadLifecycleSuccessorState(t, ctx, db, sqlite, mutation.SuccessorSessionID)
		if conversation != "[]" || turnCount != 0 || strings.Contains(runtimeState, "provider_session_id") || status != mutation.PreviousStatus {
			t.Fatalf("successor retained mutable state: conversation=%s runtime_state=%s turns=%d status=%s mutation=%#v", conversation, runtimeState, turnCount, status, mutation)
		}
	}
	replayed, err := agentfixture.CommitStatic(t, ctx, store, rotate)
	if err != nil || !replayed.Replayed || !reflect.DeepEqual(replayed.Subordinate, rotated.Subordinate) {
		t.Fatalf("exact replay = %#v err=%v, want subordinate %#v", replayed, err, rotated.Subordinate)
	}
	changed := rotate
	changed.RequestHash = "changed-plan-hash"
	if _, err := agentfixture.CommitStatic(t, ctx, store, changed); err == nil {
		t.Fatal("changed replay request was accepted")
	} else {
		var failure *runtimefailures.Error
		if !errors.As(err, &failure) || failure.Failure.Class != runtimefailures.ClassConflictingDuplicate {
			t.Fatalf("changed replay error = %v, want conflicting duplicate", err)
		}
	}

	installLifecycleCellFailure(t, ctx, db, sqlite, rotated.Generation+1)
	failedTerminate := runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "teardown", RequestHash: "injected-rollback",
		Identity: identity, AgentID: agentID, Trigger: "terminate", ExpectedEpoch: rotated.RuntimeEpoch,
		ExpectedGeneration: rotated.Generation, ExpectedPhase: rotated.Phase,
		TargetEpoch: rotated.RuntimeEpoch, TargetGeneration: rotated.Generation + 1,
		TargetPhase: runtimemanager.AgentLifecycleTerminated, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStopped,
		Subordinate: runtimesessions.LifecycleMutationPlan{
			Action:            runtimesessions.LifecycleMutationTerminateCurrentSet,
			TerminationReason: runtimesessions.TerminationReasonNormal,
		},
		Now: now.Add(3 * time.Second),
	}
	if _, err := agentfixture.CommitStatic(t, ctx, store, failedTerminate); err == nil {
		t.Fatal("injected lifecycle-cell failure committed subordinate mutation")
	}
	dropLifecycleCellFailure(t, ctx, db, sqlite)
	if got := countCurrentLifecycleSessions(t, ctx, db, sqlite, agentID); got != 2 {
		t.Fatalf("current sessions after rollback = %d, want 2", got)
	}
	if got := loadLifecycleGeneration(t, ctx, db, sqlite, agentID); got != rotated.Generation {
		t.Fatalf("generation after rollback = %d, want %d", got, rotated.Generation)
	}
	if got := countLifecycleOperation(t, ctx, db, sqlite, failedTerminate.OperationID); got != 0 {
		t.Fatalf("failed lifecycle operation evidence rows = %d, want 0", got)
	}

	failedTerminate.OperationID = uuid.NewString()
	failedTerminate.RequestHash = "successful-termination"
	terminated, err := agentfixture.CommitStatic(t, ctx, store, failedTerminate)
	if err != nil {
		t.Fatalf("terminate complete set: %v", err)
	}
	if len(terminated.Subordinate.Sessions) != 2 || countCurrentLifecycleSessions(t, ctx, db, sqlite, agentID) != 0 {
		t.Fatalf("termination outcome = %#v", terminated.Subordinate)
	}
}

func loadLifecycleSuccessorState(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, sessionID string) (string, string, int, string) {
	t.Helper()
	query := `SELECT conversation, runtime_state, turn_count, status FROM agent_sessions WHERE session_id = ?`
	args := []any{sessionID}
	if !sqlite {
		query = `SELECT conversation::text, runtime_state::text, turn_count, status FROM agent_sessions WHERE session_id = $1::uuid`
	}
	var conversation, runtimeState, status string
	var turnCount int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&conversation, &runtimeState, &turnCount, &status); err != nil {
		t.Fatalf("load successor %s: %v", sessionID, err)
	}
	return conversation, runtimeState, turnCount, status
}

func installLifecycleCellFailure(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, generation uint64) {
	t.Helper()
	if sqlite {
		_, err := db.ExecContext(ctx, fmt.Sprintf(`
			CREATE TRIGGER fail_lifecycle_cell_update
			BEFORE UPDATE OF lifecycle_generation ON agents
			WHEN NEW.lifecycle_generation = %d
			BEGIN
				SELECT RAISE(ABORT, 'forced lifecycle cell failure');
			END
		`, generation))
		if err != nil {
			t.Fatalf("install sqlite lifecycle failure: %v", err)
		}
		return
	}
	if _, err := db.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION fail_lifecycle_cell_update()
		RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'forced lifecycle cell failure';
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_lifecycle_cell_update
		BEFORE UPDATE OF lifecycle_generation ON agents
		FOR EACH ROW EXECUTE FUNCTION fail_lifecycle_cell_update()
	`); err != nil {
		t.Fatalf("install postgres lifecycle failure: %v", err)
	}
}

func dropLifecycleCellFailure(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool) {
	t.Helper()
	if sqlite {
		if _, err := db.ExecContext(ctx, `DROP TRIGGER fail_lifecycle_cell_update`); err != nil {
			t.Fatalf("drop sqlite lifecycle failure: %v", err)
		}
		return
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER fail_lifecycle_cell_update ON agents; DROP FUNCTION fail_lifecycle_cell_update()`); err != nil {
		t.Fatalf("drop postgres lifecycle failure: %v", err)
	}
}

func countCurrentLifecycleSessions(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, agentID string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM agent_sessions WHERE agent_id = ? AND status IN ('active', 'suspended')`
	if !sqlite {
		query = `SELECT COUNT(*) FROM agent_sessions WHERE agent_id = $1 AND status IN ('active', 'suspended')`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, agentID).Scan(&count); err != nil {
		t.Fatalf("count current sessions: %v", err)
	}
	return count
}

func loadLifecycleGeneration(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, agentID string) uint64 {
	t.Helper()
	query := `SELECT lifecycle_generation FROM agents WHERE agent_id = ?`
	if !sqlite {
		query = `SELECT lifecycle_generation FROM agents WHERE agent_id = $1`
	}
	var generation uint64
	if err := db.QueryRowContext(ctx, query, agentID).Scan(&generation); err != nil {
		t.Fatalf("load lifecycle generation: %v", err)
	}
	return generation
}

func countLifecycleOperation(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, operationID string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM agent_lifecycle_operations WHERE operation_id = ?`
	if !sqlite {
		query = `SELECT COUNT(*) FROM agent_lifecycle_operations WHERE operation_id = $1::uuid`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, operationID).Scan(&count); err != nil {
		t.Fatalf("count lifecycle operation: %v", err)
	}
	return count
}
