package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type lifecycleSubordinateStore interface {
	runtimemanager.AgentLifecyclePersistence
	runtimesessions.Registry
}

type lifecycleOccurrenceStore interface {
	lifecycleSubordinateStore
	runtimemanager.ManagerPersistence
}

type lifecycleOccurrenceAgent struct{ id string }

func (a lifecycleOccurrenceAgent) ID() string { return a.id }
func (lifecycleOccurrenceAgent) Type() string { return "generic" }
func (lifecycleOccurrenceAgent) Subscriptions() []events.EventType {
	return nil
}
func (lifecycleOccurrenceAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func TestLifecycleSubordinateTransactionSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveLifecycleSubordinateTransaction(t, store, store.DB, true)
}

func TestLifecycleSubordinateTransactionPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveLifecycleSubordinateTransaction(t, &PostgresStore{DB: db}, db, false)
}

func TestLifecycleReconfigureOccurrenceIdentitySQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveLifecycleReconfigureOccurrenceIdentity(t, store, store.DB, true)
}

func TestLifecycleReconfigureOccurrenceIdentityPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveLifecycleReconfigureOccurrenceIdentity(t, &PostgresStore{DB: db}, db, false)
}

func TestLifecycleConcurrentPartialReconfigureSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveLifecycleConcurrentPartialReconfigure(t, store, store.DB, true)
}

func TestLifecycleConcurrentPartialReconfigurePostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveLifecycleConcurrentPartialReconfigure(t, &PostgresStore{DB: db}, db, false)
}

func proveLifecycleConcurrentPartialReconfigure(t *testing.T, store lifecycleOccurrenceStore, db *sql.DB, sqlite bool) {
	t.Helper()
	firstBuildEntered := make(chan struct{}, 1)
	releaseFirstBuild := make(chan struct{})
	secondBuildEntered := make(chan struct{}, 1)
	releaseSecondBuild := make(chan struct{})
	manager := runtimemanager.NewAgentManagerWithOptions(nil, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		if cfg.ID == "concurrent-partial-reconfigure-agent" {
			switch {
			case cfg.ConversationMode == runtimesessions.RuntimeModeTask.String() && len(cfg.Tools) == 1 && cfg.Tools[0] == "tool-a":
				firstBuildEntered <- struct{}{}
				<-releaseFirstBuild
			case len(cfg.Tools) == 1 && cfg.Tools[0] == "tool-b":
				secondBuildEntered <- struct{}{}
				<-releaseSecondBuild
			}
		}
		return lifecycleOccurrenceAgent{id: cfg.ID}, nil
	}, runtimemanager.AgentManagerOptions{LifecycleStore: store, Sessions: store}, store)
	cfg := runtimeactors.AgentConfig{
		ID:               "concurrent-partial-reconfigure-agent",
		Role:             "worker",
		Type:             "sonnet",
		Model:            "regular",
		Mode:             "global",
		ConversationMode: runtimesessions.RuntimeModeSession.String(),
		SessionScope:     runtimesessions.SessionScopeFlow.String(),
		FlowPath:         "support/serialized",
		Tools:            []string{"tool-a"},
	}
	if err := manager.SpawnAgent(cfg); err != nil {
		t.Fatalf("spawn agent: %v", err)
	}
	agents, err := store.LoadAgents(context.Background())
	if err != nil || len(agents) != 1 {
		t.Fatalf("load spawned agent: agents=%#v err=%v", agents, err)
	}
	initialGeneration := agents[0].LifecycleGeneration
	sessionID := uuid.NewString()
	seedLifecycleSessionForPartialReconfigure(t, db, sqlite, sessionID, cfg.ID, cfg.CanonicalFlowPath())

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- manager.ReconfigureAgent(cfg.ID, runtimeactors.AgentConfig{ConversationMode: runtimesessions.RuntimeModeTask.String()})
	}()
	<-firstBuildEntered
	secondStarted := make(chan struct{})
	secondErr := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondErr <- manager.ReconfigureAgent(cfg.ID, runtimeactors.AgentConfig{Tools: []string{"tool-b"}})
	}()
	<-secondStarted
	secondBuiltBeforeFirstCommit := false
	select {
	case <-secondBuildEntered:
		secondBuiltBeforeFirstCommit = true
	case <-time.After(500 * time.Millisecond):
	}
	close(releaseFirstBuild)
	if err := <-firstErr; err != nil {
		t.Fatalf("task reconfigure: %v", err)
	}
	if !secondBuiltBeforeFirstCommit {
		<-secondBuildEntered
	}
	close(releaseSecondBuild)
	if err := <-secondErr; err != nil {
		t.Fatalf("tools reconfigure: %v", err)
	}
	if secondBuiltBeforeFirstCommit {
		t.Fatal("disjoint partial patch was built before the prior reconfigure committed and projected")
	}

	agents, err = store.LoadAgents(context.Background())
	if err != nil || len(agents) != 1 {
		t.Fatalf("load reconfigured agent: agents=%#v err=%v", agents, err)
	}
	got := agents[0]
	if got.Config.ConversationMode != runtimesessions.RuntimeModeTask.String() || len(got.Config.Tools) != 1 || got.Config.Tools[0] != "tool-b" {
		t.Fatalf("final durable config = mode:%q tools:%v, want task + tool-b", got.Config.ConversationMode, got.Config.Tools)
	}
	if got.LifecycleGeneration != initialGeneration+2 {
		t.Fatalf("final durable generation = %d, want %d", got.LifecycleGeneration, initialGeneration+2)
	}
	assertLifecyclePartialReconfigureSession(t, db, sqlite, sessionID)
	assertLifecyclePartialReconfigureOutcomes(t, db, sqlite, cfg.ID, sessionID)
}

func seedLifecycleSessionForPartialReconfigure(t *testing.T, db *sql.DB, sqlite bool, sessionID, agentID, scopeKey string) {
	t.Helper()
	now := time.Now().UTC()
	query := `INSERT INTO agent_sessions (session_id, agent_id, scope_key, scope, conversation, runtime_mode, runtime_state, status, created_at, updated_at) VALUES (?, ?, ?, 'flow', '[]', 'session', '{}', 'active', ?, ?)`
	args := []any{sessionID, agentID, scopeKey, now, now}
	if !sqlite {
		query = `INSERT INTO agent_sessions (session_id, agent_id, scope_key, scope, conversation, runtime_mode, runtime_state, status, created_at, updated_at) VALUES ($1::uuid, $2, $3, 'flow', '[]'::jsonb, 'session', '{}'::jsonb, 'active', $4, $4)`
		args = []any{sessionID, agentID, scopeKey, now}
	}
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("seed lifecycle session: %v", err)
	}
}

func assertLifecyclePartialReconfigureSession(t *testing.T, db *sql.DB, sqlite bool, sessionID string) {
	t.Helper()
	query := `SELECT status, COALESCE(successor_session_id, '') FROM agent_sessions WHERE session_id = ?`
	if !sqlite {
		query = `SELECT status, COALESCE(successor_session_id::text, '') FROM agent_sessions WHERE session_id = $1::uuid`
	}
	var status, successor string
	if err := db.QueryRowContext(context.Background(), query, sessionID).Scan(&status, &successor); err != nil {
		t.Fatalf("load predecessor session: %v", err)
	}
	if status != "terminated" || successor != "" {
		t.Fatalf("predecessor session status=%q successor=%q, want terminated without successor", status, successor)
	}
}

func assertLifecyclePartialReconfigureOutcomes(t *testing.T, db *sql.DB, sqlite bool, agentID, sessionID string) {
	t.Helper()
	query := `SELECT result FROM agent_lifecycle_operations WHERE agent_id = ? AND operation_kind = 'reconfigure' ORDER BY expected_generation`
	if !sqlite {
		query = `SELECT result::text FROM agent_lifecycle_operations WHERE agent_id = $1 AND operation_kind = 'reconfigure' ORDER BY expected_generation`
	}
	rows, err := db.QueryContext(context.Background(), query, agentID)
	if err != nil {
		t.Fatalf("query reconfigure outcomes: %v", err)
	}
	defer rows.Close()
	results := make([]runtimemanager.AgentLifecycleTransitionResult, 0, 2)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatalf("scan reconfigure outcome: %v", err)
		}
		var result runtimemanager.AgentLifecycleTransitionResult
		if err := json.Unmarshal([]byte(raw), &result); err != nil {
			t.Fatalf("decode reconfigure outcome: %v", err)
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate reconfigure outcomes: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("reconfigure outcomes = %#v, want two serialized decisions", results)
	}
	first, second := results[0].Subordinate, results[1].Subordinate
	if first.Action != runtimesessions.LifecycleMutationTerminateCurrentSet || len(first.Sessions) != 1 || first.Sessions[0].PreviousSessionID != sessionID || first.Sessions[0].SuccessorSessionID != "" {
		t.Fatalf("first subordinate outcome = %#v, want exact predecessor termination", first)
	}
	if second.Action != runtimesessions.LifecycleMutationNone || len(second.Sessions) != 0 {
		t.Fatalf("second subordinate outcome = %#v, want task-mode no-op", second)
	}
}

func proveLifecycleReconfigureOccurrenceIdentity(t *testing.T, store lifecycleOccurrenceStore, db *sql.DB, sqlite bool) {
	t.Helper()
	manager := runtimemanager.NewAgentManagerWithOptions(nil, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return lifecycleOccurrenceAgent{id: cfg.ID}, nil
	}, runtimemanager.AgentManagerOptions{LifecycleStore: store, Sessions: store}, store)
	cfg := runtimeactors.AgentConfig{
		ID:               "reconfigure-occurrence-agent",
		Role:             "worker",
		Type:             "sonnet",
		Model:            "regular",
		Mode:             "global",
		ConversationMode: runtimesessions.RuntimeModeSession.String(),
		SessionScope:     runtimesessions.SessionScopeFlow.String(),
		FlowPath:         "support/occurrence",
	}
	if err := manager.SpawnAgent(cfg); err != nil {
		t.Fatalf("spawn agent: %v", err)
	}
	agents, err := store.LoadAgents(context.Background())
	if err != nil || len(agents) != 1 {
		t.Fatalf("load spawned agent: agents=%#v err=%v", agents, err)
	}
	initialGeneration := agents[0].LifecycleGeneration
	for i, tool := range []string{"tool-a", "tool-b", "tool-a", "tool-b"} {
		if err := manager.ReconfigureAgent(cfg.ID, runtimeactors.AgentConfig{Tools: []string{tool}}); err != nil {
			t.Fatalf("reconfigure occurrence %d (%s): %v", i+1, tool, err)
		}
		agents, err = store.LoadAgents(context.Background())
		if err != nil || len(agents) != 1 {
			t.Fatalf("load occurrence %d: agents=%#v err=%v", i+1, agents, err)
		}
		if got, want := agents[0].LifecycleGeneration, initialGeneration+uint64(i)+1; got != want {
			t.Fatalf("occurrence %d generation = %d, want %d", i+1, got, want)
		}
	}
	assertLifecycleReconfigureOperationCount(t, db, sqlite, cfg.ID, 4)

	if err := manager.ReconfigureAgent(cfg.ID, runtimeactors.AgentConfig{Tools: []string{"tool-b"}}); err != nil {
		t.Fatalf("same-current reconfigure: %v", err)
	}
	assertLifecycleReconfigureOperationCount(t, db, sqlite, cfg.ID, 4)
	agents, err = store.LoadAgents(context.Background())
	if err != nil || len(agents) != 1 {
		t.Fatalf("load same-current agent: agents=%#v err=%v", agents, err)
	}
	if got, want := agents[0].LifecycleGeneration, initialGeneration+4; got != want {
		t.Fatalf("same-current generation = %d, want %d", got, want)
	}
}

func assertLifecycleReconfigureOperationCount(t *testing.T, db *sql.DB, sqlite bool, agentID string, want int) {
	t.Helper()
	query := `SELECT COUNT(*), COUNT(DISTINCT operation_id) FROM agent_lifecycle_operations WHERE agent_id = ? AND operation_kind = 'reconfigure'`
	if !sqlite {
		query = `SELECT COUNT(*), COUNT(DISTINCT operation_id) FROM agent_lifecycle_operations WHERE agent_id = $1 AND operation_kind = 'reconfigure'`
	}
	var count, distinct int
	if err := db.QueryRowContext(context.Background(), query, agentID).Scan(&count, &distinct); err != nil {
		t.Fatalf("count reconfigure operations: %v", err)
	}
	if count != want || distinct != want {
		t.Fatalf("reconfigure operations count=%d distinct=%d, want %d distinct occurrences", count, distinct, want)
	}
}

func proveLifecycleSubordinateTransaction(t *testing.T, store lifecycleSubordinateStore, db *sql.DB, sqlite bool) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	agentID := "subordinate-transaction-agent"
	rec := runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID: agentID, Role: "worker", Type: "sonnet", Model: "regular", Mode: "global",
			Config: []byte(`{"system_prompt":"test"}`),
		},
		Status: "active", HiredBy: "test", StartedAt: now,
	}
	spawned, err := store.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: "subordinate-spawn",
		AgentID: agentID, Trigger: "spawn", TargetEpoch: 71, TargetGeneration: 1,
		TargetPhase: runtimemanager.AgentLifecycleRegistered, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStopped, Agent: &rec, Now: now,
	})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	started, err := store.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "start", RequestHash: "subordinate-start",
		AgentID: agentID, Trigger: "start", ExpectedEpoch: spawned.RuntimeEpoch,
		ExpectedGeneration: spawned.Generation, ExpectedPhase: spawned.Phase,
		TargetEpoch: spawned.RuntimeEpoch, TargetGeneration: spawned.Generation + 1,
		TargetPhase: runtimemanager.AgentLifecycleRunning, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStandard, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	staleToken := runtimeeffects.LifecycleToken{RuntimeEpoch: started.RuntimeEpoch, AgentID: agentID, Generation: started.Generation}
	staleCtx := runtimeeffects.WithLifecycleToken(ctx, staleToken)
	active, err := store.Acquire(staleCtx, agentID, runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "worker-1", "global")
	if err != nil {
		t.Fatalf("acquire active session: %v", err)
	}
	if err := store.Release(staleCtx, active); err != nil {
		t.Fatalf("release active session: %v", err)
	}
	suspendedID := uuid.NewString()
	if sqlite {
		_, err = db.ExecContext(ctx, `
			INSERT INTO agent_sessions (
				session_id, agent_id, scope_key, scope, conversation, turn_count,
				runtime_mode, runtime_state, status, created_at, updated_at
			) VALUES (?, ?, 'flow/suspended', 'flow', ?, 7, 'session', ?, 'suspended', ?, ?)
		`, suspendedID, agentID, `[{"role":"user","content":"old suspended"}]`, `{"provider_session_id":"old-suspended"}`, now, now)
		_, _ = db.ExecContext(ctx, `UPDATE agent_sessions SET conversation=?, turn_count=5, runtime_state=? WHERE session_id=?`, `[{"role":"user","content":"old active"}]`, `{"provider_session_id":"old-active"}`, active.SessionID)
	} else {
		_, err = db.ExecContext(ctx, `
			INSERT INTO agent_sessions (
				session_id, agent_id, scope_key, scope, conversation, turn_count,
				runtime_mode, runtime_state, status, created_at, updated_at
			) VALUES ($1::uuid, $2, 'flow/suspended', 'flow', $3::jsonb, 7, 'session', $4::jsonb, 'suspended', $5, $5)
		`, suspendedID, agentID, `[{"role":"user","content":"old suspended"}]`, `{"provider_session_id":"old-suspended"}`, now)
		_, _ = db.ExecContext(ctx, `UPDATE agent_sessions SET conversation=$1::jsonb, turn_count=5, runtime_state=$2::jsonb WHERE session_id=$3::uuid`, `[{"role":"user","content":"old active"}]`, `{"provider_session_id":"old-active"}`, active.SessionID)
	}
	if err != nil {
		t.Fatalf("seed suspended session: %v", err)
	}

	operationID := uuid.NewString()
	rotate := runtimemanager.AgentLifecycleTransition{
		OperationID: operationID, OperationKind: "restart", RequestHash: "restart-with-complete-set-rotation",
		AgentID: agentID, Trigger: "restart", ExpectedEpoch: started.RuntimeEpoch,
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
	rotated, err := store.CommitAgentLifecycleTransition(ctx, rotate)
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
	replayed, err := store.CommitAgentLifecycleTransition(ctx, rotate)
	if err != nil || !replayed.Replayed || !reflect.DeepEqual(replayed.Subordinate, rotated.Subordinate) {
		t.Fatalf("exact replay = %#v err=%v, want subordinate %#v", replayed, err, rotated.Subordinate)
	}
	changed := rotate
	changed.RequestHash = "changed-plan-hash"
	if _, err := store.CommitAgentLifecycleTransition(ctx, changed); err == nil {
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
		AgentID: agentID, Trigger: "terminate", ExpectedEpoch: rotated.RuntimeEpoch,
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
	if _, err := store.CommitAgentLifecycleTransition(ctx, failedTerminate); err == nil {
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
	terminated, err := store.CommitAgentLifecycleTransition(ctx, failedTerminate)
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
