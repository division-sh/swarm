package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/testutil"
)

type lifecycleEffectStore interface {
	runtimemanager.AgentLifecyclePersistence
	runtimeeffects.Store
}

func TestLifecycleAndExternalEffectAuthoritySQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveLifecycleAndExternalEffectAuthority(t, store, store.DB, true)
}

func TestLifecycleAndExternalEffectAuthorityPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveLifecycleAndExternalEffectAuthority(t, &PostgresStore{DB: db}, db, false)
}

func proveLifecycleAndExternalEffectAuthority(t *testing.T, store lifecycleEffectStore, db *sql.DB, sqlite bool) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 18, 0, 0, 0, time.UTC)
	rec := runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID: "lifecycle-agent", Type: "sonnet", Role: "worker", Mode: "global", Model: "regular",
			Config: []byte(`{"system_prompt":"x"}`),
		},
		Status: "active", HiredBy: "test", StartedAt: now,
	}
	spawn := runtimemanager.AgentLifecycleTransition{
		OperationID: "00000000-0000-0000-0000-000000001901", OperationKind: "spawn", RequestHash: "spawn-hash",
		AgentID: rec.Config.ID, Trigger: "spawn", TargetEpoch: 11, TargetGeneration: 1,
		TargetPhase: runtimemanager.AgentLifecycleRegistered, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStopped, Agent: &rec, Now: now,
	}
	spawned, err := store.CommitAgentLifecycleTransition(ctx, spawn)
	if err != nil {
		t.Fatalf("spawn lifecycle transition: %v", err)
	}
	start := runtimemanager.AgentLifecycleTransition{
		OperationID: "00000000-0000-0000-0000-000000001902", OperationKind: "start", RequestHash: "start-hash",
		AgentID: rec.Config.ID, Trigger: "start", ExpectedEpoch: spawned.RuntimeEpoch,
		ExpectedGeneration: spawned.Generation, ExpectedPhase: spawned.Phase,
		TargetEpoch: 11, TargetGeneration: 2, TargetPhase: runtimemanager.AgentLifecycleRunning,
		ConfigRevision: "revision-1", RunMode: runtimemanager.AgentRunModeStandard, Now: now.Add(time.Second),
	}
	started, err := store.CommitAgentLifecycleTransition(ctx, start)
	if err != nil {
		t.Fatalf("start lifecycle transition: %v", err)
	}
	replayed, err := store.CommitAgentLifecycleTransition(ctx, start)
	if err != nil || !replayed.Replayed || replayed.Generation != started.Generation {
		t.Fatalf("lifecycle replay = %#v err=%v", replayed, err)
	}

	controller := runtimeeffects.NewController(store)
	activeCtx := runtimeeffects.WithController(runtimeeffects.WithLifecycleToken(ctx, runtimeeffects.LifecycleToken{
		RuntimeEpoch: started.RuntimeEpoch, AgentID: started.AgentID, Generation: started.Generation,
	}), controller)
	handle, err := runtimeeffects.Begin(activeCtx, "authored_http_tool", []byte("request"), map[string]string{"test": "true"})
	if err != nil {
		t.Fatalf("authorize current generation effect: %v", err)
	}
	if err := handle.MarkLaunched(activeCtx); err != nil {
		t.Fatalf("mark effect launched: %v", err)
	}
	if err := handle.MarkLaunched(activeCtx); err != nil {
		t.Fatalf("replay mark effect launched: %v", err)
	}
	uncertain := runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "http_tool_attempt_outcome_unconfirmed", "test", "dispatch", nil)
	envelope, _ := runtimefailures.EnvelopeFromError(uncertain)
	if err := handle.Settle(activeCtx, runtimeeffects.StateOutcomeUncertain, &envelope, map[string]any{"stage": "transport"}); err != nil {
		t.Fatalf("settle effect uncertain: %v", err)
	}
	if err := handle.Settle(activeCtx, runtimeeffects.StateOutcomeUncertain, &envelope, map[string]any{"stage": "transport"}); err != nil {
		t.Fatalf("replay settle effect uncertain: %v", err)
	}
	if current, err := runtimeeffects.ProjectionCurrent(activeCtx); err != nil || !current {
		t.Fatalf("current generation projection authorization = %v, err=%v", current, err)
	}

	staleCtx := runtimeeffects.WithController(runtimeeffects.WithLifecycleToken(ctx, runtimeeffects.LifecycleToken{
		RuntimeEpoch: started.RuntimeEpoch, AgentID: started.AgentID, Generation: started.Generation - 1,
	}), controller)
	if current, err := runtimeeffects.ProjectionCurrent(staleCtx); err != nil || current {
		t.Fatalf("stale generation projection authorization = %v, err=%v", current, err)
	}
	if _, err := runtimeeffects.Begin(staleCtx, "authored_http_tool", []byte("stale"), nil); err == nil {
		t.Fatal("stale generation effect authorization succeeded")
	} else {
		var failure *runtimefailures.Error
		if !errors.As(err, &failure) || failure.Failure.Class != runtimefailures.ClassSupersededGeneration {
			t.Fatalf("stale generation failure = %T %v", err, err)
		}
	}

	placeholder := "?"
	if !sqlite {
		placeholder = "$1"
	}
	var operationState, attemptState string
	if err := db.QueryRowContext(ctx, "SELECT state FROM agent_external_effect_operations WHERE operation_id = "+placeholder, handle.Attempt().OperationID).Scan(&operationState); err != nil {
		t.Fatalf("load effect operation state: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT state FROM agent_external_effect_attempts WHERE attempt_id = "+placeholder, handle.Attempt().AttemptID).Scan(&attemptState); err != nil {
		t.Fatalf("load effect attempt state: %v", err)
	}
	if operationState != string(runtimeeffects.StateOutcomeUncertain) || attemptState != string(runtimeeffects.StateOutcomeUncertain) {
		t.Fatalf("settled states operation=%s attempt=%s", operationState, attemptState)
	}
}
