package store

import (
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
	proveLifecycleAndExternalEffectAuthority(t, admitTestPostgresStore(t, db), db, false)
}

func proveLifecycleAndExternalEffectAuthority(t *testing.T, store lifecycleEffectStore, db *sql.DB, sqlite bool) {
	t.Helper()
	ctx := testAuthorActivityContext()
	now := time.Date(2026, 7, 10, 18, 0, 0, 0, time.UTC)
	rec := runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID: "lifecycle-agent", Type: "sonnet", Role: "worker", FlowID: "global", Model: "regular",
			ExecutionMode: runtimeeffects.ExecutionModeLive,
			Config:        []byte(`{"system_prompt":"x"}`),
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
	runID := managedNormalEffectStoreTestRunID(started.AgentID)
	if sqlite {
		if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, runID); err != nil {
			t.Fatalf("seed managed effect run: %v", err)
		}
	} else if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed managed effect run: %v", err)
	}

	controller := runtimeeffects.NewController(store)
	activeCtx := runtimeeffects.WithController(runtimeeffects.WithLifecycleToken(ctx, runtimeeffects.LifecycleToken{
		RuntimeEpoch: started.RuntimeEpoch, AgentID: started.AgentID, Generation: started.Generation,
	}), controller)
	activeCtx = runtimeeffects.WithLogicalOperationIdentity(activeCtx, "effect-authority-primary")
	authority := runtimeeffects.NormalAgentAuthority(runtimeeffects.LifecycleToken{RuntimeEpoch: started.RuntimeEpoch, AgentID: started.AgentID, Generation: started.Generation}, "lifecycle-test-owner", now.Add(time.Minute))
	activeCtx = managedNormalEffectStoreTestContext(t, activeCtx, authority)
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
	if _, err := runtimeeffects.Begin(activeCtx, "authored_http_tool", []byte("request"), map[string]string{"test": "true"}); err == nil {
		t.Fatal("settled logical operation replay was admitted for redispatch")
	}
	if _, err := runtimeeffects.Begin(activeCtx, "authored_http_tool", []byte("changed-request"), map[string]string{"test": "true"}); err == nil {
		t.Fatal("same logical operation accepted a changed request fingerprint")
	}
	if current, err := runtimeeffects.ProjectionCurrent(activeCtx); err != nil || !current {
		t.Fatalf("current generation projection authorization = %v, err=%v", current, err)
	}
	prelaunchCtx := runtimeeffects.WithLogicalOperationIdentity(activeCtx, "effect-authority-prelaunch")
	prelaunch, err := runtimeeffects.Begin(prelaunchCtx, "authored_http_tool", []byte("recover-prelaunch"), nil)
	if err != nil {
		t.Fatalf("authorize recoverable prelaunch effect: %v", err)
	}
	launchedCtx := runtimeeffects.WithLogicalOperationIdentity(activeCtx, "effect-authority-launched")
	launched, err := runtimeeffects.Begin(launchedCtx, "authored_http_tool", []byte("recover-launched"), nil)
	if err != nil {
		t.Fatalf("authorize recoverable launched effect: %v", err)
	}
	if err := launched.MarkLaunched(activeCtx); err != nil {
		t.Fatalf("mark recoverable effect launched: %v", err)
	}
	requireExternalOperationState(t, db, sqlite, prelaunch.Attempt().OperationID, runtimeeffects.StateAuthorized)
	requireExternalOperationState(t, db, sqlite, launched.Attempt().OperationID, runtimeeffects.StateLaunched)
	recoveryStore := store.(runtimeeffects.RecoveryStore)
	summary, err := recoveryStore.ReconcileExternalEffectAttempts(ctx, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("reconcile external effects: %v", err)
	}
	if summary.PrelaunchTerminal != 1 || summary.OutcomeUncertain != 1 {
		t.Fatalf("recovery summary = %#v, want one prelaunch terminal and one uncertain", summary)
	}
	requireExternalAttemptState(t, db, sqlite, prelaunch.Attempt().AttemptID, runtimeeffects.StateTerminalFailure)
	requireExternalAttemptState(t, db, sqlite, launched.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
	requireExternalOperationState(t, db, sqlite, prelaunch.Attempt().OperationID, runtimeeffects.StateTerminalFailure)
	requireExternalOperationState(t, db, sqlite, launched.Attempt().OperationID, runtimeeffects.StateOutcomeUncertain)

	restarted, err := store.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: "00000000-0000-0000-0000-000000001903", OperationKind: "restart", RequestHash: "restart-hash",
		AgentID: rec.Config.ID, Trigger: "restart", ExpectedEpoch: started.RuntimeEpoch,
		ExpectedGeneration: started.Generation, ExpectedPhase: started.Phase,
		TargetEpoch: started.RuntimeEpoch, TargetGeneration: started.Generation + 1, TargetPhase: runtimemanager.AgentLifecycleRunning,
		ConfigRevision: "revision-1", RunMode: runtimemanager.AgentRunModeStandard, Now: now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("restart lifecycle transition: %v", err)
	}
	restartedCtx := runtimeeffects.WithController(runtimeeffects.WithLifecycleToken(ctx, runtimeeffects.LifecycleToken{
		RuntimeEpoch: restarted.RuntimeEpoch, AgentID: restarted.AgentID, Generation: restarted.Generation,
	}), controller)
	restartedCtx = runtimeeffects.WithLogicalOperationIdentity(restartedCtx, "effect-authority-launched")
	restartedAuthority := runtimeeffects.NormalAgentAuthority(runtimeeffects.LifecycleToken{RuntimeEpoch: restarted.RuntimeEpoch, AgentID: restarted.AgentID, Generation: restarted.Generation}, "lifecycle-test-restarted-owner", now.Add(time.Minute))
	restartedCtx = managedNormalEffectStoreTestContext(t, restartedCtx, restartedAuthority)
	if _, err := runtimeeffects.Begin(restartedCtx, "authored_http_tool", []byte("recover-launched"), nil); err == nil {
		t.Fatal("successor generation redispatched the uncertain logical operation")
	} else if failure, ok := runtimefailures.As(err); !ok || failure.Failure.Class != runtimefailures.ClassOutcomeUncertain {
		t.Fatalf("successor replay failure = %v, want outcome uncertain", err)
	}

	diagnosticsStore := store.(runtimemanager.AgentLifecycleDiagnosticPersistence)
	diagnostics, err := diagnosticsStore.ListPendingAgentLifecycleDiagnostics(ctx, 10)
	if err != nil || len(diagnostics) != 3 {
		t.Fatalf("pending lifecycle diagnostics = %#v err=%v, want spawn, start, and restart", diagnostics, err)
	}
	if err := diagnosticsStore.MarkAgentLifecycleDiagnosticProjected(ctx, diagnostics[0].OutboxID, now.Add(3*time.Second)); err != nil {
		t.Fatalf("mark lifecycle diagnostic projected: %v", err)
	}
	diagnostics, err = diagnosticsStore.ListPendingAgentLifecycleDiagnostics(ctx, 10)
	if err != nil || len(diagnostics) != 2 {
		t.Fatalf("remaining lifecycle diagnostics = %#v err=%v, want two", diagnostics, err)
	}

	staleCtx := runtimeeffects.WithController(runtimeeffects.WithLifecycleToken(ctx, runtimeeffects.LifecycleToken{
		RuntimeEpoch: started.RuntimeEpoch, AgentID: started.AgentID, Generation: started.Generation - 1,
	}), controller)
	staleCtx = runtimeeffects.WithLogicalOperationIdentity(staleCtx, "effect-authority-stale")
	staleAuthority := runtimeeffects.NormalAgentAuthority(runtimeeffects.LifecycleToken{RuntimeEpoch: started.RuntimeEpoch, AgentID: started.AgentID, Generation: started.Generation - 1}, "lifecycle-test-stale-owner", now.Add(time.Minute))
	staleCtx = managedNormalEffectStoreTestContext(t, staleCtx, staleAuthority)
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

	launchFenceCtx := runtimeeffects.WithLogicalOperationIdentity(restartedCtx, "effect-authority-launch-fence")
	launchFence, err := runtimeeffects.Begin(launchFenceCtx, "authored_http_tool", []byte("launch-fence"), nil)
	if err != nil {
		t.Fatalf("authorize launch-fence effect: %v", err)
	}
	if _, err := store.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: "00000000-0000-0000-0000-000000001904", OperationKind: "restart", RequestHash: "restart-launch-fence-hash",
		AgentID: rec.Config.ID, Trigger: "restart", ExpectedEpoch: restarted.RuntimeEpoch,
		ExpectedGeneration: restarted.Generation, ExpectedPhase: restarted.Phase,
		TargetEpoch: restarted.RuntimeEpoch, TargetGeneration: restarted.Generation + 1, TargetPhase: runtimemanager.AgentLifecycleRunning,
		ConfigRevision: "revision-1", RunMode: runtimemanager.AgentRunModeStandard, Now: now.Add(4 * time.Second),
	}); err != nil {
		t.Fatalf("supersede authorized effect generation: %v", err)
	}
	primitiveCalls := 0
	if err := launchFence.MarkLaunched(launchFenceCtx); err == nil {
		primitiveCalls++
		t.Fatal("superseded authorized effect was marked launched")
	} else if failure, ok := runtimefailures.As(err); !ok || failure.Failure.Class != runtimefailures.ClassSupersededGeneration {
		t.Fatalf("superseded launch failure = %v, want superseded generation", err)
	}
	if primitiveCalls != 0 {
		t.Fatalf("superseded launch reached primitive %d times", primitiveCalls)
	}
	requireExternalOperationState(t, db, sqlite, launchFence.Attempt().OperationID, runtimeeffects.StateAuthorized)
	requireExternalAttemptState(t, db, sqlite, launchFence.Attempt().AttemptID, runtimeeffects.StateAuthorized)

	placeholder := "?"
	if !sqlite {
		placeholder = "$1"
	}
	var operationState, attemptState string
	if err := db.QueryRowContext(ctx, "SELECT state FROM runtime_external_effect_operations WHERE operation_id = "+placeholder, handle.Attempt().OperationID).Scan(&operationState); err != nil {
		t.Fatalf("load effect operation state: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT state FROM runtime_external_effect_attempts WHERE attempt_id = "+placeholder, handle.Attempt().AttemptID).Scan(&attemptState); err != nil {
		t.Fatalf("load effect attempt state: %v", err)
	}
	if operationState != string(runtimeeffects.StateOutcomeUncertain) || attemptState != string(runtimeeffects.StateOutcomeUncertain) {
		t.Fatalf("settled states operation=%s attempt=%s", operationState, attemptState)
	}
}

func requireExternalOperationState(t *testing.T, db *sql.DB, sqlite bool, operationID string, want runtimeeffects.State) {
	t.Helper()
	placeholder := "?"
	if !sqlite {
		placeholder = "$1"
	}
	var state string
	if err := db.QueryRow("SELECT state FROM runtime_external_effect_operations WHERE operation_id = "+placeholder, operationID).Scan(&state); err != nil {
		t.Fatalf("load external operation state: %v", err)
	}
	if state != string(want) {
		t.Fatalf("external operation state = %q, want %q", state, want)
	}
}

func requireExternalAttemptState(t *testing.T, db *sql.DB, sqlite bool, attemptID string, want runtimeeffects.State) {
	t.Helper()
	placeholder := "?"
	if !sqlite {
		placeholder = "$1"
	}
	var state string
	if err := db.QueryRow("SELECT state FROM runtime_external_effect_attempts WHERE attempt_id = "+placeholder, attemptID).Scan(&state); err != nil {
		t.Fatalf("load external attempt state: %v", err)
	}
	if state != string(want) {
		t.Fatalf("external attempt state = %q, want %q", state, want)
	}
}

func requireProviderHead(t *testing.T, db *sql.DB, sqlite bool, sessionID, want string) {
	t.Helper()
	query := `SELECT COALESCE(json_extract(runtime_state, '$.provider_session_id'), '') FROM agent_sessions WHERE session_id=?`
	if !sqlite {
		query = `SELECT COALESCE(runtime_state->>'provider_session_id', '') FROM agent_sessions WHERE session_id=$1::uuid`
	}
	var got string
	if err := db.QueryRow(query, sessionID).Scan(&got); err != nil {
		t.Fatalf("load provider head: %v", err)
	}
	if got != want {
		t.Fatalf("provider head = %q, want %q", got, want)
	}
}
