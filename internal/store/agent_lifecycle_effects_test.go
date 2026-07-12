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
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type lifecycleEffectStore interface {
	runtimemanager.AgentLifecyclePersistence
	runtimeeffects.Store
}

func TestLifecycleAndExternalEffectAuthoritySQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t, testutil.SQLiteDefaultTemp())
	proveLifecycleAndExternalEffectAuthority(t, store, store.DB, true)
}

func TestLifecycleAndExternalEffectAuthorityPostgres(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	proveLifecycleAndExternalEffectAuthority(t, &PostgresStore{DB: db}, db, false)
}

func TestProviderHeadSettlementFencesLifecycleAndLeaseSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t, testutil.SQLiteDefaultTemp())
	proveProviderHeadSettlementFencesLifecycleAndLease(t, store, store.DB, true)
}

func TestProviderHeadSettlementFencesLifecycleAndLeasePostgres(t *testing.T) {
	_, db, _ := testutil.AcquirePostgres(t, testutil.PostgresFreshPhysical())
	proveProviderHeadSettlementFencesLifecycleAndLease(t, &PostgresStore{DB: db}, db, false)
}

func proveProviderHeadSettlementFencesLifecycleAndLease(t *testing.T, store lifecycleEffectStore, db *sql.DB, sqlite bool) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	agentID := "provider-head-fence-agent"
	spawned, err := store.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: "provider-head-fence-spawn",
		AgentID: agentID, Trigger: "spawn", TargetEpoch: 41, TargetGeneration: 1,
		TargetPhase: runtimemanager.AgentLifecycleRegistered, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStopped, Agent: &runtimemanager.PersistedAgent{
			Config: runtimeactors.AgentConfig{ID: agentID, Type: "sonnet", Role: "worker", Mode: "global", Model: "regular", Config: []byte(`{"system_prompt":"x"}`)},
			Status: "active", HiredBy: "test", StartedAt: now,
		}, Now: now,
	})
	if err != nil {
		t.Fatalf("spawn provider-head fence agent: %v", err)
	}
	started, err := store.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "start", RequestHash: "provider-head-fence-start",
		AgentID: agentID, Trigger: "start", ExpectedEpoch: spawned.RuntimeEpoch, ExpectedGeneration: spawned.Generation, ExpectedPhase: spawned.Phase,
		TargetEpoch: spawned.RuntimeEpoch, TargetGeneration: spawned.Generation + 1, TargetPhase: runtimemanager.AgentLifecycleRunning,
		ConfigRevision: "revision-1", RunMode: runtimemanager.AgentRunModeStandard, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("start provider-head fence agent: %v", err)
	}
	var registry runtimesessions.Registry
	if sqlite {
		registry = store.(*SQLiteRuntimeStore)
	} else {
		registry = runtimesessions.NewPostgresRegistry(db, time.Minute)
	}
	lease, err := registry.Acquire(ctx, agentID, runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "provider-head-fence-owner", "global")
	if err != nil {
		t.Fatalf("acquire provider-head fence lease: %v", err)
	}
	controller := runtimeeffects.NewController(store)
	activeCtx := runtimeeffects.WithController(runtimeeffects.WithLifecycleToken(ctx, runtimeeffects.LifecycleToken{
		RuntimeEpoch: started.RuntimeEpoch, AgentID: agentID, Generation: started.Generation,
	}), controller)
	activeCtx = runtimeeffects.WithLogicalOperationIdentity(activeCtx, "provider-head-superseded-commit")
	attempt, err := runtimeeffects.Begin(activeCtx, "claude_cli", []byte("superseded-head"), nil)
	if err != nil {
		t.Fatalf("authorize superseded provider-head attempt: %v", err)
	}
	if err := attempt.MarkLaunched(activeCtx); err != nil {
		t.Fatalf("launch superseded provider-head attempt: %v", err)
	}
	restarted, err := store.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "restart", RequestHash: "provider-head-fence-restart",
		AgentID: agentID, Trigger: "restart", ExpectedEpoch: started.RuntimeEpoch, ExpectedGeneration: started.Generation, ExpectedPhase: started.Phase,
		TargetEpoch: started.RuntimeEpoch, TargetGeneration: started.Generation + 1, TargetPhase: runtimemanager.AgentLifecycleRunning,
		ConfigRevision: "revision-1", RunMode: runtimemanager.AgentRunModeStandard, Now: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("supersede provider-head attempt: %v", err)
	}
	if err := attempt.SucceedAndPromoteProviderHead(activeCtx, runtimeeffects.ProviderHeadSettlement{
		Settlement: runtimeeffects.Settlement{Evidence: map[string]any{"provider_session_id": attempt.Attempt().AttemptID}},
		AgentID:    agentID, RuntimeMode: runtimesessions.RuntimeModeSession.String(), SessionID: lease.SessionID,
		ScopeKey: lease.ScopeKey, LockOwner: lease.LockOwner, NewProviderHead: attempt.Attempt().AttemptID,
	}); err == nil {
		t.Fatal("superseded provider-head settlement succeeded")
	}
	requireExternalAttemptState(t, db, sqlite, attempt.Attempt().AttemptID, runtimeeffects.StateLaunched)
	requireProviderHead(t, db, sqlite, lease.SessionID, "")
	supersededFailure := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassSupersededGeneration, "superseded_generation", "test", "settle_provider_head", nil), "test", "settle_provider_head")
	if err := attempt.Settle(activeCtx, runtimeeffects.StateOutcomeUncertain, &supersededFailure.Failure, nil); err != nil {
		t.Fatalf("settle superseded provider-head attempt: %v", err)
	}

	restartedCtx := runtimeeffects.WithController(runtimeeffects.WithLifecycleToken(ctx, runtimeeffects.LifecycleToken{
		RuntimeEpoch: restarted.RuntimeEpoch, AgentID: agentID, Generation: restarted.Generation,
	}), controller)
	restartedCtx = runtimeeffects.WithLogicalOperationIdentity(restartedCtx, "provider-head-expired-lease")
	expiredAttempt, err := runtimeeffects.Begin(restartedCtx, "claude_cli", []byte("expired-lease-head"), nil)
	if err != nil {
		t.Fatalf("authorize expired-lease provider-head attempt: %v", err)
	}
	if err := expiredAttempt.MarkLaunched(restartedCtx); err != nil {
		t.Fatalf("launch expired-lease provider-head attempt: %v", err)
	}
	var expireErr error
	if sqlite {
		_, expireErr = db.ExecContext(ctx, `UPDATE agent_sessions SET lease_expires_at=? WHERE session_id=?`, now.Add(-time.Minute), lease.SessionID)
	} else {
		_, expireErr = db.ExecContext(ctx, `UPDATE agent_sessions SET lease_expires_at=$1 WHERE session_id=$2::uuid`, now.Add(-time.Minute), lease.SessionID)
	}
	if expireErr != nil {
		t.Fatalf("expire provider-head lease: %v", expireErr)
	}
	if err := expiredAttempt.SucceedAndPromoteProviderHead(restartedCtx, runtimeeffects.ProviderHeadSettlement{
		Settlement: runtimeeffects.Settlement{Evidence: map[string]any{"provider_session_id": expiredAttempt.Attempt().AttemptID}},
		AgentID:    agentID, RuntimeMode: runtimesessions.RuntimeModeSession.String(), SessionID: lease.SessionID,
		ScopeKey: lease.ScopeKey, LockOwner: lease.LockOwner, NewProviderHead: expiredAttempt.Attempt().AttemptID,
	}); err == nil {
		t.Fatal("expired-lease provider-head settlement succeeded")
	}
	requireExternalAttemptState(t, db, sqlite, expiredAttempt.Attempt().AttemptID, runtimeeffects.StateLaunched)
	requireProviderHead(t, db, sqlite, lease.SessionID, "")
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
	activeCtx = runtimeeffects.WithLogicalOperationIdentity(activeCtx, "effect-authority-primary")
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
	claudeRetryCtx := runtimeeffects.WithLogicalOperationIdentity(activeCtx, "claude-prelaunch-retry")
	claudeFirst, err := runtimeeffects.Begin(claudeRetryCtx, "claude_cli", []byte("stable-request"), nil)
	if err != nil {
		t.Fatalf("authorize first claude attempt: %v", err)
	}
	if err := claudeFirst.MarkLaunched(claudeRetryCtx); err != nil {
		t.Fatalf("mark first claude attempt launched before exact rejection: %v", err)
	}
	prelaunchFailure := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "claude_cli_process_start_failed", "test", "start", map[string]any{"launch_rejected": true}), "test", "start")
	if err := claudeFirst.Settle(claudeRetryCtx, runtimeeffects.StateTerminalFailure, &prelaunchFailure.Failure, map[string]any{"launch_rejected": true}); err != nil {
		t.Fatalf("settle first claude attempt prelaunch: %v", err)
	}
	claudeSecond, err := runtimeeffects.Begin(claudeRetryCtx, "claude_cli", []byte("stable-request"), nil)
	if err != nil {
		t.Fatalf("authorize second claude attempt: %v", err)
	}
	if claudeSecond.Attempt().Ordinal != 2 || claudeSecond.Attempt().AttemptID == claudeFirst.Attempt().AttemptID {
		t.Fatalf("second claude attempt = %#v, want fresh ordinal two", claudeSecond.Attempt())
	}
	if err := claudeSecond.MarkLaunched(claudeRetryCtx); err != nil {
		t.Fatalf("mark second claude attempt launched: %v", err)
	}
	uncertainClaude := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "claude_cli_attempt_outcome_unconfirmed", "test", "wait", nil), "test", "wait")
	if err := claudeSecond.Settle(claudeRetryCtx, runtimeeffects.StateOutcomeUncertain, &uncertainClaude.Failure, nil); err != nil {
		t.Fatalf("settle second claude attempt uncertain: %v", err)
	}
	if _, err := runtimeeffects.Begin(claudeRetryCtx, "claude_cli", []byte("stable-request"), nil); err == nil {
		t.Fatal("postlaunch uncertain claude attempt was redispatched")
	}
	nonRetryCtx := runtimeeffects.WithLogicalOperationIdentity(activeCtx, "claude-nonretryable-prelaunch")
	nonRetryAttempt, err := runtimeeffects.Begin(nonRetryCtx, "claude_cli", []byte("stable-nonretry-request"), nil)
	if err != nil {
		t.Fatalf("authorize non-retryable claude attempt: %v", err)
	}
	nonRetryFailure := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassAuthenticationNeeded, "provider_credential_missing", "test", "prelaunch", nil), "test", "prelaunch")
	if err := nonRetryAttempt.Settle(nonRetryCtx, runtimeeffects.StateTerminalFailure, &nonRetryFailure.Failure, map[string]any{"prelaunch": true}); err != nil {
		t.Fatalf("settle non-retryable claude attempt: %v", err)
	}
	if _, err := runtimeeffects.Begin(nonRetryCtx, "claude_cli", []byte("stable-nonretry-request"), nil); err == nil {
		t.Fatal("non-retryable prelaunch claude attempt admitted another ordinal")
	}
	var registry runtimesessions.Registry
	if sqlite {
		registry = store.(*SQLiteRuntimeStore)
	} else {
		registry = runtimesessions.NewPostgresRegistry(db, time.Minute)
	}
	lease, err := registry.Acquire(ctx, started.AgentID, runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, "provider-head-owner", "global")
	if err != nil {
		t.Fatalf("acquire provider-head session: %v", err)
	}
	headCtx := runtimeeffects.WithLogicalOperationIdentity(activeCtx, "claude-provider-head")
	headAttempt, err := runtimeeffects.Begin(headCtx, "claude_cli", []byte("head-request"), nil)
	if err != nil {
		t.Fatalf("authorize provider-head attempt: %v", err)
	}
	if err := headAttempt.MarkLaunched(headCtx); err != nil {
		t.Fatalf("mark provider-head attempt launched: %v", err)
	}
	if err := headAttempt.SucceedAndPromoteProviderHead(headCtx, runtimeeffects.ProviderHeadSettlement{
		Settlement: runtimeeffects.Settlement{Evidence: map[string]any{"provider_session_id": headAttempt.Attempt().AttemptID}},
		AgentID:    started.AgentID, RuntimeMode: runtimesessions.RuntimeModeSession.String(), SessionID: lease.SessionID,
		ScopeKey: lease.ScopeKey, LockOwner: lease.LockOwner, NewProviderHead: headAttempt.Attempt().AttemptID,
	}); err != nil {
		t.Fatalf("settle provider head: %v", err)
	}
	refreshed, err := registry.Acquire(ctx, started.AgentID, runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, lease.LockOwner, lease.ScopeKey)
	if err != nil {
		t.Fatalf("reload provider-head session: %v", err)
	}
	if refreshed.ProviderSessionID != headAttempt.Attempt().AttemptID {
		t.Fatalf("provider head = %q, want %q", refreshed.ProviderSessionID, headAttempt.Attempt().AttemptID)
	}
	requireExternalAttemptState(t, db, sqlite, headAttempt.Attempt().AttemptID, runtimeeffects.StateSettled)
	conflictCtx := runtimeeffects.WithLogicalOperationIdentity(activeCtx, "claude-provider-head-conflict")
	conflictAttempt, err := runtimeeffects.Begin(conflictCtx, "claude_cli", []byte("head-conflict-request"), nil)
	if err != nil {
		t.Fatalf("authorize provider-head conflict attempt: %v", err)
	}
	if err := conflictAttempt.MarkLaunched(conflictCtx); err != nil {
		t.Fatalf("mark provider-head conflict attempt launched: %v", err)
	}
	if err := conflictAttempt.SucceedAndPromoteProviderHead(conflictCtx, runtimeeffects.ProviderHeadSettlement{
		Settlement: runtimeeffects.Settlement{Evidence: map[string]any{"provider_session_id": conflictAttempt.Attempt().AttemptID}},
		AgentID:    started.AgentID, RuntimeMode: runtimesessions.RuntimeModeSession.String(), SessionID: lease.SessionID,
		ScopeKey: lease.ScopeKey, LockOwner: lease.LockOwner,
		ExpectedProviderHead: "stale-provider-head", NewProviderHead: conflictAttempt.Attempt().AttemptID,
	}); err == nil {
		t.Fatal("provider-head conflict settlement succeeded")
	}
	refreshed, err = registry.Acquire(ctx, started.AgentID, runtimesessions.RuntimeModeSession, runtimesessions.SessionScopeGlobal, lease.LockOwner, lease.ScopeKey)
	if err != nil {
		t.Fatalf("reload provider-head session after conflict: %v", err)
	}
	if refreshed.ProviderSessionID != headAttempt.Attempt().AttemptID {
		t.Fatalf("provider head after conflict = %q, want unchanged %q", refreshed.ProviderSessionID, headAttempt.Attempt().AttemptID)
	}
	requireExternalAttemptState(t, db, sqlite, conflictAttempt.Attempt().AttemptID, runtimeeffects.StateLaunched)
	conflictFailure := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassLifecycleConflict, "provider_head_cas_conflict", "test", "settle_provider_head", nil), "test", "settle_provider_head")
	if err := conflictAttempt.Settle(conflictCtx, runtimeeffects.StateOutcomeUncertain, &conflictFailure.Failure, nil); err != nil {
		t.Fatalf("settle provider-head conflict attempt uncertain: %v", err)
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

func requireExternalOperationState(t *testing.T, db *sql.DB, sqlite bool, operationID string, want runtimeeffects.State) {
	t.Helper()
	placeholder := "?"
	if !sqlite {
		placeholder = "$1"
	}
	var state string
	if err := db.QueryRow("SELECT state FROM agent_external_effect_operations WHERE operation_id = "+placeholder, operationID).Scan(&state); err != nil {
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
	if err := db.QueryRow("SELECT state FROM agent_external_effect_attempts WHERE attempt_id = "+placeholder, attemptID).Scan(&state); err != nil {
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
