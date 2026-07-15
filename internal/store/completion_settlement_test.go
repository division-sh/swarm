package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type completionSettlementTestStore interface {
	runtimeeffects.Store
	runtimeeffects.CompletionStore
	runtimeeffects.CompletionHeartbeatStore
	runtimeeffects.RecoveryStore
}

type completionSettlementFixture struct {
	store       completionSettlementTestStore
	db          *sql.DB
	sqlite      bool
	authority   runtimeeffects.Authority
	context     context.Context
	sessionID   string
	agentID     string
	leaseHolder string
}

func TestCompletionProviderHeadSettlementSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionProviderHeadSettlement(t, newCompletionSettlementFixture(t, store, store.DB, true))
}

func TestCompletionProviderHeadSettlementPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionProviderHeadSettlement(t, newCompletionSettlementFixture(t, &PostgresStore{DB: db}, db, false))
}

func proveCompletionProviderHeadSettlement(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "provider-head:success")
	handle := beginObservedCompletionForSettlementTest(t, ctx, "claude_cli", "success")
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "claude_cli", "provider-head-current", "provider-head-next")
	if err := handle.SettleCompletion(ctx, settlement); err != nil {
		t.Fatalf("settle completion with provider head: %v", err)
	}
	requireProviderHead(t, fixture.db, fixture.sqlite, fixture.sessionID, "provider-head-next")
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateSettled)
	requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateSettled, 1, 0)
}

func TestCompletionProviderHeadConflictCommitsUncertaintySQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionProviderHeadConflictCommitsUncertainty(t, newCompletionSettlementFixture(t, store, store.DB, true))
}

func TestCompletionProviderHeadConflictCommitsUncertaintyPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionProviderHeadConflictCommitsUncertainty(t, newCompletionSettlementFixture(t, &PostgresStore{DB: db}, db, false))
}

func proveCompletionProviderHeadConflictCommitsUncertainty(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "provider-head:conflict")
	handle := beginObservedCompletionForSettlementTest(t, ctx, "claude_cli", "conflict")
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "claude_cli", "stale-provider-head", "provider-head-next")
	err := handle.SettleCompletion(ctx, settlement)
	if err == nil {
		t.Fatal("provider-head conflict returned nil")
	}
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Detail.Code != "provider_head_cas_conflict" {
		t.Fatalf("provider-head conflict error=%v, want original provider_head_cas_conflict", err)
	}
	requireProviderHead(t, fixture.db, fixture.sqlite, fixture.sessionID, "provider-head-current")
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
	requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateOutcomeUncertain, 1, 0)

	query := `SELECT COALESCE(json_extract(failure, '$.detail.code'), '') FROM agent_turns WHERE completion_attempt_id=?`
	if !fixture.sqlite {
		query = `SELECT COALESCE(failure->'detail'->>'code', '') FROM agent_turns WHERE completion_attempt_id=$1::uuid`
	}
	var code string
	if err := fixture.db.QueryRow(query, handle.Attempt().AttemptID).Scan(&code); err != nil || code != "provider_head_cas_conflict" {
		t.Fatalf("completion turn failure code=%q err=%v, want provider_head_cas_conflict", code, err)
	}
}

func TestCompletionProviderHeadStaleAuthorityCannotSettleSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionProviderHeadStaleAuthorityCannotSettle(t, newCompletionSettlementFixture(t, store, store.DB, true))
}

func TestCompletionProviderHeadStaleAuthorityCannotSettlePostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionProviderHeadStaleAuthorityCannotSettle(t, newCompletionSettlementFixture(t, &PostgresStore{DB: db}, db, false))
}

func TestCompletionPrelaunchFailureDoesNotSpendSQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionPrelaunchFailureDoesNotSpend(t, newCompletionSettlementFixture(t, store, store.DB, true))
}

func TestCompletionPrelaunchFailureDoesNotSpendPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionPrelaunchFailureDoesNotSpend(t, newCompletionSettlementFixture(t, &PostgresStore{DB: db}, db, false))
}

func proveCompletionPrelaunchFailureDoesNotSpend(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "completion-prelaunch-failure")
	ctx = withManagedCompletionTestSurface(t, ctx, fixture.authority, "claude_cli")
	handle, err := runtimeeffects.BeginCompletion(ctx, "claude_cli", []byte("prelaunch"), nil)
	if err != nil {
		t.Fatalf("authorize prelaunch completion: %v", err)
	}
	failure := runtimefailures.FromError(context.Canceled, "completion-test", "launch_rejected")
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "claude_cli", "", "")
	settlement.Settlement = runtimeeffects.Settlement{State: runtimeeffects.StateTerminalFailure, Failure: &failure.Failure}
	settlement.Usage = runtimeeffects.CompletionUsage{ResolvedModel: "claude-test", Exactness: runtimeeffects.CompletionUsageUnavailable}
	settlement.AgentTurn.Failure = &failure.Failure
	settlement.ProviderHead = nil
	if err := handle.SettleCompletion(ctx, settlement); err != nil {
		t.Fatalf("settle prelaunch completion: %v", err)
	}
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateTerminalFailure)
	requireCompletionRecoveryRows(t, fixture, handle.Attempt().AttemptID, 1, 0, 0)
}

func TestCompletionRecoveryPreservesLiveOrdinaryAuthoritySQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionRecoveryPreservesLiveOrdinaryAuthority(t, newCompletionSettlementFixture(t, store, store.DB, true))
}

func TestCompletionRecoveryPreservesLiveOrdinaryAuthorityPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionRecoveryPreservesLiveOrdinaryAuthority(t, newCompletionSettlementFixture(t, &PostgresStore{DB: db}, db, false))
}

func TestCompletionAttemptHeartbeatFencesRecoverySQLite(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveCompletionAttemptHeartbeatFencesRecovery(t, newCompletionSettlementFixture(t, store, store.DB, true))
}

func TestCompletionAttemptHeartbeatFencesRecoveryPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveCompletionAttemptHeartbeatFencesRecovery(t, newCompletionSettlementFixture(t, &PostgresStore{DB: db}, db, false))
}

func proveCompletionAttemptHeartbeatFencesRecovery(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "completion-heartbeat")
	ctx = withManagedCompletionTestSurface(t, ctx, fixture.authority, "anthropic_api")
	handle, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte("heartbeat"), nil)
	if err != nil {
		t.Fatalf("authorize heartbeat completion: %v", err)
	}
	before := time.Now().UTC().Add(-time.Minute)
	setCompletionAttemptLease(t, fixture, handle.Attempt().AttemptID, before)
	if err := handle.Heartbeat(ctx, 2*time.Minute); err != nil {
		t.Fatalf("heartbeat authorized completion: %v", err)
	}
	after := completionAttemptLease(t, fixture, handle.Attempt().AttemptID)
	if !after.After(time.Now().UTC().Add(time.Minute)) {
		t.Fatalf("heartbeat lease=%s, want more than one minute of live authority", after)
	}
	if err := handle.MarkLaunched(ctx); err != nil {
		t.Fatalf("launch heartbeat completion: %v", err)
	}
	if err := handle.Heartbeat(ctx, 2*time.Minute); err != nil {
		t.Fatalf("heartbeat launched completion: %v", err)
	}
	stale := handle.Attempt()
	stale.Authority.FenceGeneration++
	if err := fixture.store.HeartbeatCompletionAttempt(ctx, stale, time.Now().UTC(), 2*time.Minute); err == nil {
		t.Fatal("stale completion fence renewed the attempt lease")
	}
	summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatalf("reconcile heartbeating completion: %v", err)
	}
	if summary != (runtimeeffects.RecoverySummary{}) {
		t.Fatalf("heartbeating completion recovery summary=%+v, want empty", summary)
	}
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateLaunched)
}

func setCompletionAttemptLease(t *testing.T, fixture completionSettlementFixture, attemptID string, lease time.Time) {
	t.Helper()
	query := `UPDATE runtime_external_effect_attempts SET lease_expires_at=? WHERE attempt_id=?`
	if !fixture.sqlite {
		query = `UPDATE runtime_external_effect_attempts SET lease_expires_at=$1 WHERE attempt_id=$2::uuid`
	}
	if _, err := fixture.db.Exec(query, lease.UTC(), attemptID); err != nil {
		t.Fatalf("set completion attempt lease: %v", err)
	}
}

func completionAttemptLease(t *testing.T, fixture completionSettlementFixture, attemptID string) time.Time {
	t.Helper()
	if fixture.sqlite {
		var lease conversationForkTimeValue
		if err := fixture.db.QueryRow(`SELECT lease_expires_at FROM runtime_external_effect_attempts WHERE attempt_id=?`, attemptID).Scan(&lease); err != nil {
			t.Fatalf("load sqlite completion attempt lease: %v", err)
		}
		if !lease.Valid {
			t.Fatal("sqlite completion attempt lease is null")
		}
		return lease.Time.UTC()
	}
	var lease time.Time
	if err := fixture.db.QueryRow(`SELECT lease_expires_at FROM runtime_external_effect_attempts WHERE attempt_id=$1::uuid`, attemptID).Scan(&lease); err != nil {
		t.Fatalf("load completion attempt lease: %v", err)
	}
	return lease.UTC()
}

func proveCompletionRecoveryPreservesLiveOrdinaryAuthority(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "ordinary-recovery:authorized")
	ctx = withManagedCompletionTestSurface(t, ctx, fixture.authority, "anthropic_api")
	authorized, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte("authorized"), nil)
	if err != nil {
		t.Fatalf("authorize live completion: %v", err)
	}
	now := time.Now().UTC()
	summary, err := fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), now)
	if err != nil {
		t.Fatalf("reconcile live completion: %v", err)
	}
	if summary != (runtimeeffects.RecoverySummary{}) {
		t.Fatalf("live completion recovery summary=%+v, want empty", summary)
	}
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, authorized.Attempt().AttemptID, runtimeeffects.StateAuthorized)
	requireCompletionRecoveryRows(t, fixture, authorized.Attempt().AttemptID, 0, 0, 1)

	setCompletionFixtureGeneration(t, fixture, 2)
	summary, err = fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), now.Add(time.Second))
	if err != nil {
		t.Fatalf("reconcile fenced prelaunch completion: %v", err)
	}
	if summary.PrelaunchTerminal != 1 || summary.OutcomeUncertain != 0 {
		t.Fatalf("fenced prelaunch recovery summary=%+v, want 1/0", summary)
	}
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, authorized.Attempt().AttemptID, runtimeeffects.StateTerminalFailure)
	requireCompletionRecoveryRows(t, fixture, authorized.Attempt().AttemptID, 1, 0, 0)

	setCompletionFixtureGeneration(t, fixture, 1)
	secondAuthority := fixture.authority
	secondAuthority.Target.ID = uuid.NewString()
	ctx = runtimeeffects.WithLogicalOperationIdentity(runtimeeffects.WithAuthority(fixture.context, secondAuthority), "ordinary-recovery:launched")
	ctx = withManagedCompletionTestSurface(t, ctx, secondAuthority, "anthropic_api")
	launched, err := runtimeeffects.BeginCompletion(ctx, "anthropic_api", []byte("launched"), nil)
	if err != nil {
		t.Fatalf("authorize launched completion: %v", err)
	}
	if err := launched.MarkLaunched(ctx); err != nil {
		t.Fatalf("mark completion launched: %v", err)
	}
	setCompletionFixtureGeneration(t, fixture, 2)
	summary, err = fixture.store.ReconcileExternalEffectAttempts(testAuthorActivityContext(), now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("reconcile fenced launched completion: %v", err)
	}
	if summary.PrelaunchTerminal != 0 || summary.OutcomeUncertain != 1 {
		t.Fatalf("fenced launched recovery summary=%+v, want 0/1", summary)
	}
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, launched.Attempt().AttemptID, runtimeeffects.StateOutcomeUncertain)
	requireCompletionRecoveryRows(t, fixture, launched.Attempt().AttemptID, 1, 1, 0)
}

func proveCompletionProviderHeadStaleAuthorityCannotSettle(t *testing.T, fixture completionSettlementFixture) {
	t.Helper()
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, "provider-head:stale-authority")
	handle := beginObservedCompletionForSettlementTest(t, ctx, "claude_cli", "stale-authority")
	settlement := completionSettlementForTest(t, handle.Attempt().Authority.Target, fixture, "claude_cli", "provider-head-current", "provider-head-next")
	stale := handle.Attempt()
	stale.Authority.Normal.Generation++
	stale.Authority.FenceGeneration++
	if _, err := fixture.store.SettleCompletion(ctx, stale, settlement); err == nil {
		t.Fatal("stale completion authority settled provider head")
	}
	requireProviderHead(t, fixture.db, fixture.sqlite, fixture.sessionID, "provider-head-current")
	requireExternalAttemptState(t, fixture.db, fixture.sqlite, handle.Attempt().AttemptID, runtimeeffects.StateResponseObserved)
	requireCompletionSettlementRows(t, fixture, handle.Attempt().AttemptID, settlement.AgentTurn.TurnID, runtimeeffects.StateResponseObserved, 0, 1)
}

func newCompletionSettlementFixture(t *testing.T, store completionSettlementTestStore, db *sql.DB, sqlite bool) completionSettlementFixture {
	t.Helper()
	ctx := testAuthorActivityContext()
	now := time.Now().UTC()
	agentID := "completion-settlement-agent"
	sessionID := uuid.NewString()
	runID := uuid.NewString()
	flowInstance := "global"
	leaseHolder := "completion-worker"
	if sqlite {
		if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id,status,started_at) VALUES (?,'running',?)`, runID, now); err != nil {
			t.Fatalf("seed completion run: %v", err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO agents (agent_id,flow_instance,role,model,llm_backend,memory_enabled,memory_source,status,lifecycle_runtime_epoch,lifecycle_generation,lifecycle_phase,created_at) VALUES (?,?,'worker','regular','claude_cli',1,'authored','active',1,1,'running',?)`, agentID, flowInstance, now); err != nil {
			t.Fatalf("seed completion agent: %v", err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO agent_sessions (session_id,run_id,agent_id,flow_instance,memory_enabled,memory_source,conversation,turn_count,runtime_state,lease_holder,lease_expires_at,status,created_at,updated_at) VALUES (?,?,?,?,1,'authored','[]',0,?,?,?,'active',?,?)`, sessionID, runID, agentID, flowInstance, `{"provider_session_id":"provider-head-current"}`, leaseHolder, now.Add(10*time.Minute), now, now); err != nil {
			t.Fatalf("seed completion session: %v", err)
		}
	} else {
		if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id,status,started_at) VALUES ($1::uuid,'running',$2)`, runID, now); err != nil {
			t.Fatalf("seed completion run: %v", err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO agents (agent_id,flow_instance,role,model,llm_backend,memory_enabled,memory_source,status,lifecycle_runtime_epoch,lifecycle_generation,lifecycle_phase,created_at) VALUES ($1,$2,'worker','regular','claude_cli',TRUE,'authored','active',1,1,'running',$3)`, agentID, flowInstance, now); err != nil {
			t.Fatalf("seed completion agent: %v", err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO agent_sessions (session_id,run_id,agent_id,flow_instance,memory_enabled,memory_source,conversation,turn_count,runtime_state,lease_holder,lease_expires_at,status,created_at,updated_at) VALUES ($1::uuid,$2::uuid,$3,$4,TRUE,'authored','[]'::jsonb,0,$5::jsonb,$6,$7,'active',$8,$8)`, sessionID, runID, agentID, flowInstance, `{"provider_session_id":"provider-head-current"}`, leaseHolder, now.Add(10*time.Minute), now); err != nil {
			t.Fatalf("seed completion session: %v", err)
		}
	}
	token := runtimeeffects.LifecycleToken{RuntimeEpoch: 1, AgentID: agentID, Generation: 1}
	authority := runtimeeffects.NormalAgentAuthority(token, leaseHolder, now.Add(10*time.Minute))
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), AgentID: agentID,
		RunID: runID, SessionID: sessionID, Memory: agentmemory.Authored(true), FlowInstance: flowInstance,
	}
	authority.BudgetScopes = []runtimeeffects.BudgetAdmissionScope{{Kind: "system", CapUSD: 1}}
	completionCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), runtimeeffects.NewController(store))
	return completionSettlementFixture{
		store: store, db: db, sqlite: sqlite, authority: authority, context: completionCtx,
		sessionID: sessionID, agentID: agentID, leaseHolder: leaseHolder,
	}
}

func beginObservedCompletionForSettlementTest(t *testing.T, ctx context.Context, adapter, request string) *runtimeeffects.Handle {
	t.Helper()
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok {
		t.Fatal("managed completion test authority is missing")
	}
	ctx = withManagedCompletionTestSurface(t, ctx, authority, adapter)
	handle, err := runtimeeffects.BeginCompletion(ctx, adapter, []byte(request), nil)
	if err != nil {
		t.Fatalf("authorize completion: %v", err)
	}
	if err := handle.MarkLaunched(ctx); err != nil {
		t.Fatalf("launch completion: %v", err)
	}
	if err := handle.MarkResponseObserved(ctx, map[string]any{"response_fingerprint": request}); err != nil {
		t.Fatalf("observe completion response: %v", err)
	}
	return handle
}

func completionSettlementForTest(t testing.TB, target runtimeeffects.UsageTarget, fixture completionSettlementFixture, adapter, expectedHead, newHead string) runtimeeffects.CompletionSettlement {
	t.Helper()
	input, output := int64(12), int64(4)
	settlement := runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: runtimeeffects.StateSettled, Evidence: map[string]any{"provider_result": true}},
		Usage: runtimeeffects.CompletionUsage{
			ResolvedModel: "claude-test", Exactness: runtimeeffects.CompletionUsageExact,
			InputTokens: &input, OutputTokens: &output,
		},
		AgentTurn: &runtimeeffects.CompletionAgentTurn{
			TurnID: target.ID, AgentID: target.AgentID, SessionID: target.SessionID,
			RunID: target.RunID, Memory: target.Memory, FlowInstance: target.FlowInstance, ParseOK: true,
		},
		Spend: runtimeeffects.CompletionSpend{
			FlowInstance: target.FlowInstance, AgentID: target.AgentID, Model: "regular", ModelAlias: "regular",
			BackendProfile: "test", Provider: "anthropic", Transport: "process", ResolvedModel: "claude-test",
			CostUSD: 0.25, InvocationType: "agent_turn",
		},
		ProviderHead: &runtimeeffects.CompletionProviderHead{
			Identity:  agentmemory.Identity{RunID: target.RunID, AgentID: fixture.agentID, FlowInstance: target.FlowInstance},
			SessionID: fixture.sessionID, LockOwner: fixture.leaseHolder,
			ExpectedProviderHead: expectedHead, NewProviderHead: newHead,
		},
		Now: time.Now().UTC(),
	}
	authority := fixture.authority
	authority.Target = target
	applyManagedCompletionTestSurface(t, settlement.AgentTurn, authority, adapter)
	return settlement
}

func requireCompletionSettlementRows(t *testing.T, fixture completionSettlementFixture, attemptID, turnID string, wantState runtimeeffects.State, wantRows, wantReservations int) {
	t.Helper()
	placeholder := "?"
	turnQuery := `SELECT COUNT(*) FROM agent_turns WHERE turn_id=? AND completion_attempt_id=?`
	if !fixture.sqlite {
		placeholder = "$1::uuid"
		turnQuery = `SELECT COUNT(*) FROM agent_turns WHERE turn_id=$1::uuid AND completion_attempt_id=$2::uuid`
	}
	var turns, spend, reservations int
	if err := fixture.db.QueryRow(turnQuery, turnID, attemptID).Scan(&turns); err != nil {
		t.Fatalf("count completion turns: %v", err)
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM spend_ledger WHERE external_effect_attempt_id=`+placeholder, attemptID).Scan(&spend); err != nil {
		t.Fatalf("count completion spend: %v", err)
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM runtime_effect_budget_reservations WHERE attempt_id=`+placeholder, attemptID).Scan(&reservations); err != nil {
		t.Fatalf("count completion reservations: %v", err)
	}
	if turns != wantRows || spend != wantRows || reservations != wantReservations {
		t.Fatalf("completion rows turns=%d spend=%d reservations=%d, want %d/%d/%d after %s", turns, spend, reservations, wantRows, wantRows, wantReservations, wantState)
	}
}

func setCompletionFixtureGeneration(t *testing.T, fixture completionSettlementFixture, generation int) {
	t.Helper()
	query := `UPDATE agents SET lifecycle_generation=? WHERE agent_id=?`
	if !fixture.sqlite {
		query = `UPDATE agents SET lifecycle_generation=$1 WHERE agent_id=$2`
	}
	if _, err := fixture.db.Exec(query, generation, fixture.agentID); err != nil {
		t.Fatalf("set completion fixture generation: %v", err)
	}
}

func requireCompletionRecoveryRows(t *testing.T, fixture completionSettlementFixture, attemptID string, wantTurns, wantSpend, wantReservations int) {
	t.Helper()
	placeholder := "?"
	if !fixture.sqlite {
		placeholder = "$1::uuid"
	}
	var turns, spend, reservations int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM agent_turns WHERE completion_attempt_id=`+placeholder, attemptID).Scan(&turns); err != nil {
		t.Fatalf("count recovered completion turns: %v", err)
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM spend_ledger WHERE external_effect_attempt_id=`+placeholder, attemptID).Scan(&spend); err != nil {
		t.Fatalf("count recovered completion spend: %v", err)
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM runtime_effect_budget_reservations WHERE attempt_id=`+placeholder, attemptID).Scan(&reservations); err != nil {
		t.Fatalf("count recovered completion reservations: %v", err)
	}
	if turns != wantTurns || spend != wantSpend || reservations != wantReservations {
		t.Fatalf("completion recovery rows turns=%d spend=%d reservations=%d, want %d/%d/%d", turns, spend, reservations, wantTurns, wantSpend, wantReservations)
	}
}
