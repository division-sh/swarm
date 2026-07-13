package store

import (
	"context"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestCompletionAuthorityReviewFindingParity(t *testing.T) {
	for _, backend := range []struct {
		name   string
		sqlite bool
	}{
		{name: "sqlite", sqlite: true},
		{name: "postgres", sqlite: false},
	} {
		t.Run(backend.name, func(t *testing.T) {
			t.Run("selected current authority", func(t *testing.T) {
				proveSelectedCurrentAuthorityTransition(t, backend.sqlite)
			})
			t.Run("forkchat current authority", func(t *testing.T) {
				proveForkChatCurrentAuthorityTransition(t, backend.sqlite)
			})
			t.Run("claude retry generation", func(t *testing.T) {
				proveClaudeRetryGenerationAuthority(t, backend.sqlite)
			})
			t.Run("committed budget projection", func(t *testing.T) {
				proveCommittedCompletionBudgetProjection(t, backend.sqlite)
			})
		})
	}
}

func proveSelectedCurrentAuthorityTransition(t *testing.T, sqlite bool) {
	t.Helper()
	fixture := newSelectedReviewFixture(t, sqlite)
	ctx := context.Background()
	issued, err := fixture.store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
	if err != nil {
		t.Fatalf("issue selected authority: %v", err)
	}
	authority, err := fixture.store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "review-owner", time.Minute)
	if err != nil {
		t.Fatalf("claim selected authority: %v", err)
	}

	mutations := []struct {
		name   string
		mutate func(*runtimeeffects.Authority)
	}{
		{name: "fork", mutate: func(a *runtimeeffects.Authority) { a.SelectedFork.ForkRunID = uuid.NewString() }},
		{name: "generation", mutate: func(a *runtimeeffects.Authority) { a.SelectedFork.Generation++ }},
		{name: "admission", mutate: func(a *runtimeeffects.Authority) { a.SelectedFork.AdmissionFingerprint += ":stale" }},
		{name: "container", mutate: func(a *runtimeeffects.Authority) { a.SelectedFork.ContainerPlanFingerprint += ":stale" }},
		{name: "actors", mutate: func(a *runtimeeffects.Authority) { a.SelectedFork.ActorCensusFingerprint += ":stale" }},
		{name: "config", mutate: func(a *runtimeeffects.Authority) { a.SelectedFork.EffectiveConfigFingerprint += ":stale" }},
		{name: "owner", mutate: func(a *runtimeeffects.Authority) { a.ExecutionOwner += ":stale" }},
		{name: "fence", mutate: func(a *runtimeeffects.Authority) { a.FenceGeneration++ }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			stale := authority
			mutation.mutate(&stale)
			if err := fixture.store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, stale); err == nil {
				t.Fatalf("quiesce accepted stale %s coordinate", mutation.name)
			}
			requireSelectedReviewState(t, fixture, authority.ID, "running")
		})
	}

	setSelectedReviewAuthority(t, fixture, authority.ID, "running", time.Now().UTC().Add(-time.Minute))
	if err := fixture.store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err == nil {
		t.Fatal("quiesce accepted expired selected authority")
	}
	if err := fixture.store.HeartbeatRunForkSelectedContractRuntimeExecution(ctx, authority, time.Minute); err == nil {
		t.Fatal("heartbeat resurrected expired selected authority")
	}
	requireSelectedReviewState(t, fixture, authority.ID, "running")

	setSelectedReviewAuthority(t, fixture, authority.ID, "failed", time.Time{})
	if err := fixture.store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err == nil {
		t.Fatal("quiesce accepted recovered terminal selected authority")
	}
	setSelectedReviewAuthority(t, fixture, authority.ID, "running", time.Now().UTC().Add(time.Minute))

	providerAuthority := authority
	providerAuthority.Target = selectedAgentTurnTarget(fixture.forkRun)
	providerCtx := runtimeeffects.WithLogicalOperationIdentity(
		runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, providerAuthority), runtimeeffects.NewController(fixture.store)),
		"review:selected-live-attempt",
	)
	handle, err := runtimeeffects.BeginCompletion(providerCtx, "anthropic_api", []byte("review-selected"), nil)
	if err != nil {
		t.Fatalf("authorize selected live attempt: %v", err)
	}
	if err := fixture.store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err == nil {
		t.Fatal("quiesce accepted selected authority with a live attempt")
	}
	if err := handle.MarkLaunched(providerCtx); err != nil {
		t.Fatalf("launch selected attempt: %v", err)
	}
	if err := handle.MarkResponseObserved(providerCtx, map[string]any{"review": true}); err != nil {
		t.Fatalf("observe selected attempt: %v", err)
	}
	settleSelectedCompletionForTest(t, providerCtx, handle, providerAuthority.Target, time.Now().UTC())
	if err := fixture.store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
		t.Fatalf("quiesce exact current selected authority: %v", err)
	}
	requireSelectedReviewState(t, fixture, authority.ID, "quiesced")
}

func proveForkChatCurrentAuthorityTransition(t *testing.T, sqlite bool) {
	t.Helper()
	fixture := newForkChatCompletionAuthorityFixture(t, sqlite)
	prepared := prepareForkChatCompletionGroup(t, fixture, "review-expiry", "review expiry")
	providerCtx, handle := beginForkChatCompletionAttempt(t, fixture, prepared, 1, "review-expiry")
	launchAndObserveForkChatCompletion(t, providerCtx, handle, "review-expiry")
	settleForkChatCompletionAttempt(t, providerCtx, handle, prepared, runtimeeffects.StateSettled, nil, fixture.now.Add(2*time.Second))
	expireForkChatGroupLease(t, fixture, prepared.ForkTurnID)
	if err := fixture.store.HeartbeatOperatorConversationForkChat(context.Background(), prepared, time.Now().UTC()); err == nil {
		t.Fatal("forkchat heartbeat resurrected an expired group authority")
	}
	if _, err := fixture.store.RecordOperatorConversationForkChat(context.Background(), successfulForkChatRecord(prepared, "review expiry", time.Now().UTC())); err == nil {
		t.Fatal("forkchat success accepted an expired group authority")
	}
	requireForkChatGroupState(t, fixture, prepared.ForkTurnID, "executing", false)
	if err := fixture.store.FailOperatorConversationForkChat(context.Background(), ConversationForkChatFailureRequest{
		Prepared: prepared, Cause: context.DeadlineExceeded, OutcomeUncertain: true, Now: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("terminalize expired forkchat authority outcome-uncertain: %v", err)
	}
	requireForkChatGroupState(t, fixture, prepared.ForkTurnID, "outcome_uncertain", true)

	live := prepareForkChatCompletionGroup(t, fixture, "review-live-child", "review live child")
	firstCtx, first := beginForkChatCompletionAttempt(t, fixture, live, 1, "review-live-child-1")
	launchAndObserveForkChatCompletion(t, firstCtx, first, "review-live-child-1")
	settleForkChatCompletionAttempt(t, firstCtx, first, live, runtimeeffects.StateSettled, nil, time.Now().UTC())
	_, _ = beginForkChatCompletionAttempt(t, fixture, live, 2, "review-live-child-2")
	if _, err := fixture.store.RecordOperatorConversationForkChat(context.Background(), successfulForkChatRecord(live, "review live child", time.Now().UTC())); err == nil {
		t.Fatal("forkchat success accepted a live child attempt")
	}
	requireForkChatGroupState(t, fixture, live.ForkTurnID, "executing", false)
}

func proveClaudeRetryGenerationAuthority(t *testing.T, sqlite bool) {
	t.Helper()
	fixture := newCompletionReviewFixture(t, sqlite)
	logicalID := "review:claude-generation"
	ctx := runtimeeffects.WithLogicalOperationIdentity(fixture.context, logicalID)
	handle, err := runtimeeffects.BeginCompletion(ctx, "claude_cli", []byte("retry-generation"), nil)
	if err != nil {
		t.Fatalf("authorize generation-1 Claude attempt: %v", err)
	}
	failureErr := runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "claude_cli_process_start_failed", "review", "start", map[string]any{"launch_rejected": true})
	failure, _ := runtimefailures.EnvelopeFromError(failureErr)
	settlement := completionSettlementForTest(handle.Attempt().Authority.Target, fixture, "", "")
	settlement.ProviderHead = nil
	settlement.Settlement = runtimeeffects.Settlement{State: runtimeeffects.StateTerminalFailure, Failure: &failure, Evidence: map[string]any{"launch_rejected": true}}
	settlement.Usage = runtimeeffects.CompletionUsage{ResolvedModel: "claude-test", Exactness: runtimeeffects.CompletionUsageUnavailable}
	settlement.AgentTurn.Failure = &failure
	if err := handle.SettleCompletion(ctx, settlement); err != nil {
		t.Fatalf("settle generation-1 prelaunch failure: %v", err)
	}

	setCompletionFixtureGeneration(t, fixture, 2)
	nextAuthority := fixture.authority
	nextAuthority.Normal.Generation = 2
	nextAuthority.FenceGeneration = 2
	nextAuthority.Target.ID = uuid.NewString()
	nextCtx := runtimeeffects.WithLogicalOperationIdentity(
		runtimeeffects.WithController(runtimeeffects.WithAuthority(context.Background(), nextAuthority), runtimeeffects.NewController(fixture.store)),
		logicalID,
	)
	if _, err := runtimeeffects.BeginCompletion(nextCtx, "claude_cli", []byte("retry-generation"), nil); err == nil {
		t.Fatal("generation-2 authority retried a generation-1 operation")
	}
	if _, err := fixture.store.ReconcileExternalEffectAttempts(context.Background(), time.Now().UTC().Add(time.Minute)); err != nil {
		t.Fatalf("reconcile cross-generation Claude retry evidence: %v", err)
	}
	var attempts, generation int
	query := `SELECT COUNT(*), MIN(o.generation) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.operation_id=?`
	if !fixture.sqlite {
		query = `SELECT COUNT(*), MIN(o.generation) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.operation_id=$1::uuid`
	}
	if err := fixture.db.QueryRow(query, handle.Attempt().OperationID).Scan(&attempts, &generation); err != nil {
		t.Fatalf("read Claude retry operation: %v", err)
	}
	if attempts != 1 || generation != 1 {
		t.Fatalf("Claude retry attempts/generation=%d/%d, want 1/1", attempts, generation)
	}
}

type completionProjectionCapture struct {
	items []runtimeeffects.CompletionSpendProjection
}

func (c *completionProjectionCapture) ProjectCommittedCompletionSpend(_ context.Context, projection runtimeeffects.CompletionSpendProjection) {
	c.items = append(c.items, projection)
}

func proveCommittedCompletionBudgetProjection(t *testing.T, sqlite bool) {
	t.Helper()
	fixture := newCompletionReviewFixture(t, sqlite)
	projection := &completionProjectionCapture{}
	ctx := runtimeeffects.WithLogicalOperationIdentity(
		runtimeeffects.WithController(runtimeeffects.WithAuthority(context.Background(), fixture.authority), runtimeeffects.NewCompletionController(fixture.store, projection)),
		"review:budget-projection",
	)
	handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "budget-projection")
	settlement := completionSettlementForTest(handle.Attempt().Authority.Target, fixture, "", "")
	settlement.ProviderHead = nil
	if err := handle.SettleCompletion(ctx, settlement); err != nil {
		t.Fatalf("settle projected completion: %v", err)
	}
	if len(projection.items) != 1 || projection.items[0].AttemptID != handle.Attempt().AttemptID {
		t.Fatalf("completion projections=%#v, want exact committed attempt", projection.items)
	}
	if err := handle.SettleCompletion(ctx, settlement); err == nil {
		t.Fatal("duplicate completion settlement unexpectedly succeeded")
	}
	if len(projection.items) != 1 {
		t.Fatalf("duplicate settlement projected %d times, want once", len(projection.items))
	}
	var spend int
	query := `SELECT COUNT(*) FROM spend_ledger WHERE external_effect_attempt_id=?`
	if !fixture.sqlite {
		query = `SELECT COUNT(*) FROM spend_ledger WHERE external_effect_attempt_id=$1::uuid`
	}
	if err := fixture.db.QueryRow(query, handle.Attempt().AttemptID).Scan(&spend); err != nil || spend != 1 {
		t.Fatalf("completion spend rows=%d err=%v, want exactly one", spend, err)
	}
}

func newSelectedReviewFixture(t *testing.T, sqlite bool) selectedCompletionFixture {
	t.Helper()
	if sqlite {
		s := newBootstrappedSQLiteRuntimeStoreForTest(t)
		return newSelectedCompletionFixture(t, s, s.DB, true)
	}
	_, db, _ := testutil.StartPostgres(t)
	return newSelectedCompletionFixture(t, &PostgresStore{DB: db}, db, false)
}

func newCompletionReviewFixture(t *testing.T, sqlite bool) completionSettlementFixture {
	t.Helper()
	if sqlite {
		s := newBootstrappedSQLiteRuntimeStoreForTest(t)
		return newCompletionSettlementFixture(t, s, s.DB, true)
	}
	_, db, _ := testutil.StartPostgres(t)
	return newCompletionSettlementFixture(t, &PostgresStore{DB: db}, db, false)
}

func setSelectedReviewAuthority(t *testing.T, fixture selectedCompletionFixture, executionID, state string, lease time.Time) {
	t.Helper()
	var leaseArg any
	var terminalArg any
	if !lease.IsZero() {
		leaseArg = lease.UTC()
	}
	if state == "failed" {
		terminalArg = time.Now().UTC()
	}
	query := `UPDATE run_fork_selected_contract_runtime_executions SET state=?,lease_expires_at=?,terminal_at=? WHERE execution_id=?`
	if !fixture.sqlite {
		query = `UPDATE run_fork_selected_contract_runtime_executions SET state=$1,lease_expires_at=$2,terminal_at=$3 WHERE execution_id=$4::uuid`
	}
	if _, err := fixture.db.Exec(query, state, leaseArg, terminalArg, executionID); err != nil {
		t.Fatalf("set selected review authority: %v", err)
	}
}

func requireSelectedReviewState(t *testing.T, fixture selectedCompletionFixture, executionID, want string) {
	t.Helper()
	query := `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE execution_id=?`
	if !fixture.sqlite {
		query = `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1::uuid`
	}
	var state string
	if err := fixture.db.QueryRow(query, executionID).Scan(&state); err != nil || state != want {
		t.Fatalf("selected authority state=%q err=%v, want %q", state, err, want)
	}
}
