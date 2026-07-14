package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type selectedCompletionAuthorityStore interface {
	runtimeeffects.Store
	runtimeeffects.CompletionStore
	runtimeeffects.RecoveryStore
	IssueRunForkSelectedContractRuntimeExecution(context.Context, SelectedContractRuntimeExecutionIssueRequest) (SelectedContractRuntimeExecution, error)
	ClaimRunForkSelectedContractRuntimeExecution(context.Context, SelectedContractRuntimeExecution, string, time.Duration) (runtimeeffects.Authority, error)
	HeartbeatRunForkSelectedContractRuntimeExecution(context.Context, runtimeeffects.Authority, time.Duration) error
	QuiesceRunForkSelectedContractRuntimeExecution(context.Context, runtimeeffects.Authority) error
	CloseRunForkSelectedContractRuntimeExecution(context.Context, string) error
}

type selectedCompletionFixture struct {
	store     selectedCompletionAuthorityStore
	db        *sql.DB
	sqlite    bool
	sourceRun string
	forkRun   string
	eventID   string
	admission RunForkSelectedContractExecutionAdmission
	request   SelectedContractRuntimeExecutionIssueRequest
}

func TestSelectedForkCompletionAuthorityIssuanceConsumesExactAdmissionSQLite(t *testing.T) {
	s := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveSelectedForkCompletionAuthorityIssuance(t, newSelectedCompletionFixture(t, s, s.DB, true))
}

func TestSelectedForkCompletionAuthorityIssuanceConsumesExactAdmissionPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveSelectedForkCompletionAuthorityIssuance(t, newSelectedCompletionFixture(t, &PostgresStore{DB: db}, db, false))
}

func proveSelectedForkCompletionAuthorityIssuance(t *testing.T, fixture selectedCompletionFixture) {
	t.Helper()
	ctx := context.Background()

	invalidAdmissions := []struct {
		name   string
		mutate func(*RunForkSelectedContractExecutionAdmission)
	}{
		{name: "owner", mutate: func(a *RunForkSelectedContractExecutionAdmission) { a.Owner = "caller.local" }},
		{name: "future owner", mutate: func(a *RunForkSelectedContractExecutionAdmission) { a.FutureExecutionOwner = "caller.local" }},
		{name: "mutating", mutate: func(a *RunForkSelectedContractExecutionAdmission) { a.NonMutating = false }},
		{name: "already executable", mutate: func(a *RunForkSelectedContractExecutionAdmission) { a.ExecutionSupported = true }},
		{name: "binding owner", mutate: func(a *RunForkSelectedContractExecutionAdmission) { a.ContractBindingOwner = "caller.local" }},
		{name: "admission use", mutate: func(a *RunForkSelectedContractExecutionAdmission) {
			a.AdmissionUse = RunForkSelectedContractExecutionAdmissionUseEvidenceOnly
		}},
		{name: "durable source", mutate: func(a *RunForkSelectedContractExecutionAdmission) { a.SourceRunID = uuid.NewString() }},
		{name: "durable event", mutate: func(a *RunForkSelectedContractExecutionAdmission) { a.ForkEventID = uuid.NewString() }},
	}
	for _, tc := range invalidAdmissions {
		t.Run("reject admission "+tc.name, func(t *testing.T) {
			req := fixture.request
			req.Admission = fixture.admission
			tc.mutate(&req.Admission)
			if _, err := fixture.store.IssueRunForkSelectedContractRuntimeExecution(ctx, req); err == nil {
				t.Fatalf("issuance accepted invalid %s", tc.name)
			}
		})
	}
	for _, field := range []string{"container", "actors", "config"} {
		t.Run("reject empty "+field+" fingerprint", func(t *testing.T) {
			req := fixture.request
			switch field {
			case "container":
				req.ContainerPlanFingerprint = ""
			case "actors":
				req.ActorCensusFingerprint = ""
			case "config":
				req.EffectiveConfigFingerprint = ""
			}
			if _, err := fixture.store.IssueRunForkSelectedContractRuntimeExecution(ctx, req); err == nil {
				t.Fatalf("issuance accepted empty %s fingerprint", field)
			}
		})
	}

	issued, err := fixture.store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
	if err != nil {
		t.Fatalf("issue selected completion authority: %v", err)
	}
	if issued.Generation != 1 || issued.State != "prepared" || issued.ForkRunID != fixture.forkRun {
		t.Fatalf("issued authority = %#v", issued)
	}
	if _, err := fixture.store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request); err == nil {
		t.Fatal("second current selected completion authority was issued")
	}

	claimMutations := []struct {
		name   string
		mutate func(*SelectedContractRuntimeExecution)
	}{
		{name: "admission", mutate: func(e *SelectedContractRuntimeExecution) { e.AdmissionFingerprint += ":stale" }},
		{name: "container", mutate: func(e *SelectedContractRuntimeExecution) { e.ContainerPlanFingerprint += ":stale" }},
		{name: "actors", mutate: func(e *SelectedContractRuntimeExecution) { e.ActorCensusFingerprint += ":stale" }},
		{name: "config", mutate: func(e *SelectedContractRuntimeExecution) { e.EffectiveConfigFingerprint += ":stale" }},
		{name: "generation", mutate: func(e *SelectedContractRuntimeExecution) { e.Generation++ }},
		{name: "issue owner", mutate: func(e *SelectedContractRuntimeExecution) { e.ExecutionOwner += ":stale" }},
	}
	for _, tc := range claimMutations {
		t.Run("reject claim "+tc.name, func(t *testing.T) {
			stale := issued
			tc.mutate(&stale)
			if _, err := fixture.store.ClaimRunForkSelectedContractRuntimeExecution(ctx, stale, "served-owner", time.Minute); err == nil {
				t.Fatalf("claim accepted stale %s", tc.name)
			}
		})
	}

	authority, err := fixture.store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "served-owner", time.Minute)
	if err != nil {
		t.Fatalf("claim selected completion authority: %v", err)
	}
	if !authority.Valid() || authority.Kind != runtimeeffects.AuthoritySelectedContractFork {
		t.Fatalf("claimed authority = %#v", authority)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*runtimeeffects.Authority)
	}{
		{name: "owner", mutate: func(a *runtimeeffects.Authority) { a.ExecutionOwner += ":stale" }},
		{name: "fence", mutate: func(a *runtimeeffects.Authority) { a.FenceGeneration++ }},
		{name: "generation", mutate: func(a *runtimeeffects.Authority) { a.SelectedFork.Generation++ }},
		{name: "fork", mutate: func(a *runtimeeffects.Authority) { a.SelectedFork.ForkRunID = uuid.NewString() }},
		{name: "admission", mutate: func(a *runtimeeffects.Authority) { a.SelectedFork.AdmissionFingerprint += ":stale" }},
		{name: "container", mutate: func(a *runtimeeffects.Authority) { a.SelectedFork.ContainerPlanFingerprint += ":stale" }},
		{name: "actors", mutate: func(a *runtimeeffects.Authority) { a.SelectedFork.ActorCensusFingerprint += ":stale" }},
		{name: "config", mutate: func(a *runtimeeffects.Authority) { a.SelectedFork.EffectiveConfigFingerprint += ":stale" }},
	} {
		t.Run("reject authorize "+tc.name, func(t *testing.T) {
			stale := authority
			tc.mutate(&stale)
			stale.Target = selectedAgentTurnTarget(fixture.forkRun)
			attemptCtx := runtimeeffects.WithLogicalOperationIdentity(runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, stale), runtimeeffects.NewController(fixture.store)), "stale:"+tc.name)
			if _, err := runtimeeffects.BeginCompletion(attemptCtx, "anthropic_api", []byte("request"), nil); err == nil {
				t.Fatalf("authorize accepted stale %s", tc.name)
			}
		})
	}

	providerAuthority := authority
	providerAuthority.Target = selectedAgentTurnTarget(fixture.forkRun)
	if err := fixture.store.HeartbeatRunForkSelectedContractRuntimeExecution(ctx, authority, 3*time.Minute); err != nil {
		t.Fatalf("renew selected completion authority before provider call: %v", err)
	}
	providerCtx := runtimeeffects.WithLogicalOperationIdentity(runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, providerAuthority), runtimeeffects.NewController(fixture.store)), "selected:successful-completion")
	for _, registration := range runtimeeffects.Registrations() {
		if registration.Kind == runtimeeffects.KindProviderTurn {
			continue
		}
		if _, err := runtimeeffects.NewController(fixture.store).Authorize(runtimeeffects.WithAuthority(ctx, providerAuthority), runtimeeffects.AuthorizeRequest{
			OperationID: uuid.NewString(), Adapter: registration.Adapter, RequestFingerprint: runtimeeffects.Fingerprint([]byte(registration.Adapter)),
		}); err == nil {
			t.Fatalf("selected completion authority admitted non-provider adapter %s", registration.Adapter)
		}
	}
	handle, err := runtimeeffects.BeginCompletion(providerCtx, "anthropic_api", []byte("request"), nil)
	if err != nil {
		t.Fatalf("authorize selected provider completion: %v", err)
	}
	requireSelectedAttemptUsesCurrentLease(t, fixture, handle.Attempt().AttemptID, authority.LeaseExpiresAt)
	if err := handle.MarkLaunched(providerCtx); err != nil {
		t.Fatalf("launch selected provider completion: %v", err)
	}
	if err := handle.MarkResponseObserved(providerCtx, map[string]any{"response_fingerprint": "response"}); err != nil {
		t.Fatalf("observe selected provider response: %v", err)
	}
	settleSelectedCompletionForTest(t, providerCtx, handle, providerAuthority.Target, time.Now().UTC())

	if err := fixture.store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
		t.Fatalf("quiesce selected authority: %v", err)
	}
	if _, err := runtimeeffects.BeginCompletion(providerCtx, "anthropic_api", []byte("new request"), nil); err == nil {
		t.Fatal("quiesced selected authority admitted a new provider call")
	}
	if err := fixture.store.CloseRunForkSelectedContractRuntimeExecution(ctx, authority.ID); err != nil {
		t.Fatalf("close selected authority: %v", err)
	}
	if err := fixture.store.HeartbeatRunForkSelectedContractRuntimeExecution(ctx, authority, time.Minute); err == nil {
		t.Fatal("closed selected authority accepted heartbeat")
	}
	next, err := fixture.store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
	if err != nil {
		t.Fatalf("issue next selected generation: %v", err)
	}
	if next.Generation != 2 {
		t.Fatalf("next generation = %d, want 2", next.Generation)
	}
}

func TestSelectedForkCompletionAuthoritySingleCurrentGenerationSQLite(t *testing.T) {
	s := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveSelectedForkCompletionAuthoritySingleCurrentGeneration(t, newSelectedCompletionFixture(t, s, s.DB, true))
}

func TestSelectedForkCompletionAuthoritySingleCurrentGenerationPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveSelectedForkCompletionAuthoritySingleCurrentGeneration(t, newSelectedCompletionFixture(t, &PostgresStore{DB: db}, db, false))
}

func proveSelectedForkCompletionAuthoritySingleCurrentGeneration(t *testing.T, fixture selectedCompletionFixture) {
	t.Helper()
	ctx := context.Background()
	const contenders = 2
	results := make(chan SelectedContractRuntimeExecution, contenders)
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			issued, err := fixture.store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
			if err != nil {
				errs <- err
				return
			}
			results <- issued
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	if len(results) != 1 || len(errs) != 1 {
		t.Fatalf("issue race successes=%d failures=%d, want 1/1", len(results), len(errs))
	}
	issued := <-results
	if issued.Generation != 1 {
		t.Fatalf("winning generation = %d, want 1", issued.Generation)
	}
	var current int
	query := `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=? AND state<>'closed'`
	if !fixture.sqlite {
		query = `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1::uuid AND state<>'closed'`
	}
	if err := fixture.db.QueryRowContext(ctx, query, fixture.forkRun).Scan(&current); err != nil || current != 1 {
		t.Fatalf("current selected authorities=%d err=%v, want 1", current, err)
	}
}

func TestSelectedForkCompletionAuthorityRecoveryNoRedispatchSQLite(t *testing.T) {
	s := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveSelectedForkCompletionAuthorityRecoveryNoRedispatch(t, newSelectedCompletionFixture(t, s, s.DB, true))
}

func TestSelectedForkCompletionAuthorityRecoveryNoRedispatchPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveSelectedForkCompletionAuthorityRecoveryNoRedispatch(t, newSelectedCompletionFixture(t, &PostgresStore{DB: db}, db, false))
}

func proveSelectedForkCompletionAuthorityRecoveryNoRedispatch(t *testing.T, fixture selectedCompletionFixture) {
	t.Helper()
	ctx := context.Background()
	issued, err := fixture.store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := fixture.store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "recovery-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	controller := runtimeeffects.NewController(fixture.store)
	type recoveryCase struct {
		name string
		mark func(context.Context, *runtimeeffects.Handle) error
		want runtimeeffects.State
	}
	cases := []recoveryCase{
		{name: "authorized", want: runtimeeffects.StateTerminalFailure},
		{name: "launched", mark: func(ctx context.Context, h *runtimeeffects.Handle) error { return h.MarkLaunched(ctx) }, want: runtimeeffects.StateOutcomeUncertain},
		{name: "response_observed", mark: func(ctx context.Context, h *runtimeeffects.Handle) error {
			if err := h.MarkLaunched(ctx); err != nil {
				return err
			}
			return h.MarkResponseObserved(ctx, map[string]any{"response_fingerprint": "observed"})
		}, want: runtimeeffects.StateOutcomeUncertain},
	}
	handles := make(map[string]*runtimeeffects.Handle, len(cases))
	for _, tc := range cases {
		attemptAuthority := authority
		attemptAuthority.Target = selectedAgentTurnTarget(fixture.forkRun)
		attemptCtx := runtimeeffects.WithLogicalOperationIdentity(runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, attemptAuthority), controller), "recover:"+tc.name)
		handle, err := runtimeeffects.BeginCompletion(attemptCtx, "openai_responses", []byte(tc.name), nil)
		if err != nil {
			t.Fatalf("authorize %s: %v", tc.name, err)
		}
		if tc.mark != nil {
			if err := tc.mark(attemptCtx, handle); err != nil {
				t.Fatalf("prepare %s: %v", tc.name, err)
			}
		}
		handles[tc.name] = handle
	}

	expired := time.Now().UTC().Add(-time.Minute)
	if fixture.sqlite {
		if _, err := fixture.db.ExecContext(ctx, `UPDATE runtime_external_effect_attempts SET lease_expires_at=? WHERE operation_id IN (SELECT operation_id FROM runtime_external_effect_operations WHERE selected_execution_id=?)`, expired, authority.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET lease_expires_at=? WHERE execution_id=?`, expired, authority.ID); err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err := fixture.db.ExecContext(ctx, `UPDATE runtime_external_effect_attempts SET lease_expires_at=$1 WHERE operation_id IN (SELECT operation_id FROM runtime_external_effect_operations WHERE selected_execution_id=$2::uuid)`, expired, authority.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET lease_expires_at=$1 WHERE execution_id=$2::uuid`, expired, authority.ID); err != nil {
			t.Fatal(err)
		}
	}
	summary, err := fixture.store.ReconcileExternalEffectAttempts(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("reconcile selected completions: %v", err)
	}
	if summary.PrelaunchTerminal != 1 || summary.OutcomeUncertain != 2 {
		t.Fatalf("recovery summary = %#v, want 1 terminal/2 uncertain", summary)
	}
	for _, tc := range cases {
		requireExternalAttemptState(t, fixture.db, fixture.sqlite, handles[tc.name].Attempt().AttemptID, tc.want)
	}
	if err := handles["authorized"].MarkLaunched(ctx); err == nil {
		t.Fatal("recovered selected attempt was launchable")
	}
	var parentState string
	query := `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE execution_id=?`
	if !fixture.sqlite {
		query = `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1::uuid`
	}
	if err := fixture.db.QueryRowContext(ctx, query, authority.ID).Scan(&parentState); err != nil || parentState != "closed" {
		t.Fatalf("recovered parent state=%q err=%v, want closed", parentState, err)
	}
}

func TestSelectedForkCompletionAuthorityCleanupPreservesEvidencePostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := &PostgresStore{DB: db}
	fixture := newSelectedCompletionFixture(t, store, db, false)
	ctx := context.Background()

	issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
	if err != nil {
		t.Fatalf("issue selected completion authority: %v", err)
	}
	authority, err := store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "cleanup-owner", time.Minute)
	if err != nil {
		t.Fatalf("claim selected completion authority: %v", err)
	}
	authority.Target = selectedAgentTurnTarget(fixture.forkRun)
	completionCtx := runtimeeffects.WithLogicalOperationIdentity(
		runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), runtimeeffects.NewController(store)),
		"selected:cleanup-preservation",
	)
	handle, err := runtimeeffects.BeginCompletion(completionCtx, "openai_compatible", []byte("cleanup-preservation"), nil)
	if err != nil {
		t.Fatalf("authorize selected completion: %v", err)
	}
	if err := handle.MarkLaunched(completionCtx); err != nil {
		t.Fatalf("launch selected completion: %v", err)
	}
	if err := handle.MarkResponseObserved(completionCtx, map[string]any{"response_fingerprint": "cleanup"}); err != nil {
		t.Fatalf("observe selected completion response: %v", err)
	}
	settleSelectedCompletionForTest(t, completionCtx, handle, authority.Target, time.Now().UTC())
	if err := store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
		t.Fatalf("quiesce selected completion authority: %v", err)
	}
	if err := store.CloseRunForkSelectedContractRuntimeExecution(ctx, authority.ID); err != nil {
		t.Fatalf("close selected completion authority: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE runs SET status=$2 WHERE run_id=$1::uuid`, fixture.forkRun, RunForkMaterializedStatus); err != nil {
		t.Fatalf("mark selected fork materialized for cleanup: %v", err)
	}
	assertSelectedCompletionEvidencePresent(t, db, "pre-cleanup fork revision", `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1::uuid`, fixture.forkRun)
	if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun); err != nil {
		t.Fatalf("discard mutable selected fork: %v", err)
	}

	var runStatus, authorityState string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&runStatus); err != nil || runStatus != "cancelled" {
		t.Fatalf("retained run status=%q err=%v, want cancelled", runStatus, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1::uuid AND fork_run_id=$2::uuid`, authority.ID, fixture.forkRun).Scan(&authorityState); err != nil || authorityState != "closed" {
		t.Fatalf("retained authority state=%q err=%v, want closed", authorityState, err)
	}
	assertSelectedCompletionEvidenceCount(t, db, "binding", `SELECT COUNT(*) FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1::uuid`, fixture.forkRun)
	assertSelectedCompletionEvidenceCount(t, db, "operation and attempt", `SELECT COUNT(*) FROM runtime_external_effect_operations o JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id WHERE o.selected_execution_id=$1::uuid AND a.attempt_id=$2::uuid`, authority.ID, handle.Attempt().AttemptID)
	assertSelectedCompletionEvidenceCount(t, db, "turn and attempt", `SELECT COUNT(*) FROM agent_turns t JOIN runtime_external_effect_attempts a ON a.attempt_id=t.completion_attempt_id WHERE t.turn_id=$1::uuid AND t.run_id=$2::uuid`, authority.Target.ID, fixture.forkRun)
	assertSelectedCompletionEvidenceCount(t, db, "spend and attempt", `SELECT COUNT(*) FROM spend_ledger s JOIN runtime_external_effect_attempts a ON a.attempt_id=s.external_effect_attempt_id WHERE s.external_effect_attempt_id=$1::uuid`, handle.Attempt().AttemptID)
	assertSelectedCompletionEvidenceAbsent(t, db, "fork fact revisions", `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1::uuid`, fixture.forkRun)
	assertSelectedCompletionEvidenceAbsent(t, db, "fork revision ledger", `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1::uuid`, fixture.forkRun)
	assertSelectedCompletionEvidenceAbsent(t, db, "fork revision head", `SELECT COUNT(*) FROM run_fork_revision_heads WHERE run_id=$1::uuid`, fixture.forkRun)
}

func TestSelectedForkDiscardLocksParentBeforeRevisionDeletionPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := &PostgresStore{DB: db}
	fixture := newSelectedCompletionFixture(t, store, db, false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
	if err != nil {
		t.Fatalf("issue selected completion authority: %v", err)
	}
	seedEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (event_id,run_id,event_name,scope,produced_by_type)
		VALUES ($1::uuid,$2::uuid,'selected.discard.seed','global','platform')
	`, seedEventID, fixture.forkRun); err != nil {
		t.Fatalf("seed selected discard event: %v", err)
	}
	firstRevision := captureRunForkTestRevision(t, db, fixture.forkRun, runforkrevision.FamilyEvents)
	if _, err := db.ExecContext(ctx, `UPDATE runs SET status=$2 WHERE run_id=$1::uuid`, fixture.forkRun, RunForkMaterializedStatus); err != nil {
		t.Fatalf("mark selected fork materialized: %v", err)
	}

	allocationTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin competing revision allocation: %v", err)
	}
	defer func() { _ = allocationTx.Rollback() }()
	concurrentEventID := uuid.NewString()
	if _, err := allocationTx.ExecContext(ctx, `
		INSERT INTO events (event_id,run_id,event_name,scope,produced_by_type)
		VALUES ($1::uuid,$2::uuid,'selected.discard.concurrent','global','platform')
	`, concurrentEventID, fixture.forkRun); err != nil {
		t.Fatalf("stage competing selected event: %v", err)
	}
	allocatedRevision, err := runforkrevision.Capture(ctx, allocationTx, fixture.forkRun, runforkrevision.FamilyEvents)
	if err != nil {
		t.Fatalf("capture competing selected revision: %v", err)
	}
	if allocatedRevision <= firstRevision {
		t.Fatalf("competing revision = %d, want after %d", allocatedRevision, firstRevision)
	}

	discardDone := make(chan error, 1)
	go func() {
		discardDone <- store.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun)
	}()
	waitForPostgresQueryLock(t, ctx, db, "SELECT status FROM runs WHERE run_id = $1::uuid FOR UPDATE")

	var status string
	var committedRevisionRows int
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&status); err != nil {
		t.Fatalf("load blocked discard run: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&committedRevisionRows); err != nil {
		t.Fatalf("count blocked discard revisions: %v", err)
	}
	if status != RunForkMaterializedStatus || committedRevisionRows != int(firstRevision) {
		t.Fatalf("blocked discard state = status:%q revisions:%d, want %q/%d", status, committedRevisionRows, RunForkMaterializedStatus, firstRevision)
	}

	if err := allocationTx.Commit(); err != nil {
		t.Fatalf("commit competing revision allocation: %v", err)
	}
	var discardErr error
	select {
	case discardErr = <-discardDone:
	case <-ctx.Done():
		t.Fatalf("selected fork discard did not resume after allocation: %v", ctx.Err())
	}
	if discardErr == nil || !strings.Contains(discardErr.Error(), "could not serialize access") || !strings.Contains(discardErr.Error(), "40001") {
		t.Fatalf("contended discard error = %v, want fail-closed PostgreSQL serialization failure", discardErr)
	}
	if strings.Contains(strings.ToLower(discardErr.Error()), "deadlock") {
		t.Fatalf("contended discard retained deadlock outcome: %v", discardErr)
	}

	var currentRevision, revisionRows, eventRows int
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&status); err != nil {
		t.Fatalf("load serialized selected run: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&currentRevision); err != nil {
		t.Fatalf("load serialized revision head: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&revisionRows); err != nil {
		t.Fatalf("count serialized revision ledger: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&eventRows); err != nil {
		t.Fatalf("count serialized selected events: %v", err)
	}
	if status != RunForkMaterializedStatus || currentRevision != int(allocatedRevision) || revisionRows != int(allocatedRevision) || eventRows != 2 {
		t.Fatalf("failed discard partial state = status:%q head:%d ledger:%d events:%d, want %q/%d/%d/2", status, currentRevision, revisionRows, eventRows, RunForkMaterializedStatus, allocatedRevision, allocatedRevision)
	}
	if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun); err != nil {
		t.Fatalf("retry selected fork discard after contention: %v", err)
	}

	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&status); err != nil {
		t.Fatalf("load retained selected run: %v", err)
	}
	if status != "cancelled" {
		t.Fatalf("retained selected run status = %q, want cancelled", status)
	}
	for label, query := range map[string]string{
		"revision head":   `SELECT COUNT(*) FROM run_fork_revision_heads WHERE run_id=$1::uuid`,
		"revision ledger": `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1::uuid`,
		"revision facts":  `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1::uuid`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query, fixture.forkRun).Scan(&count); err != nil {
			t.Fatalf("count %s after discard: %v", label, err)
		}
		if count != 0 {
			t.Fatalf("%s rows after discard = %d, want 0", label, count)
		}
	}
	var authorityRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1::uuid`, issued.ExecutionID).Scan(&authorityRows); err != nil {
		t.Fatalf("count retained selected authority: %v", err)
	}
	if authorityRows != 1 {
		t.Fatalf("retained selected authority rows = %d, want 1", authorityRows)
	}
}

func TestSelectedForkDiscardRejectsLiveDependentForkPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := &PostgresStore{DB: db}
	ctx := context.Background()
	now := time.Now().UTC()
	sourceRunID := uuid.NewString()
	forkRunID := uuid.NewString()
	dependentRunID := uuid.NewString()
	forkEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id,status,started_at) VALUES
			($1::uuid,'running',$3),
			($2::uuid,'paused',$3)
	`, sourceRunID, forkRunID, now); err != nil {
		t.Fatalf("seed selected fork lineage: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (event_id,run_id,event_name,scope,produced_by_type,created_at)
		VALUES ($1::uuid,$2::uuid,'fork.dependency','global','platform',$3)
	`, forkEventID, forkRunID, now); err != nil {
		t.Fatalf("seed selected fork event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id,status,started_at,forked_from_run_id,forked_from_event_id)
		VALUES ($1::uuid,'paused',$4,$2::uuid,$3::uuid)
	`, dependentRunID, forkRunID, forkEventID, now); err != nil {
		t.Fatalf("seed dependent fork: %v", err)
	}

	err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, forkRunID)
	if err == nil || !strings.Contains(err.Error(), dependentRunID) {
		t.Fatalf("discard error = %v, want dependent fork %s", err, dependentRunID)
	}
	for label, query := range map[string]string{
		"source fork":    `SELECT COUNT(*) FROM runs WHERE run_id=$1::uuid`,
		"source event":   `SELECT COUNT(*) FROM events WHERE event_id=$1::uuid`,
		"dependent fork": `SELECT COUNT(*) FROM runs WHERE run_id=$1::uuid`,
	} {
		id := forkRunID
		if label == "source event" {
			id = forkEventID
		} else if label == "dependent fork" {
			id = dependentRunID
		}
		var count int
		if err := db.QueryRowContext(ctx, query, id).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s rows=%d err=%v, want 1 after rejected discard", label, count, err)
		}
	}
}

func assertSelectedCompletionEvidenceCount(t *testing.T, db *sql.DB, name, query string, args ...any) {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil || count != 1 {
		t.Fatalf("retained %s rows=%d err=%v, want 1", name, count, err)
	}
}

func assertSelectedCompletionEvidencePresent(t *testing.T, db *sql.DB, name, query string, args ...any) {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil || count == 0 {
		t.Fatalf("retained %s rows=%d err=%v, want at least 1", name, count, err)
	}
}

func assertSelectedCompletionEvidenceAbsent(t *testing.T, db *sql.DB, name, query string, args ...any) {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil || count != 0 {
		t.Fatalf("retained %s rows=%d err=%v, want 0", name, count, err)
	}
}

func requireSelectedAttemptUsesCurrentLease(t *testing.T, fixture selectedCompletionFixture, attemptID string, originalLease time.Time) {
	t.Helper()
	query := `
		SELECT a.lease_expires_at,e.lease_expires_at
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		JOIN run_fork_selected_contract_runtime_executions e ON e.execution_id=o.selected_execution_id
		WHERE a.attempt_id=?
	`
	if fixture.sqlite {
		var attemptLease, authorityLease conversationForkTimeValue
		if err := fixture.db.QueryRow(query, attemptID).Scan(&attemptLease, &authorityLease); err != nil {
			t.Fatalf("read sqlite selected attempt lease: %v", err)
		}
		if !attemptLease.Valid || !authorityLease.Valid || !attemptLease.Time.Equal(authorityLease.Time) || !attemptLease.Time.After(originalLease) {
			t.Fatalf("sqlite selected attempt lease=%v authority=%v original=%v", attemptLease.Time, authorityLease.Time, originalLease)
		}
		return
	}
	query = `
		SELECT a.lease_expires_at,e.lease_expires_at
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id
		JOIN run_fork_selected_contract_runtime_executions e ON e.execution_id=o.selected_execution_id
		WHERE a.attempt_id=$1::uuid
	`
	var attemptLease, authorityLease time.Time
	if err := fixture.db.QueryRow(query, attemptID).Scan(&attemptLease, &authorityLease); err != nil {
		t.Fatalf("read selected attempt lease: %v", err)
	}
	if !attemptLease.Equal(authorityLease) || !attemptLease.After(originalLease) {
		t.Fatalf("selected attempt lease=%v authority=%v original=%v", attemptLease, authorityLease, originalLease)
	}
}

func newSelectedCompletionFixture(t *testing.T, store selectedCompletionAuthorityStore, db *sql.DB, sqlite bool) selectedCompletionFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	sourceRun := uuid.NewString()
	forkRun := uuid.NewString()
	eventID := uuid.NewString()
	bindingID := uuid.NewString()
	if sqlite {
		if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id,status,started_at) VALUES (?,'running',?),(?,'paused',?)`, sourceRun, now, forkRun, now); err != nil {
			t.Fatalf("seed selected runs: %v", err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO events (event_id,run_id,event_name,scope,created_at) VALUES (?,?,'selected.test','global',?)`, eventID, sourceRun, now); err != nil {
			t.Fatalf("seed selected event: %v", err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings (binding_id,fork_run_id,source_run_id,fork_event_id,mode,contracts_root,workflow_name,workflow_version,created_at) VALUES (?,?,?,?,'selected_contracts','/tmp/contracts','workflow','v1',?)`, bindingID, forkRun, sourceRun, eventID, now); err != nil {
			t.Fatalf("seed selected binding: %v", err)
		}
	} else {
		if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id,status,started_at) VALUES ($1::uuid,'running',$3),($2::uuid,'paused',$3)`, sourceRun, forkRun, now); err != nil {
			t.Fatalf("seed selected runs: %v", err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO events (event_id,run_id,event_name,scope,created_at) VALUES ($1::uuid,$2::uuid,'selected.test','global',$3)`, eventID, sourceRun, now); err != nil {
			t.Fatalf("seed selected event: %v", err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings (binding_id,fork_run_id,source_run_id,fork_event_id,mode,contracts_root,workflow_name,workflow_version,created_at) VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,'selected_contracts','/tmp/contracts','workflow','v1',$5)`, bindingID, forkRun, sourceRun, eventID, now); err != nil {
			t.Fatalf("seed selected binding: %v", err)
		}
	}
	selection := RunForkContractSelection{Mode: "selected_contracts", ContractsRoot: "/tmp/contracts", WorkflowName: "workflow", WorkflowVersion: "v1"}
	admission := RunForkSelectedContractExecutionAdmission{
		Owner: RunForkSelectedContractExecutionAdmissionOwner, FutureExecutionOwner: RunForkSelectedContractExecutionOwner,
		NonMutating: true, ExecutionSupported: false, ForkRunID: forkRun, SourceRunID: sourceRun, ForkEventID: eventID,
		ContractSelection: selection, ContractBindingOwner: RunForkSelectedContractBindingOwner,
		AdmissionOwner: "runtime.run_fork.frontier", AdmissionUse: RunForkSelectedContractExecutionAdmissionUseDurableBinding,
		ExecutionModelOwner: RunForkSelectedContractExecutionModelOwner, SourceWorkflowName: "workflow", SourceWorkflowVersion: "v1",
	}
	return selectedCompletionFixture{
		store: store, db: db, sqlite: sqlite, sourceRun: sourceRun, forkRun: forkRun, eventID: eventID, admission: admission,
		request: SelectedContractRuntimeExecutionIssueRequest{
			Admission: admission, ContainerPlanFingerprint: "sha256:container", ActorCensusFingerprint: "sha256:actors",
			EffectiveConfigFingerprint: "sha256:config", Now: now,
		},
	}
}

func selectedAgentTurnTarget(runID string) runtimeeffects.UsageTarget {
	return runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: runID,
		AgentID: "selected-agent", SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(), FlowInstance: "selected-test",
	}
}

func settleSelectedCompletionForTest(t *testing.T, ctx context.Context, handle *runtimeeffects.Handle, target runtimeeffects.UsageTarget, now time.Time) {
	t.Helper()
	input, output := int64(8), int64(3)
	err := handle.SettleCompletion(ctx, runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: runtimeeffects.StateSettled, Evidence: map[string]any{"test": true}},
		Usage: runtimeeffects.CompletionUsage{
			ResolvedModel: "test-model", Exactness: runtimeeffects.CompletionUsageExact,
			InputTokens: &input, OutputTokens: &output,
		},
		AgentTurn: &runtimeeffects.CompletionAgentTurn{
			TurnID: target.ID, RunID: target.RunID, AgentID: target.AgentID, SessionID: target.SessionID,
			Memory: target.Memory, FlowInstance: target.FlowInstance, ParseOK: true,
		},
		Spend: runtimeeffects.CompletionSpend{
			FlowInstance: target.FlowInstance, AgentID: target.AgentID, Model: "test-model", ModelAlias: "regular",
			BackendProfile: "test", Provider: "test", Transport: "http", ResolvedModel: "test-model", CostUSD: 0.01,
			InvocationType: "agent_turn",
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("settle selected completion: %v", err)
	}
}

func requireSelectedFixtureRows(t *testing.T, fixture selectedCompletionFixture) {
	t.Helper()
	for _, table := range []string{"runs", "events", "run_fork_selected_contract_bindings"} {
		var count int
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		if err := fixture.db.QueryRow(query).Scan(&count); err != nil || count == 0 {
			t.Fatalf("fixture table %s count=%d err=%v", table, count, err)
		}
	}
}
