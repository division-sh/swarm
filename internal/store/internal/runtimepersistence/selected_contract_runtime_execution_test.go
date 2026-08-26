package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type selectedCompletionAuthorityStore interface {
	runtimeeffects.Store
	runtimeeffects.CompletionStore
	runtimeeffects.CompletionHeartbeatStore
	runtimeeffects.RecoveryStore
	IssueRunForkSelectedContractRuntimeExecution(context.Context, runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error)
	ClaimRunForkSelectedContractRuntimeExecution(context.Context, runfork.SelectedContractRuntimeExecution, string, time.Duration) (runtimeeffects.Authority, error)
	HeartbeatRunForkSelectedContractRuntimeExecution(context.Context, runtimeeffects.Authority, time.Duration) error
	QuiesceRunForkSelectedContractRuntimeExecution(context.Context, runtimeeffects.Authority) error
	CloseRunForkSelectedContractRuntimeExecution(context.Context, string) error
	FailRunForkSelectedContractRuntimeExecution(context.Context, runtimeeffects.Authority, json.RawMessage) error
	MarkTerminalRun(context.Context, runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error)
}

type selectedCompletionFixture struct {
	store     selectedCompletionAuthorityStore
	db        *sql.DB
	sqlite    bool
	sourceRun string
	forkRun   string
	eventID   string
	admission runfork.RunForkSelectedContractExecutionAdmission
	request   runfork.SelectedContractRuntimeExecutionIssueRequest
}

func TestSelectedForkCompletionAuthorityIssuanceConsumesExactAdmissionSQLite(t *testing.T) {
	s := newBootstrappedSQLiteRuntimeStoreForTest(t)
	proveSelectedForkCompletionAuthorityIssuance(t, newSelectedCompletionFixture(t, s, s.backend.ConstructionHandle(), true))
}

func TestSelectedForkCompletionAuthorityIssuanceConsumesExactAdmissionPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveSelectedForkCompletionAuthorityIssuance(t, newSelectedCompletionFixture(t, admitTestPostgresStore(t, db), db, false))
}

func proveSelectedForkCompletionAuthorityIssuance(t *testing.T, fixture selectedCompletionFixture) {
	t.Helper()
	ctx := testAuthorActivityContext()

	invalidAdmissions := []struct {
		name   string
		mutate func(*runfork.RunForkSelectedContractExecutionAdmission)
	}{
		{name: "owner", mutate: func(a *runfork.RunForkSelectedContractExecutionAdmission) { a.Owner = "caller.local" }},
		{name: "future owner", mutate: func(a *runfork.RunForkSelectedContractExecutionAdmission) { a.FutureExecutionOwner = "caller.local" }},
		{name: "mutating", mutate: func(a *runfork.RunForkSelectedContractExecutionAdmission) { a.NonMutating = false }},
		{name: "already executable", mutate: func(a *runfork.RunForkSelectedContractExecutionAdmission) { a.ExecutionSupported = true }},
		{name: "binding owner", mutate: func(a *runfork.RunForkSelectedContractExecutionAdmission) { a.ContractBindingOwner = "caller.local" }},
		{name: "deferred-work admission owner", mutate: func(a *runfork.RunForkSelectedContractExecutionAdmission) {
			a.DeferredWorkAdmissionOwner = "caller.local"
		}},
		{name: "admission use", mutate: func(a *runfork.RunForkSelectedContractExecutionAdmission) {
			a.AdmissionUse = runfork.RunForkSelectedContractExecutionAdmissionUseEvidenceOnly
		}},
		{name: "durable source", mutate: func(a *runfork.RunForkSelectedContractExecutionAdmission) { a.SourceRunID = uuid.NewString() }},
		{name: "durable event", mutate: func(a *runfork.RunForkSelectedContractExecutionAdmission) { a.ForkEventID = uuid.NewString() }},
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
		mutate func(*runfork.SelectedContractRuntimeExecution)
	}{
		{name: "admission", mutate: func(e *runfork.SelectedContractRuntimeExecution) { e.AdmissionFingerprint += ":stale" }},
		{name: "container", mutate: func(e *runfork.SelectedContractRuntimeExecution) { e.ContainerPlanFingerprint += ":stale" }},
		{name: "actors", mutate: func(e *runfork.SelectedContractRuntimeExecution) { e.ActorCensusFingerprint += ":stale" }},
		{name: "config", mutate: func(e *runfork.SelectedContractRuntimeExecution) { e.EffectiveConfigFingerprint += ":stale" }},
		{name: "generation", mutate: func(e *runfork.SelectedContractRuntimeExecution) { e.Generation++ }},
		{name: "issue owner", mutate: func(e *runfork.SelectedContractRuntimeExecution) { e.ExecutionOwner += ":stale" }},
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
			attemptCtx := runtimeeffects.WithLogicalOperationIdentity(runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, stale), newCompletionControllerForTest(fixture.store)), "stale:"+tc.name)
			attemptCtx = withManagedCompletionTestSurface(t, attemptCtx, stale, "anthropic_api")
			if _, err := beginManagedCompletionForTest(t, attemptCtx, "anthropic_api", []byte("request")); err == nil {
				t.Fatalf("authorize accepted stale %s", tc.name)
			}
		})
	}

	providerAuthority := authority
	providerAuthority.Target = selectedAgentTurnTarget(fixture.forkRun)
	if err := fixture.store.HeartbeatRunForkSelectedContractRuntimeExecution(ctx, authority, 3*time.Minute); err != nil {
		t.Fatalf("renew selected completion authority before provider call: %v", err)
	}
	providerCtx := runtimeeffects.WithLogicalOperationIdentity(runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, providerAuthority), newCompletionControllerForTest(fixture.store)), "selected:successful-completion")
	providerCtx = managedSelectedExecutionStoreTestContext(t, providerCtx, providerAuthority)
	providerCtx = withManagedCompletionTestSurface(t, providerCtx, providerAuthority, "anthropic_api")
	for _, registration := range runtimeeffects.Registrations() {
		if registration.Kind == runtimeeffects.KindProviderTurn ||
			registration.Kind == runtimeeffects.KindProviderStartupProbe ||
			registration.Kind == runtimeeffects.KindServeRegistration ||
			registration.Kind == runtimeeffects.KindChannelConfirmation {
			continue
		}
		handle, err := runtimeeffects.Begin(providerCtx, registration.Adapter, []byte(registration.Adapter), nil)
		if err != nil {
			t.Fatalf("selected execution authority rejected bound non-provider adapter %s: %v", registration.Adapter, err)
		}
		if err := handle.Fail(providerCtx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassDependencyUnavailable, "selected_test_prelaunch", "test", "selected_effect", map[string]any{"launch_rejected": true}, nil); err == nil {
			t.Fatalf("selected non-provider adapter %s terminal failure returned no failure evidence", registration.Adapter)
		}
	}
	handle, err := beginManagedCompletionForTest(t, providerCtx, "anthropic_api", []byte("request"))
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
	if _, err := beginManagedCompletionForTest(t, providerCtx, "anthropic_api", []byte("new request")); err == nil {
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
	proveSelectedForkCompletionAuthoritySingleCurrentGeneration(t, newSelectedCompletionFixture(t, s, s.backend.ConstructionHandle(), true))
}

func TestSelectedForkCompletionAuthoritySingleCurrentGenerationPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveSelectedForkCompletionAuthoritySingleCurrentGeneration(t, newSelectedCompletionFixture(t, admitTestPostgresStore(t, db), db, false))
}

func TestSelectedForkRuntimeAuthorityFinalizationAfterRunTerminalSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var store selectedCompletionAuthorityStore
			var db *sql.DB
			var sqlite bool
			if backend == "sqlite" {
				selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
				store, db, sqlite = selected, selected.backend.ConstructionHandle(), true
			} else {
				_, db, _ = testutil.StartPostgres(t)
				store = admitTestPostgresStore(t, db)
			}

			quiesceFixture := newSelectedCompletionFixture(t, store, db, sqlite)
			issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, quiesceFixture.request)
			if err != nil {
				t.Fatalf("issue quiesce authority: %v", err)
			}
			authority, err := store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "terminal-quiesce-owner", time.Minute)
			if err != nil {
				t.Fatalf("claim quiesce authority: %v", err)
			}
			candidateStore, ok := store.(runLifecycleCandidateParityStore)
			if !ok {
				t.Fatalf("%T does not expose the run completion owner", store)
			}
			if _, _, err := completeRunLifecycleCandidateParity(runLifecycleCandidateParityFixture{
				store: candidateStore, db: db, postgres: !sqlite,
			}, ctx, quiesceFixture.forkRun, time.Now().UTC()); err != nil {
				t.Fatalf("complete fork before quiesce: %v", err)
			}
			if err := store.HeartbeatRunForkSelectedContractRuntimeExecution(ctx, authority, time.Minute); err == nil {
				t.Fatal("terminal fork accepted runtime heartbeat")
			}
			stale := authority
			stale.FenceGeneration++
			if err := store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, stale); err == nil {
				t.Fatal("terminal fork accepted stale quiesce authority")
			}
			if err := store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
				t.Fatalf("quiesce terminal fork authority: %v", err)
			}
			if err := store.CloseRunForkSelectedContractRuntimeExecution(ctx, authority.ID); err != nil {
				t.Fatalf("close terminal fork authority: %v", err)
			}

			failFixture := newSelectedCompletionFixture(t, store, db, sqlite)
			issued, err = store.IssueRunForkSelectedContractRuntimeExecution(ctx, failFixture.request)
			if err != nil {
				t.Fatalf("issue failure authority: %v", err)
			}
			authority, err = store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "terminal-failure-owner", time.Minute)
			if err != nil {
				t.Fatalf("claim failure authority: %v", err)
			}
			if _, _, err := store.MarkTerminalRun(ctx, runtimerunlifecycle.TerminalRequest{
				RunID: failFixture.forkRun, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("cancel fork before failure finalization: %v", err)
			}
			if err := store.HeartbeatRunForkSelectedContractRuntimeExecution(ctx, authority, time.Minute); err == nil {
				t.Fatal("cancelled fork accepted runtime heartbeat")
			}
			stale = authority
			stale.FenceGeneration++
			failure := json.RawMessage(`{"reason":"terminal fork"}`)
			if err := store.FailRunForkSelectedContractRuntimeExecution(ctx, stale, failure); err == nil {
				t.Fatal("terminal fork accepted stale failure authority")
			}
			if err := store.FailRunForkSelectedContractRuntimeExecution(ctx, authority, failure); err != nil {
				t.Fatalf("fail terminal fork authority: %v", err)
			}
			if err := store.CloseRunForkSelectedContractRuntimeExecution(ctx, authority.ID); err != nil {
				t.Fatalf("close failed terminal fork authority: %v", err)
			}
		})
	}
}

func proveSelectedForkCompletionAuthoritySingleCurrentGeneration(t *testing.T, fixture selectedCompletionFixture) {
	t.Helper()
	ctx := testAuthorActivityContext()
	const contenders = 2
	results := make(chan runfork.SelectedContractRuntimeExecution, contenders)
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
	proveSelectedForkCompletionAuthorityRecoveryNoRedispatch(t, newSelectedCompletionFixture(t, s, s.backend.ConstructionHandle(), true))
}

func TestSelectedForkCompletionAuthorityRecoveryNoRedispatchPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	proveSelectedForkCompletionAuthorityRecoveryNoRedispatch(t, newSelectedCompletionFixture(t, admitTestPostgresStore(t, db), db, false))
}

func proveSelectedForkCompletionAuthorityRecoveryNoRedispatch(t *testing.T, fixture selectedCompletionFixture) {
	t.Helper()
	ctx := testAuthorActivityContext()
	issued, err := fixture.store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := fixture.store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "recovery-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	controller := newCompletionControllerForTest(fixture.store)
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
		attemptCtx = managedSelectedExecutionStoreTestContext(t, attemptCtx, attemptAuthority)
		attemptCtx = withManagedCompletionTestSurface(t, attemptCtx, attemptAuthority, "openai_responses")
		handle, err := beginManagedCompletionForTest(t, attemptCtx, "openai_responses", []byte(tc.name))
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
	summary, err := fixture.store.ReconcileExternalEffectAttempts(ctx, liveExternalEffectRecoveryRequest(time.Now().UTC()))
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

type selectedForkDiscardStore interface {
	selectedCompletionAuthorityStore
	deliveryFixtureStore
	DiscardMaterializedSelectedContractExecutionFork(context.Context, string) error
}

type selectedForkDiscardProof struct {
	RunStatus     string
	RunRows       int
	EventRows     int
	DeliveryRows  int
	AttemptRows   int
	OutcomeRows   int
	ExecutionRows int
	BindingRows   int
}

func TestSelectedForkDiscardSelectedStoreParity(t *testing.T) {
	proofs := make(map[string]selectedForkDiscardProof, 2)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db, sqlite := selectedForkDiscardTestStore(t, backend)
			fixture := newSelectedCompletionFixture(t, store, db, sqlite)
			ctx := testAuthorActivityContext()
			issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
			if err != nil {
				t.Fatalf("issue retained selected execution: %v", err)
			}
			eventsByState, deliveries := seedSelectedForkDiscardDeliveries(t, ctx, store, fixture)

			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			before := loadSelectedForkDiscardProof(t, ctx, db, sqlite, fixture.forkRun, issued.ExecutionID)
			if err := store.DiscardMaterializedSelectedContractExecutionFork(cancelled, fixture.forkRun); err == nil {
				t.Fatal("pre-cancelled selected fork discard succeeded")
			}
			afterCancellation := loadSelectedForkDiscardProof(t, ctx, db, sqlite, fixture.forkRun, issued.ExecutionID)
			if !reflect.DeepEqual(afterCancellation, before) {
				t.Fatalf("pre-cancelled discard changed durable state:\nbefore=%#v\nafter=%#v", before, afterCancellation)
			}

			if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun); err != nil {
				t.Fatalf("discard selected fork: %v", err)
			}
			proofs[backend] = loadSelectedForkDiscardProof(t, ctx, db, sqlite, fixture.forkRun, issued.ExecutionID)
			if proofs[backend] != (selectedForkDiscardProof{RunStatus: "cancelled", RunRows: 1, ExecutionRows: 1, BindingRows: 1}) {
				t.Fatalf("selected fork discard proof = %#v", proofs[backend])
			}
			for _, claimed := range deliveries {
				if claimed.Snapshot.DeliveryID == "" {
					t.Fatal("discard proof delivery has no durable identity")
				}
			}
			for _, event := range eventsByState {
				if event.ID() == "" {
					t.Fatal("discard proof event has no durable identity")
				}
			}
		})
	}
	if !reflect.DeepEqual(proofs["sqlite"], proofs["postgres"]) {
		t.Fatalf("selected-store discard differs:\nsqlite=%#v\npostgres=%#v", proofs["sqlite"], proofs["postgres"])
	}
}

func TestSelectedForkDiscardRollbackSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, db, sqlite := selectedForkDiscardTestStore(t, backend)
			fixture := newSelectedCompletionFixture(t, store, db, sqlite)
			ctx := testAuthorActivityContext()
			issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
			if err != nil {
				t.Fatalf("issue rollback selected execution: %v", err)
			}
			seedSelectedForkDiscardDeliveries(t, ctx, store, fixture)
			installSelectedForkDiscardFailure(t, ctx, db, sqlite)
			before := loadSelectedForkDiscardProof(t, ctx, db, sqlite, fixture.forkRun, issued.ExecutionID)
			err = store.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun)
			if err == nil || !strings.Contains(err.Error(), "injected selected discard failure") {
				t.Fatalf("selected fork discard failure = %v, want injected rollback", err)
			}
			after := loadSelectedForkDiscardProof(t, ctx, db, sqlite, fixture.forkRun, issued.ExecutionID)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("selected fork discard rollback changed durable state:\nbefore=%#v\nafter=%#v", before, after)
			}
		})
	}
}

func TestSelectedForkDiscardMissingRunIsIdempotentOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store, _, _ := selectedForkDiscardTestStore(t, backend)
			missing := uuid.NewString()
			if err := store.DiscardMaterializedSelectedContractExecutionFork(testAuthorActivityContext(), missing); err != nil {
				t.Fatalf("discard missing selected fork: %v", err)
			}
			if err := store.DiscardMaterializedSelectedContractExecutionFork(testAuthorActivityContext(), missing); err != nil {
				t.Fatalf("repeat discard missing selected fork: %v", err)
			}
		})
	}
}

func selectedForkDiscardTestStore(t *testing.T, backend string) (selectedForkDiscardStore, *sql.DB, bool) {
	t.Helper()
	if backend == "sqlite" {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		return store, store.backend.ConstructionHandle(), true
	}
	_, db, _ := testutil.StartPostgres(t)
	return admitTestPostgresStore(t, db), db, false
}

func seedSelectedForkDiscardDeliveries(t *testing.T, ctx context.Context, store selectedForkDiscardStore, fixture selectedCompletionFixture) ([]events.Event, []runtimedelivery.ClaimedObligation) {
	t.Helper()
	route := testAgentDeliveryRoute(t, "selected-agent", "fixture/selected-agent")
	eventsByState := []events.Event{
		eventtest.PersistedProjection(uuid.NewString(), "selected.claimed", "selected-test", "", json.RawMessage(`{}`), 0, fixture.forkRun, "", events.EventEnvelope{}, time.Now().UTC()),
		eventtest.PersistedProjection(uuid.NewString(), "selected.settled", "selected-test", "", json.RawMessage(`{}`), 0, fixture.forkRun, "", events.EventEnvelope{}, time.Now().UTC()),
	}
	deliveries := make([]runtimedelivery.ClaimedObligation, 0, len(eventsByState))
	for _, event := range eventsByState {
		if err := commitSemanticEventFixtureWithRoutes(ctx, store, event, []events.DeliveryRoute{route}); err != nil {
			t.Fatalf("commit selected-fork delivery %s: %v", event.ID(), err)
		}
		claimed, err := claimDeliveryFixture(ctx, store, event, route)
		if err != nil {
			t.Fatalf("claim selected-fork delivery %s: %v", event.ID(), err)
		}
		deliveries = append(deliveries, claimed)
	}
	if _, err := store.SettleSuccess(ctx, deliveries[1].Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
		t.Fatalf("settle selected-fork delivery: %v", err)
	}
	return eventsByState, deliveries
}

func loadSelectedForkDiscardProof(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, runID, executionID string) selectedForkDiscardProof {
	t.Helper()
	placeholder := "$1::uuid"
	executionPlaceholder := "$2::uuid"
	if sqlite {
		placeholder = "?"
		executionPlaceholder = "?"
	}
	proof := selectedForkDiscardProof{}
	_ = db.QueryRowContext(ctx, "SELECT status FROM runs WHERE run_id = "+placeholder, runID).Scan(&proof.RunStatus)
	queries := []struct {
		dest  *int
		query string
		args  []any
	}{
		{&proof.RunRows, "SELECT COUNT(*) FROM runs WHERE run_id = " + placeholder, []any{runID}},
		{&proof.EventRows, "SELECT COUNT(*) FROM events WHERE run_id = " + placeholder, []any{runID}},
		{&proof.DeliveryRows, "SELECT COUNT(*) FROM event_deliveries WHERE run_id = " + placeholder, []any{runID}},
		{&proof.AttemptRows, "SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id = " + placeholder + ")", []any{runID}},
		{&proof.OutcomeRows, "SELECT COUNT(*) FROM event_delivery_outcomes WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id = " + placeholder + ")", []any{runID}},
		{&proof.ExecutionRows, "SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id = " + placeholder + " AND execution_id = " + executionPlaceholder, []any{runID, executionID}},
		{&proof.BindingRows, "SELECT COUNT(*) FROM run_fork_selected_contract_bindings WHERE fork_run_id = " + placeholder, []any{runID}},
	}
	for _, query := range queries {
		if err := db.QueryRowContext(ctx, query.query, query.args...).Scan(query.dest); err != nil {
			t.Fatalf("load selected fork discard proof with %q: %v", query.query, err)
		}
	}
	return proof
}

func installSelectedForkDiscardFailure(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool) {
	t.Helper()
	if sqlite {
		if _, err := db.ExecContext(ctx, `CREATE TRIGGER fail_selected_discard BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT, 'injected selected discard failure'); END`); err != nil {
			t.Fatalf("create SQLite selected-discard failure trigger: %v", err)
		}
		return
	}
	if _, err := db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION fail_selected_discard_parity() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected selected discard failure'; END $$`); err != nil {
		t.Fatalf("create PostgreSQL selected-discard failure function: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER fail_selected_discard BEFORE DELETE ON events FOR EACH STATEMENT EXECUTE FUNCTION fail_selected_discard_parity()`); err != nil {
		t.Fatalf("create PostgreSQL selected-discard failure trigger: %v", err)
	}
}

func TestSelectedForkRetainedDiscardPublishesHistoricalTombstoneRevisionPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := admitTestPostgresStore(t, db)
	fixture := newSelectedCompletionFixture(t, store, db, false)
	ctx := testAuthorActivityContext()

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
		runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), newCompletionControllerForTest(store)),
		"selected:cleanup-preservation",
	)
	completionCtx = managedSelectedExecutionStoreTestContext(t, completionCtx, authority)
	completionCtx = withManagedCompletionTestSurface(t, completionCtx, authority, "openai_compatible")
	handle, err := beginManagedCompletionForTest(t, completionCtx, "openai_compatible", []byte("cleanup-preservation"))
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
	matrix := runForkRevisionMatrixFixture{
		runID:        fixture.forkRun,
		eventID:      "00000000-0000-0000-0000-000000002291",
		entityID:     "00000000-0000-0000-0000-000000002292",
		mutationID:   "00000000-0000-0000-0000-000000002293",
		deliveryID:   "00000000-0000-0000-0000-000000002294",
		receiptID:    "00000000-0000-0000-0000-000000002295",
		deadLetterID: "00000000-0000-0000-0000-000000002296",
		timerID:      "00000000-0000-0000-0000-000000002297",
		sessionID:    "00000000-0000-0000-0000-000000002298",
		turnID:       "00000000-0000-0000-0000-000000002299",
		auditID:      "00000000-0000-0000-0000-000000002300",
		replyID:      "retained-discard-reply",
		surfaceID:    "00000000-0000-0000-0000-000000002301",
		operationID:  "00000000-0000-0000-0000-000000002302",
		attemptID:    "00000000-0000-0000-0000-000000002303",
		authorityID:  "00000000-0000-0000-0000-000000002304",
		at:           time.Date(2026, 8, 25, 20, 0, 0, 0, time.UTC),
	}
	seedTestAgentRow(t, ctx, db, true, testAgentIdentity(t, "revision-matrix-agent", ""), "active")
	matrixRoute := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("matrix-node")),
		Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "matrix-flow", FlowInstance: "matrix-flow/one"}),
	}
	matrixEvent := eventtest.PersistedProjection(
		matrix.eventID, "selected.retained_history", "selected-test", "", json.RawMessage(`{"matrix":true}`),
		0, fixture.forkRun, "", events.EventEnvelope{}, matrix.at,
	)
	if err := commitSemanticEventFixtureWithRoutes(ctx, store, matrixEvent, []events.DeliveryRoute{matrixRoute}); err != nil {
		t.Fatalf("commit retained-discard event and delivery: %v", err)
	}
	seedTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin retained-discard history seed: %v", err)
	}
	seedRunForkRevisionMatrixFacts(t, ctx, seedTx, matrix, false, true)
	effects, err := runforkrevision.ForRun(fixture.forkRun, runforkrevision.AllFamilies()...)
	if err != nil {
		t.Fatalf("declare retained-discard history effects: %v", err)
	}
	seeded, err := runforkrevision.FinalizePostgres(ctx, seedTx, effects)
	if err != nil {
		_ = seedTx.Rollback()
		t.Fatalf("finalize retained-discard history: %v", err)
	}
	preDiscardRevision := seeded[fixture.forkRun].Revision
	if !seeded[fixture.forkRun].Changed {
		_ = seedTx.Rollback()
		t.Fatal("retained-discard history seed did not publish a revision")
	}
	if err := seedTx.Commit(); err != nil {
		t.Fatalf("commit retained-discard history: %v", err)
	}
	beforePlan, err := store.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: fixture.forkRun, At: matrix.eventID})
	if err != nil {
		t.Fatalf("plan retained event before discard: %v", err)
	}
	if _, err := transitionRunForTest(ctx, store, runtimerunlifecycle.ActiveTransitionRequest{
		RunID: fixture.forkRun, State: runtimerunlifecycle.StatePaused,
	}); err != nil {
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
	var terminalRevision, ledgerRows int64
	if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&terminalRevision); err != nil {
		t.Fatalf("load retained-discard revision head: %v", err)
	}
	if terminalRevision != preDiscardRevision+1 {
		t.Fatalf("retained-discard revision = %d, want one revision after %d", terminalRevision, preDiscardRevision)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&ledgerRows); err != nil {
		t.Fatalf("count retained-discard revision ledger: %v", err)
	}
	if ledgerRows != terminalRevision {
		t.Fatalf("retained-discard ledger rows = %d, want retained contiguous ledger through %d", ledgerRows, terminalRevision)
	}
	rows, err := db.QueryContext(ctx, `SELECT family FROM run_fork_fact_revisions WHERE run_id=$1::uuid AND revision=$2 AND NOT present ORDER BY family`, fixture.forkRun, terminalRevision)
	if err != nil {
		t.Fatalf("load retained-discard tombstone families: %v", err)
	}
	var tombstoneFamilies []runforkrevision.Family
	for rows.Next() {
		var family runforkrevision.Family
		if err := rows.Scan(&family); err != nil {
			_ = rows.Close()
			t.Fatalf("scan retained-discard tombstone family: %v", err)
		}
		tombstoneFamilies = append(tombstoneFamilies, family)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close retained-discard tombstone rows: %v", err)
	}
	wantTombstones := []runforkrevision.Family{
		runforkrevision.FamilyAgentSessions,
		runforkrevision.FamilyCommittedReplayScopes,
		runforkrevision.FamilyDeadLetters,
		runforkrevision.FamilyEntityMetadata,
		runforkrevision.FamilyEntityMutations,
		runforkrevision.FamilyEventDeliveries,
		runforkrevision.FamilyEventReceipts,
		runforkrevision.FamilyEvents,
		runforkrevision.FamilyTimers,
	}
	if !reflect.DeepEqual(tombstoneFamilies, wantTombstones) {
		t.Fatalf("retained-discard tombstone families = %q, want exact removed registry %q", tombstoneFamilies, wantTombstones)
	}
	validationTx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin retained-discard completeness validation: %v", err)
	}
	if err := runforkrevision.ValidateCompletePostgres(ctx, validationTx, fixture.forkRun); err != nil {
		_ = validationTx.Rollback()
		t.Fatalf("validate retained-discard terminal projection: %v", err)
	}
	_ = validationTx.Rollback()
	afterPlan, err := store.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: fixture.forkRun, At: matrix.eventID})
	if err != nil {
		t.Fatalf("plan retained historical event after discard: %v", err)
	}
	if afterPlan.ForkPoint != beforePlan.ForkPoint || afterPlan.EventCountAtFork != beforePlan.EventCountAtFork || afterPlan.ForkPoint.EventID != matrix.eventID {
		t.Fatalf("retained historical plan changed after discard: before=%#v after=%#v", beforePlan, afterPlan)
	}
}

func TestSelectedForkDiscardDeletesClaimedAndSettledDeliveryHistoryPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := admitTestPostgresStore(t, db)
	fixture := newSelectedCompletionFixture(t, store, db, false)
	ctx := testAuthorActivityContext()
	route := testAgentDeliveryRoute(t, "selected-agent", "fixture/selected-agent")
	eventsByState := []events.Event{
		eventtest.PersistedProjection(uuid.NewString(), "selected.claimed", "selected-test", "", json.RawMessage(`{}`), 0, fixture.forkRun, "", events.EventEnvelope{}, time.Now().UTC()),
		eventtest.PersistedProjection(uuid.NewString(), "selected.settled", "selected-test", "", json.RawMessage(`{}`), 0, fixture.forkRun, "", events.EventEnvelope{}, time.Now().UTC()),
	}
	for _, evt := range eventsByState {
		if err := commitSemanticEventFixtureWithRoutes(ctx, store, evt, []events.DeliveryRoute{route}); err != nil {
			t.Fatalf("commit selected-fork delivery %s: %v", evt.ID(), err)
		}
	}
	claimed, err := claimDeliveryFixture(ctx, store, eventsByState[0], route)
	if err != nil {
		t.Fatalf("claim selected-fork delivery: %v", err)
	}
	settled, err := claimDeliveryFixture(ctx, store, eventsByState[1], route)
	if err != nil {
		t.Fatalf("claim selected-fork settled delivery: %v", err)
	}
	if _, err := store.SettleSuccess(ctx, settled.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
		t.Fatalf("settle selected-fork delivery: %v", err)
	}
	if claimed.Claim.DeliveryID() == "" {
		t.Fatal("claimed selected-fork delivery has no durable identity")
	}
	if _, err := transitionRunForTest(ctx, store, runtimerunlifecycle.ActiveTransitionRequest{
		RunID: fixture.forkRun, State: runtimerunlifecycle.StatePaused,
	}); err != nil {
		t.Fatalf("mark selected fork materialized: %v", err)
	}
	if err := store.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun); err != nil {
		t.Fatalf("discard selected fork with delivery history: %v", err)
	}
	for label, query := range map[string]string{
		"deliveries":      `SELECT COUNT(*) FROM event_deliveries WHERE delivery_id IN ($1::uuid, $2::uuid)`,
		"rule selections": `SELECT COUNT(*) FROM event_delivery_handler_rule_selections WHERE delivery_id IN ($1::uuid, $2::uuid)`,
		"attempts":        `SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id IN ($1::uuid, $2::uuid)`,
		"outcomes":        `SELECT COUNT(*) FROM event_delivery_outcomes WHERE delivery_id IN ($1::uuid, $2::uuid)`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query, claimed.Snapshot.DeliveryID, settled.Snapshot.DeliveryID).Scan(&count); err != nil {
			t.Fatalf("count selected-fork %s: %v", label, err)
		}
		if count != 0 {
			t.Fatalf("selected-fork %s after discard = %d, want 0", label, count)
		}
	}
}

func TestSelectedForkDiscardLocksParentBeforeRevisionDeletionPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := admitTestPostgresStore(t, db)
	fixture := newSelectedCompletionFixture(t, store, db, false)
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
	defer cancel()

	issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
	if err != nil {
		t.Fatalf("issue selected completion authority: %v", err)
	}
	seedEventID := uuid.NewString()
	seedPostgresSemanticEventRecordFixture(t, ctx, db, seedEventID, fixture.forkRun, "selected.discard.seed", events.EventProducerPlatform, "selected-discard", "", "", time.Now().UTC())
	firstRevision := captureRunForkTestRevision(t, db, fixture.forkRun, runforkrevision.FamilyEvents)
	if _, err := transitionRunForTest(ctx, store, runtimerunlifecycle.ActiveTransitionRequest{
		RunID: fixture.forkRun, State: runtimerunlifecycle.StatePaused,
	}); err != nil {
		t.Fatalf("mark selected fork materialized: %v", err)
	}

	allocationTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin competing revision allocation: %v", err)
	}
	defer func() { _ = allocationTx.Rollback() }()
	concurrentEventID := uuid.NewString()
	seedPostgresSemanticEventRecordFixtureTx(t, ctx, allocationTx, concurrentEventID, fixture.forkRun, "selected.discard.concurrent", events.EventProducerPlatform, "selected-discard", "", "", time.Now().UTC())
	allocatedRevision, err := finalizePostgresRunForkTestRevision(ctx, allocationTx, fixture.forkRun, runforkrevision.FamilyEvents)
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
	waitForPostgresQueryLock(t, ctx, db, "SELECT run_id::text, status, bundle_hash, bundle_source")

	var status string
	var committedRevisionRows int
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&status); err != nil {
		t.Fatalf("load blocked discard run: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&committedRevisionRows); err != nil {
		t.Fatalf("count blocked discard revisions: %v", err)
	}
	if status != runfork.RunForkMaterializedStatus || committedRevisionRows != int(firstRevision) {
		t.Fatalf("blocked discard state = status:%q revisions:%d, want %q/%d", status, committedRevisionRows, runfork.RunForkMaterializedStatus, firstRevision)
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
	if status != runfork.RunForkMaterializedStatus || currentRevision != int(allocatedRevision) || revisionRows != int(allocatedRevision) || eventRows != 2 {
		t.Fatalf("failed discard partial state = status:%q head:%d ledger:%d events:%d, want %q/%d/%d/2", status, currentRevision, revisionRows, eventRows, runfork.RunForkMaterializedStatus, allocatedRevision, allocatedRevision)
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
	var terminalRevision, terminalLedgerRows, tombstoneRows int
	if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&terminalRevision); err != nil {
		t.Fatalf("load terminal revision head: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1::uuid`, fixture.forkRun).Scan(&terminalLedgerRows); err != nil {
		t.Fatalf("count terminal revision ledger: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1::uuid AND revision=$2 AND family='events' AND NOT present`, fixture.forkRun, terminalRevision).Scan(&tombstoneRows); err != nil {
		t.Fatalf("count terminal event tombstones: %v", err)
	}
	if terminalRevision != int(allocatedRevision)+1 || terminalLedgerRows != terminalRevision || tombstoneRows != 2 {
		t.Fatalf("terminal discard revision state = head:%d ledger:%d event_tombstones:%d, want %d/%d/2", terminalRevision, terminalLedgerRows, tombstoneRows, allocatedRevision+1, allocatedRevision+1)
	}
	var authorityRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1::uuid`, issued.ExecutionID).Scan(&authorityRows); err != nil {
		t.Fatalf("count retained selected authority: %v", err)
	}
	if authorityRows != 1 {
		t.Fatalf("retained selected authority rows = %d, want 1", authorityRows)
	}
}

func TestSelectedForkRetainedDiscardRollbackIncludesRevisionPublicationPostgres(t *testing.T) {
	for _, failure := range []struct {
		name      string
		statement string
	}{
		{
			name: "after_prior_domain_deletions",
			statement: `CREATE TRIGGER fail_selected_discard_domain
				BEFORE DELETE ON entity_state FOR EACH STATEMENT
				EXECUTE FUNCTION fail_selected_discard()`,
		},
		{
			name: "during_revision_finalization",
			statement: `CREATE TRIGGER fail_selected_discard_finalization
				BEFORE INSERT ON run_fork_fact_revisions FOR EACH STATEMENT
				EXECUTE FUNCTION fail_selected_discard()`,
		},
		{
			name: "during_deferred_commit",
			statement: `CREATE CONSTRAINT TRIGGER fail_selected_discard_commit
				AFTER INSERT ON run_fork_revisions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
				EXECUTE FUNCTION fail_selected_discard()`,
		},
	} {
		t.Run(failure.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			store := admitTestPostgresStore(t, db)
			fixture := newSelectedCompletionFixture(t, store, db, false)
			ctx := testAuthorActivityContext()
			issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
			if err != nil {
				t.Fatalf("issue retained selected execution: %v", err)
			}
			eventID := uuid.NewString()
			route := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("rollback-node")),
				Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "rollback", FlowInstance: "rollback/one"}),
			}
			event := eventtest.PersistedProjection(eventID, "selected.rollback", "selected-test", "", json.RawMessage(`{}`), 0, fixture.forkRun, "", events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixtureWithRoutes(ctx, store, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatalf("commit rollback event: %v", err)
			}
			if _, err := db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION fail_selected_discard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected selected discard failure'; END $$`); err != nil {
				t.Fatalf("create retained-discard failure function: %v", err)
			}
			if _, err := db.ExecContext(ctx, failure.statement); err != nil {
				t.Fatalf("create retained-discard failure trigger: %v", err)
			}

			before := loadSelectedForkDiscardRollbackState(t, ctx, db, fixture.forkRun, eventID, issued.ExecutionID)
			err = store.DiscardMaterializedSelectedContractExecutionFork(ctx, fixture.forkRun)
			if err == nil || !strings.Contains(err.Error(), "injected selected discard failure") {
				t.Fatalf("retained-discard failure = %v, want injected rollback error", err)
			}
			after := loadSelectedForkDiscardRollbackState(t, ctx, db, fixture.forkRun, eventID, issued.ExecutionID)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("retained-discard rollback state changed:\nbefore=%#v\nafter=%#v", before, after)
			}
		})
	}
}

type selectedForkDiscardRollbackState struct {
	Status        string
	Events        int
	Deliveries    int
	ExecutionRows int
	HeadRevision  int64
	RevisionRows  int
	RevisionFacts int
}

func loadSelectedForkDiscardRollbackState(t *testing.T, ctx context.Context, db *sql.DB, runID, eventID, executionID string) selectedForkDiscardRollbackState {
	t.Helper()
	var state selectedForkDiscardRollbackState
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1::uuid`, runID).Scan(&state.Status); err != nil {
		t.Fatalf("load retained-discard rollback run: %v", err)
	}
	queries := []struct {
		query string
		args  []any
		dest  *int
	}{
		{`SELECT COUNT(*) FROM events WHERE run_id=$1::uuid AND event_id=$2::uuid`, []any{runID, eventID}, &state.Events},
		{`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1::uuid AND event_id=$2::uuid`, []any{runID, eventID}, &state.Deliveries},
		{`SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1::uuid AND execution_id=$2::uuid`, []any{runID, executionID}, &state.ExecutionRows},
		{`SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1::uuid`, []any{runID}, &state.RevisionRows},
		{`SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1::uuid`, []any{runID}, &state.RevisionFacts},
	}
	for _, query := range queries {
		if err := db.QueryRowContext(ctx, query.query, query.args...).Scan(query.dest); err != nil {
			t.Fatalf("load retained-discard rollback state with %q: %v", query.query, err)
		}
	}
	if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, runID).Scan(&state.HeadRevision); err != nil {
		t.Fatalf("load retained-discard rollback revision head: %v", err)
	}
	return state
}

func TestSelectedForkDiscardRejectsLiveDependentForkPostgres(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	now := time.Now().UTC()
	sourceRunID := uuid.NewString()
	forkRunID := uuid.NewString()
	dependentRunID := uuid.NewString()
	forkEventID := uuid.NewString()
	requireRunningRunForTest(t, ctx, store, sourceRunID, now)
	requirePausedRunForTest(t, ctx, store, forkRunID, now)
	seedPostgresSemanticEventRecordFixture(t, ctx, db, forkEventID, forkRunID, "fork.dependency", events.EventProducerPlatform, "selected-discard", "", "", now)
	captureRunForkTestRevision(t, db, forkRunID, runforkrevision.FamilyEvents)
	dependent, err := store.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
		SourceRunID: forkRunID,
		At:          forkEventID,
	})
	if err != nil {
		t.Fatalf("materialize dependent fork: %v", err)
	}
	dependentRunID = dependent.ForkRunID

	err = store.DiscardMaterializedSelectedContractExecutionFork(ctx, forkRunID)
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
	ctx := testAuthorActivityContext()
	now := time.Now().UTC()
	sourceRun := uuid.NewString()
	forkRun := uuid.NewString()
	eventID := uuid.NewString()
	bindingID := uuid.NewString()
	registrar, ok := any(store).(testAuthorActivityCatalogRegistrar)
	if !ok {
		t.Fatal("selected completion fixture store has no author activity catalog")
	}
	registerTestAuthorActivityCatalog(t, registrar)
	requireRunningRunForTest(t, ctx, store, sourceRun, now)
	requirePausedRunForTest(t, ctx, store, forkRun, now)
	eventStore, ok := any(store).(semanticEventFixtureStore)
	if !ok {
		t.Fatal("selected completion fixture store has no event commit owner")
	}
	if err := commitSemanticEventFixture(ctx, eventStore, eventtest.PersistedProjection(
		eventID, events.EventType("selected.test"), "test", "", json.RawMessage(`{}`), 0,
		sourceRun, "", events.EventEnvelope{}, now,
	)); err != nil {
		t.Fatalf("seed selected event: %v", err)
	}
	if sqlite {
		if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings (binding_id,fork_run_id,source_run_id,fork_event_id,mode,contracts_root,workflow_name,workflow_version,created_at) VALUES (?,?,?,?,'selected_contracts','/tmp/contracts','workflow','v1',?)`, bindingID, forkRun, sourceRun, eventID, now); err != nil {
			t.Fatalf("seed selected binding: %v", err)
		}
	} else {
		if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings (binding_id,fork_run_id,source_run_id,fork_event_id,mode,contracts_root,workflow_name,workflow_version,created_at) VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,'selected_contracts','/tmp/contracts','workflow','v1',$5)`, bindingID, forkRun, sourceRun, eventID, now); err != nil {
			t.Fatalf("seed selected binding: %v", err)
		}
	}
	selection := runfork.RunForkContractSelection{Mode: "selected_contracts", ContractsRoot: "/tmp/contracts", WorkflowName: "workflow", WorkflowVersion: "v1"}
	admission := runfork.RunForkSelectedContractExecutionAdmission{
		Owner: runfork.RunForkSelectedContractExecutionAdmissionOwner, FutureExecutionOwner: runfork.RunForkSelectedContractExecutionOwner,
		NonMutating: true, ExecutionSupported: false, ForkRunID: forkRun, SourceRunID: sourceRun, ForkEventID: eventID,
		ContractSelection: selection, ContractBindingOwner: runfork.RunForkSelectedContractBindingOwner,
		AdmissionOwner: "runtime.run_fork.frontier", AdmissionUse: runfork.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
		ExecutionModelOwner: runfork.RunForkSelectedContractExecutionModelOwner, SourceWorkflowName: "workflow", SourceWorkflowVersion: "v1",
		DeferredWorkAdmissionOwner: runfork.RunForkSelectedContractDeferredWorkAdmissionOwner,
	}
	return selectedCompletionFixture{
		store: store, db: db, sqlite: sqlite, sourceRun: sourceRun, forkRun: forkRun, eventID: eventID, admission: admission,
		request: runfork.SelectedContractRuntimeExecutionIssueRequest{
			Admission: admission, ContainerPlanFingerprint: "sha256:container", ActorCensusFingerprint: "sha256:actors",
			EffectiveConfigFingerprint: "sha256:config", ExecutionMode: runtimeeffects.ExecutionModeLive, Now: now,
		},
	}
}

func selectedAgentTurnTarget(runID string) runtimeeffects.UsageTarget {
	identity := mustTestAgentIdentity("selected-agent", "selected-test")
	return runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: runID,
		AgentID: "selected-agent", AgentIdentity: identity, SessionID: uuid.NewString(),
		Memory: agentmemory.PlatformDefault(), FlowInstance: "selected-test",
	}
}

func settleSelectedCompletionForTest(t *testing.T, ctx context.Context, handle *runtimeeffects.Handle, target runtimeeffects.UsageTarget, now time.Time) {
	t.Helper()
	input, output := int64(8), int64(3)
	settlement := runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: runtimeeffects.StateSettled, Evidence: map[string]any{"test": true}},
		Usage: runtimeeffects.CompletionUsage{
			ResolvedModel: "test-model", Exactness: runtimeeffects.CompletionUsageExact,
			InputTokens: &input, OutputTokens: &output,
		},
		AgentTurn: &runtimeeffects.CompletionAgentTurn{
			TurnID: target.ID, RunID: target.RunID, AgentID: target.AgentID, SessionID: target.SessionID,
			Identity: agentmemory.Identity{RunID: target.RunID, Agent: target.AgentIdentity},
			Memory:   target.Memory, FlowInstance: target.FlowInstance, ParseOK: true,
		},
		Spend: runtimeeffects.CompletionSpend{
			FlowInstance: target.FlowInstance, AgentID: target.AgentID, AgentIdentity: target.AgentIdentity,
			Model: "test-model", ModelAlias: "regular",
			BackendProfile: "test", Provider: "test", Transport: "http", ResolvedModel: "test-model", CostUSD: 0.01,
			InvocationType: "agent_turn",
		},
		Now: now,
	}
	applyManagedCompletionContextSurface(t, ctx, settlement.AgentTurn)
	_, err := handle.SettleCompletion(ctx, settlement)
	if err != nil {
		t.Fatalf("settle selected completion: %v", err)
	}
}
