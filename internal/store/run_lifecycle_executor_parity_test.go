package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type runLifecycleCandidateInterceptStore struct {
	delegate runtimerunlifecycle.CandidateStore

	blockFirstList chan struct{}
	releaseList    chan struct{}
	executed       chan runtimerunlifecycle.Candidate
	listOnce       sync.Once
}

type runLifecycleExecutorImmediateClock struct{}

func (runLifecycleExecutorImmediateClock) Now() time.Time {
	return time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
}

func (runLifecycleExecutorImmediateClock) After(time.Duration) <-chan time.Time {
	ready := make(chan time.Time, 1)
	ready <- time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	return ready
}

func (s *runLifecycleCandidateInterceptStore) ListCompletionCandidates(
	ctx context.Context,
	scope runtimerunlifecycle.CandidateScope,
	cursor runtimerunlifecycle.CandidateCursor,
	limit int,
) (runtimerunlifecycle.CandidatePage, error) {
	if s.blockFirstList != nil && cursor.RunID == "" {
		s.listOnce.Do(func() { close(s.blockFirstList) })
		select {
		case <-ctx.Done():
			return runtimerunlifecycle.CandidatePage{}, context.Cause(ctx)
		case <-s.releaseList:
		}
	}
	return s.delegate.ListCompletionCandidates(ctx, scope, cursor, limit)
}

func (s *runLifecycleCandidateInterceptStore) ExecuteCompletionCandidate(
	ctx context.Context,
	candidate runtimerunlifecycle.Candidate,
	_ runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	select {
	case <-ctx.Done():
		return runtimerunlifecycle.CompletionResult{}, context.Cause(ctx)
	case s.executed <- candidate:
		return runtimerunlifecycle.CompletionResult{Outcome: runtimerunlifecycle.OutcomeAwaitMutation}, nil
	}
}

func TestRunLifecycleExecutorNoGapParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			registrar, ok := fixture.store.(runtimerunlifecycle.CandidateRegistrar)
			if !ok {
				t.Fatalf("%s store does not expose candidate registration", backend)
			}
			baseCtx := testAuthorActivityBundleSourceContext()
			runID := uuid.NewString()
			startedAt := time.Date(2026, 7, 29, 16, 0, 0, 0, time.UTC)
			ensureRunLifecycleCandidateParityRun(t, fixture, baseCtx, runID, startedAt)
			if got, err := transitionRunLifecycleParity(
				fixture, baseCtx, runID, runtimerunlifecycle.StatePaused,
			); err != nil || got != runtimerunlifecycle.MutationApplied {
				t.Fatalf("prepare paused run = %s/%v", got, err)
			}

			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(baseCtx, occurrence)
			intercept := &runLifecycleCandidateInterceptStore{
				delegate:       fixture.store,
				blockFirstList: make(chan struct{}),
				releaseList:    make(chan struct{}),
				executed:       make(chan runtimerunlifecycle.Candidate, 4),
			}
			executor := newRunLifecycleParityExecutor(t, intercept, occurrence)
			registration, err := registrar.RegisterCompletionCandidateSink(
				runtimeCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
				executor,
			)
			if err != nil {
				t.Fatalf("register candidate executor: %v", err)
			}

			started := make(chan error, 1)
			go func() { started <- executor.Start(runtimeCtx) }()
			awaitRunLifecycleSignal(t, intercept.blockFirstList, "startup enumeration")

			if got, err := transitionRunLifecycleParity(
				fixture, runtimeCtx, runID, runtimerunlifecycle.StateRunning,
			); err != nil || got != runtimerunlifecycle.MutationApplied {
				t.Fatalf("resume during startup enumeration = %s/%v", got, err)
			}
			first := awaitRunLifecycleCandidate(t, intercept.executed, "pre-readiness candidate handoff")
			if first.RunID != runID {
				t.Fatalf("pre-readiness candidate run_id = %s, want %s", first.RunID, runID)
			}
			close(intercept.releaseList)
			if err := awaitRunLifecycleError(t, started, "candidate executor readiness"); err != nil {
				t.Fatalf("start candidate executor: %v", err)
			}
			if !executor.Ready() {
				t.Fatal("candidate executor did not publish readiness after complete enumeration")
			}

			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)

			if got, err := transitionRunLifecycleParity(
				fixture, baseCtx, runID, runtimerunlifecycle.StatePaused,
			); err != nil || got != runtimerunlifecycle.MutationApplied {
				t.Fatalf("pause after retirement = %s/%v", got, err)
			}
			if got, err := transitionRunLifecycleParity(
				fixture, baseCtx, runID, runtimerunlifecycle.StateRunning,
			); !errors.Is(err, worklifetime.ErrRetired) || got != "" {
				t.Fatalf("post-retirement resume = %s/%v, want retired rejection", got, err)
			}
			state, duePresent, _ := loadRunLifecycleCandidateFacts(t, fixture, baseCtx, runID)
			if state != string(runtimerunlifecycle.StatePaused) || duePresent {
				t.Fatalf("post-retirement rollback = state:%s due:%v", state, duePresent)
			}
			registration.Release()

			if got, err := transitionRunLifecycleParity(
				fixture, baseCtx, runID, runtimerunlifecycle.StateRunning,
			); err != nil || got != runtimerunlifecycle.MutationApplied {
				t.Fatalf("durable successor handoff = %s/%v", got, err)
			}
			successorProcess := worklifetime.NewProcess()
			successorOccurrence := newRunLifecycleExecutorOccurrence(t, successorProcess)
			successorCtx := worklifetime.WithRuntimeOccurrence(baseCtx, successorOccurrence)
			successorStore := &runLifecycleCandidateInterceptStore{
				delegate: fixture.store,
				executed: make(chan runtimerunlifecycle.Candidate, 2),
			}
			successor := newRunLifecycleParityExecutor(t, successorStore, successorOccurrence)
			successorRegistration, err := registrar.RegisterCompletionCandidateSink(
				successorCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
				successor,
			)
			if err != nil {
				t.Fatalf("register successor candidate executor: %v", err)
			}
			if err := successor.Start(successorCtx); err != nil {
				t.Fatalf("start successor candidate executor: %v", err)
			}
			recovered := awaitRunLifecycleCandidate(t, successorStore.executed, "successor startup rehydration")
			if recovered.RunID != runID {
				t.Fatalf("successor candidate run_id = %s, want %s", recovered.RunID, runID)
			}
			if err := successor.Retire(context.Background()); err != nil {
				t.Fatalf("retire successor candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, successorOccurrence)
			successorRegistration.Release()
			retireRunLifecycleProcess(t, process)
			retireRunLifecycleProcess(t, successorProcess)
		})
	}
}

func TestRunLifecycleCandidateCommitAcrossSinkRegistrationParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			registrar, ok := fixture.store.(runtimerunlifecycle.CandidateRegistrar)
			if !ok {
				t.Fatalf("%s store does not expose candidate registration", backend)
			}
			baseCtx := testAuthorActivityBundleSourceContext()
			runID := uuid.NewString()
			ensureRunLifecycleCandidateParityRun(
				t,
				fixture,
				baseCtx,
				runID,
				time.Date(2026, 7, 29, 16, 30, 0, 0, time.UTC),
			)

			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(baseCtx, occurrence)
			intercept := &runLifecycleCandidateInterceptStore{
				delegate: fixture.store,
				executed: make(chan runtimerunlifecycle.Candidate, 2),
			}
			executor := newRunLifecycleParityExecutor(t, intercept, occurrence)

			candidatePrepared := make(chan struct{})
			allowCommit := make(chan struct{})
			mutationDone := make(chan error, 1)
			go func() {
				mutationDone <- fixture.store.RunRuntimeMutationContext(runtimeCtx, func(txctx context.Context) error {
					disposition, err := fixture.store.RequestCompletionCandidate(
						txctx,
						runtimerunlifecycle.ImmediateCandidate(runID),
					)
					if err != nil {
						return err
					}
					if disposition != runtimerunlifecycle.CandidateRequested {
						return fmt.Errorf("candidate disposition = %s, want requested", disposition)
					}
					close(candidatePrepared)
					select {
					case <-txctx.Done():
						return context.Cause(txctx)
					case <-allowCommit:
						return nil
					}
				})
			}()
			awaitRunLifecycleSignal(t, candidatePrepared, "uncommitted candidate preparation")

			type registrationResult struct {
				registration runtimerunlifecycle.CandidateRegistration
				err          error
			}
			registered := make(chan registrationResult, 1)
			go func() {
				registration, err := registrar.RegisterCompletionCandidateSink(
					runtimeCtx,
					runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
					executor,
				)
				registered <- registrationResult{registration: registration, err: err}
			}()
			awaitRunLifecycleSinkInstallation(t, fixture, executor)
			select {
			case result := <-registered:
				if result.registration != nil {
					result.registration.Release()
				}
				t.Fatalf("registration returned before the pre-registration candidate transaction settled: %v", result.err)
			default:
			}

			close(allowCommit)
			if err := awaitRunLifecycleError(t, mutationDone, "candidate commit"); err != nil {
				t.Fatalf("commit candidate across sink registration: %v", err)
			}
			result := <-registered
			if result.err != nil {
				t.Fatalf("register candidate executor: %v", result.err)
			}
			defer result.registration.Release()

			if err := executor.Start(runtimeCtx); err != nil {
				t.Fatalf("start candidate executor: %v", err)
			}
			candidate := awaitRunLifecycleCandidate(t, intercept.executed, "post-registration reconciliation")
			if candidate.RunID != runID {
				t.Fatalf("reconciled candidate run_id = %s, want %s", candidate.RunID, runID)
			}

			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)
			retireRunLifecycleProcess(t, process)
		})
	}
}

func TestRunLifecycleDirectHandoffCommitAcrossSinkRegistrationParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			registrar, ok := fixture.store.(runtimerunlifecycle.CandidateRegistrar)
			if !ok {
				t.Fatalf("%s store does not expose candidate registration", backend)
			}
			baseCtx := testAuthorActivityBundleSourceContext()
			runID := uuid.NewString()
			ensureRunLifecycleCandidateParityRun(
				t,
				fixture,
				baseCtx,
				runID,
				time.Date(2026, 7, 29, 16, 45, 0, 0, time.UTC),
			)

			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(baseCtx, occurrence)
			intercept := &runLifecycleCandidateInterceptStore{
				delegate: fixture.store,
				executed: make(chan runtimerunlifecycle.Candidate, 2),
			}
			executor := newRunLifecycleParityExecutor(t, intercept, occurrence)
			registry := candidateRegistryForFixture(t, fixture)

			candidatePrepared := make(chan struct{})
			allowCommit := make(chan struct{})
			mutationDone := make(chan error, 1)
			go func() {
				handoff, err := reserveRunLifecycleCandidateHandoff(runtimeCtx)
				if err != nil {
					mutationDone <- err
					return
				}
				defer handoff.rollback()
				var tx *sql.Tx
				switch store := fixture.store.(type) {
				case *PostgresStore:
					tx, err = store.backend.db.BeginTx(runtimeCtx, nil)
				case *SQLiteRuntimeStore:
					tx, err = store.backend.db.BeginTx(runtimeCtx, nil)
				default:
					err = errors.New("unsupported direct candidate handoff store")
				}
				if err != nil {
					mutationDone <- err
					return
				}
				defer func() { _ = tx.Rollback() }()
				var result runtimerunlifecycle.CandidateRequestResult
				switch store := fixture.store.(type) {
				case *PostgresStore:
					result, err = requestPostgresCompletionCandidateTx(runtimeCtx, tx, runID, nil, false)
				case *SQLiteRuntimeStore:
					result, err = requestSQLiteCompletionCandidateTx(runtimeCtx, tx, runID, nil, store.now(), false)
				}
				if err == nil {
					err = handoff.prepare(registry, result)
				}
				if err != nil {
					mutationDone <- err
					return
				}
				close(candidatePrepared)
				select {
				case <-runtimeCtx.Done():
					mutationDone <- context.Cause(runtimeCtx)
					return
				case <-allowCommit:
				}
				if err := tx.Commit(); err != nil {
					mutationDone <- err
					return
				}
				mutationDone <- handoff.commit()
			}()
			awaitRunLifecycleSignal(t, candidatePrepared, "uncommitted direct candidate preparation")

			type registrationResult struct {
				registration runtimerunlifecycle.CandidateRegistration
				err          error
			}
			registered := make(chan registrationResult, 1)
			go func() {
				registration, err := registrar.RegisterCompletionCandidateSink(
					runtimeCtx,
					runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
					executor,
				)
				registered <- registrationResult{registration: registration, err: err}
			}()
			awaitRunLifecycleSinkInstallation(t, fixture, executor)
			select {
			case result := <-registered:
				if result.registration != nil {
					result.registration.Release()
				}
				t.Fatalf("registration returned before the direct candidate transaction settled: %v", result.err)
			default:
			}

			close(allowCommit)
			if err := awaitRunLifecycleError(t, mutationDone, "direct candidate commit"); err != nil {
				t.Fatalf("commit direct candidate across sink registration: %v", err)
			}
			result := <-registered
			if result.err != nil {
				t.Fatalf("register candidate executor: %v", result.err)
			}
			defer result.registration.Release()

			if err := executor.Start(runtimeCtx); err != nil {
				t.Fatalf("start candidate executor: %v", err)
			}
			candidate := awaitRunLifecycleCandidate(t, intercept.executed, "direct post-registration reconciliation")
			if candidate.RunID != runID {
				t.Fatalf("reconciled direct candidate run_id = %s, want %s", candidate.RunID, runID)
			}

			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)
			retireRunLifecycleProcess(t, process)
		})
	}
}

func TestPostgresPipelineCompletionHandoffSurvivesPostCommitCleanupError(t *testing.T) {
	for _, operation := range []string{"mark_decision_processed", "settle"} {
		operation := operation
		t.Run(operation, func(t *testing.T) {
			fixture := openPostgresAuthorActivityReceiptFixture(t)
			selected, ok := fixture.store.(*PostgresStore)
			if !ok {
				t.Fatalf("fixture store = %T, want *PostgresStore", fixture.store)
			}
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Minute))
			owner := selected.PipelineObligations()

			var claim runtimepipelineobligation.Claim
			var settlementOutcome runtimepipelineobligation.SettlementOutcome
			switch operation {
			case "mark_decision_processed":
				insertProducerIdentityDecisionObligation(t, fixture, ctx, eventID, runID, time.Now().UTC().Add(-time.Minute))
				work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeDecisionRoute)
				if err != nil {
					t.Fatalf("claim decision route: %v", err)
				}
				claim = work.Claim
			case "settle":
				work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
				if err != nil {
					t.Fatalf("claim recovery work: %v", err)
				}
				claim = work.Claim
			default:
				t.Fatalf("unsupported operation %q", operation)
			}

			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(ctx, occurrence)
			intercept := &runLifecycleCandidateInterceptStore{
				delegate: selected,
				executed: make(chan runtimerunlifecycle.Candidate, 2),
			}
			executor := newRunLifecycleParityExecutor(t, intercept, occurrence)
			registration, err := selected.RegisterCompletionCandidateSink(
				runtimeCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
				executor,
			)
			if err != nil {
				t.Fatalf("register candidate executor: %v", err)
			}
			defer registration.Release()
			if err := executor.Start(runtimeCtx); err != nil {
				t.Fatalf("start candidate executor: %v", err)
			}

			state, err := selected.postgresPipelineClaimState(claim)
			if err != nil {
				t.Fatalf("load pipeline claim state: %v", err)
			}
			injectedErr := errors.New("injected post-commit session cleanup failure")
			session := state.postgresLease.session
			session.mu.Lock()
			session.testEndTxError = func() error { return injectedErr }
			session.mu.Unlock()

			switch operation {
			case "mark_decision_processed":
				err = owner.MarkDecisionProcessed(runtimeCtx, claim)
			case "settle":
				settlementOutcome, err = owner.Settle(runtimeCtx, claim, runtimepipelineobligation.Acknowledged("processed"))
			}
			session.mu.Lock()
			session.testEndTxError = nil
			session.mu.Unlock()
			if !errors.Is(err, injectedErr) {
				t.Fatalf("%s error = %v, want injected cleanup failure", operation, err)
			}
			if operation == "settle" && (!settlementOutcome.Committed() || !settlementOutcome.DeliveryHandoffCommitted()) {
				t.Fatalf("settlement outcome after cleanup failure = committed:%v handoff:%v, want true/true",
					settlementOutcome.Committed(), settlementOutcome.DeliveryHandoffCommitted())
			}

			candidate := awaitRunLifecycleCandidate(t, intercept.executed, operation+" live candidate handoff")
			if candidate.RunID != runID {
				t.Fatalf("%s candidate run_id = %s, want %s", operation, candidate.RunID, runID)
			}
			lifecycleFixture := runLifecycleCandidateParityFixture{
				store: selected, db: fixture.db, postgres: true,
			}
			stateName, duePresent, revision := loadRunLifecycleCandidateFacts(
				t, lifecycleFixture, ctx, runID,
			)
			if stateName != string(runtimerunlifecycle.StateRunning) || !duePresent || revision != candidate.Revision {
				t.Fatalf(
					"%s durable candidate = state:%s due:%v revision:%d, want running/true/%d",
					operation, stateName, duePresent, revision, candidate.Revision,
				)
			}

			if operation == "settle" {
				if _, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery); !errors.Is(err, runtimepipelineobligation.ErrIneligible) {
					t.Fatalf("settled claim remained eligible after cleanup failure: %v", err)
				}
			} else {
				count, outcome, reason := readExactPipelineReceipt(t, ctx, fixture, eventID)
				if count != 1 || outcome != "success" || reason != "decision_route_processed" {
					t.Fatalf(
						"decision route receipt = count:%d outcome:%q reason:%q",
						count, outcome, reason,
					)
				}
				if err := owner.Release(ctx, claim); err != nil {
					t.Fatalf("release processed decision claim: %v", err)
				}
			}

			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire candidate executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)
			retireRunLifecycleProcess(t, process)
		})
	}
}

func candidateRegistryForFixture(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
) *runLifecycleCandidateSinkRegistry {
	t.Helper()
	switch store := fixture.store.(type) {
	case *PostgresStore:
		return &store.runLifecycleSinks
	case *SQLiteRuntimeStore:
		return &store.runLifecycleSinks
	default:
		t.Fatal("unsupported candidate registry store")
		return nil
	}
}

func awaitRunLifecycleSinkInstallation(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	sink runtimerunlifecycle.CandidateSink,
) {
	t.Helper()
	registry := candidateRegistryForFixture(t, fixture)
	deadline := time.Now().Add(5 * time.Second)
	for {
		registry.mu.Lock()
		entry := registry.entries[runLifecycleCandidateParityBundleHash]
		installed := entry != nil && entry.sink == sink
		registry.mu.Unlock()
		if installed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for candidate sink installation")
		}
		runtime.Gosched()
	}
}

func newRunLifecycleExecutorOccurrence(
	t *testing.T,
	process *worklifetime.Process,
) *worklifetime.RuntimeOccurrence {
	t.Helper()
	occurrence, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: uuid.NewString(),
		BundleHash:        runLifecycleCandidateParityBundleHash,
	})
	if err != nil {
		t.Fatalf("create candidate runtime occurrence: %v", err)
	}
	return occurrence
}

func newRunLifecycleParityExecutor(
	t *testing.T,
	store runtimerunlifecycle.CandidateStore,
	occurrence *worklifetime.RuntimeOccurrence,
) *runtimerunlifecycle.Executor {
	t.Helper()
	executor, err := runtimerunlifecycle.NewExecutor(
		store,
		runtimerunlifecycle.CandidateScope{BundleHash: runLifecycleCandidateParityBundleHash},
		runtimerunlifecycle.TerminalCatalog{},
		occurrence,
		runtimerunlifecycle.ExecutorOptions{Clock: runLifecycleExecutorImmediateClock{}},
	)
	if err != nil {
		t.Fatalf("create candidate executor: %v", err)
	}
	return executor
}

func retireRunLifecycleExecutorOccurrence(t *testing.T, occurrence *worklifetime.RuntimeOccurrence) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := occurrence.RetireAndWait(ctx); err != nil {
		t.Fatalf("retire candidate runtime occurrence: %v", err)
	}
}

func retireRunLifecycleProcess(t *testing.T, process *worklifetime.Process) {
	t.Helper()
	process.Retire()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := process.Join(ctx); err != nil {
		t.Fatalf("join candidate process owner: %v", err)
	}
}

func awaitRunLifecycleSignal(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func awaitRunLifecycleCandidate(
	t *testing.T,
	ch <-chan runtimerunlifecycle.Candidate,
	label string,
) runtimerunlifecycle.Candidate {
	t.Helper()
	select {
	case candidate := <-ch:
		return candidate
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
		return runtimerunlifecycle.Candidate{}
	}
}

func awaitRunLifecycleError(t *testing.T, ch <-chan error, label string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
		return nil
	}
}
