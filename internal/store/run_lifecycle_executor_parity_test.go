package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
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
