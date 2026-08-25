package runlifecycle

import (
	"context"
	"errors"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

const executorTestBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type executorTestStore struct {
	list    func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error)
	execute func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error)
}

func (s *executorTestStore) ListCompletionCandidates(
	ctx context.Context,
	scope CandidateScope,
	cursor CandidateCursor,
	limit int,
) (CandidatePage, error) {
	return s.list(ctx, scope, cursor, limit)
}

func (s *executorTestStore) ExecuteCompletionCandidate(
	ctx context.Context,
	candidate Candidate,
	catalog TerminalCatalog,
) (CompletionResult, error) {
	return s.execute(ctx, candidate, catalog)
}

type immediateRetryPolicy struct{}

func (immediateRetryPolicy) Delay(int) time.Duration { return 0 }

func TestExecutorRepresentsCandidateAcrossStartupEnumerationOverlap(t *testing.T) {
	candidate := executorTestCandidate(1)
	enumerating := make(chan struct{})
	releaseEnumeration := make(chan struct{})
	executed := make(chan Candidate, 2)
	var enumerateOnce sync.Once
	store := &executorTestStore{
		list: func(ctx context.Context, _ CandidateScope, cursor CandidateCursor, _ int) (CandidatePage, error) {
			if cursor.RunID != "" {
				return CandidatePage{Exhausted: true}, nil
			}
			enumerateOnce.Do(func() { close(enumerating) })
			select {
			case <-ctx.Done():
				return CandidatePage{}, context.Cause(ctx)
			case <-releaseEnumeration:
			}
			return CandidatePage{Candidates: []Candidate{candidate}, Next: CandidateCursor{RunID: candidate.RunID}, Exhausted: true}, nil
		},
		execute: func(_ context.Context, got Candidate, _ TerminalCatalog) (CompletionResult, error) {
			executed <- got
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{})

	started := make(chan error, 1)
	go func() { started <- executor.Start(context.Background()) }()
	receiveSignal(t, enumerating, "startup enumeration")

	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit candidate during startup enumeration: %v", err)
	}
	got := receiveValue(t, executed, "candidate execution")
	if got.RunID != candidate.RunID || got.Revision != candidate.Revision {
		t.Fatalf("executed candidate = %#v, want %#v", got, candidate)
	}
	close(releaseEnumeration)
	if err := receiveValue(t, started, "executor startup"); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if !executor.Ready() {
		t.Fatal("executor did not publish readiness after complete enumeration")
	}

	retireExecutorTestSubject(t, executor, occurrence)
}

func TestExecutorRetriesCurrentRevisionAndRearmsBeforeSettling(t *testing.T) {
	candidate := executorTestCandidate(1)
	completed := make(chan struct{})
	var (
		mu    sync.Mutex
		calls int
	)
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(_ context.Context, got Candidate, _ TerminalCatalog) (CompletionResult, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			switch calls {
			case 1:
				return CompletionResult{}, errors.New("transient selected-store failure")
			case 2:
				got.DueAt = got.DueAt.Add(time.Second)
				return CompletionResult{Outcome: OutcomeRearmAt, Candidate: got}, nil
			default:
				close(completed)
				return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
			}
		},
	}
	clock := &executorTestClock{now: candidate.DueAt.Add(2 * time.Second)}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{
		Clock:       clock,
		RetryPolicy: immediateRetryPolicy{},
	})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit candidate: %v", err)
	}
	receiveSignal(t, completed, "retry and rearm completion")
	mu.Lock()
	gotCalls := calls
	mu.Unlock()
	if gotCalls != 3 {
		t.Fatalf("candidate executions = %d, want 3", gotCalls)
	}

	retireExecutorTestSubject(t, executor, occurrence)
}

func TestExecutorRetirementJoinsAcceptedPersistenceAndRejectsNewAdmission(t *testing.T) {
	candidate := executorTestCandidate(1)
	executing := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan struct{})
	canceled := make(chan error, 1)
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(ctx context.Context, _ Candidate, _ TerminalCatalog) (CompletionResult, error) {
			close(executing)
			select {
			case <-ctx.Done():
				canceled <- context.Cause(ctx)
				return CompletionResult{}, context.Cause(ctx)
			case <-release:
			}
			close(completed)
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit candidate: %v", err)
	}
	receiveSignal(t, executing, "candidate execution")

	if err := executor.Retire(context.Background()); err != nil {
		t.Fatalf("retire executor: %v", err)
	}
	if _, err := executor.ReserveCompletionCandidate(context.Background()); !errors.Is(err, worklifetime.ErrRetired) {
		t.Fatalf("post-retirement admission error = %v, want %v", err, worklifetime.ErrRetired)
	}
	retired := make(chan error, 1)
	go func() {
		_, err := occurrence.RetireAndWait(context.Background())
		retired <- err
	}()
	select {
	case err := <-retired:
		t.Fatalf("runtime occurrence retired before persistence completed: %v", err)
	default:
	}
	close(release)
	select {
	case <-completed:
	case err := <-canceled:
		t.Fatalf("accepted persistence was canceled during retirement: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for candidate persistence completion")
	}
	if err := receiveValue(t, retired, "runtime occurrence retirement"); err != nil {
		t.Fatalf("retire runtime occurrence: %v", err)
	}
}

func TestExecutorRetirementRejectsZeroDelayRetryAfterActiveAttemptSettles(t *testing.T) {
	candidate := executorTestCandidate(1)
	executing := make(chan struct{})
	release := make(chan struct{})
	executions := make(chan int, 2)
	var calls atomic.Int64
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
			call := int(calls.Add(1))
			executions <- call
			if call == 1 {
				close(executing)
				<-release
				return CompletionResult{}, errors.New("retry after admitted attempt")
			}
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{RetryPolicy: immediateRetryPolicy{}})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit candidate: %v", err)
	}
	receiveSignal(t, executing, "candidate execution")
	if got := receiveValue(t, executions, "first candidate execution"); got != 1 {
		t.Fatalf("first candidate execution = %d, want 1", got)
	}

	if err := executor.Retire(context.Background()); err != nil {
		t.Fatalf("retire executor: %v", err)
	}
	close(release)
	retireRuntimeOccurrence(t, occurrence)
	select {
	case call := <-executions:
		t.Fatalf("candidate execution %d started after retirement", call)
	default:
	}
}

func TestExecutorRetirementRejectsElapsedRearmAfterActiveAttemptSettles(t *testing.T) {
	candidate := executorTestCandidate(1)
	executing := make(chan struct{})
	release := make(chan struct{})
	executions := make(chan int, 2)
	var calls atomic.Int64
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(_ context.Context, got Candidate, _ TerminalCatalog) (CompletionResult, error) {
			call := int(calls.Add(1))
			executions <- call
			if call == 1 {
				close(executing)
				<-release
				return CompletionResult{Outcome: OutcomeRearmAt, Candidate: got}, nil
			}
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	clock := &executorTestClock{now: candidate.DueAt.Add(time.Second)}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{Clock: clock})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit candidate: %v", err)
	}
	receiveSignal(t, executing, "candidate execution")
	if got := receiveValue(t, executions, "first candidate execution"); got != 1 {
		t.Fatalf("first candidate execution = %d, want 1", got)
	}

	if err := executor.Retire(context.Background()); err != nil {
		t.Fatalf("retire executor: %v", err)
	}
	close(release)
	retireRuntimeOccurrence(t, occurrence)
	select {
	case call := <-executions:
		t.Fatalf("candidate execution %d started after retirement", call)
	default:
	}
}

func TestExecutorRetirementIsContextBoundAndRejectsDelayedReservedSubmission(t *testing.T) {
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
			t.Fatal("retired executor executed a delayed reserved candidate")
			return CompletionResult{}, nil
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	admission, err := executor.ReserveCompletionCandidate(context.Background())
	if err != nil {
		t.Fatalf("reserve candidate admission: %v", err)
	}

	retireCtx, cancelRetire := context.WithCancel(context.Background())
	retired := make(chan error, 1)
	go func() { retired <- executor.Retire(retireCtx) }()
	retirementDeadline := time.NewTimer(5 * time.Second)
	defer retirementDeadline.Stop()
	for executor.Ready() {
		select {
		case <-retirementDeadline.C:
			t.Fatal("timed out waiting for executor retirement fence")
		default:
			goruntime.Gosched()
		}
	}
	if _, err := executor.ReserveCompletionCandidate(context.Background()); !errors.Is(err, worklifetime.ErrRetired) {
		t.Fatalf("post-retirement admission error = %v, want %v", err, worklifetime.ErrRetired)
	}
	cancelRetire()
	if err := receiveValue(t, retired, "bounded executor retirement"); !errors.Is(err, context.Canceled) {
		t.Fatalf("retire executor error = %v, want context cancellation", err)
	}
	if err := admission.Submit(executorTestCandidate(1)); !errors.Is(err, worklifetime.ErrRetired) {
		t.Fatalf("delayed reserved submission error = %v, want %v", err, worklifetime.ErrRetired)
	}
	retireRuntimeOccurrence(t, occurrence)
}

type executorTestClock struct {
	now time.Time
}

func (c *executorTestClock) Now() time.Time {
	return c.now
}

func (c *executorTestClock) After(time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- c.now
	return ch
}

func executorTestCandidate(revision int64) Candidate {
	return Candidate{
		RunID:      "11111111-1111-4111-8111-111111111111",
		BundleHash: executorTestBundleHash,
		Revision:   revision,
		DueAt:      time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func newExecutorTestSubject(
	t *testing.T,
	store CandidateStore,
	options ExecutorOptions,
) (*Executor, *worklifetime.RuntimeOccurrence) {
	t.Helper()
	process := worklifetime.NewProcess()
	occurrence, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "22222222-2222-4222-8222-222222222222",
		BundleHash:        executorTestBundleHash,
	})
	if err != nil {
		t.Fatalf("create runtime occurrence: %v", err)
	}
	executor, err := NewExecutor(
		store,
		CandidateScope{BundleHash: executorTestBundleHash},
		TerminalCatalog{},
		occurrence,
		options,
	)
	if err != nil {
		t.Fatalf("create executor: %v", err)
	}
	return executor, occurrence
}

func retireExecutorTestSubject(t *testing.T, executor *Executor, occurrence *worklifetime.RuntimeOccurrence) {
	t.Helper()
	if err := executor.Retire(context.Background()); err != nil {
		t.Fatalf("retire executor: %v", err)
	}
	retireRuntimeOccurrence(t, occurrence)
}

func retireRuntimeOccurrence(t *testing.T, occurrence *worklifetime.RuntimeOccurrence) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := occurrence.RetireAndWait(ctx); err != nil {
		t.Fatalf("retire runtime occurrence: %v", err)
	}
}

func receiveSignal(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func receiveValue[T any](t *testing.T, ch <-chan T, label string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		var zero T
		t.Fatalf("timed out waiting for %s", label)
		return zero
	}
}
