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

type blockingRetryPolicy struct {
	started chan struct{}
	release chan struct{}
}

type recordingGenericScheduleWakeupOwner struct {
	activationIDs chan string
	queued        bool
	err           error
}

func (o *recordingGenericScheduleWakeupOwner) ReconcileWakeupWithRecovery(_ context.Context, activationID string) (bool, error) {
	o.activationIDs <- activationID
	return o.queued, o.err
}

func TestExecutorHandsCommittedGenericScheduleToRecoveryBackedOwnerBeforeDroppingCandidate(t *testing.T) {
	candidate := executorTestCandidate(1)
	activation, err := NewCommittedGenericScheduleActivation("33333333-3333-4333-8333-333333333333")
	if err != nil {
		t.Fatal(err)
	}
	executions := make(chan struct{}, 1)
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
			executions <- struct{}{}
			return CompletionResult{Outcome: OutcomeAwaitMutation, GenericScheduleActivations: []CommittedGenericScheduleActivation{activation}}, nil
		},
	}
	wakeups := &recordingGenericScheduleWakeupOwner{activationIDs: make(chan string, 1)}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{GenericSchedules: wakeups})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}
	receiveSignal(t, executions, "candidate execution")
	if got := receiveValue(t, wakeups.activationIDs, "generic schedule handoff"); got != activation.ID() {
		t.Fatalf("generic schedule handoff = %s, want %s", got, activation.ID())
	}
	awaitExecutorCandidates(t, executor, 0)
	retireExecutorTestSubject(t, executor, occurrence)
}

func TestExecutorAcceptsRecoveryOwnershipAfterImmediateGenericScheduleProjectionFailure(t *testing.T) {
	activation, err := NewCommittedGenericScheduleActivation("44444444-4444-4444-8444-444444444444")
	if err != nil {
		t.Fatal(err)
	}
	var executions atomic.Int64
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
			executions.Add(1)
			return CompletionResult{Outcome: OutcomeAwaitMutation, GenericScheduleActivations: []CommittedGenericScheduleActivation{activation}}, nil
		},
	}
	wakeups := &recordingGenericScheduleWakeupOwner{
		activationIDs: make(chan string, 1), queued: true, err: errors.New("injected immediate projection failure"),
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{GenericSchedules: wakeups})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), executorTestCandidate(1)); err != nil {
		t.Fatal(err)
	}
	receiveValue(t, wakeups.activationIDs, "recovery-backed generic schedule handoff")
	awaitExecutorCandidates(t, executor, 0)
	if got := executions.Load(); got != 1 {
		t.Fatalf("candidate executions = %d, want one committed execution", got)
	}
	retireExecutorTestSubject(t, executor, occurrence)
}

func (p *blockingRetryPolicy) Delay(int) time.Duration {
	close(p.started)
	<-p.release
	return 0
}

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

func TestExecutorSameRevisionHandoffDuringAttemptForcesSerializedRecheck(t *testing.T) {
	for _, outcome := range []CompletionOutcome{
		OutcomeAwaitMutation,
		OutcomeExactNoop,
		OutcomeTerminallyEligible,
	} {
		outcome := outcome
		t.Run(string(outcome), func(t *testing.T) {
			candidate := executorTestCandidate(1)
			firstStarted := make(chan struct{})
			releaseFirst := make(chan struct{})
			secondCompleted := make(chan struct{})
			var calls atomic.Int64
			store := &executorTestStore{
				list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
					return CandidatePage{Exhausted: true}, nil
				},
				execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
					switch calls.Add(1) {
					case 1:
						close(firstStarted)
						<-releaseFirst
					case 2:
						close(secondCompleted)
					}
					return CompletionResult{Outcome: outcome}, nil
				},
			}
			executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{})
			if err := executor.Start(context.Background()); err != nil {
				t.Fatalf("start executor: %v", err)
			}
			if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
				t.Fatalf("submit first candidate: %v", err)
			}
			receiveSignal(t, firstStarted, "first candidate attempt")
			if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
				t.Fatalf("submit same-revision handoff: %v", err)
			}
			close(releaseFirst)
			receiveSignal(t, secondCompleted, "same-revision recheck")
			awaitExecutorCandidates(t, executor, 0)
			if got := calls.Load(); got != 2 {
				t.Fatalf("candidate executions = %d, want 2", got)
			}
			retireExecutorTestSubject(t, executor, occurrence)
		})
	}
}

func TestExecutorSameRevisionHandoffAfterResultBeforeRemovalForcesRecheck(t *testing.T) {
	candidate := executorTestCandidate(1)
	handoffResult := make(chan error, 1)
	secondCompleted := make(chan struct{})
	var calls atomic.Int64
	var executor *Executor
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
			if calls.Add(1) == 1 {
				// Return values are evaluated before deferred functions run. This
				// commits the handoff after the result exists but before the
				// executor can enter its result/removal transition.
				defer func() {
					handoffResult <- executor.SubmitCompletionCandidate(context.Background(), candidate)
				}()
			} else {
				close(secondCompleted)
			}
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	var occurrence *worklifetime.RuntimeOccurrence
	executor, occurrence = newExecutorTestSubject(t, store, ExecutorOptions{})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit first candidate: %v", err)
	}
	if err := receiveValue(t, handoffResult, "result/removal handoff"); err != nil {
		t.Fatalf("submit candidate in result/removal window: %v", err)
	}
	receiveSignal(t, secondCompleted, "result/removal recheck")
	awaitExecutorCandidates(t, executor, 0)
	if got := calls.Load(); got != 2 {
		t.Fatalf("candidate executions = %d, want 2", got)
	}
	retireExecutorTestSubject(t, executor, occurrence)
}

func TestExecutorSameRevisionHandoffAfterRemovalStartsNewChain(t *testing.T) {
	candidate := executorTestCandidate(1)
	executed := make(chan struct{}, 2)
	var calls atomic.Int64
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
			calls.Add(1)
			executed <- struct{}{}
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit first candidate: %v", err)
	}
	receiveSignal(t, executed, "first candidate execution")
	awaitExecutorCandidates(t, executor, 0)
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit post-removal candidate: %v", err)
	}
	receiveSignal(t, executed, "post-removal candidate execution")
	awaitExecutorCandidates(t, executor, 0)
	if got := calls.Load(); got != 2 {
		t.Fatalf("candidate executions = %d, want 2", got)
	}
	retireExecutorTestSubject(t, executor, occurrence)
}

func TestExecutorCoalescesSameRevisionNotificationsPerAttempt(t *testing.T) {
	candidate := executorTestCandidate(1)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseSecond := make(chan struct{})
	thirdCompleted := make(chan struct{})
	var calls atomic.Int64
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
			switch calls.Add(1) {
			case 1:
				close(firstStarted)
				<-releaseFirst
			case 2:
				close(secondStarted)
				<-releaseSecond
			case 3:
				close(thirdCompleted)
			}
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit first candidate: %v", err)
	}
	receiveSignal(t, firstStarted, "first candidate attempt")
	for i := 0; i < 5; i++ {
		if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
			t.Fatalf("submit coalesced notification %d: %v", i, err)
		}
	}
	close(releaseFirst)
	receiveSignal(t, secondStarted, "coalesced candidate recheck")
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit notification during recheck: %v", err)
	}
	close(releaseSecond)
	receiveSignal(t, thirdCompleted, "notification-during-recheck attempt")
	awaitExecutorCandidates(t, executor, 0)
	if got := calls.Load(); got != 3 {
		t.Fatalf("candidate executions = %d, want 3", got)
	}
	retireExecutorTestSubject(t, executor, occurrence)
}

func TestExecutorSerializesAndCollapsesNewerSuccessors(t *testing.T) {
	first := executorTestCandidate(1)
	second := executorTestCandidate(2)
	third := executorTestCandidate(3)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	thirdCompleted := make(chan struct{})
	executed := make(chan Candidate, 3)
	var active atomic.Int64
	var maxActive atomic.Int64
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(_ context.Context, candidate Candidate, _ TerminalCatalog) (CompletionResult, error) {
			current := active.Add(1)
			updateAtomicMax(&maxActive, current)
			defer active.Add(-1)
			executed <- candidate
			switch candidate.Revision {
			case 1:
				close(firstStarted)
				<-releaseFirst
			case 3:
				close(thirdCompleted)
			}
			return CompletionResult{Outcome: OutcomeExactNoop}, nil
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), first); err != nil {
		t.Fatalf("submit first candidate: %v", err)
	}
	receiveSignal(t, firstStarted, "first candidate attempt")
	if err := executor.SubmitCompletionCandidate(context.Background(), second); err != nil {
		t.Fatalf("submit second candidate: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), third); err != nil {
		t.Fatalf("submit third candidate: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), first); err != nil {
		t.Fatalf("submit stale first candidate: %v", err)
	}
	close(releaseFirst)
	receiveSignal(t, thirdCompleted, "newest successor attempt")
	awaitExecutorCandidates(t, executor, 0)
	close(executed)
	var revisions []int64
	for candidate := range executed {
		revisions = append(revisions, candidate.Revision)
	}
	if len(revisions) != 2 || revisions[0] != 1 || revisions[1] != 3 {
		t.Fatalf("executed revisions = %v, want [1 3]", revisions)
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("max concurrent same-run executions = %d, want 1", got)
	}
	retireExecutorTestSubject(t, executor, occurrence)
}

func TestExecutorRejectsConflictingSameRevisionIdentity(t *testing.T) {
	candidate := executorTestCandidate(1)
	started := make(chan struct{})
	release := make(chan struct{})
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
			close(started)
			<-release
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
	receiveSignal(t, started, "candidate attempt")
	conflicting := candidate
	conflicting.DueAt = conflicting.DueAt.Add(time.Microsecond)
	if err := executor.SubmitCompletionCandidate(context.Background(), conflicting); err == nil {
		t.Fatal("conflicting same-revision identity was accepted")
	}
	close(release)
	awaitExecutorCandidates(t, executor, 0)
	retireExecutorTestSubject(t, executor, occurrence)
}

func TestExecutorHandoffBeforeAttemptAdmissionIsRepresentedByThatAttempt(t *testing.T) {
	candidate := executorTestCandidate(1)
	candidate.DueAt = candidate.DueAt.Add(time.Hour)
	clock := &gatedExecutorTestClock{
		now:     candidate.DueAt.Add(-time.Hour),
		started: make(chan struct{}),
		release: make(chan time.Time),
	}
	executed := make(chan struct{})
	var calls atomic.Int64
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
			calls.Add(1)
			close(executed)
			return CompletionResult{Outcome: OutcomeExactNoop}, nil
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{Clock: clock})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit candidate: %v", err)
	}
	receiveSignal(t, clock.started, "future candidate wait")
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit pre-admission handoff: %v", err)
	}
	close(clock.release)
	receiveSignal(t, executed, "future candidate execution")
	awaitExecutorCandidates(t, executor, 0)
	if got := calls.Load(); got != 1 {
		t.Fatalf("candidate executions = %d, want 1", got)
	}
	retireExecutorTestSubject(t, executor, occurrence)
}

func TestExecutorAllowsDifferentRunsToExecuteConcurrently(t *testing.T) {
	first := executorTestCandidate(1)
	second := first
	second.RunID = "33333333-3333-4333-8333-333333333333"
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var active atomic.Int64
	var maxActive atomic.Int64
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
			current := active.Add(1)
			updateAtomicMax(&maxActive, current)
			started <- struct{}{}
			<-release
			active.Add(-1)
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), first); err != nil {
		t.Fatalf("submit first run: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), second); err != nil {
		t.Fatalf("submit second run: %v", err)
	}
	receiveSignal(t, started, "first concurrent run")
	receiveSignal(t, started, "second concurrent run")
	close(release)
	awaitExecutorCandidates(t, executor, 0)
	if got := maxActive.Load(); got != 2 {
		t.Fatalf("max concurrent different-run executions = %d, want 2", got)
	}
	retireExecutorTestSubject(t, executor, occurrence)
}

func TestExecutorCrossBundleSameRevisionRetainsNewScopeRepresentation(t *testing.T) {
	oldCandidate := executorTestCandidate(1)
	newBundleHash := "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	newCandidate := oldCandidate
	newCandidate.BundleHash = newBundleHash
	oldStarted := make(chan struct{})
	releaseOld := make(chan struct{})
	newCompleted := make(chan struct{})
	var rowLock sync.Mutex
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(_ context.Context, candidate Candidate, _ TerminalCatalog) (CompletionResult, error) {
			rowLock.Lock()
			defer rowLock.Unlock()
			if candidate.BundleHash == executorTestBundleHash {
				close(oldStarted)
				<-releaseOld
			} else {
				close(newCompleted)
			}
			return CompletionResult{Outcome: OutcomeExactNoop}, nil
		},
	}
	oldExecutor, oldOccurrence := newExecutorTestSubjectForBundle(t, store, ExecutorOptions{}, executorTestBundleHash)
	newExecutor, newOccurrence := newExecutorTestSubjectForBundle(t, store, ExecutorOptions{}, newBundleHash)
	if err := oldExecutor.Start(context.Background()); err != nil {
		t.Fatalf("start old executor: %v", err)
	}
	if err := newExecutor.Start(context.Background()); err != nil {
		t.Fatalf("start new executor: %v", err)
	}
	if err := oldExecutor.SubmitCompletionCandidate(context.Background(), oldCandidate); err != nil {
		t.Fatalf("submit old-bundle candidate: %v", err)
	}
	receiveSignal(t, oldStarted, "old-bundle execution")
	if err := newExecutor.SubmitCompletionCandidate(context.Background(), newCandidate); err != nil {
		t.Fatalf("submit new-bundle candidate: %v", err)
	}
	awaitExecutorCandidates(t, newExecutor, 1)
	select {
	case <-newCompleted:
		t.Fatal("new-bundle selected-store execution bypassed run-row serialization")
	default:
	}
	close(releaseOld)
	receiveSignal(t, newCompleted, "new-bundle retained execution")
	awaitExecutorCandidates(t, oldExecutor, 0)
	awaitExecutorCandidates(t, newExecutor, 0)
	retireExecutorTestSubject(t, oldExecutor, oldOccurrence)
	retireExecutorTestSubject(t, newExecutor, newOccurrence)
}

func TestExecutorRetriesCurrentRevisionAndRearmsBeforeSettling(t *testing.T) {
	candidate := executorTestCandidate(1)
	completed := make(chan struct{})
	var (
		mu         sync.Mutex
		calls      int
		candidates []Candidate
	)
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(_ context.Context, got Candidate, _ TerminalCatalog) (CompletionResult, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			candidates = append(candidates, got)
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
	gotCandidates := append([]Candidate(nil), candidates...)
	mu.Unlock()
	if gotCalls != 3 {
		t.Fatalf("candidate executions = %d, want 3", gotCalls)
	}
	if gotCandidates[2].DueAt != candidate.DueAt.Add(time.Second) {
		t.Fatalf("rearmed due_at = %s, want %s", gotCandidates[2].DueAt, candidate.DueAt.Add(time.Second))
	}

	retireExecutorTestSubject(t, executor, occurrence)
}

func TestExecutorDirtyNotificationSurvivesRetryAndRearmResults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result func(Candidate) CompletionResult
		want   func(Candidate) Candidate
	}{
		{
			name: "retry_current",
			result: func(Candidate) CompletionResult {
				return CompletionResult{Outcome: OutcomeRetryCurrent, Retryable: errors.New("retry")}
			},
			want: func(candidate Candidate) Candidate { return candidate },
		},
		{
			name:   "invalid_result_converted_to_retry",
			result: func(Candidate) CompletionResult { return CompletionResult{} },
			want:   func(candidate Candidate) Candidate { return candidate },
		},
		{
			name: "rearm_at_same_revision",
			result: func(candidate Candidate) CompletionResult {
				candidate.DueAt = candidate.DueAt.Add(time.Microsecond)
				return CompletionResult{Outcome: OutcomeRearmAt, Candidate: candidate}
			},
			want: func(candidate Candidate) Candidate {
				candidate.DueAt = candidate.DueAt.Add(time.Microsecond)
				return candidate
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := executorTestCandidate(1)
			firstStarted := make(chan struct{})
			releaseFirst := make(chan struct{})
			secondExecuted := make(chan Candidate, 1)
			var calls atomic.Int64
			store := &executorTestStore{
				list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
					return CandidatePage{Exhausted: true}, nil
				},
				execute: func(_ context.Context, got Candidate, _ TerminalCatalog) (CompletionResult, error) {
					if calls.Add(1) == 1 {
						close(firstStarted)
						<-releaseFirst
						return tc.result(got), nil
					}
					secondExecuted <- got
					return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
				},
			}
			executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{RetryPolicy: immediateRetryPolicy{}})
			if err := executor.Start(context.Background()); err != nil {
				t.Fatalf("start executor: %v", err)
			}
			if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
				t.Fatalf("submit first candidate: %v", err)
			}
			receiveSignal(t, firstStarted, "first candidate attempt")
			if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
				t.Fatalf("submit dirty notification: %v", err)
			}
			close(releaseFirst)
			got := receiveValue(t, secondExecuted, "dirty-result recheck")
			if want := tc.want(candidate); !got.SameIdentity(want) {
				t.Fatalf("recheck candidate = %#v, want %#v", got, want)
			}
			awaitExecutorCandidates(t, executor, 0)
			if got := calls.Load(); got != 2 {
				t.Fatalf("candidate executions = %d, want 2", got)
			}
			retireExecutorTestSubject(t, executor, occurrence)
		})
	}
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

func TestExecutorRetirementDropsDirtyGenerationAndPendingSuccessor(t *testing.T) {
	candidate := executorTestCandidate(1)
	successor := candidate
	successor.Revision++
	executing := make(chan struct{})
	release := make(chan struct{})
	executions := make(chan Candidate, 3)
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(_ context.Context, got Candidate, _ TerminalCatalog) (CompletionResult, error) {
			executions <- got
			if got.SameIdentity(candidate) {
				close(executing)
				<-release
			}
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
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit dirty notification: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), successor); err != nil {
		t.Fatalf("submit pending successor: %v", err)
	}

	if err := executor.Retire(context.Background()); err != nil {
		t.Fatalf("retire executor: %v", err)
	}
	close(release)
	retireRuntimeOccurrence(t, occurrence)
	if got := receiveValue(t, executions, "first candidate execution"); !got.SameIdentity(candidate) {
		t.Fatalf("first candidate execution = %#v, want %#v", got, candidate)
	}
	select {
	case got := <-executions:
		t.Fatalf("candidate execution %#v started after retirement fence", got)
	default:
	}
	if got := executor.ActiveCandidates(); got != 0 {
		t.Fatalf("active candidates after retirement = %d, want 0", got)
	}
}

func TestExecutorRetirementDuringRetryDelayRejectsSuccessorAttempt(t *testing.T) {
	candidate := executorTestCandidate(1)
	executions := make(chan int, 2)
	policy := &blockingRetryPolicy{started: make(chan struct{}), release: make(chan struct{})}
	var calls atomic.Int64
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
			call := int(calls.Add(1))
			executions <- call
			if call == 1 {
				return CompletionResult{}, errors.New("retry after admitted attempt")
			}
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{RetryPolicy: policy})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit candidate: %v", err)
	}
	receiveSignal(t, policy.started, "retry policy evaluation")
	if got := receiveValue(t, executions, "first candidate execution"); got != 1 {
		t.Fatalf("first candidate execution = %d, want 1", got)
	}

	if err := executor.Retire(context.Background()); err != nil {
		t.Fatalf("retire executor: %v", err)
	}
	close(policy.release)
	retireRuntimeOccurrence(t, occurrence)
	select {
	case call := <-executions:
		t.Fatalf("candidate execution %d started after retirement during retry evaluation", call)
	default:
	}
}

func TestExecutorRetirementDuringRearmDueEvaluationRejectsSuccessorAttempt(t *testing.T) {
	candidate := executorTestCandidate(1)
	executions := make(chan int, 2)
	clock := &blockingSecondNowClock{
		now:           candidate.DueAt.Add(time.Second),
		secondStarted: make(chan struct{}),
		releaseSecond: make(chan struct{}),
	}
	var calls atomic.Int64
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(_ context.Context, got Candidate, _ TerminalCatalog) (CompletionResult, error) {
			call := int(calls.Add(1))
			executions <- call
			if call == 1 {
				return CompletionResult{Outcome: OutcomeRearmAt, Candidate: got}, nil
			}
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{Clock: clock})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("submit candidate: %v", err)
	}
	receiveSignal(t, clock.secondStarted, "rearm due-time evaluation")
	if got := receiveValue(t, executions, "first candidate execution"); got != 1 {
		t.Fatalf("first candidate execution = %d, want 1", got)
	}

	if err := executor.Retire(context.Background()); err != nil {
		t.Fatalf("retire executor: %v", err)
	}
	close(clock.releaseSecond)
	retireRuntimeOccurrence(t, occurrence)
	select {
	case call := <-executions:
		t.Fatalf("candidate execution %d started after retirement during rearm evaluation", call)
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

type gatedExecutorTestClock struct {
	now     time.Time
	started chan struct{}
	release chan time.Time
	once    sync.Once
}

func (c *gatedExecutorTestClock) Now() time.Time { return c.now }

func (c *gatedExecutorTestClock) After(time.Duration) <-chan time.Time {
	c.once.Do(func() { close(c.started) })
	return c.release
}

func (c *executorTestClock) Now() time.Time {
	return c.now
}

func (c *executorTestClock) After(time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- c.now
	return ch
}

type blockingSecondNowClock struct {
	now           time.Time
	secondStarted chan struct{}
	releaseSecond chan struct{}
	calls         atomic.Int64
}

func (c *blockingSecondNowClock) Now() time.Time {
	if c.calls.Add(1) == 2 {
		close(c.secondStarted)
		<-c.releaseSecond
	}
	return c.now
}

func (c *blockingSecondNowClock) After(time.Duration) <-chan time.Time {
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
	return newExecutorTestSubjectForBundle(t, store, options, executorTestBundleHash)
}

func newExecutorTestSubjectForBundle(
	t *testing.T,
	store CandidateStore,
	options ExecutorOptions,
	bundleHash string,
) (*Executor, *worklifetime.RuntimeOccurrence) {
	t.Helper()
	process := worklifetime.NewProcess()
	occurrence, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "22222222-2222-4222-8222-222222222222",
		BundleHash:        bundleHash,
	})
	if err != nil {
		t.Fatalf("create runtime occurrence: %v", err)
	}
	executor, err := NewExecutor(
		store,
		CandidateScope{BundleHash: bundleHash},
		TerminalCatalog{},
		occurrence,
		options,
	)
	if err != nil {
		t.Fatalf("create executor: %v", err)
	}
	return executor, occurrence
}

func updateAtomicMax(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if value <= current || target.CompareAndSwap(current, value) {
			return
		}
	}
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

func awaitExecutorCandidates(t *testing.T, executor *Executor, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for executor.ActiveCandidates() != want {
		if time.Now().After(deadline) {
			t.Fatalf("active candidates = %d, want %d", executor.ActiveCandidates(), want)
		}
		goruntime.Gosched()
	}
}
