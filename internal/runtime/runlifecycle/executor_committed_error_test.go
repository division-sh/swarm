package runlifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestExecutorCommittedErrorPreservesContinuationWithoutReplay(t *testing.T) {
	primary := errors.New("acknowledged candidate cleanup failure")
	activation, err := NewCommittedGenericScheduleActivation("33333333-3333-4333-8333-333333333333")
	if err != nil {
		t.Fatal(err)
	}
	var executions atomic.Int32
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, FinalCatalog) (CompletionResult, error) {
			if executions.Add(1) == 1 {
				return CompletionResult{Committed: true, Outcome: OutcomeAwaitMutation, GenericScheduleActivations: []CommittedGenericScheduleActivation{activation}}, primary
			}
			return CompletionResult{Outcome: OutcomeAwaitMutation}, nil
		},
	}
	wakeups := &recordingGenericScheduleWakeupOwner{activationIDs: make(chan string, 2), queued: true}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{GenericSchedules: wakeups, RetryPolicy: immediateRetryPolicy{}})
	t.Cleanup(func() { _ = executor.Retire(context.Background()); retireRuntimeOccurrence(t, occurrence) })
	if err := executor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), executorTestCandidate(1)); err != nil {
		t.Fatal(err)
	}
	awaitExecutorCandidates(t, executor, 0)
	if executions.Load() != 1 {
		t.Errorf("acknowledged mutation replayed: %d executions", executions.Load())
	}
	select {
	case got := <-wakeups.activationIDs:
		if got != activation.ID() {
			t.Errorf("wrong continuation: %s", got)
		}
	default:
		t.Error("committed generic-schedule continuation was lost")
	}
	if err := executor.Retire(context.Background()); !errors.Is(err, primary) {
		t.Errorf("retirement erased cleanup failure: %v", err)
	}
}

func TestExecutorLateCommittedDiagnosticSurvivesJoin(t *testing.T) {
	primary := errors.New("committed completion cleanup failed during retirement")
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var executions atomic.Int32
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, FinalCatalog) (CompletionResult, error) {
			executions.Add(1)
			close(started)
			<-release
			return CompletionResult{Committed: true, Outcome: OutcomeAwaitMutation}, primary
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), executorTestCandidate(1)); err != nil {
		t.Fatal(err)
	}
	receiveSignal(t, started, "completion persistence")
	retireErr := executor.Retire(context.Background())
	releaseOnce.Do(func() { close(release) })
	joinErr := executor.Wait(context.Background())
	retireRuntimeOccurrence(t, occurrence)
	if retireErr != nil || !CompletionJoinSucceeded(joinErr) || !errors.Is(joinErr, primary) {
		t.Fatalf("late committed diagnostic lost after retirement: retire=%v join=%v", retireErr, joinErr)
	}
	if executions.Load() != 1 || executor.ActiveCandidates() != 0 || occurrence.ActiveCount() != 0 {
		t.Fatalf("acknowledged work replayed or retained: executions=%d candidates=%d leases=%d", executions.Load(), executor.ActiveCandidates(), occurrence.ActiveCount())
	}
	for _, err := range []error{primary, context.Canceled, fmt.Errorf("wrapped: %w", joinErr), errors.Join(joinErr, context.DeadlineExceeded)} {
		if CompletionJoinSucceeded(err) {
			t.Fatalf("non-owner result falsely proves a successful join: %v", err)
		}
	}
}

func TestExecutorCanceledJoinRetainsAcceptedWork(t *testing.T) {
	primary := errors.New("committed cleanup diagnostic")
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	store := &executorTestStore{
		list: func(context.Context, CandidateScope, CandidateCursor, int) (CandidatePage, error) {
			return CandidatePage{Exhausted: true}, nil
		},
		execute: func(context.Context, Candidate, FinalCatalog) (CompletionResult, error) {
			close(started)
			<-release
			return CompletionResult{Committed: true, Outcome: OutcomeAwaitMutation}, primary
		},
	}
	executor, occurrence := newExecutorTestSubject(t, store, ExecutorOptions{})
	if err := executor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := executor.SubmitCompletionCandidate(context.Background(), executorTestCandidate(1)); err != nil {
		t.Fatal(err)
	}
	receiveSignal(t, started, "completion persistence")
	if err := executor.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := executor.Wait(canceled); !errors.Is(err, context.Canceled) || CompletionJoinSucceeded(err) {
		t.Fatalf("incomplete join confused with a settled diagnostic: %v", err)
	}
	if executor.ActiveCandidates() != 1 || occurrence.ActiveCount() == 0 {
		t.Fatal("incomplete join discarded accepted work")
	}
	releaseOnce.Do(func() { close(release) })
	if err := executor.Wait(context.Background()); !CompletionJoinSucceeded(err) || !errors.Is(err, primary) {
		t.Fatalf("final join lost committed diagnostic: %v", err)
	}
	retireRuntimeOccurrence(t, occurrence)
}
