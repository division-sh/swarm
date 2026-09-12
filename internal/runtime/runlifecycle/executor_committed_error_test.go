package runlifecycle

import (
	"context"
	"errors"
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
		execute: func(context.Context, Candidate, TerminalCatalog) (CompletionResult, error) {
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
