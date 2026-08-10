package runcontrol

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
)

func TestControllerStopReconcilesExactTimerCancellationsAfterCommit(t *testing.T) {
	refs := []runtimetimercancellation.Ref{{
		Family: runtimetimercancellation.FamilyGenericSchedule, ActivationID: "activation-1", DueAt: time.Now(),
	}}
	store := &fakeRunControlStore{stopState: State{RunID: "run-1", TimerCancellations: refs}}
	reconciler := &fakeTimerCancellationReconciler{}
	controller := NewController(store, nil, Options{TimerCancellations: reconciler})

	result, err := controller.Stop(context.Background(), TransitionRequest{RunID: "run-1"})
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if !store.stopped || len(reconciler.refs) != 1 || reconciler.refs[0].ActivationID != "activation-1" {
		t.Fatalf("post-commit reconciliation store=%v refs=%#v", store.stopped, reconciler.refs)
	}
	if result.Recovery.Disposition != RecoveryComplete || result.Recovery.Err != nil {
		t.Fatalf("Stop() recovery = %#v", result.Recovery)
	}
}

func TestControllerStopReportsPostCommitReconciliationFailureWithoutReplayingStop(t *testing.T) {
	reconcileErr := errors.New("retire wakeup failed")
	store := &fakeRunControlStore{stopState: State{RunID: "run-1", TimerCancellations: []runtimetimercancellation.Ref{{
		Family: runtimetimercancellation.FamilyGenericSchedule, ActivationID: "activation-1", DueAt: time.Now(),
	}}}}
	reconciler := &fakeTimerCancellationReconciler{err: reconcileErr}
	controller := NewController(store, nil, Options{TimerCancellations: reconciler})

	result, err := controller.Stop(context.Background(), TransitionRequest{RunID: "run-1"})
	if err != nil {
		t.Fatalf("Stop() error = %v, want committed transition with recovery evidence", err)
	}
	if store.stopCalls != 1 || result.Status != StatusCancelled || result.Recovery.Disposition != RecoveryFailed || !errors.Is(result.Recovery.Err, reconcileErr) {
		t.Fatalf("Stop() result=%#v stop_calls=%d", result, store.stopCalls)
	}
}

func TestControllerStopReportsQueuedPostCommitRecoveryWithoutReplayingStop(t *testing.T) {
	ref := runtimetimercancellation.Ref{
		Family: runtimetimercancellation.FamilyGenericSchedule, ActivationID: "activation-1", DueAt: time.Now(),
	}
	reconcileErr := &runtimetimercancellation.ReconciliationError{Pending: []runtimetimercancellation.Failure{{
		Ref: ref, Err: errors.New("retire wakeup failed"),
	}}}
	store := &fakeRunControlStore{stopState: State{RunID: "run-1", TimerCancellations: []runtimetimercancellation.Ref{ref}}}
	controller := NewController(store, nil, Options{TimerCancellations: &fakeTimerCancellationReconciler{err: reconcileErr}})

	result, err := controller.Stop(context.Background(), TransitionRequest{RunID: "run-1"})
	if err != nil {
		t.Fatalf("Stop() error = %v, want committed transition with queued recovery", err)
	}
	if store.stopCalls != 1 || result.Recovery.Disposition != RecoveryPending || !errors.Is(result.Recovery.Err, reconcileErr) {
		t.Fatalf("Stop() result=%#v stop_calls=%d", result, store.stopCalls)
	}
}

func TestControllerContinueDoesNotFailAfterCommittedTransitionWhenReleaseFails(t *testing.T) {
	releaseErr := errors.New("release failed after commit")
	store := &fakeRunControlStore{}
	queue := &fakeRunControlQueue{err: releaseErr}
	controller := NewController(store, queue, Options{})

	result, err := controller.Continue(context.Background(), TransitionRequest{RunID: "run-1"})
	if err != nil {
		t.Fatalf("Continue() error = %v, want nil", err)
	}
	if !store.continued {
		t.Fatal("Continue() did not commit the store transition")
	}
	if !queue.called {
		t.Fatal("Continue() did not attempt queue release")
	}
	if result.RunID != "run-1" || result.Status != StatusRunning {
		t.Fatalf("Continue() result = %#v", result)
	}
	if result.Recovery.Disposition != RecoveryFailed ||
		!errors.Is(result.Recovery.Err, releaseErr) ||
		result.Recovery.Sweep.Settled != 0 {
		t.Fatalf("post-commit recovery = %#v, want typed failure after committed transition", result.Recovery)
	}
}

func TestControllerContinueDrainsUntilExplicitExhaustion(t *testing.T) {
	store := &fakeRunControlStore{}
	queue := &fakeRunControlQueue{results: []runtimepipelineobligation.SweepResult{
		{Examined: 2},
		{Settled: 1, Examined: 1, Exhausted: true},
	}}
	controller := NewController(store, queue, Options{})

	result, err := controller.Continue(context.Background(), TransitionRequest{RunID: "run-1"})
	if err != nil {
		t.Fatalf("Continue() error = %v, want nil", err)
	}
	if result.Recovery.Disposition != RecoveryExhausted ||
		result.Recovery.Sweep.Settled != 1 ||
		result.Recovery.Err != nil {
		t.Fatalf("post-commit recovery = %#v, want one settlement and explicit exhaustion", result.Recovery)
	}
	if queue.calls != 2 {
		t.Fatalf("queue release calls = %d, want 2", queue.calls)
	}
}

type fakeRunControlStore struct {
	continued bool
	stopped   bool
	stopCalls int
	stopState State
	stopErr   error
}

func (s *fakeRunControlStore) StopRunControl(_ context.Context, req TransitionRequest) (State, error) {
	s.stopped = true
	s.stopCalls++
	if s.stopErr != nil {
		return State{}, s.stopErr
	}
	state := s.stopState
	if state.RunID == "" {
		state.RunID = req.RunID
	}
	return state, nil
}

type fakeTimerCancellationReconciler struct {
	refs []runtimetimercancellation.Ref
	err  error
}

func (r *fakeTimerCancellationReconciler) Reconcile(_ context.Context, refs []runtimetimercancellation.Ref) error {
	r.refs = append([]runtimetimercancellation.Ref(nil), refs...)
	return r.err
}

func (s *fakeRunControlStore) PauseRunControl(context.Context, TransitionRequest) (State, error) {
	return State{}, errors.New("not implemented")
}

func (s *fakeRunControlStore) ContinueRunControl(_ context.Context, req TransitionRequest) (State, error) {
	s.continued = true
	return State{RunID: req.RunID, Status: StatusRunning, ControlStatus: StatusRunning}, nil
}

func (s *fakeRunControlStore) RunDispatchBlocked(context.Context, string) (bool, error) {
	return false, nil
}

type fakeRunControlQueue struct {
	called  bool
	calls   int
	err     error
	results []runtimepipelineobligation.SweepResult
}

func (q *fakeRunControlQueue) ReleaseRunQueue(context.Context, string, int) (runtimepipelineobligation.SweepResult, error) {
	q.called = true
	q.calls++
	if len(q.results) > 0 {
		result := q.results[0]
		q.results = q.results[1:]
		return result, nil
	}
	if q.err != nil {
		return runtimepipelineobligation.SweepResult{}, q.err
	}
	return runtimepipelineobligation.SweepResult{Settled: 1, Exhausted: true}, nil
}
