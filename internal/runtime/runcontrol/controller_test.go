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

func TestControllerStopKeepsAcknowledgedStateAndReconcilesAfterCleanupError(t *testing.T) {
	cleanupErr := errors.New("stop cleanup failed")
	ref := runtimetimercancellation.Ref{Family: runtimetimercancellation.FamilyGenericSchedule, ActivationID: "activation-1", DueAt: time.Now()}
	store := &fakeRunControlStore{stopState: State{RunID: "run-1", TimerCancellations: []runtimetimercancellation.Ref{ref}}, stopErr: cleanupErr}
	reconciler := &fakeTimerCancellationReconciler{}
	result, err := NewController(store, nil, Options{TimerCancellations: reconciler}).Stop(context.Background(), TransitionRequest{RunID: "run-1"})
	if !errors.Is(err, cleanupErr) || result.RunID != "run-1" || result.Status != StatusCancelled || store.stopCalls != 1 {
		t.Fatalf("stop result=%+v err=%v calls=%d", result, err, store.stopCalls)
	}
	if len(reconciler.refs) != 1 || reconciler.refs[0].ActivationID != ref.ActivationID {
		t.Fatalf("committed timer reconciliation = %+v", reconciler.refs)
	}
}

func TestControllerPauseAndContinueKeepAcknowledgedStateAfterCleanupError(t *testing.T) {
	cleanupErr := errors.New("transition cleanup failed")
	store := &fakeRunControlStore{pauseErr: cleanupErr, continueErr: cleanupErr}
	queue := &fakeRunControlQueue{}
	controller := NewController(store, queue, Options{})
	paused, pauseErr := controller.Pause(context.Background(), TransitionRequest{RunID: "run-1"})
	if !errors.Is(pauseErr, cleanupErr) || paused.RunID != "run-1" || paused.Status != StatusPaused {
		t.Fatalf("pause result=%+v err=%v", paused, pauseErr)
	}
	continued, continueErr := controller.Continue(context.Background(), TransitionRequest{RunID: "run-1"})
	if !errors.Is(continueErr, cleanupErr) || continued.RunID != "run-1" || continued.Status != StatusRunning || !queue.called {
		t.Fatalf("continue result=%+v err=%v queue_called=%v", continued, continueErr, queue.called)
	}
}

func TestControllerRejectsUnacknowledgedValueWithoutFollowUp(t *testing.T) {
	fault := errors.New("commit not acknowledged")
	store := &fakeRunControlStore{stopState: State{TimerCancellations: []runtimetimercancellation.Ref{{Family: runtimetimercancellation.FamilyGenericSchedule, ActivationID: "activation-1"}}}, stopUnack: true, stopErr: fault, pauseUnack: true, pauseErr: fault, continueUnack: true, continueErr: fault}
	queue := &fakeRunControlQueue{}
	reconciler := &fakeTimerCancellationReconciler{}
	controller := NewController(store, queue, Options{TimerCancellations: reconciler})
	for _, transition := range []struct {
		name string
		run  func(context.Context, TransitionRequest) (TransitionResult, error)
	}{
		{name: "stop", run: controller.Stop},
		{name: "pause", run: controller.Pause},
		{name: "continue", run: controller.Continue},
	} {
		t.Run(transition.name, func(t *testing.T) {
			result, err := transition.run(context.Background(), TransitionRequest{RunID: "run-1"})
			if !errors.Is(err, fault) || result.RunID != "" {
				t.Fatalf("unacknowledged result=%+v err=%v", result, err)
			}
		})
	}
	if queue.called || len(reconciler.refs) != 0 {
		t.Fatalf("unacknowledged follow-up queue=%v reconciled=%+v", queue.called, reconciler.refs)
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

func TestControllerContinueReleasesAfterCallerCancelsAtAcknowledgedCommit(t *testing.T) {
	type contextKey struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "committed"))
	defer cancel()
	cleanupErr := errors.New("post-commit handoff failed")
	store := &fakeRunControlStore{continueErr: cleanupErr, onContinueCommit: cancel}
	queue := &fakeRunControlQueue{}
	queue.onRelease = func(releaseCtx context.Context) {
		if releaseCtx.Value(contextKey{}) != "committed" {
			t.Error("queue release lost caller context values")
		}
	}

	result, err := NewController(store, queue, Options{}).Continue(ctx, TransitionRequest{RunID: "run-1"})
	if !errors.Is(err, cleanupErr) || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("continue err=%v caller context=%v", err, ctx.Err())
	}
	if !queue.called || queue.releaseContextErr != nil || result.Recovery.Sweep.Settled != 1 || result.RunID != "run-1" || !errors.Is(result.Recovery.Err, cleanupErr) {
		t.Fatalf("committed queue release result=%+v called=%v release context=%v", result, queue.called, queue.releaseContextErr)
	}
}

func TestControllerContinuePreflightsQueueBeforeRunMutation(t *testing.T) {
	preflightErr := errors.New("live queue rejected")
	store := &fakeRunControlStore{}
	queue := &fakeRunControlQueue{preflightErr: preflightErr}
	controller := NewController(store, queue, Options{})

	if _, err := controller.Continue(context.Background(), TransitionRequest{RunID: "run-1"}); !errors.Is(err, preflightErr) {
		t.Fatalf("Continue() error = %v, want preflight rejection", err)
	}
	if store.continued || queue.called || queue.preflightCalls != 1 {
		t.Fatalf("preflight calls=%d continued=%v released=%v, want 1/false/false", queue.preflightCalls, store.continued, queue.called)
	}
}

type fakeRunControlStore struct {
	continued        bool
	stopped          bool
	stopCalls        int
	stopState        State
	stopErr          error
	stopUnack        bool
	pauseErr         error
	pauseUnack       bool
	continueErr      error
	continueUnack    bool
	onContinueCommit func()
}

func (s *fakeRunControlStore) StopRunControlOutcome(_ context.Context, req TransitionRequest) (StoreTransition, error) {
	s.stopped = true
	s.stopCalls++
	if s.stopUnack {
		return StoreTransition{State: State{RunID: req.RunID, TimerCancellations: s.stopState.TimerCancellations}}, s.stopErr
	}
	state := s.stopState
	if state.RunID == "" {
		state.RunID = req.RunID
	}
	return StoreTransition{State: state, Acknowledged: true}, s.stopErr
}

type fakeTimerCancellationReconciler struct {
	refs []runtimetimercancellation.Ref
	err  error
}

func (r *fakeTimerCancellationReconciler) Reconcile(_ context.Context, refs []runtimetimercancellation.Ref) error {
	r.refs = append([]runtimetimercancellation.Ref(nil), refs...)
	return r.err
}

func (s *fakeRunControlStore) PauseRunControlOutcome(_ context.Context, req TransitionRequest) (StoreTransition, error) {
	if s.pauseUnack {
		return StoreTransition{State: State{RunID: req.RunID, Status: StatusPaused}}, s.pauseErr
	}
	return StoreTransition{State: State{RunID: req.RunID, Status: StatusPaused}, Acknowledged: true}, s.pauseErr
}

func (s *fakeRunControlStore) ContinueRunControlOutcome(_ context.Context, req TransitionRequest) (StoreTransition, error) {
	s.continued = true
	if s.continueUnack {
		return StoreTransition{State: State{RunID: req.RunID, Status: StatusRunning}}, s.continueErr
	}
	if s.onContinueCommit != nil {
		s.onContinueCommit()
	}
	return StoreTransition{State: State{RunID: req.RunID, Status: StatusRunning, ControlStatus: StatusRunning}, Acknowledged: true}, s.continueErr
}

func (s *fakeRunControlStore) RunDispatchBlocked(context.Context, string) (bool, error) {
	return false, nil
}

type fakeRunControlQueue struct {
	called            bool
	calls             int
	preflightCalls    int
	preflightErr      error
	err               error
	results           []runtimepipelineobligation.SweepResult
	onRelease         func(context.Context)
	releaseContextErr error
}

func (q *fakeRunControlQueue) PreflightRunQueue(context.Context, string) error {
	q.preflightCalls++
	return q.preflightErr
}

func (q *fakeRunControlQueue) ReleaseRunQueue(ctx context.Context, _ string, _ int) (runtimepipelineobligation.SweepResult, error) {
	q.called = true
	q.calls++
	q.releaseContextErr = ctx.Err()
	if q.onRelease != nil {
		q.onRelease(ctx)
	}
	if err := ctx.Err(); err != nil {
		return runtimepipelineobligation.SweepResult{}, err
	}
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
