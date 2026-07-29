package runcontrol

import (
	"context"
	"errors"
	"testing"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

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
}

func (s *fakeRunControlStore) StopRunControl(context.Context, TransitionRequest) (State, error) {
	return State{}, errors.New("not implemented")
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
