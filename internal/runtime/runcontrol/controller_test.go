package runcontrol

import (
	"context"
	"errors"
	"testing"
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
	if result.ReleasedDeliveries != 0 {
		t.Fatalf("released deliveries = %d, want 0 after release failure", result.ReleasedDeliveries)
	}
}

func TestControllerContinueRetriesTransientEmptyQueueRelease(t *testing.T) {
	store := &fakeRunControlStore{}
	queue := &fakeRunControlQueue{releases: []int{0, 1}}
	controller := NewController(store, queue, Options{})

	result, err := controller.Continue(context.Background(), TransitionRequest{RunID: "run-1"})
	if err != nil {
		t.Fatalf("Continue() error = %v, want nil", err)
	}
	if result.ReleasedDeliveries != 1 {
		t.Fatalf("released deliveries = %d, want 1 after retry", result.ReleasedDeliveries)
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
	called   bool
	calls    int
	err      error
	releases []int
}

func (q *fakeRunControlQueue) ReleaseRunQueue(context.Context, string, int) (int, error) {
	q.called = true
	q.calls++
	if len(q.releases) > 0 {
		released := q.releases[0]
		q.releases = q.releases[1:]
		return released, nil
	}
	if q.err != nil {
		return 0, q.err
	}
	return 1, nil
}
