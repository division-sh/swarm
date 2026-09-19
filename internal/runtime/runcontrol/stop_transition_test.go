package runcontrol

import (
	"context"
	"errors"
	"testing"
)

type testStopTransition struct{ done bool }

func (t *testStopTransition) Done() { t.done = true }

func (q *fakeRunControlQueue) BeginRunStop(context.Context, string) (StopTransition, error) {
	return &testStopTransition{}, nil
}

type stopTransitionQueue struct {
	fakeRunControlQueue
	transition testStopTransition
	begin      func() error
}

func (q *stopTransitionQueue) BeginRunStop(context.Context, string) (StopTransition, error) {
	if err := q.begin(); err != nil {
		return nil, err
	}
	return &q.transition, nil
}

func TestStopRetainsParentTransitionThroughStoreOutcome(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[fail], func(t *testing.T) {
			store := &fakeRunControlStore{}
			if fail {
				store.stopErr = errors.New("stop transaction failed")
			}
			queue := &stopTransitionQueue{begin: func() error {
				if store.stopCalls != 0 {
					t.Fatal("stop transaction began before parent exclusion")
				}
				return nil
			}}
			_, err := NewController(store, queue, Options{}).Stop(context.Background(), TransitionRequest{RunID: "run"})
			if !errors.Is(err, store.stopErr) || store.stopCalls != 1 || !queue.transition.done {
				t.Fatalf("stop outcome=%v calls=%d released=%v", err, store.stopCalls, queue.transition.done)
			}
		})
	}
}

func TestStopParentTransitionFailurePrecedesMutation(t *testing.T) {
	cause := errors.New("parent drain canceled")
	store := &fakeRunControlStore{}
	queue := &stopTransitionQueue{begin: func() error { return cause }}
	_, err := NewController(store, queue, Options{}).Stop(context.Background(), TransitionRequest{RunID: "run"})
	if !errors.Is(err, cause) || store.stopCalls != 0 || queue.transition.done {
		t.Fatalf("drain failure=%v calls=%d released=%v", err, store.stopCalls, queue.transition.done)
	}
}
