package deliverycontinuation

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type drainReadContextKey struct{}

type drainReadStore struct {
	coordinatorTestStore
	stage                         string
	target                        int32
	pages                         atomic.Int32
	reads                         atomic.Int32
	entered, release, initialWake chan struct{}
	failure                       error
	t                             *testing.T
}

func (s *drainReadStore) read(ctx context.Context, stage string) error {
	s.reads.Add(1)
	if ctx.Value(drainReadContextKey{}) != "owner-value" {
		s.t.Error("read lost owner context values")
	}
	if stage != s.stage || s.pages.Load() != s.target {
		return nil
	}
	close(s.entered)
	<-s.release
	if ctx.Done() != nil || ctx.Err() != nil || context.Cause(ctx) != nil {
		s.t.Errorf("admitted %s read received cancellation", stage)
	}
	return s.failure
}

func (s *drainReadStore) ScanDeliveryContinuations(ctx context.Context, _ runtimedelivery.ExecutionAuthority, _ runtimedelivery.ContinuationCursor, _ int) (runtimedelivery.ContinuationPage, error) {
	page := s.pages.Add(1)
	if page == 2 && s.target == 3 {
		close(s.initialWake)
	}
	if err := s.read(ctx, "page"); err != nil {
		return runtimedelivery.ContinuationPage{}, err
	}
	if page != s.target || s.stage == "held" {
		return runtimedelivery.ContinuationPage{Exhausted: true}, nil
	}
	// If a stopped read is consumed, this requests another read and dispatch.
	return runtimedelivery.ContinuationPage{Items: []runtimedelivery.ContinuationItem{{DeliveryID: "ready", Disposition: runtimedelivery.ClaimReclaimable, Snapshot: runtimedelivery.Snapshot{RunID: "run"}}}}, nil
}

func (s *drainReadStore) ObserveDeliveryContinuation(ctx context.Context, _ runtimedelivery.ExecutionAuthority, id string) (runtimedelivery.ContinuationObservation, error) {
	return runtimedelivery.ContinuationObservation{DeliveryID: id, Disposition: runtimedelivery.ClaimDeferred}, s.read(ctx, "held")
}

func (s *drainReadStore) StandingRunRestartDisposition(ctx context.Context, _ string) (runtimepipeline.StandingRestartDisposition, error) {
	return runtimepipeline.StandingRestartDisposition{}, s.read(ctx, "standing")
}

func TestCoordinatorDrainsAdmittedReadsBeforeRetirement(t *testing.T) {
	for _, phase := range []string{"startup", "wake", "synchronize"} {
		for _, stage := range []string{"page", "standing", "held"} {
			for _, stop := range []string{"retire", "parent_cancel"} {
				for _, fail := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/failure=%t", phase, stage, stop, fail), func(t *testing.T) {
						authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
						defer cleanup()
						ctx, cancel := context.WithCancel(context.WithValue(context.Background(), drainReadContextKey{}, "owner-value"))
						defer cancel()
						want := errors.New("independent admitted read failure")
						s := &drainReadStore{t: t, stage: stage, target: map[string]int32{"startup": 1, "wake": 2, "synchronize": 3}[phase], entered: make(chan struct{}), release: make(chan struct{}), initialWake: make(chan struct{})}
						if fail {
							s.failure = want
						}
						dispatch := &coordinatorTestDispatcher{}
						reported := make(chan error, 2)
						c, err := New(s, s, authority, owner, dispatch, func(_ context.Context, err error) { reported <- err })
						if err != nil {
							t.Fatal(err)
						}
						if stage == "held" {
							if err := c.observe("held"); err != nil {
								t.Fatal(err)
							}
						}
						result := make(chan error, 1)
						if phase == "startup" {
							go func() { result <- c.Start(ctx) }()
						} else {
							if err := c.Start(ctx); err != nil {
								t.Fatal(err)
							}
							if phase == "synchronize" {
								awaitRetirementProof(t, s.initialWake)
								go func() { result <- c.Synchronize(context.Background()) }()
							}
						}
						awaitRetirementProof(t, s.entered)
						if stop == "retire" {
							retireWithoutWaiting(c)
						} else {
							cancel()
						}
						select {
						case <-c.done:
							t.Error("read owner settled before read cleanup")
						default:
						}
						if owner.ActiveCount() == 0 {
							t.Error("read lost its existing worker lease")
						}
						reads := s.reads.Load()
						close(s.release)
						if phase != "wake" {
							err := <-result
							if err == nil || (fail && !errors.Is(err, want)) {
								t.Errorf("stopped startup/synchronization=%v", err)
							}
						}
						if !fail {
							want = nil
						}
						assertRetirementResult(t, c, owner, reported, want, phase != "startup")
						if s.reads.Load() != reads || s.pages.Load() != s.target || dispatch.callCount() != 0 {
							t.Fatalf("post-stop work: reads=%d want=%d pages=%d want=%d dispatch=%d", s.reads.Load(), reads, s.pages.Load(), s.target, dispatch.callCount())
						}
					})
				}
			}
		}
	}
}

func TestCoordinatorSynchronizeWaiterCancellationKeepsReadLease(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	ctx := context.WithValue(context.Background(), drainReadContextKey{}, "owner-value")
	s := &drainReadStore{t: t, stage: "page", target: 3, entered: make(chan struct{}), release: make(chan struct{}), initialWake: make(chan struct{})}
	dispatch := &coordinatorTestDispatcher{}
	c, err := New(s, s, authority, owner, dispatch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	awaitRetirementProof(t, s.initialWake)
	waitCtx, cancelWait := context.WithCancel(context.Background())
	defer cancelWait()
	result := make(chan error, 1)
	go func() { result <- c.Synchronize(waitCtx) }()
	awaitRetirementProof(t, s.entered)
	cancelWait()
	if err := <-result; err != context.Canceled {
		t.Errorf("synchronization waiter cancellation=%v", err)
	}
	select {
	case <-c.done:
		t.Error("waiter cancellation released coordinator read")
	default:
	}
	if owner.ActiveCount() == 0 {
		t.Error("waiter cancellation released read lease")
	}
	retireWithoutWaiting(c)
	close(s.release)
	if err := c.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if owner.ActiveCount() != 0 || s.pages.Load() != 3 || dispatch.callCount() != 0 {
		t.Fatal("read drain lost lease settlement or admitted post-retirement work")
	}
}
