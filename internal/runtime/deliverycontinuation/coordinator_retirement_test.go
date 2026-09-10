package deliverycontinuation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type retirementScanStore struct {
	coordinatorTestStore
	call atomic.Int32
	scan func(context.Context, int) (runtimedelivery.ContinuationPage, error)
}

func (s *retirementScanStore) ScanDeliveryContinuations(ctx context.Context, _ runtimedelivery.ExecutionAuthority, _ runtimedelivery.ContinuationCursor, _ int) (runtimedelivery.ContinuationPage, error) {
	return s.scan(ctx, int(s.call.Add(1)))
}

type retirementOccurrence struct {
	worklifetime.Occurrence
	begin func(context.Context) (*worklifetime.Lease, error)
}

func (o retirementOccurrence) BeginStanding(ctx context.Context) (*worklifetime.Lease, error) {
	return o.begin(ctx)
}

func awaitRetirementProof(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle proof barrier did not settle")
	}
}

func retireWithoutWaiting(c *Coordinator) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = c.Retire(ctx)
}

func assertRetirementResult(t *testing.T, c *Coordinator, owner *worklifetime.RuntimeOccurrence, reported <-chan error, want error, asynchronous bool) {
	t.Helper()
	awaitRetirementProof(t, c.done)
	for range 2 {
		if err := c.Retire(context.Background()); !errors.Is(err, want) || (want == nil && err != nil) {
			t.Fatalf("retirement result=%v, want %v", err, want)
		}
	}
	if got := owner.ActiveCount(); got != 0 {
		t.Fatalf("completion published with %d active leases", got)
	}
	if want != nil && !errors.Is(c.Synchronize(context.Background()), want) {
		t.Fatal("synchronization lost the terminal failure")
	}
	select {
	case err := <-reported:
		if !asynchronous || want == nil || !errors.Is(err, want) {
			t.Fatalf("unexpected failure callback: %v", err)
		}
	default:
		if asynchronous && want != nil {
			t.Fatal("genuine asynchronous failure was not reported")
		}
	}
	select {
	case err := <-reported:
		t.Fatalf("duplicate failure callback: %v", err)
	default:
	}
}

func TestCoordinatorRetireBeforeStartAndDrainAcceptedCarrier(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	c, err := New(&coordinatorTestStore{}, coordinatorTestRestarts{}, authority, owner, &coordinatorTestDispatcher{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := runtimedelivery.AdmitDurableHandoffProof("accepted", "event", "route", authority)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err != nil {
		t.Fatal(err)
	}
	carrier, err := acquireCoordinatorTestCapability(c, "accepted")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := c.Retire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(context.Background()); err == nil {
		t.Fatal("retired coordinator started")
	}
	requireCoordinatorRejectsPostFailureAdmission(t, c, authority)
	if result, err := carrier.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil || result != worklifetime.DeliveryContinuationReturned {
		t.Fatalf("accepted carrier could not return: %v %v", result, err)
	}
	if _, err := carrier.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err == nil {
		t.Fatal("carrier returned twice")
	}
	if err := c.Release("accepted"); err != nil {
		t.Fatal(err)
	}
	if len(c.heldEntries()) != 0 || owner.ActiveCount() != 0 {
		t.Fatal("retirement retained accepted ownership")
	}
}

func TestCoordinatorRetirementDuringLeaseAcquisition(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("lease_error=%t", fail), func(t *testing.T) {
			authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
			defer cleanup()
			entered, release := make(chan struct{}), make(chan struct{})
			want := errors.New("independent standing lease admission failure")
			occurrence := retirementOccurrence{Occurrence: owner, begin: func(ctx context.Context) (*worklifetime.Lease, error) {
				close(entered)
				<-release
				if fail {
					return nil, want
				}
				return owner.BeginStanding(ctx)
			}}
			store := &coordinatorTestStore{}
			reported := make(chan error, 2)
			c, err := New(store, coordinatorTestRestarts{}, authority, occurrence, &coordinatorTestDispatcher{}, func(_ context.Context, err error) { reported <- err })
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan error, 1)
			go func() { started <- c.Start(context.Background()) }()
			awaitRetirementProof(t, entered)
			retireWithoutWaiting(c)
			close(release)
			if err := <-started; err == nil || (fail && !errors.Is(err, want)) {
				t.Fatalf("aborted startup=%v", err)
			}
			if !fail {
				want = nil
			}
			assertRetirementResult(t, c, owner, reported, want, false)
			if store.scanCalls() != 0 {
				t.Fatal("late lease reached startup enumeration")
			}
		})
	}
}

func TestCoordinatorRetirementScanOutcomeMatrix(t *testing.T) {
	for _, stage := range []string{"initial", "wake", "synchronize"} {
		for _, outcome := range []string{"empty", "deferred", "canceled", "failure", "joined"} {
			t.Run(stage+"/"+outcome, func(t *testing.T) {
				authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
				defer cleanup()
				entered, release, initialWake := make(chan struct{}), make(chan struct{}), make(chan struct{})
				want := errors.New("independent selected-store failure")
				block := map[string]int{"initial": 1, "wake": 2, "synchronize": 3}[stage]
				store := &retirementScanStore{scan: func(ctx context.Context, call int) (runtimedelivery.ContinuationPage, error) {
					if call != block {
						if call == 2 {
							close(initialWake)
						}
						return runtimedelivery.ContinuationPage{Exhausted: true}, nil
					}
					close(entered)
					<-release
					switch outcome {
					case "failure":
						return runtimedelivery.ContinuationPage{}, want
					case "joined":
						return runtimedelivery.ContinuationPage{}, errors.Join(ctx.Err(), want)
					case "canceled":
						return runtimedelivery.ContinuationPage{}, ctx.Err()
					case "deferred":
						return runtimedelivery.ContinuationPage{Exhausted: true, Items: []runtimedelivery.ContinuationItem{{DeliveryID: "deferred", Disposition: runtimedelivery.ClaimDeferred}}}, nil
					default:
						return runtimedelivery.ContinuationPage{Exhausted: true}, nil
					}
				}}
				reported := make(chan error, 2)
				c, err := New(store, coordinatorTestRestarts{}, authority, owner, &coordinatorTestDispatcher{}, func(_ context.Context, err error) { reported <- err })
				if err != nil {
					t.Fatal(err)
				}
				result := make(chan error, 1)
				if stage == "initial" {
					go func() { result <- c.Start(context.Background()) }()
				} else {
					if err := c.Start(context.Background()); err != nil {
						t.Fatal(err)
					}
					if stage == "synchronize" {
						awaitRetirementProof(t, initialWake)
						go func() { result <- c.Synchronize(context.Background()) }()
					}
				}
				awaitRetirementProof(t, entered)
				retireWithoutWaiting(c)
				close(release)
				fatal := outcome == "failure" || outcome == "joined"
				if stage != "wake" {
					err := <-result
					if fatal && !errors.Is(err, want) {
						t.Fatalf("synchronous result lost failure: %v", err)
					}
					if err == nil {
						t.Fatal("retired enumeration or synchronization reported readiness")
					}
				}
				if !fatal {
					want = nil
				}
				assertRetirementResult(t, c, owner, reported, want, stage != "initial")
				requireCoordinatorRejectsPostFailureAdmission(t, c, authority)
			})
		}
	}
}

func TestCoordinatorRetirementBeforeCancellationIsNotFatal(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	entered, release := make(chan struct{}), make(chan struct{})
	store := &retirementScanStore{scan: func(_ context.Context, call int) (runtimedelivery.ContinuationPage, error) {
		if call == 1 {
			return runtimedelivery.ContinuationPage{Exhausted: true}, nil
		}
		close(entered)
		<-release
		return runtimedelivery.ContinuationPage{Exhausted: true, Items: []runtimedelivery.ContinuationItem{{DeliveryID: "retiring", Disposition: runtimedelivery.ClaimDeferred}}}, nil
	}}
	reported := make(chan error, 2)
	c, err := New(store, coordinatorTestRestarts{}, authority, owner, &coordinatorTestDispatcher{}, func(_ context.Context, err error) { reported <- err })
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	awaitRetirementProof(t, entered)
	cancelEntered, cancelRelease := make(chan struct{}), make(chan struct{})
	c.mu.Lock()
	cancel := c.cancel
	var once sync.Once
	c.cancel = func() { once.Do(func() { close(cancelEntered); <-cancelRelease }); cancel() }
	c.mu.Unlock()
	retired := make(chan error, 1)
	go func() { retired <- c.Retire(context.Background()) }()
	awaitRetirementProof(t, cancelEntered)
	close(release)
	awaitRetirementProof(t, c.done)
	close(cancelRelease)
	if err := <-retired; err != nil {
		t.Fatal(err)
	}
	assertRetirementResult(t, c, owner, reported, nil, true)
}

type retirementFailureDispatcher struct {
	entered chan struct{}
	failure error
}

func (d retirementFailureDispatcher) DispatchDeliveryContinuation(ctx context.Context, _ events.Event, _ events.DeliveryRoute) DispatchResult {
	close(d.entered)
	<-ctx.Done()
	return Fatal(errors.Join(ctx.Err(), d.failure))
}

func TestCoordinatorRetirementPreservesFatalDispatch(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	want := errors.New("independent dispatch failure")
	entered := make(chan struct{})
	store := &coordinatorTestStore{pages: []runtimedelivery.ContinuationPage{{Exhausted: true}, {Exhausted: true, Items: []runtimedelivery.ContinuationItem{{DeliveryID: "dispatch", Disposition: runtimedelivery.ClaimAcquired}}}}}
	reported := make(chan error, 2)
	c, err := New(store, coordinatorTestRestarts{}, authority, owner, retirementFailureDispatcher{entered, want}, func(_ context.Context, err error) { reported <- err })
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	awaitRetirementProof(t, entered)
	retireWithoutWaiting(c)
	assertRetirementResult(t, c, owner, reported, want, true)
}

func TestCoordinatorStartupPreservesLeaseCleanupFailure(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	var lease *worklifetime.Lease
	occurrence := retirementOccurrence{Occurrence: owner, begin: func(ctx context.Context) (*worklifetime.Lease, error) {
		var err error
		lease, err = owner.BeginStanding(ctx)
		return lease, err
	}}
	want := errors.New("initial scan failed independently")
	store := &retirementScanStore{scan: func(context.Context, int) (runtimedelivery.ContinuationPage, error) {
		if err := lease.Done(); err != nil {
			t.Fatal(err)
		}
		return runtimedelivery.ContinuationPage{}, want
	}}
	c, err := New(store, coordinatorTestRestarts{}, authority, occurrence, &coordinatorTestDispatcher{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = c.Start(context.Background())
	if !errors.Is(err, want) || !errors.Is(err, worklifetime.ErrAlreadySettled) {
		t.Fatalf("startup lost failure/cleanup evidence: %v", err)
	}
	err = c.Retire(context.Background())
	if !errors.Is(err, want) || !errors.Is(err, worklifetime.ErrAlreadySettled) {
		t.Fatalf("retirement lost failure/cleanup evidence: %v", err)
	}
	if owner.ActiveCount() != 0 {
		t.Fatal("startup cleanup leaked work")
	}
}
