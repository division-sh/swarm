package deliverycontinuation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type retirementDispatchFunc func(context.Context, events.Event, events.DeliveryRoute) DispatchResult

func (f retirementDispatchFunc) DispatchDeliveryContinuation(ctx context.Context, event events.Event, route events.DeliveryRoute) DispatchResult {
	return f(ctx, event, route)
}

type retirementDispatchOccurrence struct {
	worklifetime.Occurrence
	runCtx context.Context
	worker *worklifetime.Lease
}

func (o *retirementDispatchOccurrence) BeginStanding(ctx context.Context) (*worklifetime.Lease, error) {
	lease, err := o.Occurrence.BeginStanding(ctx)
	if err == nil {
		o.runCtx = lease.Context()
	}
	return lease, err
}

func (o *retirementDispatchOccurrence) Begin(ctx context.Context) (*worklifetime.Lease, error) {
	lease, err := o.Occurrence.Begin(ctx)
	if err == nil {
		o.worker = lease
	}
	return lease, err
}

func TestCoordinatorRetirementDispatchOutcomeMatrix(t *testing.T) {
	for _, selected := range []bool{false, true} {
		name := "normal"
		if selected {
			name = "selected"
		}
		for _, scenario := range []string{
			"before_acquire", "after_acquire", "settled_before_retire", "independent",
			"joined_retirement", "joined_cancellation", "invalid_result", "carrier_cleanup",
			"lease_cleanup", "unowned_retirement", "parent_cancel",
		} {
			for _, canceled := range []bool{false, true} {
				phase := "live"
				if canceled {
					phase = "canceled"
				}
				if scenario == "parent_cancel" && !canceled {
					continue
				}
				t.Run(name+"/"+scenario+"_"+phase, func(t *testing.T) {
					testRetirementDispatchOutcome(t, selected, scenario, canceled)
				})
			}
		}
	}
}

func testRetirementDispatchOutcome(t *testing.T, selected bool, scenario string, canceled bool) {
	t.Helper()
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	t.Cleanup(cleanup)
	if selected {
		var err error
		authority, err = runtimedelivery.NewSelectedExecutionAuthority(authority.SourceArtifact(), coordinatorTestEvent("selected-retirement").ID(), coordinatorTestEvent("selected-retirement").RunID(), 1)
		if err != nil {
			t.Fatal(err)
		}
	}
	occurrence := &retirementDispatchOccurrence{Occurrence: owner}
	parent, cancelParent := context.WithCancel(context.Background())
	t.Cleanup(cancelParent)
	scanEntered, scanRelease := make(chan struct{}), make(chan struct{})
	dispatchEntered, dispatchRelease := make(chan struct{}), make(chan struct{})
	cancelEntered, cancelRelease := make(chan struct{}), make(chan struct{})
	var scanOnce, dispatchOnce, cancelOnce sync.Once
	releaseScan := func() { scanOnce.Do(func() { close(scanRelease) }) }
	releaseDispatch := func() { dispatchOnce.Do(func() { close(dispatchRelease) }) }
	releaseCancel := func() { cancelOnce.Do(func() { close(cancelRelease) }) }
	store := &retirementScanStore{scan: func(_ context.Context, call int) (runtimedelivery.ContinuationPage, error) {
		if call == 2 {
			close(scanEntered)
			<-scanRelease
		}
		return runtimedelivery.ContinuationPage{Exhausted: true}, nil
	}}
	independent := errors.New("independent dispatch failure")
	reported := make(chan error, 2)
	const id = "retiring-worker"
	var c *Coordinator
	var carrier worklifetime.DeliveryContinuation
	var carrierCleanup error
	dispatcher := retirementDispatchFunc(func(ctx context.Context, _ events.Event, _ events.DeliveryRoute) DispatchResult {
		if scenario == "after_acquire" || scenario == "carrier_cleanup" || scenario == "settled_before_retire" {
			var err error
			carrier, err = acquireCoordinatorTestCapability(c, id)
			if err != nil {
				return Fatal(err)
			}
			if scenario == "settled_before_retire" {
				if _, err := carrier.Resolve(ctx, worklifetime.DeliveryContinuationConsume); err != nil {
					return Fatal(err)
				}
				if err := c.Release(id); err != nil {
					return Fatal(err)
				}
			}
		}
		close(dispatchEntered)
		<-dispatchRelease
		switch scenario {
		case "independent":
			return Fatal(independent)
		case "joined_retirement":
			return Fatal(errors.Join(errCoordinatorRetired, independent))
		case "joined_cancellation":
			return Fatal(errors.Join(context.Canceled, independent))
		case "invalid_result":
			return DispatchResult{}
		case "unowned_retirement":
			return Fatal(errCoordinatorRetired)
		case "parent_cancel":
			return Fatal(ctx.Err())
		case "settled_before_retire":
			return Transferred()
		}
		var refusal error
		if carrier == nil {
			_, refusal = c.Acquire(id)
		} else {
			_, refusal = carrier.Resolve(ctx, worklifetime.DeliveryContinuationConsume)
			resolution, err := carrier.Resolve(ctx, worklifetime.DeliveryContinuationReturn)
			if err != nil || resolution != worklifetime.DeliveryContinuationReturned {
				return Fatal(errors.Join(refusal, fmt.Errorf("exact carrier return: %v %w", resolution, err)))
			}
			_, carrierCleanup = carrier.Resolve(ctx, worklifetime.DeliveryContinuationReturn)
			if carrierCleanup == nil {
				return Fatal(errors.New("duplicate carrier return succeeded"))
			}
			if scenario == "carrier_cleanup" {
				return Fatal(errors.Join(refusal, carrierCleanup))
			}
		}
		if refusal == nil {
			return Fatal(errors.New("retired admission unexpectedly succeeded"))
		}
		if scenario == "lease_cleanup" {
			if err := occurrence.worker.Done(); err != nil {
				return Fatal(errors.Join(refusal, err))
			}
		}
		return Fatal(fmt.Errorf("owned dispatch refusal: %w", refusal))
	})
	var err error
	if selected {
		c, err = NewSelected(store, authority, occurrence, dispatcher, func(_ context.Context, err error) { reported <- err })
	} else {
		c, err = New(store, coordinatorTestRestarts{}, authority, occurrence, dispatcher, func(_ context.Context, err error) { reported <- err })
	}
	if err != nil {
		t.Fatal(err)
	}
	c.workerLimit = 1
	t.Cleanup(func() {
		releaseDispatch()
		releaseScan()
		releaseCancel()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Retire(ctx)
	})
	if err := c.Start(parent); err != nil {
		t.Fatal(err)
	}
	awaitRetirementProof(t, scanEntered)
	if err := c.observe(id); err != nil {
		t.Fatal(err)
	}
	if err := c.schedule(occurrence.runCtx, runtimedelivery.ContinuationItem{DeliveryID: id}); err != nil {
		t.Fatal(err)
	}
	awaitRetirementProof(t, dispatchEntered)
	ownedRetirement := scenario != "unowned_retirement" && scenario != "parent_cancel"
	retired := make(chan error, 1)
	if ownedRetirement {
		c.mu.Lock()
		cancel := c.cancel
		var fenceOnce sync.Once
		// Hold cancellation after Retire publishes its fence, not before admission closes.
		c.cancel = func() { fenceOnce.Do(func() { close(cancelEntered); <-cancelRelease }); cancel() }
		c.mu.Unlock()
		go func() { retired <- c.Retire(context.Background()) }()
		awaitRetirementProof(t, cancelEntered)
		if canceled {
			cancel()
		}
	} else if canceled {
		cancelParent()
	}
	if got := occurrence.worker.Context().Err() != nil; got != canceled {
		t.Fatalf("dispatch cancellation visible=%t, want %t", got, canceled)
	}
	if owner.ActiveCount() != 2 {
		t.Fatalf("admitted scan/worker lost exact lifetime: %d", owner.ActiveCount())
	}
	releaseDispatch()
	if canceled {
		// A canceled context is not evidence that result classification has completed.
		workerFinished := make(chan struct{})
		go func() { c.workers.Wait(); close(workerFinished) }()
		awaitRetirementProof(t, workerFinished)
	} else {
		awaitRetirementProof(t, occurrence.worker.Context().Done())
	}
	releaseScan()
	awaitRetirementProof(t, c.done)
	releaseCancel()
	if ownedRetirement {
		err = <-retired
	} else {
		err = c.Retire(context.Background())
	}
	fatal := scenario != "before_acquire" && scenario != "after_acquire" && scenario != "settled_before_retire" && scenario != "parent_cancel"
	if fatal != (err != nil) {
		t.Fatalf("retirement error=%v, want fatal=%t", err, fatal)
	}
	if strings.Contains(scenario, "independent") || strings.HasPrefix(scenario, "joined_") {
		if !errors.Is(err, independent) {
			t.Fatalf("independent failure lost: %v", err)
		}
	}
	if scenario == "carrier_cleanup" && !errors.Is(err, carrierCleanup) {
		t.Fatalf("exact carrier cleanup error lost: %v", err)
	}
	if scenario == "lease_cleanup" && !errors.Is(err, worklifetime.ErrAlreadySettled) {
		t.Fatalf("exact worker lease cleanup error lost: %v", err)
	}
	if scenario == "unowned_retirement" && !errors.Is(err, errCoordinatorRetired) {
		t.Fatalf("foreign refusal was suppressed: %v", err)
	}
	if owner.ActiveCount() != 0 {
		t.Fatalf("retirement finished with %d active leases", owner.ActiveCount())
	}
	for range 2 {
		if next := c.Retire(context.Background()); (next != nil) != fatal || (fatal && !errors.Is(next, err)) {
			t.Fatalf("repeated retirement lost its exact result: %v", next)
		}
	}
	select {
	case got := <-reported:
		if !fatal || !errors.Is(got, err) {
			t.Fatalf("unexpected failure report: %v", got)
		}
	default:
		if fatal {
			t.Fatal("independent failure was not reported")
		}
	}
	select {
	case duplicate := <-reported:
		t.Fatalf("duplicate failure report: %v", duplicate)
	default:
	}
	if _, err := c.Acquire(id); err == nil {
		t.Fatal("retired coordinator admitted a successor carrier")
	}
}
