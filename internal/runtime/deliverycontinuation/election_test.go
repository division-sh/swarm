package deliverycontinuation

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type electionDispatcher func(context.Context, events.Event, events.DeliveryRoute) DispatchResult

func (d electionDispatcher) DispatchDeliveryContinuation(ctx context.Context, evt events.Event, route events.DeliveryRoute) DispatchResult {
	return d(ctx, evt, route)
}

func TestCoordinatorCarrierWinsBeforeScanDispatch(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	evt, route := coordinatorTestEvent("election"), coordinatorTestAgentRoute(t, "winner")
	id, err := runtimedelivery.DeliveryID(evt.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	store := &coordinatorTestStore{pages: []runtimedelivery.ContinuationPage{{
		Items: []runtimedelivery.ContinuationItem{{DeliveryID: id, Event: evt, Disposition: runtimedelivery.ClaimAcquired, Snapshot: runtimedelivery.Snapshot{DeliveryID: id, Route: route, Authority: authority, Status: runtimedelivery.StatusPending}}}, Exhausted: true,
	}}}
	var c *Coordinator
	var winner worklifetime.DeliveryContinuation
	dispatched := make(chan struct{}, 1)
	dispatcher := electionDispatcher(func(context.Context, events.Event, events.DeliveryRoute) DispatchResult {
		// The real competing carrier wins after the store's scan observation
		// and before dispatch's election. No timing or goroutine-start proxy.
		first, err := c.Acquire(id)
		if err != nil {
			return Fatal(err)
		}
		var acquired bool
		winner, acquired = first.Acquired()
		if !acquired {
			t.Fatal("competing carrier did not win")
		}
		second, err := c.Acquire(id)
		if err != nil {
			return Fatal(err)
		}
		if second.Disposition() != worklifetime.DeliveryAlreadyOwned {
			t.Fatal("scan fabricated a second winner")
		}
		defer func() { dispatched <- struct{}{} }()
		return AlreadyOwned()
	})
	c, err = New(store, authority, owner, dispatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("lawful loser failed startup: %v", err)
	}
	select {
	case <-dispatched:
	case <-ctx.Done():
		t.Fatal("scan did not reach real acquisition")
	}
	if winner == nil {
		t.Fatal("scan did not reach real acquisition")
	}
	if err := c.Synchronize(ctx); err != nil {
		t.Fatalf("lawful winner broke synchronization: %v", err)
	}
	if err := c.Release(id); err != nil {
		t.Fatal(err)
	}
	if resolution, err := winner.Resolve(ctx, worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationTerminal {
		t.Fatalf("winner settlement: %d %v", resolution, err)
	}
	if err := c.Retire(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorElectionPreservesHandoffAttemptAndTerminalEvidence(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	c, err := New(&coordinatorTestStore{}, authority, owner, &coordinatorTestDispatcher{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	const id = "exact-election"
	if _, err := c.Acquire(id); err == nil {
		t.Fatal("absence was treated as contention or terminal evidence")
	}
	proof, err := runtimedelivery.AdmitDurableHandoffProof(id, "event", "route", authority)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.observe(id); err != nil {
		t.Fatal(err)
	}
	first, err := acquireCoordinatorTestCapability(c, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err != nil {
		t.Fatalf("scan-before-handoff rejected lawful carrier: %v", err)
	}
	if a, err := c.Acquire(id); err != nil || a.Disposition() != worklifetime.DeliveryAlreadyOwned {
		t.Fatalf("handoff overwrote carrier: %+v %v", a, err)
	}
	if _, err := first.Resolve(context.Background(), worklifetime.DeliveryContinuationConsume); err != nil {
		t.Fatal(err)
	}
	oldObservation := c.heldEntries()[id]
	c.reclaimAttempt(id, oldObservation)
	second, err := acquireCoordinatorTestCapability(c, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Resolve(context.Background(), worklifetime.DeliveryContinuationConsume); err != nil {
		t.Fatal(err)
	}
	c.reclaimAttempt(id, oldObservation)
	if err := c.Retain(runtimedelivery.Snapshot{DeliveryID: id, Status: runtimedelivery.StatusFailed, Authority: authority}); err != nil {
		t.Fatal(err)
	}
	if a, err := c.Acquire(id); err != nil || a.Disposition() != worklifetime.DeliveryAlreadyOwned {
		t.Fatalf("stale observation/retain stole new attempt: %+v %v", a, err)
	}
	if _, err := (&capability{coordinator: c, deliveryID: id}).Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err == nil {
		t.Fatal("same-ID forged capability resolved")
	}
	if err := c.Release(id); err != nil {
		t.Fatal(err)
	}
	if err := c.observe(id); err != nil {
		t.Fatal(err)
	}
	if err := c.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err != nil {
		t.Fatal(err)
	}
	if err := c.Retain(runtimedelivery.Snapshot{DeliveryID: id, Status: runtimedelivery.StatusFailed, Authority: authority}); err != nil {
		t.Fatal(err)
	}
	if a, err := c.Acquire(id); err != nil || a.Disposition() != worklifetime.DeliveryTerminallyFenced {
		t.Fatalf("late evidence revived terminal delivery: %+v %v", a, err)
	}
	if _, err := first.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err == nil {
		t.Fatal("stale settled capability resolved twice")
	}
	c.entries["corrupt"] = entry{state: 99}
	if _, err := c.Acquire("corrupt"); err == nil {
		t.Fatal("corrupt owner treated as lawful competition")
	}
}

func TestCoordinatorRetirementSettlesPredecessorWithoutAdmittingSuccessorWork(t *testing.T) {
	for _, terminalFirst := range []bool{false, true} {
		name := "return_accepted"
		if terminalFirst {
			name = "terminal_fence"
		}
		t.Run(name, func(t *testing.T) {
			authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
			defer cleanup()
			newOwner := func(authority runtimedelivery.ExecutionAuthority) *Coordinator {
				c, err := New(&coordinatorTestStore{}, authority, owner, &coordinatorTestDispatcher{}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := c.observe("retirement-delivery"); err != nil {
					t.Fatal(err)
				}
				return c
			}
			nextAuthority, err := runtimedelivery.NewNormalExecutionAuthority(authority.SourceArtifact(), authority.ExecutionID(), authority.Generation()+1)
			if err != nil {
				t.Fatal(err)
			}
			predecessor, successor := newOwner(authority), newOwner(nextAuthority)
			old, err := acquireCoordinatorTestCapability(predecessor, "retirement-delivery")
			if err != nil {
				t.Fatal(err)
			}
			current, err := acquireCoordinatorTestCapability(successor, "retirement-delivery")
			if err != nil {
				t.Fatal(err)
			}
			if terminalFirst {
				if err := predecessor.Release(old.DeliveryID()); err != nil {
					t.Fatal(err)
				}
			}
			if err := predecessor.Retire(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := predecessor.Acquire(old.DeliveryID()); err == nil {
				t.Fatal("retired owner admitted a carrier")
			}
			want := worklifetime.DeliveryContinuationReturned
			if terminalFirst {
				want = worklifetime.DeliveryContinuationTerminal
			} else if _, err := old.Resolve(context.Background(), worklifetime.DeliveryContinuationConsume); err == nil {
				t.Fatal("retired predecessor admitted an execution attempt")
			}
			if resolution, err := old.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil || resolution != want {
				t.Fatalf("accepted cleanup: %v %v", resolution, err)
			}
			if _, err := old.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err == nil {
				t.Fatal("repeated predecessor settlement succeeded")
			}
			if result, err := successor.Acquire(current.DeliveryID()); err != nil || result.Disposition() != worklifetime.DeliveryAlreadyOwned {
				t.Fatalf("predecessor changed successor: %+v %v", result, err)
			}
			if _, err := current.Resolve(context.Background(), worklifetime.DeliveryContinuationConsume); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCoordinatorSynchronousDeferralDoesNotScheduleItsOwnRetry(t *testing.T) {
	authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
	defer cleanup()
	evt, route := coordinatorTestEvent("synchronous-deferral"), coordinatorTestAgentRoute(t, "missing")
	id, err := runtimedelivery.DeliveryID(evt.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	store := &coordinatorTestStore{pages: []runtimedelivery.ContinuationPage{{
		Items: []runtimedelivery.ContinuationItem{{DeliveryID: id, Event: evt, Disposition: runtimedelivery.ClaimAcquired, Snapshot: runtimedelivery.Snapshot{DeliveryID: id, Route: route, Authority: authority, Status: runtimedelivery.StatusPending}}}, Exhausted: true,
	}}}
	var c *Coordinator
	dispatcher := electionDispatcher(func(ctx context.Context, _ events.Event, _ events.DeliveryRoute) DispatchResult {
		cap, err := acquireCoordinatorTestCapability(c, id)
		if err != nil {
			return Fatal(err)
		}
		guard, err := worklifetime.NewDeliveryContinuationGuard(ctx, cap)
		if err != nil {
			return Fatal(err)
		}
		if _, err := guard.Complete(nil); err != nil {
			return Fatal(err)
		}
		return Deferred(DispatchWakeAgentRouteLifecycle)
	})
	c, err = New(store, authority, owner, dispatcher, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, wake, err := c.scan(context.Background(), false); err != nil || wake {
		t.Fatalf("scan wake=%v err=%v", wake, err)
	}
	job := <-c.jobs
	if err := job.lease.Done(); err != nil {
		t.Fatal(err)
	}
	c.completeDispatchWithWake(job.deliveryID, nil, DispatchTransferred, 0)
	select {
	case <-c.wake:
		t.Fatal("synchronous deferral scheduled its own retry without a named lifecycle transition")
	default:
	}
	c.Signal()
	select {
	case <-c.wake:
	default:
		t.Fatal("named lifecycle wake was lost")
	}
}

func TestCoordinatorRearmsReturnedCarrierAfterAlreadyOwnedReservation(t *testing.T) {
	for _, returned := range []bool{false, true} {
		authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
		defer cleanup()
		evt, route := coordinatorTestEvent("reserved-return"), coordinatorTestAgentRoute(t, "receiver")
		id, err := runtimedelivery.DeliveryID(evt.ID(), route)
		if err != nil {
			t.Fatal(err)
		}
		page := runtimedelivery.ContinuationPage{Items: []runtimedelivery.ContinuationItem{{DeliveryID: id, Event: evt, Disposition: runtimedelivery.ClaimAcquired, Snapshot: runtimedelivery.Snapshot{DeliveryID: id, Route: route, Authority: authority, Status: runtimedelivery.StatusPending}}}, Exhausted: true}
		var c *Coordinator
		var acquisition worklifetime.DeliveryAcquisition
		c, err = New(&coordinatorTestStore{pages: []runtimedelivery.ContinuationPage{page, page, page}}, authority, owner, electionDispatcher(func(context.Context, events.Event, events.DeliveryRoute) DispatchResult {
			if acquisition, err = c.Acquire(id); err != nil {
				return Fatal(err)
			}
			if acquisition.Disposition() == worklifetime.DeliveryAlreadyOwned {
				return AlreadyOwned()
			}
			return Transferred()
		}), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer c.finish(t.Context(), func() {}, nil, nil, false)
		scan := func(wantJobs int) {
			if _, wake, err := c.scan(t.Context(), true); err != nil || wake || len(c.jobs) != wantJobs {
				t.Fatalf("scan: jobs=%d want=%d timer=%t err=%v", len(c.jobs), wantJobs, wake, err)
			}
		}
		scan(1)
		carrier, err := acquireCoordinatorTestCapability(c, id)
		if err != nil {
			t.Fatal(err)
		}
		job := <-c.jobs
		disposition, err := c.dispatchAttempt(t.Context(), job)
		if err != nil || disposition != DispatchAlreadyOwned || acquisition.Disposition() != worklifetime.DeliveryAlreadyOwned {
			t.Fatalf("first job did not lose to the exact carrier: %v, %v", acquisition.Disposition(), err)
		}
		if returned {
			if resolution, err := carrier.Resolve(t.Context(), worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationReturned || len(c.wake) != 1 {
				t.Fatalf("queued carrier did not return and signal: %v, %v", resolution, err)
			}
			<-c.wake
			scan(0)
		}
		c.completeDispatchWithWake(job.deliveryID, nil, disposition, job.wakeVersion)
		if _, held := c.reserved[id]; held || (len(c.wake) == 1) != returned {
			t.Fatalf("after reservation clearance: held=%t wake=%d, want wake=%t", held, len(c.wake), returned)
		}
		if returned {
			<-c.wake
			scan(1)
			job = <-c.jobs
			_, err = c.dispatchAttempt(t.Context(), job)
			if next, acquired := acquisition.Acquired(); err != nil || job.deliveryID != id || !acquired || next == carrier || next.DeliveryID() != id {
				t.Fatalf("rearmed scan did not transfer the exact delivery to a new carrier: %v, %v", acquisition.Disposition(), err)
			}
		}
	}
}
