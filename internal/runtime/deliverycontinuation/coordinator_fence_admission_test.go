package deliverycontinuation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestCoordinatorPublishedDispatchUsesPreFenceLease(t *testing.T) {
	dispatched := make(chan struct{}, 1)
	dispatcher := &coordinatorTestDispatcher{dispatched: dispatched, result: Transferred()}
	f := newPublishedDispatchTestFixture(t, dispatcher, nil)
	if err := f.coordinator.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	event := coordinatorTestEvent("pre-fence-publication")
	route := publishedDispatchTestNodeRoute(t, "pre-fence")
	f.accept(t, event, route)
	if err := f.owner.Fence(); err != nil {
		t.Fatal(err)
	}
	if err := f.coordinator.DispatchPublished(event, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("committed handoff reacquired fenced admission: %v", err)
	}
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("pre-fence committed handoff did not execute")
	}
	publishedDispatchTestQuiesce(t, f.owner)
	if err := f.coordinator.Retire(t.Context()); err != nil || f.owner.ActiveCount() != 0 {
		t.Fatalf("accepted dispatch retirement: active=%d err=%v", f.owner.ActiveCount(), err)
	}
	if err := f.coordinator.DispatchPublished(event, []events.DeliveryRoute{route}); !errors.Is(err, errCoordinatorRetired) {
		t.Fatalf("retired coordinator admitted a late handoff: %v", err)
	}
}

func TestCoordinatorDispatchUsesExactPreFenceLease(t *testing.T) {
	for _, variant := range []string{"accepted", "unowned", "foreign", "settled", "retired"} {
		t.Run(variant, func(t *testing.T) {
			authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
			defer cleanup()
			standing, err := owner.BeginStanding(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			settled := false
			defer func() {
				if !settled {
					if err := standing.Done(); err != nil {
						t.Error(err)
					}
				}
			}()
			c, err := New(&coordinatorTestStore{}, coordinatorTestRestarts{}, authority, owner, &coordinatorTestDispatcher{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx := standing.Context()
			switch variant {
			case "unowned":
				ctx = context.Background()
			case "foreign":
				_, foreign, cleanupForeign := coordinatorTestAuthorityAndOwner(t)
				defer cleanupForeign()
				lease, err := foreign.BeginStanding(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := lease.Done(); err != nil {
						t.Error(err)
					}
				}()
				ctx = lease.Context()
			case "settled":
				if err := standing.Done(); err != nil {
					t.Fatal(err)
				}
				settled = true
			case "retired":
				owner.Retire()
			}
			if err := owner.Fence(); err != nil && variant != "retired" {
				t.Fatal(err)
			}
			before := owner.ActiveCount()
			err = c.schedule(ctx, runtimedelivery.ContinuationItem{
				DeliveryID: "exact-pre-fence", Event: coordinatorTestEvent(variant),
			})
			if variant != "accepted" {
				if err == nil || owner.ActiveCount() != before || len(c.jobs) != 0 || len(c.reserved) != 0 {
					t.Fatalf("invalid ancestor admitted work: err=%v active=%d/%d jobs=%d reserved=%d", err, owner.ActiveCount(), before, len(c.jobs), len(c.reserved))
				}
				return
			}
			if err != nil {
				t.Fatalf("accepted coordinator reacquired fenced admission: %v", err)
			}
			job := <-c.jobs
			if job.lease == nil || job.lease.Context().Err() != nil || owner.ActiveCount() != before+1 {
				t.Fatal("pre-fence dispatch lost its live exact lease")
			}
			if _, err := owner.Begin(ctx); !errors.Is(err, worklifetime.ErrAdmissionFenced) {
				t.Fatalf("new admission bypassed the fence: %v", err)
			}
			if err := job.lease.Done(); err != nil || owner.ActiveCount() != before {
				t.Fatalf("exact dispatch settlement: err=%v active=%d/%d", err, owner.ActiveCount(), before)
			}
			if err := job.lease.Done(); !errors.Is(err, worklifetime.ErrAlreadySettled) || owner.ActiveCount() != before {
				t.Fatal("duplicate dispatch settlement changed its ancestor")
			}
		})
	}
}
