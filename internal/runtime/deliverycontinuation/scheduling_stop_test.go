package deliverycontinuation

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestCoordinatorSchedulingStopPreservesItsExactOwner(t *testing.T) {
	for _, stop := range []string{"canceled", "deadline", "retired", "retired_and_canceled"} {
		t.Run(stop, func(t *testing.T) {
			authority, owner, cleanup := coordinatorTestAuthorityAndOwner(t)
			defer cleanup()
			c, err := New(&coordinatorTestStore{}, coordinatorTestRestarts{}, authority, owner, &coordinatorTestDispatcher{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if stop == "deadline" {
				var stopDeadline context.CancelFunc
				ctx, stopDeadline = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer stopDeadline()
			}
			if stop == "canceled" || stop == "retired_and_canceled" {
				cancel()
			}
			retired := stop == "retired" || stop == "retired_and_canceled"
			if retired {
				c.BeginRetirement()
			}
			err = c.schedule(ctx, runtimedelivery.ContinuationItem{DeliveryID: "stopped-before-schedule"})
			want := ctx.Err()
			if retired {
				want = errCoordinatorRetired
			}
			if !errors.Is(err, want) || (!retired && errors.Is(err, errCoordinatorRetired)) {
				t.Fatalf("schedule refusal=%v, want exact stop owner %v (retired=%t)", err, want, retired)
			}
			if !ordinaryCoordinatorStop(ctx, err, retired) {
				t.Fatalf("owned schedule stop would become a fatal joined shutdown: %v", err)
			}
			if len(c.jobs) != 0 || len(c.reserved) != 0 || owner.ActiveCount() != 0 || c.retired != retired {
				t.Fatal("stopped scheduling admitted work or invented retirement state")
			}
		})
	}
}
