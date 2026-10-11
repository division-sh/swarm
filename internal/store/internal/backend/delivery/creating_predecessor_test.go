package delivery

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestCreatingPredecessorRequiresOnlyFirstExistingClaim(t *testing.T) {
	target := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: "one"})
	for _, test := range []struct {
		name    string
		status  deliverylifecycle.Status
		version int64
		ready   bool
	}{
		{"unclaimed", deliverylifecycle.StatusPending, 0, false},
		{"busy_or_reclaimable", deliverylifecycle.StatusInProgress, 1, true},
		{"retry_scheduled_or_eligible", deliverylifecycle.StatusFailed, 1, true},
		{"terminal", deliverylifecycle.StatusDelivered, 1, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := deliveryRecord{Snapshot: deliverylifecycle.Snapshot{Route: events.DeliveryRoute{Target: target}, Status: test.status, ClaimVersion: test.version}}
			ready, err := (&Adapter{}).creatingPredecessorReady(context.Background(), nil, record)
			if ready != test.ready || (err == nil) != test.ready {
				t.Fatalf("first-claim construction boundary: ready=%t err=%v want=%t", ready, err, test.ready)
			}
		})
	}
}
