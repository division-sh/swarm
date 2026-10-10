package delivery

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

const managerHeartbeatProofLease = 1500 * time.Millisecond

// These fixed-duration proof cuts share the native admission, transaction and
// acknowledgment path. Production methods retain their normal lease duration.
func (s *DeliveryPostgresOwner) ClaimManagerHeartbeatProofForTest(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
	return s.claimDeliveryWithLease(ctx, authority, event, route, managerHeartbeatProofLease)
}

func (s *DeliverySQLiteOwner) ClaimManagerHeartbeatProofForTest(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
	return s.claimDeliveryWithLease(ctx, authority, event, route, managerHeartbeatProofLease)
}

func (s *DeliveryPostgresOwner) RenewManagerHeartbeatProofForTest(ctx context.Context, claim deliverylifecycle.Claim) (deliverylifecycle.ClaimCommit, error) {
	return s.renewDeliveryWithLease(ctx, claim, managerHeartbeatProofLease)
}

func (s *DeliverySQLiteOwner) RenewManagerHeartbeatProofForTest(ctx context.Context, claim deliverylifecycle.Claim) (deliverylifecycle.ClaimCommit, error) {
	return s.renewDeliveryWithLease(ctx, claim, managerHeartbeatProofLease)
}
