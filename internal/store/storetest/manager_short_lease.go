package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ClaimManagerHeartbeatProof(ctx context.Context, selected any, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
	return private.ClaimManagerHeartbeatProofForTest(ctx, selected, authority, event, route)
}

func RenewManagerHeartbeatProof(ctx context.Context, selected any, claim deliverylifecycle.Claim) (deliverylifecycle.ClaimCommit, error) {
	return private.RenewManagerHeartbeatProofForTest(ctx, selected, claim)
}
