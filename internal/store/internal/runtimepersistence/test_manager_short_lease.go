package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func ClaimManagerHeartbeatProofForTest(ctx context.Context, selected any, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return deliverylifecycle.ClaimResult{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.deliveryPostgresOwner.ClaimManagerHeartbeatProofForTest(ctx, authority, event, route)
	case *SQLiteRuntimeStore:
		return owner.deliverySQLiteOwner.ClaimManagerHeartbeatProofForTest(ctx, authority, event, route)
	default:
		return deliverylifecycle.ClaimResult{}, fmt.Errorf("manager heartbeat proof requires original selected owner")
	}
}

func RenewManagerHeartbeatProofForTest(ctx context.Context, selected any, claim deliverylifecycle.Claim) (deliverylifecycle.ClaimCommit, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return deliverylifecycle.ClaimCommit{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.deliveryPostgresOwner.RenewManagerHeartbeatProofForTest(ctx, claim)
	case *SQLiteRuntimeStore:
		return owner.deliverySQLiteOwner.RenewManagerHeartbeatProofForTest(ctx, claim)
	default:
		return deliverylifecycle.ClaimCommit{}, fmt.Errorf("manager heartbeat proof requires original selected owner")
	}
}
