package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *DeliveryPostgresOwner) SettleProviderCanceledOriginTx(ctx context.Context, mutation *mutationprotocol.Attempt, claim deliverylifecycle.Claim, reason deliverylifecycle.CancellationReason, duration time.Duration) (deliverylifecycle.Snapshot, error) {
	return settleProviderCanceledOrigin(ctx, mutation, postgresDeliveryAdapter, claim, reason, duration)
}

func (s *DeliverySQLiteOwner) SettleProviderCanceledOriginTx(ctx context.Context, mutation *mutationprotocol.Attempt, claim deliverylifecycle.Claim, reason deliverylifecycle.CancellationReason, duration time.Duration) (deliverylifecycle.Snapshot, error) {
	return settleProviderCanceledOrigin(ctx, mutation, sqliteDeliveryAdapter, claim, reason, duration)
}

func settleProviderCanceledOrigin(ctx context.Context, mutation *mutationprotocol.Attempt, adapter *Adapter, claim deliverylifecycle.Claim, reason deliverylifecycle.CancellationReason, duration time.Duration) (deliverylifecycle.Snapshot, error) {
	return withDeliverySQL(ctx, mutation, func(ctx context.Context, tx *sql.Tx) (deliverylifecycle.Snapshot, error) {
		terminal, err := adapter.prepareProviderOriginRecovery(ctx, tx, mutation, claim)
		if err != nil {
			return deliverylifecycle.Snapshot{}, err
		}
		if terminal {
			snapshot, err := adapter.Snapshot(ctx, tx, claim.DeliveryID())
			if err != nil {
				return deliverylifecycle.Snapshot{}, err
			}
			if !snapshot.MatchesSettlementClaim(claim) || snapshot.Status != deliverylifecycle.StatusCanceled || snapshot.ReasonCode != string(reason) {
				return deliverylifecycle.Snapshot{}, fmt.Errorf("%w: canceled turn contradicts settled origin", deliverylifecycle.ErrConflict)
			}
			return snapshot, nil
		}
		return adapter.SettleCanceled(ctx, mutation, claim, reason, duration)
	})
}
