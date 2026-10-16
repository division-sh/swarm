package delivery

import (
	"context"
	"database/sql"
	"fmt"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *DeliveryPostgresOwner) QueueProviderResumeTx(ctx context.Context, mutation *mutationprotocol.Attempt, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (bool, error) {
	return queueProviderResume(ctx, mutation, postgresDeliveryAdapter, claim, settlement, s.SettleProviderOriginRecoveryFailureTx)
}

func (s *DeliverySQLiteOwner) QueueProviderResumeTx(ctx context.Context, mutation *mutationprotocol.Attempt, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement) (bool, error) {
	return queueProviderResume(ctx, mutation, sqliteDeliveryAdapter, claim, settlement, s.SettleProviderOriginRecoveryFailureTx)
}

func queueProviderResume(ctx context.Context, mutation *mutationprotocol.Attempt, adapter *Adapter, claim runtimedelivery.Claim, settlement runtimedelivery.Settlement, settle func(context.Context, *mutationprotocol.Attempt, runtimedelivery.Claim, runtimedelivery.Settlement) error) (queued bool, err error) {
	if claim.SubscriberClass() != runtimedelivery.SubscriberAgent || settlement.Disposition != runtimedelivery.FailureRetry {
		return false, fmt.Errorf("provider resume requires an agent retry settlement")
	}
	err = mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		pending, err := adapter.providerOriginPendingTx(ctx, tx, claim)
		if err != nil || !pending {
			return err
		}
		record, err := adapter.loadByID(ctx, tx, claim.DeliveryID(), true)
		if err != nil {
			return err
		}
		if record.MaxRetries != runtimedelivery.AgentMaxRetries {
			return fmt.Errorf("provider resume origin has a different retry contract")
		}
		if record.RetryCount >= record.MaxRetries {
			return nil
		}
		if err := settle(ctx, mutation, claim, settlement); err != nil {
			return err
		}
		queued = true
		return nil
	})
	return queued, err
}
