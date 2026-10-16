package delivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *DeliveryPostgresOwner) ValidateProviderResumeClaimTx(ctx context.Context, tx *sql.Tx, previous, current runtimedelivery.Claim) error {
	return postgresDeliveryAdapter.validateProviderResumeClaimTx(ctx, tx, previous, current)
}

func (s *DeliverySQLiteOwner) ValidateProviderResumeClaimTx(ctx context.Context, tx *sql.Tx, previous, current runtimedelivery.Claim) error {
	return sqliteDeliveryAdapter.validateProviderResumeClaimTx(ctx, tx, previous, current)
}

func (a *Adapter) validateProviderResumeClaimTx(ctx context.Context, tx *sql.Tx, previous, current runtimedelivery.Claim) error {
	if previous.Validate() != nil || current.Validate() != nil || previous.SubscriberClass() != runtimedelivery.SubscriberAgent ||
		previous.DeliveryID() != current.DeliveryID() || previous.RunID() != current.RunID() ||
		previous.RouteIdentity() != current.RouteIdentity() || previous.SubscriberID() != current.SubscriberID() ||
		current.SubscriberClass() != previous.SubscriberClass() || current.Version() != previous.Version()+1 ||
		current.PersistenceToken() == previous.PersistenceToken() {
		return fmt.Errorf("provider resume requires the exact next origin claim")
	}
	if err := a.ValidateCurrentClaim(ctx, tx, current); err != nil {
		return err
	}
	record, err := a.loadByID(ctx, tx, current.DeliveryID(), true)
	if err != nil {
		return err
	}
	if record.MaxRetries != runtimedelivery.AgentMaxRetries || record.RetryCount != runtimedelivery.AgentMaxRetries ||
		events.EncodeDeliveryRouteIdentity(record.RouteIdentity) != previous.RouteIdentity() {
		return fmt.Errorf("provider resume requires its original once-only retry charge")
	}
	var charged int
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts
		WHERE CAST(delivery_id AS TEXT)=$1 AND claim_version=$2 AND CAST(claim_token AS TEXT)=$3
		AND outcome='retry_scheduled' AND reason_code='provider_resume_queued'
		AND closure_kind='settled' AND open_marker=FALSE AND completed_at IS NOT NULL`,
		previous.DeliveryID(), previous.Version(), previous.PersistenceToken()).Scan(&charged)
	if err != nil {
		return err
	}
	if charged != 1 {
		return fmt.Errorf("provider resume lacks its exact settled retry outcome")
	}
	return nil
}

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
