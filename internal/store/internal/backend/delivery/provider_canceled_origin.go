package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *DeliveryPostgresOwner) SettleProviderCanceledOriginTx(ctx context.Context, mutation *mutationprotocol.Attempt, claim deliverylifecycle.Claim, reason deliverylifecycle.CancellationReason, duration time.Duration) (deliverylifecycle.Snapshot, error) {
	return settleProviderCanceledOrigin(ctx, mutation, postgresDeliveryAdapter, claim, reason, duration, "")
}

func (s *DeliverySQLiteOwner) SettleProviderCanceledOriginTx(ctx context.Context, mutation *mutationprotocol.Attempt, claim deliverylifecycle.Claim, reason deliverylifecycle.CancellationReason, duration time.Duration) (deliverylifecycle.Snapshot, error) {
	return settleProviderCanceledOrigin(ctx, mutation, sqliteDeliveryAdapter, claim, reason, duration, "")
}

func (s *DeliveryPostgresOwner) SettleSelectedCanceledOriginRecoveryTx(ctx context.Context, mutation *mutationprotocol.Attempt, claim deliverylifecycle.Claim, executionID string, reason deliverylifecycle.CancellationReason, duration time.Duration) (deliverylifecycle.Snapshot, error) {
	if executionID == "" {
		return deliverylifecycle.Snapshot{}, fmt.Errorf("selected cancellation recovery requires exact execution identity")
	}
	return settleProviderCanceledOrigin(ctx, mutation, postgresDeliveryAdapter, claim, reason, duration, executionID)
}

func (s *DeliverySQLiteOwner) SettleSelectedCanceledOriginRecoveryTx(ctx context.Context, mutation *mutationprotocol.Attempt, claim deliverylifecycle.Claim, executionID string, reason deliverylifecycle.CancellationReason, duration time.Duration) (deliverylifecycle.Snapshot, error) {
	if executionID == "" {
		return deliverylifecycle.Snapshot{}, fmt.Errorf("selected cancellation recovery requires exact execution identity")
	}
	return settleProviderCanceledOrigin(ctx, mutation, sqliteDeliveryAdapter, claim, reason, duration, executionID)
}

func settleProviderCanceledOrigin(ctx context.Context, mutation *mutationprotocol.Attempt, adapter *Adapter, claim deliverylifecycle.Claim, reason deliverylifecycle.CancellationReason, duration time.Duration, recoveryExecutionID string) (deliverylifecycle.Snapshot, error) {
	return withDeliverySQL(ctx, mutation, func(ctx context.Context, tx *sql.Tx) (deliverylifecycle.Snapshot, error) {
		if _, err := deliverylifecycle.ParseCancellationReason(string(reason)); err != nil || duration < 0 || claim.SubscriberClass() != deliverylifecycle.SubscriberAgent {
			return deliverylifecycle.Snapshot{}, fmt.Errorf("provider cancellation requires an exact agent origin, reason and duration")
		}
		if recoveryExecutionID != "" {
			if err := validateSelectedOriginExecution(ctx, tx, adapter, claim, recoveryExecutionID); err != nil {
				return deliverylifecycle.Snapshot{}, err
			}
			var fenced bool
			if err := tx.QueryRowContext(ctx, `SELECT state IN ('failed','closed') AND lease_expires_at IS NULL FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, recoveryExecutionID).Scan(&fenced); err != nil {
				return deliverylifecycle.Snapshot{}, err
			}
			if !fenced {
				return deliverylifecycle.Snapshot{}, fmt.Errorf("selected canceled origin requires its fenced recovery owner")
			}
		}
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
		if recoveryExecutionID == "" {
			return adapter.SettleCanceled(ctx, mutation, claim, reason, duration)
		}
		record, now, err := adapter.requireExactClaim(ctx, tx, claim)
		if err != nil {
			return deliverylifecycle.Snapshot{}, err
		}
		return adapter.settleExactClaim(ctx, tx, mutation, claim, deliverylifecycle.Settlement{Duration: duration, RuleSelection: handlerselection.NotReached()}, reason, record, now)
	})
}
