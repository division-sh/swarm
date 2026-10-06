package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func recoverUnstartedCanceledTurn(ctx context.Context, tx *sql.Tx, delivery providerDrainDeliveryOwner, row canceledTurnRecoveryRow) (effects.TurnExecutionResult, error) {
	if !row.originEvidence.Valid || row.firstAttempt.Valid || row.bound.Valid || row.emit.Valid || row.timeoutEvent.Valid || row.launched != nil || row.reason != string(deliverylifecycle.CancellationTerminate) {
		return effects.TurnExecutionResult{}, fmt.Errorf("unstarted recovery has incomplete or contradictory origin evidence")
	}
	origin, owner, err := decodeUnstartedOrigin([]byte(row.originEvidence.String))
	if err != nil {
		return effects.TurnExecutionResult{}, err
	}
	id, _, err := businessTurnIdentity(origin)
	if err != nil || id != row.turnID || origin.Kind != effects.CompletionOriginDelivery || owner.RunID != row.runID || owner.Route.InstancePath != row.flow || delivery == nil {
		return effects.TurnExecutionResult{}, fmt.Errorf("unstarted recovery contradicts its actual origin/owner")
	}
	if err := delivery.ValidateUnstartedClaimOwnerTx(ctx, tx, origin.Delivery, owner, row.agentID); err != nil {
		return effects.TurnExecutionResult{}, err
	}
	var physical int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE origin_kind='delivery' AND origin_delivery_id=$1`, origin.Delivery.DeliveryID()).Scan(&physical); err != nil {
		return effects.TurnExecutionResult{}, err
	}
	if physical != 0 {
		return effects.TurnExecutionResult{}, fmt.Errorf("unstarted recovery cannot consume physical attempt ownership")
	}
	at, valid, err := sqliteTimeValue(row.requested)
	if err != nil || !valid {
		return effects.TurnExecutionResult{}, fmt.Errorf("unstarted recovery has no exact intent timestamp")
	}
	intent := effects.TurnCancellation{Committed: true, Requested: true, Origin: origin, Reason: deliverylifecycle.CancellationTerminate, CauseEvent: row.cause, RequestedAt: at}
	if err := intent.ValidateIntent(); err != nil {
		return effects.TurnExecutionResult{}, err
	}
	// No provider attempt or execution mode exists to re-admit. This result
	// carries only the already-canceled work origin for owned cleanup/settlement.
	return effects.TurnExecutionResult{Attempt: effects.Attempt{Origin: origin}, Cancellation: intent}, nil
}
