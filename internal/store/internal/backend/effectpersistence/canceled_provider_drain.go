package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

// A captured physical tail still records its immutable effects when authored
// cancellation owns the origin. The origin/reaction commit is separate from
// physical settlement and must wait for the complete accepted physical set.
type providerOriginCancellationDisposition uint8

const (
	providerOriginNotCanceled providerOriginCancellationDisposition = iota
	providerOriginCancelPending
	providerOriginCancelSettled
)

type providerOriginCancellation struct {
	disposition providerOriginCancellationDisposition
	intent      *runtimeeffects.TurnCancellation
}

func observeCanceledProviderOrigin(ctx context.Context, tx *sql.Tx, postgres bool, attempt runtimeeffects.Attempt, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner) (providerOriginCancellation, error) {
	pending, err := providerTurnPendingTx(ctx, tx, attempt, delivery, directives)
	if err != nil {
		return providerOriginCancellation{}, err
	}
	turnID, _, err := businessTurnIdentity(attempt.Origin)
	if err != nil {
		return providerOriginCancellation{}, err
	}
	query := `SELECT cancel_reason,cancel_cause_event_id::text,cancel_requested_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1::uuid FOR UPDATE`
	if !postgres {
		query = `SELECT cancel_reason,cancel_cause_event_id,cancel_requested_at FROM runtime_agent_turn_lifetimes WHERE turn_id=?`
	}
	var reason sql.NullString
	var cause sql.NullString
	var requested any
	if err := tx.QueryRowContext(ctx, query, turnID).Scan(&reason, &cause, &requested); err != nil {
		return providerOriginCancellation{}, fmt.Errorf("read captured provider turn lifetime: %w", err)
	}
	if !reason.Valid {
		return providerOriginCancellation{disposition: providerOriginNotCanceled}, nil
	}
	at, valid, err := sqliteTimeValue(requested)
	if err != nil || !valid || !cause.Valid {
		return providerOriginCancellation{}, fmt.Errorf("captured turn cancellation has incomplete intent")
	}
	if err := requireBusinessTurnBindingTx(ctx, tx, postgres, turnID, attempt, at); err != nil {
		return providerOriginCancellation{}, err
	}
	intent := runtimeeffects.TurnCancellation{Committed: true, Requested: true, Origin: attempt.Origin, Reason: deliverylifecycle.CancellationReason(reason.String), CauseEvent: cause.String, RequestedAt: at}
	if err := intent.ValidateIntent(); err != nil {
		return providerOriginCancellation{}, err
	}
	// Only the outer mutation acknowledgment stamps the returned evidence.
	intent.Committed = false
	result := providerOriginCancellation{disposition: providerOriginCancelPending, intent: &intent}
	if !pending {
		result.disposition = providerOriginCancelSettled
		intent.OriginSettled = true
		return result, nil
	}
	// Physical-tail settlement is immutable evidence, not authority to cancel
	// the business origin without its reaction. Manager or startup consumes the
	// same CommitCanceledTurn operation after the complete physical set joins.
	return result, nil
}
