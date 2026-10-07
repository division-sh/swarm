package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func settleCanceledDeliveryTurn(ctx context.Context, mutation *mutationprotocol.Attempt, postgres bool, delivery providerDrainDeliveryOwner, attempt runtimeeffects.Attempt, selectedRecoveryExecutionID string) (deliverylifecycle.Snapshot, error) {
	if delivery == nil || !attempt.Authority.HasBusinessTurnOrigin() ||
		attempt.Kind != runtimeeffects.KindProviderTurn || attempt.Origin.Kind != runtimeeffects.CompletionOriginDelivery || attempt.Origin.Validate() != nil {
		return deliverylifecycle.Snapshot{}, fmt.Errorf("canceled delivery turn requires its exact admitted provider origin")
	}
	var snapshot deliverylifecycle.Snapshot
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		// Lock order matches timeout admission: delivery, physical origin, then
		// logical turn. Completion cannot be overtaken by cancellation.
		if _, err := delivery.ProviderOriginPendingTx(ctx, tx, attempt.Origin.Delivery); err != nil {
			return err
		}
		facts, err := prepareCanceledTurnSettlementTx(ctx, tx, postgres, attempt)
		if err != nil {
			return err
		}
		if selectedRecoveryExecutionID == "" {
			snapshot, err = delivery.SettleProviderCanceledOriginTx(ctx, mutation, attempt.Origin.Delivery, facts.reason, facts.duration)
		} else {
			snapshot, err = delivery.SettleSelectedCanceledOriginRecoveryTx(ctx, mutation, attempt.Origin.Delivery, selectedRecoveryExecutionID, facts.reason, facts.duration)
		}
		if err != nil {
			return err
		}
		if err := completeCanceledTurnTx(ctx, tx, postgres, facts.turnID, snapshot.SettledAt); err != nil {
			return err
		}
		// The recorded response remains immutable history, but canceled work
		// cannot remain an executable completion continuation.
		return deactivateCanceledDeliveryContinuationsTx(ctx, tx, postgres, attempt.Origin)
	})
	return snapshot, err
}

func deactivateCanceledDeliveryContinuationsTx(ctx context.Context, tx *sql.Tx, postgres bool, origin runtimeeffects.CompletionOrigin) error {
	if origin.Kind != runtimeeffects.CompletionOriginDelivery {
		return nil
	}
	return deactivateCanceledDeliveryContinuationsForDeliveryTx(ctx, tx, postgres, origin.Delivery.DeliveryID())
}

func deactivateCanceledDeliveryContinuationsForDeliveryTx(ctx context.Context, tx *sql.Tx, postgres bool, deliveryID string) error {
	query := `UPDATE runtime_external_effect_attempts SET completion_continuation_active=FALSE WHERE origin_delivery_id=$1::uuid AND completion_continuation_active=TRUE`
	if !postgres {
		query = `UPDATE runtime_external_effect_attempts SET completion_continuation_active=0 WHERE origin_delivery_id=? AND completion_continuation_active=1`
	}
	_, err := tx.ExecContext(ctx, query, deliveryID)
	return err
}

type canceledTurnSettlementFacts struct {
	turnID   string
	intent   runtimeeffects.TurnCancellation
	reason   deliverylifecycle.CancellationReason
	now      time.Time
	duration time.Duration
}

type pendingCanceledTurnEffects struct{ count int }

func (e *pendingCanceledTurnEffects) Error() string {
	return fmt.Sprintf("canceled turn still owns %d unsettled provider attempts", e.count)
}

func prepareCanceledTurnSettlementTx(ctx context.Context, tx *sql.Tx, postgres bool, attempt runtimeeffects.Attempt) (canceledTurnSettlementFacts, error) {
	if err := requireExactLaunchAttempt(ctx, tx, postgres, attempt); err != nil {
		return canceledTurnSettlementFacts{}, err
	}
	turnID, _, err := businessTurnIdentity(attempt.Origin)
	if err != nil {
		return canceledTurnSettlementFacts{}, err
	}
	owner, err := businessTurnOwner(attempt.Authority)
	if err != nil {
		return canceledTurnSettlementFacts{}, err
	}
	query := `SELECT run_id::text,agent_id,flow_instance,cancel_reason,cancel_cause_event_id::text,cancel_requested_at,first_launched_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1::uuid FOR UPDATE`
	if !postgres {
		query = `SELECT run_id,agent_id,flow_instance,cancel_reason,cancel_cause_event_id,cancel_requested_at,first_launched_at FROM runtime_agent_turn_lifetimes WHERE turn_id=?`
	}
	var runID, agentID, flow string
	var reason, cause sql.NullString
	var requested, launched any
	if err := tx.QueryRowContext(ctx, query, turnID).Scan(&runID, &agentID, &flow, &reason, &cause, &requested, &launched); err != nil {
		return canceledTurnSettlementFacts{}, err
	}
	if runID != owner.RunID || agentID != attempt.Authority.Target.AgentID || flow != owner.Route.InstancePath || !reason.Valid || !cause.Valid || requested == nil {
		return canceledTurnSettlementFacts{}, fmt.Errorf("canceled turn lacks exact durable authored intent")
	}
	cancellation, err := deliverylifecycle.ParseCancellationReason(reason.String)
	if err != nil {
		return canceledTurnSettlementFacts{}, err
	}
	requestedAt, valid, err := sqliteTimeValue(requested)
	if err != nil || !valid {
		return canceledTurnSettlementFacts{}, fmt.Errorf("canceled turn has an invalid intent timestamp")
	}
	if err := requireBusinessTurnBindingTx(ctx, tx, postgres, turnID, attempt, requestedAt); err != nil {
		return canceledTurnSettlementFacts{}, err
	}
	intent := runtimeeffects.TurnCancellation{Committed: true, Requested: true, Origin: attempt.Origin, Reason: cancellation, CauseEvent: cause.String, RequestedAt: requestedAt}
	if err := intent.ValidateIntent(); err != nil {
		return canceledTurnSettlementFacts{}, err
	}
	if err := requireCanceledPhysicalSetClosed(ctx, tx, attempt.Origin); err != nil {
		return canceledTurnSettlementFacts{}, err
	}
	query = `SELECT clock_timestamp()`
	if !postgres {
		query = `SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')`
	}
	var rawNow any
	if err := tx.QueryRowContext(ctx, query).Scan(&rawNow); err != nil {
		return canceledTurnSettlementFacts{}, err
	}
	now, valid, err := sqliteTimeValue(rawNow)
	if err != nil || !valid {
		return canceledTurnSettlementFacts{}, fmt.Errorf("canceled turn settlement has no database time")
	}
	facts := canceledTurnSettlementFacts{turnID: turnID, intent: intent, reason: cancellation, now: now}
	if first, valid, err := sqliteTimeValue(launched); err != nil {
		return canceledTurnSettlementFacts{}, err
	} else if valid && now.After(first) {
		facts.duration = now.Sub(first)
	}
	return facts, nil
}

func requireCanceledPhysicalSetClosed(ctx context.Context, tx *sql.Tx, origin runtimeeffects.CompletionOrigin) error {
	_, originID, err := businessTurnIdentity(origin)
	if err != nil {
		return err
	}
	column := "origin_delivery_id"
	if origin.Kind == runtimeeffects.CompletionOriginDirective {
		column = "origin_directive_operation_id"
	}
	var pending int
	query := fmt.Sprintf(`SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE %s=$1 AND state IN ('authorized','launched','response_observed')`, column)
	if err := tx.QueryRowContext(ctx, query, originID).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return &pendingCanceledTurnEffects{count: pending}
	}
	return nil
}

func completeCanceledTurnTx(ctx context.Context, tx *sql.Tx, postgres bool, turnID string, settledAt time.Time) error {
	query := `UPDATE runtime_agent_turn_lifetimes SET settled_at=$1 WHERE turn_id=$2::uuid AND settled_at IS NULL`
	if !postgres {
		query = `UPDATE runtime_agent_turn_lifetimes SET settled_at=? WHERE turn_id=? AND settled_at IS NULL`
	}
	_, err := tx.ExecContext(ctx, query, settledAt, turnID)
	return err
}
