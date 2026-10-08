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

func (s *EffectPostgresOwner) RequestTurnTimeout(ctx context.Context, attempt runtimeeffects.Attempt, now time.Time) (runtimeeffects.TurnCancellation, error) {
	if err := s.requireCurrent(); err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, mutation *mutationprotocol.Attempt) (runtimeeffects.TurnCancellation, error) {
		var cancellation runtimeeffects.TurnCancellation
		err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			pending, err := providerTurnPendingTx(ctx, tx, attempt, s.delivery, s.directives)
			if err != nil {
				return err
			}
			cancellation, err = requestTurnTimeoutTx(ctx, tx, true, attempt, now, pending)
			return err
		})
		return cancellation, err
	})
	cancellation, committed := result.Value()
	cancellation.Committed = committed
	return cancellation, result.Err()
}

func (s *EffectSQLiteOwner) RequestTurnTimeout(ctx context.Context, attempt runtimeeffects.Attempt, now time.Time) (runtimeeffects.TurnCancellation, error) {
	if err := s.requireCurrent(); err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite request turn timeout", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, mutation *mutationprotocol.Attempt) (runtimeeffects.TurnCancellation, error) {
		var cancellation runtimeeffects.TurnCancellation
		err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			pending, err := providerTurnPendingTx(ctx, tx, attempt, s.delivery, s.directives)
			if err != nil {
				return err
			}
			cancellation, err = requestTurnTimeoutTx(ctx, tx, false, attempt, now, pending)
			return err
		})
		return cancellation, err
	})
	cancellation, committed := result.Value()
	cancellation.Committed = committed
	return cancellation, result.Err()
}

func requestTurnTimeoutTx(ctx context.Context, tx *sql.Tx, postgres bool, attempt runtimeeffects.Attempt, now time.Time, pending bool) (runtimeeffects.TurnCancellation, error) {
	if attempt.Kind != runtimeeffects.KindProviderTurn || !attempt.Authority.HasBusinessTurnOrigin() {
		return runtimeeffects.TurnCancellation{}, fmt.Errorf("turn timeout requires an admitted business provider turn")
	}
	if err := requireExactLaunchAttempt(ctx, tx, postgres, attempt); err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	turnID, _, err := businessTurnIdentity(attempt.Origin)
	if err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	owner, err := businessTurnOwner(attempt.Authority)
	if err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	query := `SELECT run_id::text,agent_id,flow_instance,first_launched_at,bound_ns,timeout_event_id::text,cancel_reason,cancel_cause_event_id::text,cancel_requested_at,settled_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1::uuid FOR UPDATE`
	if !postgres {
		query = `SELECT run_id,agent_id,flow_instance,first_launched_at,bound_ns,timeout_event_id,cancel_reason,cancel_cause_event_id,cancel_requested_at,settled_at FROM runtime_agent_turn_lifetimes WHERE turn_id=?`
	}
	var runID, agentID, flow string
	var event, reason, cause sql.NullString
	var launchedRaw, requestedRaw, settledRaw any
	var bound sql.NullInt64
	if err := tx.QueryRowContext(ctx, query, turnID).Scan(&runID, &agentID, &flow, &launchedRaw, &bound, &event, &reason, &cause, &requestedRaw, &settledRaw); err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	if runID != owner.RunID || agentID != attempt.Authority.Target.AgentID || flow != owner.Route.InstancePath {
		return runtimeeffects.TurnCancellation{}, fmt.Errorf("turn timeout contradicts its exact origin owner")
	}
	if err := requireBusinessTurnBindingTx(ctx, tx, postgres, turnID, attempt, now); err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	result := runtimeeffects.TurnCancellation{Origin: attempt.Origin, OriginSettled: !pending}
	if !pending && settledRaw == nil {
		query = `UPDATE runtime_agent_turn_lifetimes SET settled_at=$1 WHERE turn_id=$2::uuid AND settled_at IS NULL`
		if !postgres {
			query = `UPDATE runtime_agent_turn_lifetimes SET settled_at=? WHERE turn_id=? AND settled_at IS NULL`
		}
		write, err := tx.ExecContext(ctx, query, now.UTC(), turnID)
		if err := requireExternalAttemptTransition(write, err); err != nil {
			return runtimeeffects.TurnCancellation{}, err
		}
		settledRaw = now.UTC()
	}
	if reason.Valid {
		return decodeTurnTimeoutIntent(result, reason, cause, requestedRaw)
	}
	firstLaunch, launched, err := sqliteTimeValue(launchedRaw)
	if err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	if settledRaw != nil || !launched || !bound.Valid || firstLaunch.Add(time.Duration(bound.Int64)).After(now) {
		return result, nil
	}
	if !event.Valid || event.String == "" {
		return runtimeeffects.TurnCancellation{}, fmt.Errorf("launched turn timeout has no stable event identity")
	}
	result.RequestedAt, err = persistTurnTimeoutIntentTx(ctx, tx, postgres, turnID, event.String, now)
	if err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	result.Requested, result.Reason, result.CauseEvent = true, deliverylifecycle.CancellationTurnTimeout, event.String
	return result, nil
}

func persistTurnTimeoutIntentTx(ctx context.Context, tx *sql.Tx, postgres bool, turnID, eventID string, now time.Time) (time.Time, error) {
	var requestedRaw any
	query := `UPDATE runtime_agent_turn_lifetimes SET cancel_reason='turn_timeout',cancel_cause_event_id=$1::uuid,cancel_requested_at=$2 WHERE turn_id=$3::uuid AND cancel_reason IS NULL AND settled_at IS NULL`
	if !postgres {
		query = `UPDATE runtime_agent_turn_lifetimes SET cancel_reason='turn_timeout',cancel_cause_event_id=?,cancel_requested_at=? WHERE turn_id=? AND cancel_reason IS NULL AND settled_at IS NULL`
	}
	write, err := tx.ExecContext(ctx, query, eventID, now.UTC(), turnID)
	if err := requireExternalAttemptTransition(write, err); err != nil {
		return time.Time{}, err
	}
	query = `SELECT cancel_requested_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1::uuid`
	if !postgres {
		query = `SELECT cancel_requested_at FROM runtime_agent_turn_lifetimes WHERE turn_id=?`
	}
	if err := tx.QueryRowContext(ctx, query, turnID).Scan(&requestedRaw); err != nil {
		return time.Time{}, err
	}
	requestedAt, valid, err := sqliteTimeValue(requestedRaw)
	if err != nil || !valid {
		return time.Time{}, fmt.Errorf("acknowledged timeout omitted its request timestamp")
	}
	return requestedAt, nil
}

func decodeTurnTimeoutIntent(result runtimeeffects.TurnCancellation, reason, cause sql.NullString, requestedRaw any) (runtimeeffects.TurnCancellation, error) {
	var err error
	result.Reason, err = deliverylifecycle.ParseCancellationReason(reason.String)
	if err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	var valid bool
	result.RequestedAt, valid, err = sqliteTimeValue(requestedRaw)
	if err != nil || !valid || !cause.Valid {
		return runtimeeffects.TurnCancellation{}, fmt.Errorf("turn cancellation has incomplete intent evidence")
	}
	result.Requested, result.CauseEvent = true, cause.String
	return result, nil
}

func providerTurnPendingTx(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner) (bool, error) {
	if attempt.Kind != runtimeeffects.KindProviderTurn || !attempt.Authority.HasBusinessTurnOrigin() || attempt.Origin.Validate() != nil {
		return false, fmt.Errorf("turn timeout requires an admitted business provider origin")
	}
	switch attempt.Origin.Kind {
	case runtimeeffects.CompletionOriginDelivery:
		if delivery == nil {
			return false, fmt.Errorf("turn lifetime delivery owner is not bound")
		}
		return delivery.ProviderOriginPendingTx(ctx, tx, attempt.Origin.Delivery)
	case runtimeeffects.CompletionOriginDirective:
		if directives == nil {
			return false, fmt.Errorf("turn lifetime directive owner is not bound")
		}
		return directives.ProviderDirectiveOriginPendingTx(ctx, tx, attempt.Origin.Directive, attempt.Authority.Target.RunID, attempt.Authority.Target.AgentIdentity)
	default:
		return false, fmt.Errorf("turn timeout has invalid origin")
	}
}
