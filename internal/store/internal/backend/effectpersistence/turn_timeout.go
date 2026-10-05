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
			var err error
			cancellation, err = requestTurnTimeoutTx(ctx, tx, true, attempt, now)
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
			var err error
			cancellation, err = requestTurnTimeoutTx(ctx, tx, false, attempt, now)
			return err
		})
		return cancellation, err
	})
	cancellation, committed := result.Value()
	cancellation.Committed = committed
	return cancellation, result.Err()
}

func requestTurnTimeoutTx(ctx context.Context, tx *sql.Tx, postgres bool, attempt runtimeeffects.Attempt, now time.Time) (runtimeeffects.TurnCancellation, error) {
	if attempt.Kind != runtimeeffects.KindProviderTurn || attempt.Authority.Kind != runtimeeffects.AuthorityNormalAgent {
		return runtimeeffects.TurnCancellation{}, fmt.Errorf("turn timeout requires an admitted business provider turn")
	}
	if err := requireExactLaunchAttempt(ctx, tx, postgres, attempt); err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	turnID, _, err := businessTurnIdentity(attempt.Origin)
	if err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	query := `SELECT run_id::text,agent_id,flow_instance,deadline_at,timeout_event_id::text,cancel_reason,cancel_cause_event_id::text,cancel_requested_at,settled_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1::uuid FOR UPDATE`
	if !postgres {
		query = `SELECT run_id,agent_id,flow_instance,deadline_at,timeout_event_id,cancel_reason,cancel_cause_event_id,cancel_requested_at,settled_at FROM runtime_agent_turn_lifetimes WHERE turn_id=?`
	}
	var runID, agentID, flow string
	var event, reason, cause sql.NullString
	var deadlineRaw, requestedRaw, settledRaw any
	if err := tx.QueryRowContext(ctx, query, turnID).Scan(&runID, &agentID, &flow, &deadlineRaw, &event, &reason, &cause, &requestedRaw, &settledRaw); err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	if runID != attempt.Authority.Target.RunID || agentID != attempt.Authority.Target.AgentID || flow != attempt.Authority.Target.FlowInstance {
		return runtimeeffects.TurnCancellation{}, fmt.Errorf("turn timeout contradicts its exact origin owner")
	}
	result := runtimeeffects.TurnCancellation{Origin: attempt.Origin}
	if reason.Valid {
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
	deadline, launched, err := sqliteTimeValue(deadlineRaw)
	if err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	if settledRaw != nil || !launched || deadline.After(now) {
		return result, nil
	}
	if !event.Valid || event.String == "" {
		return runtimeeffects.TurnCancellation{}, fmt.Errorf("launched turn timeout has no stable event identity")
	}
	query = `UPDATE runtime_agent_turn_lifetimes SET cancel_reason='turn_timeout',cancel_cause_event_id=$1::uuid,cancel_requested_at=$2 WHERE turn_id=$3::uuid AND cancel_reason IS NULL AND settled_at IS NULL`
	if !postgres {
		query = `UPDATE runtime_agent_turn_lifetimes SET cancel_reason='turn_timeout',cancel_cause_event_id=?,cancel_requested_at=? WHERE turn_id=? AND cancel_reason IS NULL AND settled_at IS NULL`
	}
	write, err := tx.ExecContext(ctx, query, event.String, now.UTC(), turnID)
	if err := requireExternalAttemptTransition(write, err); err != nil {
		return runtimeeffects.TurnCancellation{}, err
	}
	result.Requested, result.Reason, result.CauseEvent, result.RequestedAt = true, deliverylifecycle.CancellationTurnTimeout, event.String, now.UTC()
	return result, nil
}
