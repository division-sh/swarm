package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func requestWorkflowTurnTermination(ctx context.Context, mutation *mutationprotocol.Attempt, postgres bool, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, command workflowlifecycle.TurnTermination) (runtimeeffects.WorkflowTurnTerminationResult, error) {
	if err := command.Validate(); err != nil {
		return runtimeeffects.WorkflowTurnTerminationResult{}, err
	}
	owner, cause := command.Owner(), command.Cause()
	transition, _ := cause.Transition()
	var result runtimeeffects.WorkflowTurnTerminationResult
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var eventRun, eventType string
		if err := tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT),event_name FROM events WHERE event_id=$1`, cause.EventID()).Scan(&eventRun, &eventType); err != nil {
			return err
		}
		if eventRun != owner.RunID || eventType != cause.EventType() {
			return fmt.Errorf("authored termination contradicts its persisted cause event")
		}
		query := `SELECT entity_id,current_state,status,terminated_at FROM flow_instances WHERE run_id=? AND instance_path=?`
		if postgres {
			query = `SELECT entity_id::text,current_state,status,terminated_at FROM flow_instances WHERE run_id=$1::uuid AND instance_path=$2 FOR UPDATE`
		}
		var entity, stage, status string
		var terminated any
		if err := tx.QueryRowContext(ctx, query, owner.RunID, owner.Route.InstancePath).Scan(&entity, &stage, &status, &terminated); err != nil {
			return err
		}
		if entity != cause.EntityID().String() || stage != transition.To() || status != "active" || terminated != nil {
			return fmt.Errorf("authored termination disagrees with its guarded constructed header")
		}
		var err error
		result.Queued, err = cancelQueuedWorkflowTurns(ctx, tx, mutation, postgres, delivery, command)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT CAST(turn_id AS TEXT),CAST(admitted_attempt_id AS TEXT) FROM runtime_agent_turn_lifetimes WHERE run_id=$1 AND flow_instance=$2 AND settled_at IS NULL ORDER BY origin_kind,origin_id`, owner.RunID, owner.Route.InstancePath)
		if err != nil {
			return err
		}
		type turnRow struct {
			id, admitted string
		}
		var turns []turnRow
		for rows.Next() {
			var row turnRow
			if err := rows.Scan(&row.id, &row.admitted); err != nil {
				_ = rows.Close()
				return err
			}
			turns = append(turns, row)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, row := range turns {
			now, err := selectedStoreGrantDecisionNowTx(ctx, tx, postgres)
			if err != nil {
				return err
			}
			attempt, _, err := loadBusinessTurnFirstAttempt(ctx, tx, postgres, row.admitted, now)
			if err != nil {
				return err
			}
			id, _, err := businessTurnIdentity(attempt.Origin)
			if err != nil || id != row.id || attempt.Authority.Target.RunID != owner.RunID || attempt.Authority.Target.FlowInstance != owner.Route.InstancePath {
				return fmt.Errorf("authored termination found contradictory origin ownership")
			}
			// Match timeout's origin -> physical -> logical lock order. No
			// generation replacement or provider-tail retirement occurs here.
			pending, err := providerTurnPendingTx(ctx, tx, attempt, delivery, directives)
			if err != nil {
				return err
			}
			if err := requireExactLaunchAttempt(ctx, tx, postgres, attempt); err != nil {
				return err
			}
			query = `SELECT cancel_reason,cancel_cause_event_id,cancel_requested_at FROM runtime_agent_turn_lifetimes WHERE turn_id=?`
			if postgres {
				query = `SELECT cancel_reason,cancel_cause_event_id::text,cancel_requested_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1::uuid FOR UPDATE`
			}
			var reason, cancelCause sql.NullString
			var rawRequested any
			if err := tx.QueryRowContext(ctx, query, row.id).Scan(&reason, &cancelCause, &rawRequested); err != nil {
				return err
			}
			if !pending {
				query = `UPDATE runtime_agent_turn_lifetimes SET settled_at=$1 WHERE turn_id=$2 AND settled_at IS NULL`
				if _, err := tx.ExecContext(ctx, query, now, row.id); err != nil {
					return err
				}
				continue
			}
			if !reason.Valid {
				query = `UPDATE runtime_agent_turn_lifetimes SET cancel_reason='terminate',cancel_cause_event_id=$1,cancel_requested_at=$2 WHERE turn_id=$3 AND cancel_reason IS NULL AND settled_at IS NULL`
				if _, err := tx.ExecContext(ctx, query, cause.EventID(), now, row.id); err != nil {
					return err
				}
				reason, cancelCause = sql.NullString{String: "terminate", Valid: true}, sql.NullString{String: cause.EventID(), Valid: true}
				// Read the selected store's canonical occurrence precision.
				query = `SELECT cancel_requested_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1`
				if err := tx.QueryRowContext(ctx, query, row.id).Scan(&rawRequested); err != nil {
					return err
				}
			}
			at, valid, err := sqliteTimeValue(rawRequested)
			if err != nil || !valid || !cancelCause.Valid {
				return fmt.Errorf("authored termination has incomplete intent evidence")
			}
			intent := runtimeeffects.TurnCancellation{Requested: true, Origin: attempt.Origin, Reason: deliverylifecycle.CancellationReason(reason.String), CauseEvent: cancelCause.String, RequestedAt: at}
			if err := intent.ValidateFacts(); err != nil {
				return err
			}
			result.Active = append(result.Active, intent)
		}
		return nil
	})
	if err != nil {
		return runtimeeffects.WorkflowTurnTerminationResult{}, err
	}
	return result, nil
}

func (s *EffectPostgresOwner) RequestWorkflowTurnTerminationTx(ctx context.Context, mutation *mutationprotocol.Attempt, command workflowlifecycle.TurnTermination) (runtimeeffects.WorkflowTurnTerminationResult, error) {
	if err := s.requireCurrent(); err != nil {
		return runtimeeffects.WorkflowTurnTerminationResult{}, err
	}
	return requestWorkflowTurnTermination(ctx, mutation, true, s.delivery, s.directives, command)
}

func (s *EffectSQLiteOwner) RequestWorkflowTurnTerminationTx(ctx context.Context, mutation *mutationprotocol.Attempt, command workflowlifecycle.TurnTermination) (runtimeeffects.WorkflowTurnTerminationResult, error) {
	if err := s.requireCurrent(); err != nil {
		return runtimeeffects.WorkflowTurnTerminationResult{}, err
	}
	return requestWorkflowTurnTermination(ctx, mutation, false, s.delivery, s.directives, command)
}
