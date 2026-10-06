package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func cancelQueuedWorkflowTurns(ctx context.Context, tx *sql.Tx, mutation *mutationprotocol.Attempt, postgres bool, delivery providerDrainDeliveryOwner, command workflowlifecycle.TurnTermination) ([]deliverylifecycle.Snapshot, error) {
	owner := command.Owner()
	queued, err := delivery.QueuedAgentFlowSnapshotsTx(ctx, tx, owner)
	if err != nil {
		return nil, err
	}
	result := make([]deliverylifecycle.Snapshot, 0, len(queued))
	for _, snapshot := range queued {
		// The delivery lock excludes claim/authorization. No physical lease may
		// be declared closed merely because its business origin is retryable.
		var active int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts
			WHERE origin_kind='delivery' AND origin_delivery_id=$1 AND state IN ('authorized','launched','response_observed')`, snapshot.DeliveryID).Scan(&active); err != nil {
			return nil, err
		}
		if active != 0 {
			return nil, fmt.Errorf("queued terminate origin still owns physical provider work")
		}
		if _, err := recordQueuedTurnTermination(ctx, tx, postgres, effects.CompletionOriginDelivery, snapshot.DeliveryID, snapshot.SubscriberID, command); err != nil {
			return nil, err
		}
		canceled, err := delivery.CancelQueuedAgentTx(ctx, mutation, snapshot)
		if err != nil {
			return nil, err
		}
		if err := deactivateCanceledDeliveryContinuationsForDeliveryTx(ctx, tx, postgres, snapshot.DeliveryID); err != nil {
			return nil, err
		}
		result = append(result, canceled)
	}
	return result, nil
}

func cancelQueuedDirectiveTurns(ctx context.Context, tx *sql.Tx, mutation *mutationprotocol.Attempt, postgres bool, directives providerDrainDirectiveOwner, command workflowlifecycle.TurnTermination) ([]agentcontrol.DirectiveOperation, error) {
	if directives == nil {
		return nil, fmt.Errorf("queued termination requires its directive owner")
	}
	queued, err := directives.PreparedDirectiveFlowOperationsTx(ctx, tx, command.Owner())
	if err != nil {
		return nil, err
	}
	result := make([]agentcontrol.DirectiveOperation, 0, len(queued))
	for _, op := range queued {
		var physical int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE origin_kind='directive' AND origin_directive_operation_id=$1`, op.OperationID).Scan(&physical); err != nil {
			return nil, err
		}
		if physical != 0 {
			return nil, fmt.Errorf("prepared directive cannot own physical provider history")
		}
		at, err := recordQueuedTurnTermination(ctx, tx, postgres, effects.CompletionOriginDirective, op.OperationID, op.AgentID(), command)
		if err != nil {
			return nil, err
		}
		canceled, err := directives.CancelPreparedDirectiveTx(ctx, mutation, op, at)
		if err != nil {
			return nil, err
		}
		result = append(result, canceled)
	}
	return result, nil
}

func recordQueuedTurnTermination(ctx context.Context, tx *sql.Tx, postgres bool, kind effects.CompletionOriginKind, originID, actor string, command workflowlifecycle.TurnTermination) (time.Time, error) {
	owner := command.Owner()
	turnID, err := businessTurnID(kind, originID)
	if err != nil {
		return time.Time{}, err
	}
	now, err := selectedStoreGrantDecisionNowTx(ctx, tx, postgres)
	if err != nil {
		return time.Time{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO runtime_agent_turn_lifetimes
			(turn_id,origin_kind,origin_id,run_id,agent_id,flow_instance,cancel_reason,cancel_cause_event_id,cancel_requested_at,settled_at,created_at)
			VALUES($1,$2,$3,$4,$5,$6,'terminate',$7,$8,$8,$8) ON CONFLICT(origin_kind,origin_id) DO NOTHING`,
		turnID, string(kind), originID, owner.RunID, actor, owner.Route.InstancePath, command.Cause().EventID(), now)
	if err != nil {
		return time.Time{}, err
	}
	query := `SELECT CAST(run_id AS TEXT),agent_id,flow_instance,cancel_reason,CAST(cancel_cause_event_id AS TEXT),cancel_requested_at,settled_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1`
	if postgres {
		query += ` FOR UPDATE`
	}
	var runID, agentID, path string
	var reason, cause sql.NullString
	var requested, settled any
	if err := tx.QueryRowContext(ctx, query, turnID).Scan(&runID, &agentID, &path, &reason, &cause, &requested, &settled); err != nil {
		return time.Time{}, err
	}
	if runID != owner.RunID || agentID != actor || path != owner.Route.InstancePath {
		return time.Time{}, fmt.Errorf("queued termination contradicts its exact logical origin")
	}
	if reason.Valid && reason.String != "terminate" {
		return time.Time{}, fmt.Errorf("queued termination cannot replace a prior authored cancellation")
	}
	if reason.Valid {
		_, valid, err := sqliteTimeValue(requested)
		if err != nil || !valid || !cause.Valid {
			return time.Time{}, fmt.Errorf("queued termination has incomplete intent evidence")
		}
		if _, err := uuid.Parse(cause.String); err != nil {
			return time.Time{}, fmt.Errorf("queued termination has invalid cause evidence: %w", err)
		}
	} else if cause.Valid || requested != nil {
		return time.Time{}, fmt.Errorf("queued termination has contradictory intent evidence")
	}
	if !reason.Valid {
		if settled != nil {
			return time.Time{}, fmt.Errorf("queued origin contradicts a settled logical turn")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runtime_agent_turn_lifetimes SET cancel_reason='terminate',cancel_cause_event_id=$1,cancel_requested_at=$2,settled_at=$2 WHERE turn_id=$3 AND cancel_reason IS NULL AND settled_at IS NULL`, command.Cause().EventID(), now, turnID); err != nil {
			return time.Time{}, err
		}
	} else if settled == nil {
		if _, err := tx.ExecContext(ctx, `UPDATE runtime_agent_turn_lifetimes SET settled_at=$1 WHERE turn_id=$2 AND cancel_reason='terminate' AND settled_at IS NULL`, now, turnID); err != nil {
			return time.Time{}, err
		}
	}
	return now, nil
}
