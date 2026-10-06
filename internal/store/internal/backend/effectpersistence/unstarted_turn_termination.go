package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

func requestUnstartedClaimedTurnTermination(ctx context.Context, tx *sql.Tx, postgres bool, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, command workflowlifecycle.TurnTermination) ([]effects.TurnCancellation, error) {
	claimed, err := delivery.ClaimedAgentFlowOriginsTx(ctx, tx, command.Owner())
	if err != nil {
		return nil, err
	}
	var intents []effects.TurnCancellation
	for _, work := range claimed {
		origin, err := effects.DeliveryCompletionOrigin(work.Claim)
		if err != nil {
			return nil, err
		}
		intent, err := recordUnstartedTurnTermination(ctx, tx, postgres, origin, work.Snapshot.SubscriberID, command)
		if err != nil {
			return nil, err
		}
		if intent.Requested {
			intents = append(intents, intent)
		}
	}
	if directives == nil {
		return nil, fmt.Errorf("unstarted termination requires its directive owner")
	}
	ops, err := directives.ExecutingDirectiveFlowOriginsTx(ctx, tx, command.Owner())
	if err != nil {
		return nil, err
	}
	for _, op := range ops {
		execution, err := agentcontrol.NewDirectiveExecutionOrigin(op)
		if err != nil {
			return nil, err
		}
		origin, err := effects.DirectiveCompletionOrigin(execution)
		if err != nil {
			return nil, err
		}
		intent, err := recordUnstartedTurnTermination(ctx, tx, postgres, origin, op.AgentID(), command)
		if err != nil {
			return nil, err
		}
		if intent.Requested {
			intents = append(intents, intent)
		}
	}
	return intents, nil
}

func unstartedPhysicalAttemptCount(ctx context.Context, tx *sql.Tx, origin effects.CompletionOrigin) (int, error) {
	query, id := `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE origin_kind='directive' AND origin_directive_operation_id=$1`, origin.Directive.OperationID
	if origin.Kind == effects.CompletionOriginDelivery {
		var count int
		err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE origin_kind='delivery' AND origin_delivery_id=$1 AND origin_claim_token=$2 AND origin_claim_version=$3`, origin.Delivery.DeliveryID(), origin.Delivery.PersistenceToken(), origin.Delivery.Version()).Scan(&count)
		return count, err
	}
	var count int
	err := tx.QueryRowContext(ctx, query, id).Scan(&count)
	return count, err
}

func recordUnstartedTurnTermination(ctx context.Context, tx *sql.Tx, postgres bool, origin effects.CompletionOrigin, agentID string, command workflowlifecycle.TurnTermination) (effects.TurnCancellation, error) {
	physical, err := unstartedPhysicalAttemptCount(ctx, tx, origin)
	if err != nil || physical != 0 {
		return effects.TurnCancellation{}, err
	}
	turnID, originID, err := businessTurnIdentity(origin)
	if err != nil {
		return effects.TurnCancellation{}, err
	}
	evidence, err := encodeUnstartedOrigin(origin, command.Owner())
	if err != nil {
		return effects.TurnCancellation{}, err
	}
	now, err := selectedStoreGrantDecisionNowTx(ctx, tx, postgres)
	if err != nil {
		return effects.TurnCancellation{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO runtime_agent_turn_lifetimes
			(turn_id,origin_kind,origin_id,run_id,agent_id,flow_instance,origin_evidence,cancel_reason,cancel_cause_event_id,cancel_requested_at,created_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,'terminate',$8,$9,$9) ON CONFLICT(origin_kind,origin_id) DO NOTHING`,
		turnID, string(origin.Kind), originID, command.Owner().RunID, agentID, command.Owner().Route.InstancePath, string(evidence), command.Cause().EventID(), now)
	if err != nil {
		return effects.TurnCancellation{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_agent_turn_lifetimes SET current_attempt_id=NULL,origin_evidence=$1,cancel_reason='terminate',cancel_cause_event_id=$2,cancel_requested_at=$3 WHERE turn_id=$4 AND cancel_reason IS NULL AND settled_at IS NULL`, string(evidence), command.Cause().EventID(), now, turnID); err != nil {
		return effects.TurnCancellation{}, err
	}
	query := `SELECT CAST(origin_evidence AS TEXT),cancel_reason,CAST(cancel_cause_event_id AS TEXT),cancel_requested_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1 AND current_attempt_id IS NULL AND settled_at IS NULL`
	if postgres {
		query += ` FOR UPDATE`
	}
	var stored, reason, cause string
	var requested any
	if err := tx.QueryRowContext(ctx, query, turnID).Scan(&stored, &reason, &cause, &requested); err != nil {
		return effects.TurnCancellation{}, err
	}
	exact, owner, err := decodeUnstartedOrigin([]byte(stored))
	if err != nil || !exact.Same(origin) || owner != command.Owner() || reason != string(deliverylifecycle.CancellationTerminate) {
		return effects.TurnCancellation{}, fmt.Errorf("unstarted termination contradicts its actual claimed origin")
	}
	at, valid, err := sqliteTimeValue(requested)
	if err != nil || !valid {
		return effects.TurnCancellation{}, fmt.Errorf("unstarted termination has incomplete request evidence")
	}
	intent := effects.TurnCancellation{Requested: true, Origin: origin, Reason: deliverylifecycle.CancellationTerminate, CauseEvent: cause, RequestedAt: at}
	if err := intent.ValidateFacts(); err != nil {
		return effects.TurnCancellation{}, err
	}
	return intent, nil
}
