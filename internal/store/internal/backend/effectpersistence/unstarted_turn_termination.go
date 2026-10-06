package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

func requestUnstartedClaimedTurnTermination(ctx context.Context, tx *sql.Tx, postgres bool, delivery providerDrainDeliveryOwner, command workflowlifecycle.TurnTermination) ([]effects.TurnCancellation, error) {
	claimed, err := delivery.ClaimedAgentFlowOriginsTx(ctx, tx, command.Owner())
	if err != nil {
		return nil, err
	}
	var intents []effects.TurnCancellation
	for _, work := range claimed {
		var physical int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE origin_kind='delivery' AND origin_delivery_id=$1`, work.Claim.DeliveryID()).Scan(&physical); err != nil {
			return nil, err
		}
		if physical != 0 {
			continue
		}
		origin, err := effects.DeliveryCompletionOrigin(work.Claim)
		if err != nil {
			return nil, err
		}
		turnID, originID, err := businessTurnIdentity(origin)
		if err != nil {
			return nil, err
		}
		evidence, err := encodeUnstartedOrigin(origin, command.Owner())
		if err != nil {
			return nil, err
		}
		now, err := selectedStoreGrantDecisionNowTx(ctx, tx, postgres)
		if err != nil {
			return nil, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO runtime_agent_turn_lifetimes
			(turn_id,origin_kind,origin_id,run_id,agent_id,flow_instance,origin_evidence,cancel_reason,cancel_cause_event_id,cancel_requested_at,created_at)
			VALUES($1,'delivery',$2,$3,$4,$5,$6,'terminate',$7,$8,$8) ON CONFLICT(origin_kind,origin_id) DO NOTHING`,
			turnID, originID, command.Owner().RunID, work.Snapshot.SubscriberID, command.Owner().Route.InstancePath, string(evidence), command.Cause().EventID(), now)
		if err != nil {
			return nil, err
		}
		query := `SELECT CAST(origin_evidence AS TEXT),cancel_reason,CAST(cancel_cause_event_id AS TEXT),cancel_requested_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1 AND admitted_attempt_id IS NULL AND settled_at IS NULL`
		if postgres {
			query += ` FOR UPDATE`
		}
		var stored, reason, cause string
		var requested any
		if err := tx.QueryRowContext(ctx, query, turnID).Scan(&stored, &reason, &cause, &requested); err != nil {
			return nil, err
		}
		exact, owner, err := decodeUnstartedOrigin([]byte(stored))
		if err != nil || !exact.Same(origin) || owner != command.Owner() || reason != string(deliverylifecycle.CancellationTerminate) {
			return nil, fmt.Errorf("unstarted termination contradicts its actual claimed origin")
		}
		at, valid, err := sqliteTimeValue(requested)
		if err != nil || !valid {
			return nil, fmt.Errorf("unstarted termination has incomplete request evidence")
		}
		intent := effects.TurnCancellation{Requested: true, Origin: origin, Reason: deliverylifecycle.CancellationTerminate, CauseEvent: cause, RequestedAt: at}
		if err := intent.ValidateFacts(); err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	return intents, nil
}
