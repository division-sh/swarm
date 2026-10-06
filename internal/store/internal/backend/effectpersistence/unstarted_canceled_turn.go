package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func commitUnstartedCanceledTurn(ctx context.Context, mutation *mutationprotocol.Attempt, postgres bool, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, origin effects.CompletionOrigin) (effects.CanceledTurnCommit, error) {
	result := effects.CanceledTurnCommit{Origin: origin}
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if origin.Kind != effects.CompletionOriginDelivery || delivery == nil {
			return fmt.Errorf("unstarted cancellation requires its real delivery owner")
		}
		if _, err := delivery.ProviderOriginPendingTx(ctx, tx, origin.Delivery); err != nil {
			return err
		}
		turnID, _, err := businessTurnIdentity(origin)
		if err != nil {
			return err
		}
		query := `SELECT CAST(admitted_attempt_id AS TEXT),CAST(origin_evidence AS TEXT),CAST(run_id AS TEXT),agent_id,flow_instance,cancel_reason,CAST(cancel_cause_event_id AS TEXT),cancel_requested_at FROM runtime_agent_turn_lifetimes WHERE turn_id=$1`
		if postgres {
			query += ` FOR UPDATE`
		}
		var admitted, evidence, reason, cause sql.NullString
		var runID, agentID, path string
		var requested any
		if err := tx.QueryRowContext(ctx, query, turnID).Scan(&admitted, &evidence, &runID, &agentID, &path, &reason, &cause, &requested); err != nil {
			return err
		}
		if admitted.Valid || !evidence.Valid || !reason.Valid || reason.String != string(deliverylifecycle.CancellationTerminate) || !cause.Valid {
			return fmt.Errorf("unstarted settlement lacks exact origin-only termination evidence")
		}
		stored, owner, err := decodeUnstartedOrigin([]byte(evidence.String))
		if err != nil || !stored.Same(origin) || owner.RunID != runID || owner.Route.InstancePath != path {
			return fmt.Errorf("unstarted settlement contradicts its persisted origin")
		}
		if err := delivery.ValidateUnstartedClaimOwnerTx(ctx, tx, origin.Delivery, owner, agentID); err != nil {
			return err
		}
		now, err := selectedStoreGrantDecisionNowTx(ctx, tx, postgres)
		if err != nil {
			return err
		}
		at, valid, err := sqliteTimeValue(requested)
		if err != nil || !valid {
			return fmt.Errorf("unstarted termination has no exact request time")
		}
		result.Cancellation = effects.TurnCancellation{Requested: true, Origin: origin, Reason: deliverylifecycle.CancellationTerminate, CauseEvent: cause.String, RequestedAt: at, OriginSettled: true}
		if err := result.Cancellation.ValidateFacts(); err != nil {
			return err
		}
		var physical int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE origin_kind='delivery' AND origin_delivery_id=$1`, origin.Delivery.DeliveryID()).Scan(&physical); err != nil {
			return err
		}
		if physical != 0 {
			return fmt.Errorf("origin-only cancellation cannot bypass provider-attempt ownership")
		}
		result.Delivery, err = delivery.SettleProviderCanceledOriginTx(ctx, mutation, origin.Delivery, deliverylifecycle.CancellationTerminate, 0)
		if err != nil {
			return err
		}
		return completeCanceledTurnTx(ctx, tx, postgres, turnID, now)
	})
	return result, err
}
