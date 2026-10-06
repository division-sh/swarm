package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func commitUnstartedCanceledTurn(ctx context.Context, mutation *mutationprotocol.Attempt, postgres bool, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, origin effects.CompletionOrigin) (effects.CanceledTurnCommit, error) {
	result := effects.CanceledTurnCommit{Origin: origin}
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var directive agentcontrol.DirectiveOperation
		switch origin.Kind {
		case effects.CompletionOriginDelivery:
			if delivery == nil {
				return fmt.Errorf("unstarted cancellation requires its delivery owner")
			}
			if _, err := delivery.ProviderOriginPendingTx(ctx, tx, origin.Delivery); err != nil {
				return err
			}
		case effects.CompletionOriginDirective:
			if directives == nil {
				return fmt.Errorf("unstarted cancellation requires its directive owner")
			}
			var err error
			directive, err = directives.DirectiveTurnOriginTx(ctx, tx, origin.Directive, true)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unstarted cancellation requires a real work origin")
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
		if err := validateUnstartedTurnOwner(ctx, tx, delivery, origin, owner, agentID, directive); err != nil {
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
		physical, err := unstartedPhysicalAttemptCount(ctx, tx, origin)
		if err != nil {
			return err
		}
		if physical != 0 {
			return fmt.Errorf("origin-only cancellation cannot bypass provider-attempt ownership")
		}
		if origin.Kind == effects.CompletionOriginDelivery {
			result.Delivery, err = delivery.SettleProviderCanceledOriginTx(ctx, mutation, origin.Delivery, deliverylifecycle.CancellationTerminate, 0)
		} else {
			result.Directive, err = directives.SettleProviderCanceledDirectiveTx(ctx, mutation, origin.Directive, deliverylifecycle.CancellationTerminate, now)
		}
		if err != nil {
			return err
		}
		return completeCanceledTurnTx(ctx, tx, postgres, turnID, now)
	})
	return result, err
}

func validateUnstartedTurnOwner(ctx context.Context, tx *sql.Tx, delivery providerDrainDeliveryOwner, origin effects.CompletionOrigin, owner flowidentity.RunScopedFlowInstance, agentID string, directive agentcontrol.DirectiveOperation) error {
	if origin.Kind == effects.CompletionOriginDelivery {
		if delivery == nil {
			return fmt.Errorf("unstarted cancellation lacks its delivery owner")
		}
		return delivery.ValidateUnstartedClaimOwnerTx(ctx, tx, origin.Delivery, owner, agentID)
	}
	if owner.Validate() != nil || directive.ResolvedRunID != owner.RunID || directive.AgentID() != agentID || !owner.MatchesAgentRoute(directive.AgentIdentity) {
		return fmt.Errorf("unstarted directive contradicts its recorded constructed owner")
	}
	if directive.State != agentcontrol.DirectiveOperationExecuting && (directive.State != agentcontrol.DirectiveOperationCanceled || directive.CancellationReason != deliverylifecycle.CancellationTerminate) {
		return fmt.Errorf("unstarted directive has incompatible settlement evidence")
	}
	return nil
}
