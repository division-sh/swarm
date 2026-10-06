package manager

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func (am *AgentManager) prepareCanceledTurn(ctx context.Context, turn effects.TurnExecutionResult) (effects.CanceledTurnStore, effects.TurnTimeoutReactionOwner, effects.CanceledTurnCommand, error) {
	if err := turn.Cancellation.ValidateIntent(); err != nil {
		return nil, nil, effects.CanceledTurnCommand{}, err
	}
	if !turn.Cancellation.Origin.Same(turn.Attempt.Origin) {
		return nil, nil, effects.CanceledTurnCommand{}, fmt.Errorf("cancellation substituted its exact execution origin")
	}
	store, ok := am.roles.LifecycleEffects.(effects.CanceledTurnStore)
	if !ok {
		return nil, nil, effects.CanceledTurnCommand{}, fmt.Errorf("cancellation requires its selected-store commit owner")
	}
	command := effects.CanceledTurnCommand{Origin: turn.Cancellation.Origin}
	if turn.Attempt.AttemptID != "" {
		command = effects.CanceledTurnCommandForAttempt(turn.Attempt, nil)
	}
	if turn.Cancellation.Reason != deliverylifecycle.CancellationTurnTimeout {
		return store, nil, command, nil
	}
	publication, ok := am.bus.(effects.TurnTimeoutReactionOwner)
	if !ok {
		return nil, nil, command, fmt.Errorf("timeout cancellation requires its publication owner")
	}
	plan, err := publication.PrepareTurnTimeoutReaction(ctx, turn)
	if err != nil {
		if plan != nil {
			err = errors.Join(err, publication.ReleaseTurnTimeoutReaction(context.WithoutCancel(ctx), plan))
		}
		return nil, nil, command, err
	}
	if plan == nil || plan.ValidateDurablePublicationPlan() != nil || plan.DurablePublicationEventID() != turn.Cancellation.CauseEvent {
		if plan != nil {
			return nil, nil, command, errors.Join(errors.New("timeout cancellation received foreign publication evidence"), publication.ReleaseTurnTimeoutReaction(context.WithoutCancel(ctx), plan))
		}
		return nil, nil, command, errors.New("timeout cancellation received no publication evidence")
	}
	command.Publication = plan
	return store, publication, command, nil
}

func (am *AgentManager) settleCanceledDelivery(ctx context.Context, event events.Event, heartbeat *deliverylifecycle.ClaimHeartbeat, turn effects.TurnExecutionResult, observed effects.CompletionSettlementObservation) (result effects.CanceledTurnCommit, err error) {
	claim, ok := deliverylifecycle.ClaimFromContext(ctx)
	if !ok || turn.Attempt.Origin.Kind != effects.CompletionOriginDelivery || !claim.Same(turn.Attempt.Origin.Delivery) {
		return result, fmt.Errorf("canceled delivery requires its exact claimed execution")
	}
	captured := observed.Disposition == effects.CompletionSettlementDrained
	ctx, err = canceledTurnSettlementContext(ctx, turn, observed)
	if err != nil {
		return result, err
	}
	store, publication, command, err := am.prepareCanceledTurn(ctx, turn)
	if err != nil {
		return result, err
	}
	if command.Publication != nil {
		defer func() {
			err = errors.Join(err, publication.ReleaseTurnTimeoutReaction(context.WithoutCancel(ctx), command.Publication))
		}()
	}
	var guard *deliverylifecycle.ClaimSettlementGuard
	var heartbeatErr error
	if captured {
		// Captured evidence retains settlement rights, not predecessor renewal
		// rights. Join the old renewal owner before the native atomic commit.
		heartbeatErr = heartbeat.Stop()
	} else {
		guard, err = heartbeat.BeginSettlement()
		if err != nil {
			return result, err
		}
		ctx = guard.Context()
	}
	result, err = store.CommitCanceledTurn(ctx, command)
	exact := result.Acknowledged && result.Validate() == nil && result.Origin.Same(turn.Attempt.Origin)
	if !exact && err == nil {
		err = fmt.Errorf("canceled delivery returned no exact settlement acknowledgment")
	}
	if guard != nil {
		heartbeatErr = guard.Finish(exact)
	}
	err = errors.Join(err, heartbeatErr)
	if !exact {
		return result, err
	}
	if am.roles.DeliveryRuntime == nil {
		err = errors.Join(err, fmt.Errorf("canceled delivery requires its exact continuation owner"))
	} else {
		err = errors.Join(err, am.roles.DeliveryRuntime.ReleaseDeliveryContinuation(claim.DeliveryID()))
	}
	postCtx := context.WithoutCancel(ctx)
	am.logDeliveryLifecycle(postCtx, result.Delivery)
	am.notifyTestDeliveryStatus(postCtx, event, claim.SubscriberID(), result.Delivery.Status)
	if result.Publication != nil {
		err = errors.Join(err, publication.DispatchTurnTimeoutReaction(postCtx, result.Publication))
	}
	return result, err
}

func (am *AgentManager) settleCanceledDirective(ctx context.Context, turn effects.TurnExecutionResult, observed effects.CompletionSettlementObservation) (result effects.CanceledTurnCommit, err error) {
	if turn.Attempt.Origin.Kind != effects.CompletionOriginDirective {
		return result, fmt.Errorf("canceled directive requires its exact operation origin")
	}
	ctx, err = canceledTurnSettlementContext(ctx, turn, observed)
	if err != nil {
		return result, err
	}
	store, publication, command, err := am.prepareCanceledTurn(ctx, turn)
	if err != nil {
		return result, err
	}
	if command.Publication != nil {
		defer func() {
			err = errors.Join(err, publication.ReleaseTurnTimeoutReaction(context.WithoutCancel(ctx), command.Publication))
		}()
	}
	result, err = store.CommitCanceledTurn(ctx, command)
	if !result.Acknowledged || result.Validate() != nil || !result.Origin.Same(turn.Attempt.Origin) {
		if err == nil {
			err = fmt.Errorf("canceled directive returned no exact settlement acknowledgment")
		}
		return result, err
	}
	if result.Publication != nil {
		err = errors.Join(err, publication.DispatchTurnTimeoutReaction(context.WithoutCancel(ctx), result.Publication))
	}
	return result, err
}

func canceledTurnSettlementContext(ctx context.Context, turn effects.TurnExecutionResult, observed effects.CompletionSettlementObservation) (context.Context, error) {
	if observed.Disposition != effects.CompletionSettlementDrained {
		return ctx, nil
	}
	if observed.AttemptID == "" || !observed.Origin.Same(turn.Attempt.Origin) || observed.Cancellation == nil ||
		observed.Cancellation.ValidateIntent() != nil || !observed.Cancellation.Origin.Same(turn.Attempt.Origin) ||
		observed.Cancellation.CauseEvent != turn.Cancellation.CauseEvent || observed.Cancellation.Reason != turn.Cancellation.Reason ||
		!observed.Cancellation.RequestedAt.Equal(turn.Cancellation.RequestedAt) {
		return nil, fmt.Errorf("captured cancellation requires exact committed physical-tail evidence")
	}
	return context.WithoutCancel(ctx), nil
}

func (am *AgentManager) reconcileCanceledTurnsForStartup(ctx context.Context, request effects.RecoveryRequest) error {
	recovery, ok := am.roles.EffectsRecovery.(effects.CanceledTurnRecoveryStore)
	if !ok {
		return fmt.Errorf("effect recovery requires its exact canceled-turn snapshot owner")
	}
	for {
		turns, err := recovery.ListCanceledTurnRecoveries(ctx, request)
		if err != nil {
			return err
		}
		if len(turns) == 0 {
			return nil
		}
		for _, turn := range turns {
			if err := am.commitRecoveredCanceledTurn(ctx, turn); err != nil {
				return err
			}
		}
	}
}

func (am *AgentManager) commitRecoveredCanceledTurn(ctx context.Context, turn effects.TurnExecutionResult) (err error) {
	store, publication, command, err := am.prepareCanceledTurn(ctx, turn)
	if err != nil {
		return err
	}
	if command.Publication != nil {
		defer func() {
			err = errors.Join(err, publication.ReleaseTurnTimeoutReaction(context.WithoutCancel(ctx), command.Publication))
		}()
	}
	result, err := store.CommitCanceledTurn(ctx, command)
	if !result.Acknowledged || result.Validate() != nil || !result.Origin.Same(turn.Attempt.Origin) {
		return errors.Join(err, fmt.Errorf("recovered cancellation has no exact atomic acknowledgment"))
	}
	if result.Origin.Kind == effects.CompletionOriginDelivery {
		if am.roles.DeliveryRuntime == nil {
			return errors.Join(err, fmt.Errorf("recovered cancellation lacks its continuation owner"))
		}
		err = errors.Join(err, am.roles.DeliveryRuntime.ReleaseDeliveryContinuation(result.Origin.Delivery.DeliveryID()))
		am.logDeliveryLifecycle(ctx, result.Delivery)
	}
	// The same transaction persisted the reaction and its pipeline obligations.
	// Startup must not execute them before agent/runtime admission; the existing
	// RecoverAfterStartupAdmission pass takes their released publication claims.
	return err
}
