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
	command := effects.CanceledTurnCommand{Attempt: turn.Attempt}
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

func (am *AgentManager) settleCanceledDelivery(ctx context.Context, event events.Event, heartbeat *deliverylifecycle.ClaimHeartbeat, turn effects.TurnExecutionResult) (result effects.CanceledTurnCommit, err error) {
	claim, ok := deliverylifecycle.ClaimFromContext(ctx)
	if !ok || turn.Attempt.Origin.Kind != effects.CompletionOriginDelivery || !claim.Same(turn.Attempt.Origin.Delivery) {
		return result, fmt.Errorf("canceled delivery requires its exact claimed execution")
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
	guard, err := heartbeat.BeginSettlement()
	if err != nil {
		return result, err
	}
	result, err = store.CommitCanceledTurn(guard.Context(), command)
	exact := result.Acknowledged && result.Validate() == nil && result.Origin.Same(turn.Attempt.Origin)
	if !exact && err == nil {
		err = fmt.Errorf("canceled delivery returned no exact settlement acknowledgment")
	}
	err = errors.Join(err, guard.Finish(exact))
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

func (am *AgentManager) settleCanceledDirective(ctx context.Context, turn effects.TurnExecutionResult) (result effects.CanceledTurnCommit, err error) {
	if turn.Attempt.Origin.Kind != effects.CompletionOriginDirective {
		return result, fmt.Errorf("canceled directive requires its exact operation origin")
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
