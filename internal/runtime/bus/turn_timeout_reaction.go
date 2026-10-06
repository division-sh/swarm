package bus

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
)

func (eb *EventBus) PrepareTurnTimeoutReaction(ctx context.Context, turn effects.TurnExecutionResult) (effects.TurnReactionPlan, error) {
	return eb.prepareTurnTimeoutReaction(ctx, turn, false)
}

// Recovery commits publication before executable admission. Existing subscribed
// pipeline recovery plans recipients after admission; no grant is minted here.
func (eb *EventBus) PrepareRecoveredTurnTimeoutReaction(ctx context.Context, turn effects.TurnExecutionResult) (effects.TurnReactionPlan, error) {
	return eb.prepareTurnTimeoutReaction(ctx, turn, true)
}

type startupTurnTimeoutPublicationKey struct{}
type startupTurnTimeoutPublication struct{ eventID, runID string }

func (eb *EventBus) prepareTurnTimeoutReaction(ctx context.Context, turn effects.TurnExecutionResult, recovery bool) (effects.TurnReactionPlan, error) {
	intent, clock, attempt := turn.Cancellation, turn.Clock, turn.Attempt
	if intent.ValidateIntent() != nil || intent.Reason != deliverylifecycle.CancellationTurnTimeout ||
		clock == nil || clock.Validate() != nil || clock.Timeout == nil ||
		!intent.Origin.Same(attempt.Origin) || !clock.Origin.Same(attempt.Origin) || clock.TimeoutEvent != intent.CauseEvent {
		return nil, fmt.Errorf("timeout reaction requires exact acknowledged launch and cancellation evidence")
	}
	target := attempt.Authority.Target
	flowID, _, path, err := attempt.Authority.BusinessTurnCoordinates()
	if err != nil {
		return nil, fmt.Errorf("timeout reaction requires its admitted constructed actor identity")
	}
	source, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: flowID, FlowInstance: path, EntityID: target.EntityID})
	if err != nil {
		return nil, err
	}
	event, err := events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{
		RunID: target.RunID,
		Facts: events.EventFacts{
			ID: intent.CauseEvent, Type: events.EventType(clock.Timeout.Emit),
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: effects.TurnTimeoutProducerID()},
			Payload:  []byte(`{}`), RoutingSource: source, CreatedAt: intent.RequestedAt,
			Envelope: events.EventEnvelope{EntityID: target.EntityID}, ExecutionMode: attempt.Authority.ExecutionMode,
		},
	})
	if err != nil {
		return nil, err
	}
	if recovery {
		ctx = context.WithValue(ctx, startupTurnTimeoutPublicationKey{}, startupTurnTimeoutPublication{eventID: event.ID(), runID: event.RunID()})
	}
	plans, err := eb.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
	if err != nil {
		return nil, err
	}
	if len(plans) != 1 {
		return nil, errors.Join(fmt.Errorf("timeout reaction requires exactly one admitted publication"), eb.ReleaseEnginePublications(context.WithoutCancel(ctx), plans))
	}
	return plans[0], nil
}

func (eb *EventBus) ReleaseTurnTimeoutReaction(ctx context.Context, value effects.TurnReactionPlan) error {
	plan, ok := value.(EnginePublicationPlan)
	if !ok {
		return fmt.Errorf("timeout reaction plan has unexpected type %T", value)
	}
	return eb.ReleaseEnginePublications(ctx, []engine.DurablePublicationPlan{plan})
}

func (eb *EventBus) DispatchTurnTimeoutReaction(ctx context.Context, value effects.CommittedTurnReaction) error {
	committed, ok := value.(CommittedEnginePublication)
	if !ok {
		return fmt.Errorf("committed timeout reaction has unexpected type %T", value)
	}
	if err := committed.ValidateCommittedDurablePublication(); err != nil {
		return err
	}
	finalizeErr := eb.FinalizeEnginePublications(ctx, []engine.CommittedDurablePublication{committed})
	return errors.Join(finalizeErr, eb.EngineDispatcher().DispatchPostCommit(ctx, []engine.EmitIntent{committed.CommittedDurablePublicationIntent()}))
}
