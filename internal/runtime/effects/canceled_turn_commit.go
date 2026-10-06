package effects

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// CanceledTurnCommand carries the already-admitted timeout reaction. The
// selected-store owner commits it with the exact canceled origin, not before
// physical work has settled and not in a second publication transaction.
type CanceledTurnCommand struct {
	Attempt     Attempt
	Publication TurnReactionPlan
}

// These are the existing publication evidence contracts restricted to the
// facts the cancellation owner consumes; effects cannot depend on the engine.
type TurnReactionPlan interface {
	DurablePublicationEventID() string
	ValidateDurablePublicationPlan() error
}

type CommittedTurnReaction interface {
	CommittedDurablePublicationEventID() string
	ValidateCommittedDurablePublication() error
}

type CanceledTurnCommit struct {
	Acknowledged bool
	Origin       CompletionOrigin
	Cancellation TurnCancellation
	Delivery     deliverylifecycle.Snapshot
	Directive    agentcontrol.DirectiveOperation
	Publication  CommittedTurnReaction
}

type CanceledTurnStore interface {
	CommitCanceledTurn(context.Context, CanceledTurnCommand) (CanceledTurnCommit, error)
}

// Startup reconstructs the immutable first physical attempt, not a new turn
// or a current-generation token. The atomic commit revalidates this evidence.
type CanceledTurnRecoveryStore interface {
	ListCanceledTurnRecoveries(context.Context, RecoveryRequest) ([]TurnExecutionResult, error)
}

func TurnTimeoutProducerID() string { return "agent-turn-timeout" }

type TurnTimeoutReactionOwner interface {
	PrepareTurnTimeoutReaction(context.Context, TurnExecutionResult) (TurnReactionPlan, error)
	ReleaseTurnTimeoutReaction(context.Context, TurnReactionPlan) error
	DispatchTurnTimeoutReaction(context.Context, CommittedTurnReaction) error
}

func (c CanceledTurnCommit) Validate() error {
	if !c.Acknowledged || c.Origin.Validate() != nil || c.Cancellation.ValidateIntent() != nil ||
		!c.Cancellation.OriginSettled || !c.Origin.Same(c.Cancellation.Origin) {
		return fmt.Errorf("canceled turn commit requires acknowledged exact settled intent")
	}
	switch c.Origin.Kind {
	case CompletionOriginDelivery:
		if !c.Delivery.MatchesSettlementClaim(c.Origin.Delivery) || c.Directive.OperationID != "" ||
			c.Delivery.ReasonCode != string(c.Cancellation.Reason) {
			return fmt.Errorf("canceled turn commit has foreign or mixed origin evidence")
		}
		if err := deliverylifecycle.ValidateCanceledSnapshot(c.Delivery); err != nil {
			return err
		}
	case CompletionOriginDirective:
		if c.Directive.OperationID != c.Origin.Directive.OperationID || !c.Directive.Acknowledged || c.Delivery.DeliveryID != "" ||
			c.Directive.State != agentcontrol.DirectiveOperationCanceled || c.Directive.CancellationReason != c.Cancellation.Reason {
			return fmt.Errorf("canceled turn commit has foreign or mixed origin evidence")
		}
		if err := agentcontrol.ValidateDirectiveOperationEvidence(c.Directive); err != nil {
			return err
		}
	}
	if c.Cancellation.Reason == deliverylifecycle.CancellationTurnTimeout {
		if c.Publication == nil || c.Publication.CommittedDurablePublicationEventID() != c.Cancellation.CauseEvent {
			return fmt.Errorf("canceled turn commit omitted its exact timeout reaction")
		}
		return c.Publication.ValidateCommittedDurablePublication()
	}
	if c.Publication != nil {
		return fmt.Errorf("terminate cancellation cannot invent a timeout reaction")
	}
	return nil
}
