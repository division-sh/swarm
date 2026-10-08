package workflowlifecycle

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/google/uuid"
)

// TurnTermination is selected authored transition evidence, not a destination
// stage property and not resource-retirement authority.
type TurnTermination struct {
	owner flowidentity.RunScopedFlowInstance
	cause Effect
}

func NewTurnTermination(owner flowidentity.RunScopedFlowInstance, cause Effect) (TurnTermination, error) {
	value := TurnTermination{owner: owner, cause: cause}
	return value, value.Validate()
}

func (t TurnTermination) Owner() flowidentity.RunScopedFlowInstance { return t.owner }
func (t TurnTermination) Cause() Effect                             { return t.cause }

func (t TurnTermination) Validate() error {
	if err := t.owner.Validate(); err != nil {
		return err
	}
	transition, found := t.cause.Transition()
	if !found || !transition.TerminatesAgentTurns() || transition.Validate() != nil || t.cause.Kind() != KindAcceptedEvent ||
		t.cause.Route() != t.owner.Route || transition.FlowID() != t.owner.Route.ScopeKey || t.cause.EntityID().IsZero() || t.cause.OccurredAt().IsZero() || t.cause.EventType() == "" {
		return fmt.Errorf("turn termination requires its exact selected authored transition")
	}
	if _, err := uuid.Parse(t.cause.EventID()); err != nil {
		return fmt.Errorf("turn termination requires its exact cause event: %w", err)
	}
	if t.cause.occurrenceKind == "" || t.cause.occurrenceID == "" {
		return fmt.Errorf("turn termination requires its admitted execution occurrence")
	}
	return nil
}
