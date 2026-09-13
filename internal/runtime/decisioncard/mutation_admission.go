package decisioncard

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/gateruntime"
)

// Refusals carry immutable semantic identity across preparation, transaction and
// API projection. Their text is diagnostic, never admission authority.
type mutationRefusal uint8

const (
	ErrAlreadyTerminal mutationRefusal = iota + 1
	ErrSuperseded
)

func (r mutationRefusal) Error() string {
	switch r {
	case ErrAlreadyTerminal:
		return "decision card is already terminal"
	case ErrSuperseded:
		return "decision card is superseded"
	default:
		return "invalid decision card mutation refusal"
	}
}

// RequirePendingMutation is for fresh decide/defer/begin-input operations, not
// completed-request replay, draft cancellation, or scheduler eligibility.
func (c Card) RequirePendingMutation() error {
	switch c.Status {
	case StatusPending:
		return nil
	case StatusSuperseded:
		return ErrSuperseded
	case StatusDecided, StatusExpired:
		return ErrAlreadyTerminal
	default:
		return fmt.Errorf("decision card has invalid mutation status %q", c.Status)
	}
}

func (c Card) RequireGateMutation(activation gateruntime.Activation, found bool, currentStage string) error {
	if err := c.RequirePendingMutation(); err != nil {
		return err
	}
	anchor, err := c.Anchor.StageGate()
	if err != nil {
		return err
	}
	if !found {
		return ErrSuperseded
	}
	if err := activation.Validate(); err != nil {
		return err
	}
	if activation.FlowID != anchor.FlowID || activation.ActivationID != anchor.StageActivationID || activation.CardID != c.CardID || activation.Stage != anchor.Stage || currentStage != anchor.Stage {
		return ErrSuperseded
	}
	switch activation.Status {
	case gateruntime.StatusOpen:
		return nil
	case gateruntime.StatusSuperseded:
		return ErrSuperseded
	case gateruntime.StatusDecisionCommitted, gateruntime.StatusRouted:
		return ErrAlreadyTerminal
	default:
		return fmt.Errorf("invalid gate mutation status %q", activation.Status)
	}
}

func (c HumanTaskContinuation) RequirePendingMutation(card Card) error {
	if err := card.RequirePendingMutation(); err != nil {
		return err
	}
	if err := c.Validate(card); err != nil {
		return err
	}
	switch c.State {
	case HumanTaskContinuationPending:
		return nil
	case HumanTaskContinuationSuperseded:
		return ErrSuperseded
	case HumanTaskContinuationDecisionCommitted, HumanTaskContinuationOutcomeDispatched, HumanTaskContinuationExpired:
		return ErrAlreadyTerminal
	default:
		return fmt.Errorf("invalid human-task mutation state %q", c.State)
	}
}

func (c ProposedEffectContinuation) RequirePendingMutation(card Card) error {
	if err := card.RequirePendingMutation(); err != nil {
		return err
	}
	if err := c.Validate(card); err != nil {
		return err
	}
	switch c.State {
	case ProposedEffectPending:
		return nil
	case ProposedEffectSuperseded:
		return ErrSuperseded
	case ProposedEffectDecisionCommitted, ProposedEffectRequestReleased, ProposedEffectOutcomeDispatched:
		return ErrAlreadyTerminal
	default:
		return fmt.Errorf("invalid proposed-effect mutation state %q", c.State)
	}
}
