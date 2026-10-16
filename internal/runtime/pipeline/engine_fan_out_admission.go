package pipeline

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func (c WorkflowEngineMutationCommand) validateFanOutIntents(runID string) error {
	seenIntents := make(map[fanoutobligation.IntentKey]bool, len(c.FanOutIntents))
	barrierBound := false
	for _, intent := range c.FanOutIntents {
		if err := intent.Validate(); err != nil {
			return fmt.Errorf("workflow engine fan-out intent: %w", err)
		}
		if seenIntents[intent.Key] {
			return fmt.Errorf("workflow engine fan-out intent is duplicated")
		}
		seenIntents[intent.Key] = true
		if c.DeliverySuccess == nil {
			return fmt.Errorf("workflow engine fan-out intent requires exact delivery settlement")
		}
		if intent.Key.RunID != runID || intent.Key.TriggeringDeliveryID != c.DeliverySuccess.Claim.DeliveryID() {
			return fmt.Errorf("workflow engine fan-out intent disagrees with the settled delivery")
		}
		if c.FanOutBarrier != nil && c.FanOutBarrier.IntentKey == intent.Key && c.FanOutBarrier.PlanRef == intent.PlanRef {
			barrierBound = true
		}
	}
	if c.FanOutBarrier != nil && len(c.FanOutIntents) == 0 {
		return fmt.Errorf("workflow engine fan-out delivery barrier must be committed with its exact intent")
	}
	if c.FanOutBarrier != nil {
		if err := c.FanOutBarrier.Validate(); err != nil {
			return fmt.Errorf("workflow engine fan-out barrier: %w", err)
		}
		if !barrierBound {
			return fmt.Errorf("workflow engine fan-out barrier disagrees with its exact intent")
		}
	}
	return nil
}
