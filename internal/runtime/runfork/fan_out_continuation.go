package runfork

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

// A closed issuance cursor does not settle its published recipients or barrier.
// Only inert fixed-cut history can retain its plan without a selected executable.
func FanOutContinuationRequirements(plan RunForkPlan) (map[fanoutobligation.IntentKey]bool, error) {
	if err := ValidateFanOutPendingReplayAdmission(plan); err != nil {
		return nil, err
	}
	out := make(map[fanoutobligation.IntentKey]bool, len(plan.FanOutObligations))
	for _, obligation := range plan.FanOutObligations {
		key := obligation.Intent.Request.Key
		if _, duplicate := out[key]; duplicate {
			return nil, fmt.Errorf("fork repeats fan-out intent %s", key.String())
		}
		owed, err := obligation.requiresSelectedPlan()
		if err != nil {
			return nil, err
		}
		out[key] = owed
	}
	return out, nil
}

func (o RunForkFanOutObligation) requiresSelectedPlan() (bool, error) {
	if err := o.Intent.Validate(); err != nil {
		return false, err
	}
	owed := o.Intent.Status == fanoutobligation.StatusOpen || o.Intent.Status == fanoutobligation.StatusBlocked ||
		len(o.PendingReplays) != 0 || len(o.PendingDeployment) != 0
	for _, outcome := range o.Outcomes {
		if err := outcome.Validate(); err != nil {
			return false, err
		}
		// A local publication is not proof of recipient settlement.
		owed = owed || outcome.EventID != ""
	}
	if o.Barrier != nil {
		if err := o.Barrier.Validate(); err != nil {
			return false, err
		}
		if o.Barrier.Registration.IntentKey != o.Intent.Request.Key || o.Barrier.Registration.PlanRef != o.Intent.Request.PlanRef {
			return false, fmt.Errorf("fork fan-out barrier disagrees with its exact intent")
		}
		owed = owed || o.Barrier.Status.BlocksCompletion()
	}
	return owed, nil
}
