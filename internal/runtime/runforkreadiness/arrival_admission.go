package runforkreadiness

import (
	"fmt"
	"slices"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type inheritedArrivalAdmission struct {
	digest      string
	ref         timeridentity.JoinRef
	disposition genericschedule.ForkJoinDisposition
}

func admitInheritedArrivals(source semanticview.Source, plan runfork.RunForkPlan) (map[string]inheritedArrivalAdmission, error) {
	out := make(map[string]inheritedArrivalAdmission, len(plan.JoinSchedules))
	for _, schedule := range plan.JoinSchedules {
		if schedule.Command.RunID != plan.SourceRunID {
			return nil, fmt.Errorf("selected arrival belongs to another source run")
		}
		digest, err := schedule.EvidenceDigest()
		if err != nil {
			return nil, err
		}
		payload, ok := schedule.Command.Payload.Interface().(map[string]any)
		if !ok {
			return nil, fmt.Errorf("selected arrival lacks its retained declaration")
		}
		_, ref, ok := timeridentity.ParseJoinHandle(payload)
		if !ok || ref.Mode() != timeridentity.JoinRefModeArrival {
			return nil, fmt.Errorf("selected arrival lacks an exact arrival handle")
		}
		if _, duplicate := out[schedule.ID]; duplicate {
			return nil, fmt.Errorf("selected arrival repeats source identity")
		}
		disposition := genericschedule.ForkJoinRetained
		if _, declared := semanticview.WorkflowJoinPlanForRef(source, ref); !declared && schedule.Status == genericschedule.StatusActive {
			disposition = genericschedule.ForkJoinRuleRemoved
		}
		out[schedule.ID] = inheritedArrivalAdmission{digest: digest, ref: ref, disposition: disposition}
	}
	return out, nil
}

func (a Admission) SelectInheritedArrivalJoin(source genericschedule.Activation) (genericschedule.ForkJoinDisposition, error) {
	if a.sealed == nil {
		return "", fmt.Errorf("inherited arrival requires selected readiness admission")
	}
	sealed, found := a.sealed.arrivals[source.ID]
	digest, err := source.EvidenceDigest()
	if err != nil || !found || digest != sealed.digest {
		return "", fmt.Errorf("inherited arrival differs from its exact sealed source")
	}
	return sealed.disposition, sealed.disposition.Validate(source)
}

func (a Admission) RemovedInheritedArrivalRefs() ([]timeridentity.JoinRef, error) {
	if a.sealed == nil {
		return nil, fmt.Errorf("removed arrivals require selected readiness admission")
	}
	refs := make(map[string]timeridentity.JoinRef)
	for _, admission := range a.sealed.arrivals {
		if admission.disposition == genericschedule.ForkJoinRuleRemoved {
			refs[admission.ref.Key()] = admission.ref
		}
	}
	keys := make([]string, 0, len(refs))
	for key := range refs {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	out := make([]timeridentity.JoinRef, 0, len(keys))
	for _, key := range keys {
		out = append(out, refs[key])
	}
	return out, nil
}
