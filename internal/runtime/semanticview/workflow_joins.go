package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

// effectiveWorkflowJoins lowers fan-in barrier seam identity into the join
// lifecycle owner. Validation rejects authored duplicates and incomplete
// associations; this function only materializes canonical executable facts.
func effectiveWorkflowJoins(source Source, plans []runtimecontracts.WorkflowJoinPlan) []runtimecontracts.WorkflowJoinPlan {
	out := append([]runtimecontracts.WorkflowJoinPlan(nil), plans...)
	for idx := range out {
		out[idx].Spec = cloneEffectiveJoinSpec(out[idx].Spec)
	}
	if source == nil {
		return out
	}
	census := BuildAuthoredEventEndpointCensus(source)
	for idx := range out {
		plan := &out[idx]
		association := census.ResolveFanInInputForHandler(plan.FlowID, plan.NodeID, plan.HandlerEvent)
		endpoint, ok := association.Endpoint()
		if !ok {
			continue
		}
		pin, ok := source.FlowInputEventPin(plan.FlowID, endpoint.PinName)
		if !ok || !strings.EqualFold(strings.TrimSpace(pin.Resolution.Aggregation), "barrier") {
			continue
		}
		dedup := normalizedJoinDerivationValues(pin.Resolution.DedupBy)
		if len(dedup) == 1 {
			plan.Spec.Members.By = dedup[0]
			plan.Spec.Members.ByPath = paths.Parse(dedup[0])
			plan.Derivation.MembersBy = dedup[0]
			plan.Derivation.MembersByFrom = "resolution.dedup_by"
		}
		if window := strings.TrimSpace(pin.Resolution.Window); window != "" && plan.Spec.Window != nil {
			plan.Spec.Window.By = window
			plan.Spec.Window.ByPath = paths.Parse(window)
			plan.Derivation.WindowBy = window
			plan.Derivation.WindowByFrom = "resolution.window"
		}
		plan.Derivation.FanInPin = strings.TrimSpace(endpoint.PinName)
	}
	return out
}

func cloneEffectiveJoinSpec(spec runtimecontracts.JoinSpec) runtimecontracts.JoinSpec {
	clone := spec
	if spec.Window != nil {
		window := *spec.Window
		clone.Window = &window
	}
	return clone
}

func WorkflowJoinPlanForHandler(source Source, flowID, nodeID, handlerEvent string) (runtimecontracts.WorkflowJoinPlan, bool) {
	if source == nil {
		return runtimecontracts.WorkflowJoinPlan{}, false
	}
	flowID, nodeID, handlerEvent = strings.TrimSpace(flowID), strings.TrimSpace(nodeID), strings.TrimSpace(handlerEvent)
	for _, plan := range source.WorkflowJoins() {
		if strings.TrimSpace(plan.FlowID) == flowID && strings.TrimSpace(plan.NodeID) == nodeID && strings.TrimSpace(plan.HandlerEvent) == handlerEvent {
			return plan, true
		}
	}
	return runtimecontracts.WorkflowJoinPlan{}, false
}

func normalizedJoinDerivationValues(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			out = append(out, value)
		}
	}
	return out
}
