package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
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
		association := census.ResolveFanInInputForHandler(plan.Node, plan.HandlerEvent)
		endpoint, ok := association.Endpoint()
		if !ok {
			continue
		}
		pin, ok := source.FlowInputEventPin(plan.Node.FlowID(), endpoint.PinName)
		if !ok || !strings.EqualFold(strings.TrimSpace(pin.Resolution().Aggregation), "barrier") {
			continue
		}
		resolution := pin.Resolution()
		dedup := normalizedJoinDerivationValues(resolution.DedupBy)
		if len(dedup) == 1 {
			plan.Spec.Members.By = dedup[0]
			plan.Spec.Members.ByPath = paths.Parse(dedup[0])
			plan.Derivation.MembersBy = dedup[0]
			plan.Derivation.MembersByFrom = "resolution.dedup_by"
		}
		if window := strings.TrimSpace(resolution.Window); window != "" && plan.Spec.Window != nil {
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

func WorkflowJoinPlanForHandler(source Source, node runtimeidentity.ExecutableNode, handlerEvent string) (runtimecontracts.WorkflowJoinPlan, bool) {
	if source == nil || !node.Valid() {
		return runtimecontracts.WorkflowJoinPlan{}, false
	}
	handlerEvent = strings.TrimSpace(handlerEvent)
	for _, plan := range source.WorkflowJoins() {
		if plan.Node.Equal(node) && strings.TrimSpace(plan.HandlerEvent) == handlerEvent {
			return plan, true
		}
	}
	return runtimecontracts.WorkflowJoinPlan{}, false
}

// WorkflowJoinPlanForExecutionHandler maps one exact authored handler scope to
// its join declaration. Root declarations retain an explicit empty FlowID even
// though their handlers execute in the authored root flow scope.
func WorkflowJoinPlanForExecutionHandler(source Source, node runtimeidentity.ExecutableNode, handlerEvent string, spec runtimecontracts.JoinSpec) (runtimecontracts.WorkflowJoinPlan, bool) {
	if source == nil || !node.Valid() {
		return runtimecontracts.WorkflowJoinPlan{}, false
	}
	handlerEvent = strings.TrimSpace(handlerEvent)
	if handlerEvent == "" {
		return runtimecontracts.WorkflowJoinPlan{}, false
	}
	if _, ok := source.ExecutableNode(node); !ok {
		return runtimecontracts.WorkflowJoinPlan{}, false
	}
	plan, ok := WorkflowJoinPlanForHandler(source, node, handlerEvent)
	if !ok || strings.TrimSpace(plan.Spec.Stage) != strings.TrimSpace(spec.Stage) || plan.Spec.EffectiveID() != spec.EffectiveID() {
		return runtimecontracts.WorkflowJoinPlan{}, false
	}
	return plan, true
}

func WorkflowJoinPlanForRef(source Source, ref timeridentity.JoinRef) (runtimecontracts.WorkflowJoinPlan, bool) {
	if source == nil || !ref.Valid() {
		return runtimecontracts.WorkflowJoinPlan{}, false
	}
	plan, ok := WorkflowJoinPlanForHandler(source, ref.Node(), ref.HandlerEvent())
	if !ok || strings.TrimSpace(plan.Spec.Stage) != ref.Stage() || plan.Spec.EffectiveID() != ref.JoinID() {
		return runtimecontracts.WorkflowJoinPlan{}, false
	}
	return plan, true
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
