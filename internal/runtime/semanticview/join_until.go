package semanticview

import (
	"sort"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// WorkflowJoinUntilPlansForNode projects compiler-owned closure consumers, not
// authored event handlers. One event may close several declarations on a node.
func WorkflowJoinUntilPlansForNode(source Source, node runtimeidentity.ExecutableNode) []runtimecontracts.WorkflowJoinPlan {
	if source == nil || !node.Valid() {
		return nil
	}
	var plans []runtimecontracts.WorkflowJoinPlan
	for _, plan := range source.WorkflowJoins() {
		if plan.Node.Equal(node) && plan.UntilEvent != "" {
			plans = append(plans, plan.Clone())
		}
	}
	sort.SliceStable(plans, func(i, j int) bool {
		if plans[i].UntilEvent != plans[j].UntilEvent {
			return plans[i].UntilEvent < plans[j].UntilEvent
		}
		return plans[i].HandlerEvent < plans[j].HandlerEvent
	})
	return plans
}

// WorkflowJoinUntilPlansForEvent requires the exact compiled event identity.
// Routing/localization belongs to the canonical subscription admission owner.
func WorkflowJoinUntilPlansForEvent(source Source, node runtimeidentity.ExecutableNode, eventType string) []runtimecontracts.WorkflowJoinPlan {
	var plans []runtimecontracts.WorkflowJoinPlan
	for _, plan := range WorkflowJoinUntilPlansForNode(source, node) {
		if plan.UntilEvent == eventType {
			plans = append(plans, plan)
		}
	}
	return plans
}
