package bus

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// The compiled ledger owns selected flows. Actor state ownership cannot
// rewrite those selections or another recipient's construction coordinate.
func validateConnectIntentReceivers(plan RoutePlan, source semanticview.Source) error {
	for _, intent := range plan.DeliveryIntents {
		if intent.ConnectPlan.Empty() {
			continue
		}
		if intent.TargetOwnership.Empty() || intent.TargetOwnership.Validate() != nil {
			return fmt.Errorf("connect recipient has no admitted target ownership")
		}
		receiver := intent.TargetOwnership.Route()
		root := false
		if intent.Recipient.IsAgent() {
			receiver = intent.ReceiverBlueprint
			if !intent.Initialization.Empty() {
				bound := intent.Initialization.ConstructionTarget().Route()
				if !receiver.Empty() && receiver != bound {
					return fmt.Errorf("connect receiver contradicts construction dependency")
				}
				receiver = bound
			}
			root = intent.AgentIdentity.Route.Presence == agentidentity.RouteRoot
		} else if source != nil {
			root = intent.Handler.ExecutionFlowID(source) == semanticview.RootExecutionFlowID(source)
		}
		matched := false
		for _, selection := range plan.ConnectEvaluation.Plans() {
			if selection.PlanIdentity() != intent.ConnectPlan {
				continue
			}
			for _, target := range selection.Targets() {
				if target.Empty() && root || !receiver.Empty() && target.FlowID == receiver.FlowID && target.FlowInstance == receiver.FlowInstance && (target.EntityID == "" || target.EntityID == receiver.EntityID) {
					matched = true
				}
			}
		}
		if !matched {
			return fmt.Errorf("connect recipient has no agreeing selected flow target")
		}
	}
	return nil
}
