package bus

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
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
		receiver, root, err := connectIntentReceiver(intent, source)
		if err != nil {
			return err
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

func connectIntentReceiver(intent RoutePlanDeliveryIntent, source semanticview.Source) (events.RouteIdentity, bool, error) {
	if intent.TargetOwnership.Empty() || intent.TargetOwnership.Validate() != nil {
		return events.RouteIdentity{}, false, fmt.Errorf("connect recipient has no admitted target ownership")
	}
	if !intent.Recipient.IsAgent() {
		root := source != nil && intent.Handler.ExecutionFlowID(source) == semanticview.RootExecutionFlowID(source)
		return intent.TargetOwnership.Route(), root, nil
	}
	receiver := intent.ReceiverBlueprint
	if !intent.Initialization.Empty() {
		bound := intent.Initialization.ConstructionTarget().Route()
		if !receiver.Empty() && receiver != bound {
			return events.RouteIdentity{}, false, fmt.Errorf("connect receiver contradicts construction dependency")
		}
		receiver = bound
	}
	return receiver, intent.AgentIdentity.Route.Presence == agentidentity.RouteRoot, nil
}
