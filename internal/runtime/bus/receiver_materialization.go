package bus

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
)

func (intent RoutePlanDeliveryIntent) deliveryRoute() events.DeliveryRoute {
	return events.DeliveryRoute{Recipient: intent.Recipient, AgentIdentity: intent.AgentIdentity,
		Target: intent.TargetOwnership, Context: intent.Context, PayloadProjection: intent.PayloadProjection,
		ConnectClaim: intent.ConnectClaim, Materialization: intent.Materialization}
}

// The node classifier owns acquisition. This pass only binds an exact compiled
// receiver's dependent agents to that admitted future acquisition, without
// presenting it as an active descriptor or applying any lifecycle work.
func (p selectedRunTargetOwnerProjection) bindReceiverMaterializations(plan *RoutePlan) error {
	groups := make(map[int][]int)
	for ai := range plan.DeliveryIntents {
		agent := &plan.DeliveryIntents[ai]
		if !agent.Persist || !agent.Recipient.IsAgent() || (!agent.TargetOwnership.Empty() && !agent.TargetOwnership.MaterializingEntity()) {
			continue
		}
		pin, found := agent.ConnectClaim.ReceiverIdentity()
		if !found {
			continue
		}
		var candidates []int
		for ni, node := range plan.DeliveryIntents {
			if !node.Persist || !node.Recipient.IsNode() || !node.TargetOwnership.MaterializingEntity() {
				continue
			}
			nodePin, found := node.ConnectClaim.ReceiverIdentity()
			if !found || nodePin != pin {
				continue
			}
			target := node.TargetOwnership.Route()
			if agent.TargetBlueprint.FlowID != target.FlowID || agent.TargetBlueprint.FlowInstance != target.FlowInstance {
				continue
			}
			if agent.TargetBlueprint.EntityID != "" && agent.TargetBlueprint.EntityID != target.EntityID {
				return fmt.Errorf("receiver dependency contradicts the selected agent entity")
			}
			materializes, err := node.Handler.MaterializesReceiver(p.source, plan.Event.Type())
			if err != nil {
				return err
			}
			if materializes {
				candidates = append(candidates, ni)
			}
		}
		if len(candidates) == 0 {
			continue
		}
		if len(candidates) != 1 {
			return fmt.Errorf("receiver dependency has ambiguous canonical materializers")
		}
		ni := candidates[0]
		if !agent.TargetOwnership.Empty() && !events.SameDeliveryTargetOwnership(agent.TargetOwnership, plan.DeliveryIntents[ni].TargetOwnership) {
			return fmt.Errorf("receiver dependency contradicts admitted agent target ownership")
		}
		if p.agentsAvailable {
			descriptor, exists := p.agents[agent.AgentIdentity.Normalize()]
			if !exists && !agent.AgentLifecycle.pending() {
				return fmt.Errorf("receiver dependency has no active agent or admitted lifecycle creation")
			}
			if exists && descriptor.EntityID != "" && descriptor.EntityID != plan.DeliveryIntents[ni].TargetOwnership.Route().EntityID {
				return fmt.Errorf("receiver dependency contradicts exact active agent ownership")
			}
		}
		agent.TargetOwnership = plan.DeliveryIntents[ni].TargetOwnership
		agent.TargetBlueprint = agent.TargetOwnership.Route()
		groups[ni] = append(groups[ni], ai)
	}
	// All receiving decisions are fixed before constructing any dependency;
	// recipient enumeration order cannot manufacture another owner candidate.
	publication := plan.DeliveryRoutes()
	for ni, indices := range groups {
		dependents := make([]events.DeliveryRoute, 0, len(indices))
		for _, ai := range indices {
			dependents = append(dependents, plan.DeliveryIntents[ai].deliveryRoute())
		}
		dependency, err := events.AdmitReceiverMaterializationPlan(plan.Event, plan.DeliveryIntents[ni].deliveryRoute(), dependents, publication)
		if err != nil {
			return err
		}
		for _, ai := range indices {
			plan.DeliveryIntents[ai].Materialization = dependency
		}
	}
	return nil
}
