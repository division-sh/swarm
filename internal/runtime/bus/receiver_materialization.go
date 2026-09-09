package bus

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
)

func (intent RoutePlanDeliveryIntent) deliveryRoute() events.DeliveryRoute {
	return events.DeliveryRoute{Recipient: intent.Recipient, AgentIdentity: intent.AgentIdentity,
		Target: intent.TargetOwnership, Context: intent.Context, PayloadProjection: intent.PayloadProjection,
		ConnectClaim: intent.ConnectClaim, Materialization: intent.Materialization, Initialization: intent.Initialization}
}

// The node classifier owns acquisition. This pass only binds an exact compiled
// receiver's dependent agents to that admitted future acquisition, without
// presenting it as an active descriptor or applying any lifecycle work.
func (p selectedRunTargetOwnerProjection) bindReceiverMaterializations(plan *RoutePlan) error {
	for index := range plan.DeliveryIntents {
		intent := &plan.DeliveryIntents[index]
		if !intent.TargetOwnership.MaterializingEntity() {
			continue
		}
		if supplier, found, err := flowReceiverInitialization(*plan, intent.TargetOwnership); err != nil {
			return err
		} else if found {
			intent.Initialization = supplier
			continue
		}
		if intent.Recipient.IsNode() {
			materializes, err := intent.Handler.MaterializesReceiver(p.source, plan.Event.Type())
			if err != nil {
				return err
			}
			if materializes {
				intent.Initialization, err = events.AdmitNodeReceiverInitialization(plan.Event, intent.TargetOwnership, intent.Handler.Node())
				if err != nil {
					return err
				}
			}
		}
	}
	groups := make(map[int][]int)
	for ai := range plan.DeliveryIntents {
		agent := &plan.DeliveryIntents[ai]
		if !agent.Persist || !agent.Recipient.IsAgent() || (!agent.TargetOwnership.Empty() && !agent.TargetOwnership.MaterializingEntity()) {
			continue
		}
		if agent.Initialization.FlowLifecycle() {
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
			if node.Initialization.IsInitializer(node.deliveryRoute()) {
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
		agent.Initialization = plan.DeliveryIntents[ni].Initialization
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

func flowReceiverInitialization(plan RoutePlan, target events.DeliveryTargetOwnership) (events.ReceiverInitialization, bool, error) {
	for _, activation := range plan.ActivationPlans {
		if err := activation.Validate(); err != nil {
			return events.ReceiverInitialization{}, false, err
		}
		identity := activation.Identity
		route := target.Route()
		if activation.Readiness.RunID != plan.Event.RunID() || identity.TemplateID != route.FlowID || identity.InstancePath != route.FlowInstance || identity.EntityID != route.EntityID {
			continue
		}
		supplier, err := events.AdmitFlowReceiverInitialization(plan.Event, target)
		return supplier, err == nil, err
	}
	return events.ReceiverInitialization{}, false, nil
}

func completeReceiverInitializations(plan *RoutePlan) error {
	for index := range plan.DeliveryIntents {
		intent := &plan.DeliveryIntents[index]
		if !intent.TargetOwnership.MaterializingEntity() || !intent.Initialization.Empty() {
			continue
		}
		if supplier, found, err := flowReceiverInitialization(*plan, intent.TargetOwnership); err != nil {
			return err
		} else if found {
			intent.Initialization = supplier
			continue
		}
		pin, found := intent.ConnectClaim.ReceiverIdentity()
		for _, other := range plan.DeliveryIntents {
			otherPin, otherFound := other.ConnectClaim.ReceiverIdentity()
			if !found || !otherFound || pin != otherPin || !events.SameDeliveryTargetOwnership(intent.TargetOwnership, other.TargetOwnership) || !other.Initialization.IsInitializer(other.deliveryRoute()) {
				continue
			}
			if !intent.Initialization.Empty() && !intent.Initialization.Equal(other.Initialization) {
				return fmt.Errorf("receiver has multiple initialization suppliers")
			}
			intent.Initialization = other.Initialization
		}
		if intent.Initialization.Empty() {
			return fmt.Errorf("future receiver has no admitted initialization supplier")
		}
	}
	return nil
}
