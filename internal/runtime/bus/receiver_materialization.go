package bus

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
)

func (intent RoutePlanDeliveryIntent) deliveryRoute() events.DeliveryRoute {
	return events.DeliveryRoute{Recipient: intent.Recipient, AgentIdentity: intent.AgentIdentity,
		Target: intent.TargetOwnership, Context: intent.Context, PayloadProjection: intent.PayloadProjection,
		ConnectClaim: intent.ConnectClaim, Initialization: intent.Initialization}
}

// A future target is authorized only by the same publication's canonical
// construction plan. Handler completion never supplies construction authority.
func (p publicationTargetOwnerEvidence) bindReceiverMaterializations(plan *RoutePlan) error {
	for index := range plan.DeliveryIntents {
		intent := &plan.DeliveryIntents[index]
		if !intent.TargetOwnership.MaterializingEntity() || !intent.Initialization.Empty() {
			continue
		}
		initialization, found, err := flowReceiverInitialization(*plan, intent.TargetOwnership)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("future receiver requires an admitted canonical construction plan")
		}
		intent.Initialization = initialization
	}
	return nil
}

func flowReceiverInitialization(plan RoutePlan, target events.DeliveryTargetOwnership) (events.ReceiverInitialization, bool, error) {
	for _, tree := range plan.ActivationPlans {
		for _, activation := range tree.ConstructionPlans() {
			if err := activation.Validate(); err != nil {
				return events.ReceiverInitialization{}, false, err
			}
			identity := activation.Identity
			route := target.Route()
			if activation.Readiness.RunID != plan.Event.RunID() || identity.TemplateID != route.FlowID || identity.InstancePath != route.FlowInstance || identity.EntityID != route.EntityID {
				continue
			}
			if activation.CreatingInput.EventID != plan.Event.ID() {
				return events.ReceiverInitialization{}, false, fmt.Errorf("future receiver construction belongs to another creating event")
			}
			receiver, err := identity.Route().AgentIdentityRoute()
			if err != nil {
				return events.ReceiverInitialization{}, false, err
			}
			supplier, err := events.AdmitFlowReceiverInitialization(plan.Event, target, receiver)
			return supplier, err == nil, err
		}
	}
	return events.ReceiverInitialization{}, false, nil
}
