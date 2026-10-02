package bus

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func (intent RoutePlanDeliveryIntent) deliveryRoute() events.DeliveryRoute {
	return events.DeliveryRoute{Recipient: intent.Recipient, AgentIdentity: intent.AgentIdentity,
		Target: intent.TargetOwnership, Context: intent.Context, PayloadProjection: intent.PayloadProjection,
		ConnectClaim: intent.ConnectClaim, Initialization: intent.Initialization}
}

// A future target is authorized only by the same publication's canonical
// construction plan. Handler completion never supplies construction authority.
func (p selectedRunTargetOwnerProjection) bindReceiverMaterializations(plan *RoutePlan) error {
	for index := range plan.DeliveryIntents {
		intent := &plan.DeliveryIntents[index]
		if !intent.TargetOwnership.MaterializingEntity() {
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
	var constructionPlans []runtimepipeline.FlowInstanceActivationPlan
	for _, activation := range plan.ActivationPlans {
		constructionPlans = append(constructionPlans, activation.ConstructionPlans()...)
	}
	for _, activation := range constructionPlans {
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
		if intent.Initialization.Empty() {
			return fmt.Errorf("future receiver has no admitted initialization supplier")
		}
	}
	return nil
}
