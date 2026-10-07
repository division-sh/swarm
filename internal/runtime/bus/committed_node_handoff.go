package bus

import (
	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// Callers first establish acknowledged publication and receiver readiness.
// Route kind or equal counts alone cannot prove a complete execution handoff.
func (eb *EventBus) canTransferNodeDeliveries(event events.Event, plan RoutePlan, handoffs []runtimedelivery.DurableHandoffProof) bool {
	owner := eb.DeliveryContinuationOwner()
	if owner == nil || !owner.OwnsPersistedRecovery() {
		return false
	}
	switch event.AdmissionClass() {
	case events.EventAdmissionRootIngress, events.EventAdmissionOperatorInjected, events.EventAdmissionChild,
		events.EventAdmissionReplay, events.EventAdmissionInheritedFanOut:
	default:
		// Runtime control/diagnostic events can carry timer or decision work
		// before node interception. Selected execution has its own lifetime.
		return false
	}
	routes := plan.DeliveryRoutes()
	if !plan.TargetFailure.Empty() || len(routes) == 0 || len(routes) != len(handoffs) || !nodeRoutesCoverLiveRecipients(plan.LiveRecipients, routes) {
		return false
	}
	authority, err := eb.DeliveryAuthority()
	if err != nil {
		return false
	}
	expected := make(map[string]struct{}, len(routes))
	for _, route := range routes {
		if !route.Recipient.IsNode() {
			return false
		}
		id, err := runtimedelivery.DeliveryID(event.ID(), route)
		if err != nil {
			return false
		}
		expected[id] = struct{}{}
	}
	for _, handoff := range handoffs {
		if err := handoff.Validate(); err != nil || handoff.EventID() != event.ID() || !handoff.Authority().Equal(authority) {
			return false
		}
		if _, exists := expected[handoff.DeliveryID()]; !exists {
			return false
		}
		delete(expected, handoff.DeliveryID())
	}
	if len(expected) != 0 {
		return false
	}
	eventInterceptors, _ := splitDeliveryRouteInterceptors(eb.interceptorsSnapshot())
	return len(eventInterceptors) == 0
}
