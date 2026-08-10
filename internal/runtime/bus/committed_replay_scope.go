package bus

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func (eb *EventBus) currentInternalRecipientsForCommittedEvent(ctx context.Context, evt events.Event) ([]string, error) {
	plan, err := eb.deliveryPlanner.Plan(ctx, evt)
	if err != nil {
		return nil, err
	}
	return filterOutAgentIDs(plan.RecipientIDs(), plan.PersistedRecipientIDs()), nil
}

func (eb *EventBus) replayRecipientsForCommittedEvent(
	ctx context.Context,
	evt events.Event,
	persisted []string,
	scope runtimepipelineobligation.CommittedScope,
) ([]string, []string, []events.DeliveryRoute, error) {
	persisted = uniqueStrings(persisted)
	persistedRoutes := eb.deliveryRoutesForEvent(ctx, evt.ID())
	persisted = uniqueStrings(append(persisted, deliveryRouteAgentRecipientIDs(persistedRoutes)...))
	if scope == runtimepipelineobligation.ScopeDirect && len(persistedRoutes) > 0 {
		internal := []string(nil)
		live := deliveryRouteAgentRecipientIDs(persistedRoutes)
		if len(live) > 0 {
			return live, internal, persistedRoutes, nil
		}
	}
	if scope == runtimepipelineobligation.ScopeSubscribed && hasNodeDeliveryRoute(persistedRoutes) {
		internal, err := eb.replayNodeCarriersForCommittedEvent(ctx, evt, persistedRoutes)
		if err != nil {
			return nil, nil, nil, err
		}
		live := uniqueStrings(append(deliveryRouteAgentRecipientIDs(persistedRoutes), internal...))
		return live, internal, persistedRoutes, nil
	}
	switch scope {
	case runtimepipelineobligation.ScopeDirect:
		if len(persisted) > 0 {
			return nil, nil, nil, fmt.Errorf("replay event %s has persisted agent recipients without exact identity-bearing delivery routes", evt.ID())
		}
		return nil, nil, nil, nil
	case runtimepipelineobligation.ScopeSubscribed:
		internal, err := eb.currentInternalRecipientsForCommittedEvent(ctx, evt)
		if err != nil {
			return nil, nil, nil, err
		}
		live := uniqueStrings(append(append([]string(nil), persisted...), internal...))
		routes := append([]events.DeliveryRoute(nil), persistedRoutes...)
		if len(persisted) > 0 && !deliveryRoutesCoverAgentRecipients(routes, persisted) {
			return nil, nil, nil, fmt.Errorf("replay event %s has persisted agent recipients without exact identity-bearing delivery routes", evt.ID())
		}
		for _, recipient := range internal {
			typedRecipient := events.MustNodeDeliveryRecipient(recipient)
			if hasDeliveryRouteRecipient(routes, typedRecipient) {
				continue
			}
			routes = append(routes, events.DeliveryRoute{Recipient: typedRecipient})
		}
		return live, internal, events.NormalizeDeliveryRoutes(routes), nil
	default:
		return nil, nil, nil, fmt.Errorf("replay recipients: unsupported scope %q", strings.TrimSpace(string(scope)))
	}
}

func (eb *EventBus) replayNodeCarriersForCommittedEvent(_ context.Context, _ events.Event, routes []events.DeliveryRoute) ([]string, error) {
	persisted := deliveryRouteNodeRecipientIDs(routes)
	eb.mu.RLock()
	if eb.internalHandles[workflowRuntimeInternalCarrierID] != nil {
		eb.mu.RUnlock()
		return []string{workflowRuntimeInternalCarrierID}, nil
	}
	exact := make([]string, 0, len(persisted))
	for _, recipient := range persisted {
		if eb.internalHandles[recipient] != nil {
			exact = append(exact, recipient)
		}
	}
	eb.mu.RUnlock()
	if exact = uniqueStrings(exact); len(exact) > 0 {
		return exact, nil
	}
	return persisted, nil
}

func hasNodeDeliveryRoute(routes []events.DeliveryRoute) bool {
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.Recipient.IsNode() {
			return true
		}
	}
	return false
}

func hasDeliveryRouteRecipient(routes []events.DeliveryRoute, recipient events.DeliveryRecipient) bool {
	if recipient.Empty() {
		return false
	}
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.Recipient == recipient {
			return true
		}
	}
	return false
}

func deliveryRouteRecipientIDs(routes []events.DeliveryRoute) []string {
	routes = events.NormalizeDeliveryRoutes(routes)
	out := make([]string, 0, len(routes))
	for _, route := range routes {
		if !route.Recipient.Empty() {
			out = append(out, route.Recipient.ID())
		}
	}
	return uniqueStrings(out)
}

func deliveryRouteNodeRecipientIDs(routes []events.DeliveryRoute) []string {
	routes = events.NormalizeDeliveryRoutes(routes)
	out := make([]string, 0, len(routes))
	for _, route := range routes {
		if !route.Recipient.IsNode() {
			continue
		}
		out = append(out, route.Recipient.ID())
	}
	return uniqueStrings(out)
}

func deliveryRouteAgentRecipientIDs(routes []events.DeliveryRoute) []string {
	routes = events.NormalizeDeliveryRoutes(routes)
	out := make([]string, 0, len(routes))
	for _, route := range routes {
		if !route.Recipient.IsAgent() {
			continue
		}
		out = append(out, route.Recipient.ID())
	}
	return uniqueStrings(out)
}
