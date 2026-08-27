package bus

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func (eb *EventBus) replayRecipientsForCommittedEvent(
	ctx context.Context,
	evt events.Event,
	persisted []string,
	scope runtimepipelineobligation.CommittedScope,
) ([]string, []string, []events.DeliveryRoute, error) {
	persisted = uniqueStrings(persisted)
	prepared, err := eb.preparedEventForReplay(ctx, evt.ID())
	if err != nil {
		return nil, nil, nil, err
	}
	persistedRoutes := prepared.DeliveryRoutes
	persisted = uniqueStrings(append(persisted, deliveryRouteAgentRecipientIDs(persistedRoutes)...))
	if prepared.Settlement.NoDelivery() {
		if len(persisted) > 0 {
			return nil, nil, nil, fmt.Errorf("replay event %s has no-delivery settlement with persisted recipients", evt.ID())
		}
		return nil, nil, nil, nil
	}
	if !deliveryRoutesCoverAgentRecipients(persistedRoutes, persisted) {
		return nil, nil, nil, fmt.Errorf("replay event %s has persisted agent recipients without exact identity-bearing delivery routes", evt.ID())
	}
	switch scope {
	case runtimepipelineobligation.ScopeDirect:
		if hasNodeDeliveryRoute(persistedRoutes) {
			return nil, nil, nil, fmt.Errorf("direct replay event %s cannot carry node delivery routes", evt.ID())
		}
		return deliveryRouteAgentRecipientIDs(persistedRoutes), nil, persistedRoutes, nil
	case runtimepipelineobligation.ScopeSubscribed:
		internal, err := eb.replayNodeCarriersForCommittedEvent(ctx, evt, persistedRoutes)
		if err != nil {
			return nil, nil, nil, err
		}
		live := uniqueStrings(append(deliveryRouteAgentRecipientIDs(persistedRoutes), internal...))
		return live, internal, persistedRoutes, nil
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
