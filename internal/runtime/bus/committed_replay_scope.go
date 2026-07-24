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
	persisted = uniqueStrings(append(persisted, deliveryRouteRecipientIDsByType(persistedRoutes, "agent")...))
	if scope == runtimepipelineobligation.ScopeDirect && len(persistedRoutes) > 0 {
		internal := []string(nil)
		live := deliveryRouteRecipientIDsByType(persistedRoutes, "agent")
		if len(live) > 0 {
			return live, internal, persistedRoutes, nil
		}
	}
	if scope == runtimepipelineobligation.ScopeSubscribed && hasDeliveryRouteSubscriberType(persistedRoutes, "node") {
		if hasFlowInstanceNodeDeliveryRoute(persistedRoutes) {
			internal := deliveryRouteRecipientIDsByType(persistedRoutes, "node")
			live := uniqueStrings(append(deliveryRouteRecipientIDsByType(persistedRoutes, "agent"), internal...))
			return live, internal, persistedRoutes, nil
		}
		internal, err := eb.currentInternalRecipientsForCommittedEvent(ctx, evt)
		if err != nil {
			return nil, nil, nil, err
		}
		live := uniqueStrings(append(deliveryRouteRecipientIDsByType(persistedRoutes, "agent"), internal...))
		routes := append([]events.DeliveryRoute(nil), persistedRoutes...)
		for _, recipient := range internal {
			if hasDeliveryRouteRecipient(routes, "node", recipient) {
				continue
			}
			routes = append(routes, events.DeliveryRoute{SubscriberType: "node", SubscriberID: recipient})
		}
		return live, internal, events.NormalizeDeliveryRoutes(routes), nil
	}
	switch scope {
	case runtimepipelineobligation.ScopeDirect:
		return persisted, nil, deliveryRoutesFromTargetMap(persisted, "agent", eb.deliveryTargetsForEvent(ctx, evt.ID())), nil
	case runtimepipelineobligation.ScopeSubscribed:
		internal, err := eb.currentInternalRecipientsForCommittedEvent(ctx, evt)
		if err != nil {
			return nil, nil, nil, err
		}
		live := uniqueStrings(append(append([]string(nil), persisted...), internal...))
		routes := append([]events.DeliveryRoute(nil), persistedRoutes...)
		if len(routes) == 0 {
			routes = deliveryRoutesFromTargetMap(persisted, "agent", eb.deliveryTargetsForEvent(ctx, evt.ID()))
		}
		for _, recipient := range internal {
			if hasDeliveryRouteRecipient(routes, "node", recipient) {
				continue
			}
			routes = append(routes, events.DeliveryRoute{SubscriberType: "node", SubscriberID: recipient})
		}
		return live, internal, events.NormalizeDeliveryRoutes(routes), nil
	default:
		return nil, nil, nil, fmt.Errorf("replay recipients: unsupported scope %q", strings.TrimSpace(string(scope)))
	}
}

func hasDeliveryRouteSubscriberType(routes []events.DeliveryRoute, subscriberType string) bool {
	subscriberType = strings.TrimSpace(subscriberType)
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.SubscriberType == subscriberType {
			return true
		}
	}
	return false
}

func hasFlowInstanceNodeDeliveryRoute(routes []events.DeliveryRoute) bool {
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.SubscriberType != "node" {
			continue
		}
		target := route.Target.Normalized()
		if target.FlowID != "" && target.FlowInstance != "" {
			return true
		}
	}
	return false
}

func hasDeliveryRouteRecipient(routes []events.DeliveryRoute, subscriberType, subscriberID string) bool {
	subscriberType = strings.TrimSpace(subscriberType)
	subscriberID = strings.TrimSpace(subscriberID)
	if subscriberType == "" || subscriberID == "" {
		return false
	}
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if route.SubscriberType == subscriberType && route.SubscriberID == subscriberID {
			return true
		}
	}
	return false
}

func deliveryRouteRecipientIDs(routes []events.DeliveryRoute) []string {
	routes = events.NormalizeDeliveryRoutes(routes)
	out := make([]string, 0, len(routes))
	for _, route := range routes {
		if route.SubscriberID != "" {
			out = append(out, route.SubscriberID)
		}
	}
	return uniqueStrings(out)
}

func deliveryRouteRecipientIDsByType(routes []events.DeliveryRoute, subscriberType string) []string {
	routes = events.NormalizeDeliveryRoutes(routes)
	subscriberType = strings.TrimSpace(subscriberType)
	out := make([]string, 0, len(routes))
	for _, route := range routes {
		if route.SubscriberType != subscriberType || route.SubscriberID == "" {
			continue
		}
		out = append(out, route.SubscriberID)
	}
	return uniqueStrings(out)
}
