package bus

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
)

func (eb *EventBus) authoritativeReplayScopeForEvent(ctx context.Context, eventID string) (runtimereplayclaim.CommittedReplayScope, error) {
	reader, ok := eb.store.(runtimereplayclaim.ScopeReader)
	if !ok || reader == nil {
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	}
	scope, err := reader.LoadCommittedReplayScope(ctx, eventID)
	if err != nil {
		return "", err
	}
	switch scope {
	case runtimereplayclaim.CommittedReplayScopeDirect, runtimereplayclaim.CommittedReplayScopeSubscribed:
		return scope, nil
	default:
		return "", fmt.Errorf("authoritative replay scope: unsupported scope %q", strings.TrimSpace(string(scope)))
	}
}

func (eb *EventBus) currentInternalRecipientsForCommittedEvent(ctx context.Context, evt events.Event) ([]string, error) {
	plan, err := eb.deliveryPlanner.Plan(ctx, evt)
	if err != nil {
		return nil, err
	}
	return filterOutAgentIDs(plan.Recipients, plan.PersistedRecipients), nil
}

func (eb *EventBus) replayRecipientsForCommittedEvent(
	ctx context.Context,
	evt events.Event,
	persisted []string,
	scope runtimereplayclaim.CommittedReplayScope,
) ([]string, []string, []events.DeliveryRoute, error) {
	persisted = uniqueStrings(persisted)
	persistedRoutes := eb.deliveryRoutesForEvent(ctx, evt.ID)
	if scope == runtimereplayclaim.CommittedReplayScopeDirect && len(persistedRoutes) > 0 {
		live := deliveryRouteRecipientIDs(persistedRoutes)
		internal := []string(nil)
		live = deliveryRouteRecipientIDsByType(persistedRoutes, "agent")
		if len(live) > 0 {
			return live, internal, persistedRoutes, nil
		}
	}
	if scope == runtimereplayclaim.CommittedReplayScopeSubscribed && hasDeliveryRouteSubscriberType(persistedRoutes, "node") {
		internal, err := eb.currentInternalRecipientsForCommittedEvent(ctx, evt)
		if err != nil {
			return nil, nil, nil, err
		}
		live := uniqueStrings(append(deliveryRouteRecipientIDsByType(persistedRoutes, "agent"), internal...))
		if len(live) == 0 {
			live = deliveryRouteRecipientIDs(persistedRoutes)
			internal = deliveryRouteRecipientIDsByType(persistedRoutes, "node")
		}
		if len(live) > 0 {
			routes := append([]events.DeliveryRoute(nil), persistedRoutes...)
			for _, recipient := range internal {
				if hasDeliveryRouteRecipient(routes, "node", recipient) {
					continue
				}
				routes = append(routes, events.DeliveryRoute{SubscriberType: "node", SubscriberID: recipient})
			}
			return live, internal, events.NormalizeDeliveryRoutes(routes), nil
		}
	}
	switch scope {
	case runtimereplayclaim.CommittedReplayScopeDirect:
		return persisted, nil, deliveryRoutesFromTargetMap(persisted, "agent", eb.deliveryTargetsForEvent(ctx, evt.ID)), nil
	case runtimereplayclaim.CommittedReplayScopeSubscribed:
		internal, err := eb.currentInternalRecipientsForCommittedEvent(ctx, evt)
		if err != nil {
			return nil, nil, nil, err
		}
		live := uniqueStrings(append(append([]string(nil), persisted...), internal...))
		routes := append([]events.DeliveryRoute(nil), persistedRoutes...)
		if len(routes) == 0 {
			routes = deliveryRoutesFromTargetMap(persisted, "agent", eb.deliveryTargetsForEvent(ctx, evt.ID))
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

func replayScopePersistenceRequired(store any) bool {
	_, hasLister := store.(runtimereplayclaim.Lister)
	_, hasOwner := store.(runtimereplayclaim.Owner)
	return hasLister && hasOwner
}
