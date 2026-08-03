package bus

import (
	"context"

	"github.com/division-sh/swarm/internal/events"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
)

func (eb *EventBus) notifyTestEventPersisted(ctx context.Context, evt events.Event) {
	if eb == nil || eb.testLifecycleProbe == nil {
		return
	}
	eb.testLifecycleProbe.NotifyLifecycle(ctx, runtimelifecycleprobe.Signal{
		Kind:      runtimelifecycleprobe.EventPersisted,
		EventID:   evt.ID(),
		EventType: string(evt.Type()),
	})
}

func (eb *EventBus) notifyTestDeliveryRowsPersisted(ctx context.Context, evt events.Event, routePlan RoutePlan) {
	if eb == nil || eb.testLifecycleProbe == nil {
		return
	}
	for _, signal := range lifecycleDeliveryPersistedSignals(evt, routePlan) {
		eb.testLifecycleProbe.NotifyLifecycle(ctx, signal)
		eb.testLifecycleProbe.NotifyLifecycle(ctx, runtimelifecycleprobe.Signal{
			Kind:           runtimelifecycleprobe.DeliveryStatusChanged,
			EventID:        signal.EventID,
			EventType:      signal.EventType,
			SubscriberType: signal.SubscriberType,
			SubscriberID:   signal.SubscriberID,
			Status:         "pending",
		})
	}
}

func (eb *EventBus) notifyTestPublishPersisted(ctx context.Context, evt events.Event, routePlan RoutePlan) {
	eb.notifyTestEventPersisted(ctx, evt)
	eb.notifyTestDeliveryRowsPersisted(ctx, evt, routePlan)
}

func (eb *EventBus) notifyTestPostCommitDispatchStarted(ctx context.Context, evt events.Event) {
	if eb == nil || eb.testLifecycleProbe == nil {
		return
	}
	eb.testLifecycleProbe.NotifyLifecycle(ctx, runtimelifecycleprobe.Signal{
		Kind:      runtimelifecycleprobe.PostCommitDispatchStarted,
		EventID:   evt.ID(),
		EventType: string(evt.Type()),
	})
}

func (eb *EventBus) notifyTestPostCommitDispatchCompleted(ctx context.Context, evt events.Event) {
	if eb == nil || eb.testLifecycleProbe == nil {
		return
	}
	eb.testLifecycleProbe.NotifyLifecycle(ctx, runtimelifecycleprobe.Signal{
		Kind:      runtimelifecycleprobe.PostCommitDispatchCompleted,
		EventID:   evt.ID(),
		EventType: string(evt.Type()),
	})
}

func lifecycleDeliveryPersistedSignals(evt events.Event, routePlan RoutePlan) []runtimelifecycleprobe.Signal {
	routePlan = routePlan.Normalized()
	seen := map[events.DeliveryRecipient]struct{}{}
	out := make([]runtimelifecycleprobe.Signal, 0, len(routePlan.DeliveryRoutes())+len(routePlan.PersistedRecipientIDs()))
	for _, route := range routePlan.DeliveryRoutes() {
		if route.Recipient.Empty() {
			continue
		}
		if _, ok := seen[route.Recipient]; ok {
			continue
		}
		seen[route.Recipient] = struct{}{}
		out = append(out, runtimelifecycleprobe.Signal{
			Kind:           runtimelifecycleprobe.DeliveryPersisted,
			EventID:        evt.ID(),
			EventType:      string(evt.Type()),
			SubscriberType: route.Recipient.Code(),
			SubscriberID:   route.Recipient.ID(),
			Status:         "pending",
		})
	}
	for _, recipient := range routePlan.PersistedRecipientIDs() {
		key := events.MustAgentDeliveryRecipient(recipient)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, runtimelifecycleprobe.Signal{
			Kind:           runtimelifecycleprobe.DeliveryPersisted,
			EventID:        evt.ID(),
			EventType:      string(evt.Type()),
			SubscriberType: "agent",
			SubscriberID:   recipient,
			Status:         "pending",
		})
	}
	return out
}
