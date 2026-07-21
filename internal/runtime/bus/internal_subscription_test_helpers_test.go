package bus

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

func subscribeInternalForTest(t *testing.T, eventBus *EventBus, subscriberID string, eventTypes ...events.EventType) worklifetime.InternalSubscription {
	t.Helper()
	subscription, err := eventBus.SubscribeInternal(context.Background(), subscriberID, eventTypes...)
	if err != nil {
		t.Fatalf("SubscribeInternal(%s): %v", subscriberID, err)
	}
	subscription.MarkReady()
	t.Cleanup(func() { _ = subscription.Complete(false) })
	return subscription
}

func subscribeInternalDeliveriesForTest(t *testing.T, eventBus *EventBus, subscriberID string, eventTypes ...events.EventType) <-chan *LocalDelivery {
	t.Helper()
	return subscribeInternalForTest(t, eventBus, subscriberID, eventTypes...).Deliveries()
}
