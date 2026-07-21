package manager

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

func managerInternalDeliveriesForTest(t *testing.T, eventBus *runtimebus.EventBus, subscriberID string, eventTypes ...events.EventType) <-chan *worklifetime.EventDelivery {
	t.Helper()
	subscription, err := eventBus.SubscribeInternal(context.Background(), subscriberID, eventTypes...)
	if err != nil {
		t.Fatalf("SubscribeInternal(%s): %v", subscriberID, err)
	}
	subscription.MarkReady()
	t.Cleanup(func() { _ = subscription.Complete(false) })
	return subscription.Deliveries()
}
