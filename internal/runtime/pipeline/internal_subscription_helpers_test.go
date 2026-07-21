package pipeline

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

type internalSubscriptionSource interface {
	SubscribeInternal(context.Context, string, ...events.EventType) (worklifetime.InternalSubscription, error)
}

func internalSubscriptionDeliveriesForTest(t *testing.T, source internalSubscriptionSource, subscriberID string, eventTypes ...events.EventType) <-chan *worklifetime.EventDelivery {
	t.Helper()
	subscription, err := source.SubscribeInternal(context.Background(), subscriberID, eventTypes...)
	if err != nil {
		t.Fatalf("SubscribeInternal(%s): %v", subscriberID, err)
	}
	subscription.MarkReady()
	t.Cleanup(func() { _ = subscription.Complete(false) })
	return subscription.Deliveries()
}
