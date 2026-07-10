package pipeline

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
)

func TestWorkflowNodeDeliveryRouteInstallsReplyContext(t *testing.T) {
	route := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "provider-node",
		Context: events.DeliveryContext{
			Reply: &events.ReplyContextRef{ID: "reply-v1:node-delivery"},
		},
	}
	ctx := withWorkflowNodeDeliveryRoute(context.Background(), route)
	if got := events.DeliveryContextFromContext(ctx).ReplyContextID(); got != route.Context.ReplyContextID() {
		t.Fatalf("node delivery reply context = %q, want %q", got, route.Context.ReplyContextID())
	}
}
