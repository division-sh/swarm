package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

func TestWorkflowNodeDeliveryRouteInstallsReplyContext(t *testing.T) {
	route := events.DeliveryRoute{
		SubscriberType: "node",
		SubscriberID:   "provider-node",
		Context: events.DeliveryContext{
			Reply: &events.ReplyContextRef{ID: "reply-v1:node-delivery"},
		},
	}
	ctx := withWorkflowNodeDeliveryRoute(testAuthorActivityContext(t, context.Background()), route)
	if got := events.DeliveryContextFromContext(ctx).ReplyContextID(); got != route.Context.ReplyContextID() {
		t.Fatalf("node delivery reply context = %q, want %q", got, route.Context.ReplyContextID())
	}
}

func TestAppendEmitIntentsAsEventsPreservesIntentDeliveryContext(t *testing.T) {
	want := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:deferred-node-emit"}}
	collector := []events.Event{}
	appendEmitIntentsAsEvents(&collector, []runtimeengine.EmitIntent{{
		Event: eventtest.PersistedProjection(
			"event-1",
			events.EventType("provider.replied"),
			"provider-node",
			"",
			nil,
			0,
			eventtest.UUID("persisted-projection-run"),
			"",
			events.EventEnvelope{},
			time.Now().UTC(),
		),
		Context: want,
	}})
	if len(collector) != 1 {
		t.Fatalf("collected events = %d, want 1", len(collector))
	}
	if got := collector[0].DeliveryContext().ReplyContextID(); got != want.ReplyContextID() {
		t.Fatalf("collected reply context = %q, want %q", got, want.ReplyContextID())
	}
}
