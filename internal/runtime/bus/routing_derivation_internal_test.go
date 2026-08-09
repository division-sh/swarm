package bus

import (
	"context"
	"testing"

	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
)

func TestRouteTableResolve_WildcardSubscriberMatchesActiveConcreteChildEventWithoutMaterializedKey(t *testing.T) {
	const pattern = "component-scaffold/*/component.scaffolded"
	const eventType = "component-scaffold/component-a/component.scaffolded"
	rt := newRouteTable(nil)
	rt.eventPath[eventType] = struct{}{}
	rt.patterns = []routePattern{{
		EventPattern: pattern,
		Subscriber:   Subscriber{Recipient: events.MustNodeDeliveryRecipient("operating-accumulator")},
	}}
	rt.rebuildLocked()
	delete(rt.routes, eventType)

	got := rt.Resolve(eventType)
	if len(got) != 1 {
		t.Fatalf("Resolve concrete child event = %#v, want one wildcard subscriber", got)
	}
	if got[0].Recipient.ID() != "operating-accumulator" || !got[0].Recipient.IsNode() {
		t.Fatalf("resolved subscriber = %#v, want operating-accumulator node", got[0])
	}
	if got[0].MatchPattern != pattern {
		t.Fatalf("matched pattern = %q, want %q", got[0].MatchPattern, pattern)
	}
	if got := rt.Resolve("component-scaffold/component-a/component.failed"); len(got) != 0 {
		t.Fatalf("Resolve unrelated event = %#v, want none", got)
	}
	if got := rt.Resolve("component-scaffold/component-b/component.scaffolded"); len(got) != 0 {
		t.Fatalf("Resolve never-added instance event = %#v, want none", got)
	}
	if got := rt.Resolve("other-scaffold/component-a/component.scaffolded"); len(got) != 0 {
		t.Fatalf("Resolve unrelated path = %#v, want none", got)
	}
}

func TestRouteTableResolve_WildcardSubscriberDoesNotMatchRemovedConcreteChildEvent(t *testing.T) {
	const pattern = "component-scaffold/*/component.scaffolded"
	const eventType = "component-scaffold/component-a/component.scaffolded"
	rt := newRouteTable(nil)
	rt.eventPath[eventType] = struct{}{}
	rt.patterns = []routePattern{{
		EventPattern: pattern,
		Subscriber:   Subscriber{Recipient: events.MustNodeDeliveryRecipient("operating-accumulator")},
	}}
	rt.rebuildLocked()
	if got := rt.Resolve(eventType); len(got) != 1 {
		t.Fatalf("Resolve active event = %#v, want one subscriber before removal", got)
	}

	delete(rt.eventPath, eventType)
	rt.rebuildLocked()

	if got := rt.Resolve(eventType); len(got) != 0 {
		t.Fatalf("Resolve removed event = %#v, want none", got)
	}
}

func TestRouteTableResolve_ExactAndWildcardMatchesDeduplicateSameSubscriber(t *testing.T) {
	const (
		exact   = "component-scaffold/component-a/component.scaffolded"
		pattern = "component-scaffold/*/component.scaffolded"
	)
	rt := newRouteTable(nil)
	rt.eventPath[exact] = struct{}{}
	rt.patterns = []routePattern{
		{
			EventPattern: exact,
			Subscriber:   Subscriber{Recipient: events.MustNodeDeliveryRecipient("dual-listener")},
		},
		{
			EventPattern: pattern,
			Subscriber:   Subscriber{Recipient: events.MustNodeDeliveryRecipient("dual-listener")},
		},
		{
			EventPattern: pattern,
			Subscriber:   Subscriber{Recipient: events.MustNodeDeliveryRecipient("wildcard-listener")},
		},
		{
			EventPattern: exact,
			Subscriber:   Subscriber{Recipient: events.MustNodeDeliveryRecipient("exact-listener")},
		},
	}
	rt.rebuildLocked()

	got := rt.Resolve(exact)
	if len(got) != 3 {
		t.Fatalf("Resolve exact child event = %#v, want three distinct subscribers", got)
	}
	ids := subscriberIDsForTest(got)
	for _, want := range []string{"dual-listener", "wildcard-listener", "exact-listener"} {
		if ids[want] != 1 {
			t.Fatalf("subscriber %q count = %d in %#v, want 1", want, ids[want], got)
		}
	}
}

func TestEventBusPublish_UsesRouteTableWildcardSubscriberResolution(t *testing.T) {
	const pattern = "component-scaffold/*/component.scaffolded"
	const eventType = "component-scaffold/component-a/component.scaffolded"
	rt := newRouteTable(nil)
	rt.eventPath[eventType] = struct{}{}
	rt.patterns = []routePattern{{
		EventPattern: pattern,
		Subscriber:   Subscriber{Recipient: events.MustAgentDeliveryRecipient("operating-observer"), routeSource: subscriberRouteSourceSubscription},
	}}
	rt.rebuildLocked()
	delete(rt.routes, eventType)
	eb, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{RouteTable: rt})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := subscribeTestAgent(t, eb, "operating-observer")
	defer unsubscribeTestAgent(eb, "operating-observer")
	recorder := NewEmittedEventsRecorder()
	ctx := WithEmittedEventsRecorder(context.Background(), recorder)

	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress("", eventType, "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	evt := requireBusEvent(t, ch, "routed wildcard delivery")
	if evt.Type() != events.EventType(eventType) {
		t.Fatalf("delivered event type = %q, want concrete child event", evt.Type())
	}

	diags := recorder.SnapshotPublishes()
	if len(diags) != 1 || len(diags[0].RoutedRecipients) != 1 {
		t.Fatalf("publish diagnostics = %#v, want one routed recipient", diags)
	}
	if got := diags[0].RoutedRecipients[0].ID; got != "operating-observer" {
		t.Fatalf("routed recipient = %q, want operating-observer", got)
	}
	if got := diags[0].RoutedRecipients[0].MatchedPattern; got != pattern {
		t.Fatalf("matched pattern = %q, want %q", got, pattern)
	}
}

func subscriberIDsForTest(in []Subscriber) map[string]int {
	out := make(map[string]int, len(in))
	for _, subscriber := range in {
		out[subscriber.Recipient.ID()]++
	}
	return out
}
