package bus

import (
	"context"
	"testing"

	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestRouteTableResolve_WildcardSubscriberMatchesActiveConcreteChildEventWithoutMaterializedKey(t *testing.T) {
	const pattern = "component-scaffold/*/component.scaffolded"
	const eventType = "component-scaffold/component-a/component.scaffolded"
	rt := newRouteTable(nil)
	rt.eventPath[eventType] = struct{}{}
	rt.patterns = []routePattern{{
		EventPattern: pattern,
		Subscriber:   Subscriber{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "operating-accumulator"))},
	}}
	rt.rebuildLocked()
	delete(rt.routes, routeResolutionKey{eventType: eventType})

	got := rt.ResolveForRun(busInternalTestRunID, eventType)
	if len(got) != 1 {
		t.Fatalf("Resolve concrete child event = %#v, want one wildcard subscriber", got)
	}
	if got[0].Recipient.LocalID() != "operating-accumulator" || !got[0].Recipient.IsNode() {
		t.Fatalf("resolved subscriber = %#v, want operating-accumulator node", got[0])
	}
	if got[0].MatchPattern != pattern {
		t.Fatalf("matched pattern = %q, want %q", got[0].MatchPattern, pattern)
	}
	if got := rt.ResolveForRun(busInternalTestRunID, "component-scaffold/component-a/component.failed"); len(got) != 0 {
		t.Fatalf("Resolve unrelated event = %#v, want none", got)
	}
	if got := rt.ResolveForRun(busInternalTestRunID, "component-scaffold/component-b/component.scaffolded"); len(got) != 0 {
		t.Fatalf("Resolve never-added instance event = %#v, want none", got)
	}
	if got := rt.ResolveForRun(busInternalTestRunID, "other-scaffold/component-a/component.scaffolded"); len(got) != 0 {
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
		Subscriber:   Subscriber{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "operating-accumulator"))},
	}}
	rt.rebuildLocked()
	if got := rt.ResolveForRun(busInternalTestRunID, eventType); len(got) != 1 {
		t.Fatalf("Resolve active event = %#v, want one subscriber before removal", got)
	}

	delete(rt.eventPath, eventType)
	rt.rebuildLocked()

	if got := rt.ResolveForRun(busInternalTestRunID, eventType); len(got) != 0 {
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
			Subscriber:   Subscriber{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "dual-listener"))},
		},
		{
			EventPattern: pattern,
			Subscriber:   Subscriber{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "dual-listener"))},
		},
		{
			EventPattern: pattern,
			Subscriber:   Subscriber{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "wildcard-listener"))},
		},
		{
			EventPattern: exact,
			Subscriber:   Subscriber{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "exact-listener"))},
		},
	}
	rt.rebuildLocked()

	got := rt.ResolveForRun(busInternalTestRunID, exact)
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

func TestRouteTableResolveForRunRejectsSiblingExactAndWildcardOwnersAcrossReplacement(t *testing.T) {
	const (
		runA      = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		runB      = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
		eventType = "worker/one/work.ready"
		wildcard  = "worker/*/work.ready"
	)
	rt := newRouteTable(nil)
	rt.eventPath[eventType] = struct{}{}
	global := Subscriber{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "global-listener"))}
	exact := Subscriber{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "exact-listener"))}
	wild := Subscriber{Recipient: events.MustNodeDeliveryRecipient(testRootNode(t, "wildcard-listener"))}
	rt.patterns = []routePattern{
		{EventPattern: eventType, Subscriber: global},
		{RunID: runA, EventPattern: eventType, Subscriber: exact},
		{RunID: runA, EventPattern: wildcard, Subscriber: wild},
	}
	rt.rebuildLocked()

	if got := subscriberIDsForTest(rt.ResolveForRun(runA, eventType)); len(got) != 3 || got["global-listener"] != 1 || got["exact-listener"] != 1 || got["wildcard-listener"] != 1 {
		t.Fatalf("run A routes = %#v, want global plus exact A owners", got)
	}
	if got := subscriberIDsForTest(rt.ResolveForRun(runB, eventType)); len(got) != 1 || got["global-listener"] != 1 {
		t.Fatalf("run B routes = %#v, want only authored global owner", got)
	}

	rt.patterns = []routePattern{
		{EventPattern: eventType, Subscriber: global},
		{RunID: runB, EventPattern: eventType, Subscriber: exact},
		{RunID: runB, EventPattern: wildcard, Subscriber: wild},
	}
	rt.rebuildLocked()
	if got := subscriberIDsForTest(rt.ResolveForRun(runA, eventType)); len(got) != 1 || got["global-listener"] != 1 {
		t.Fatalf("retired run A routes = %#v, want only authored global owner", got)
	}
	if got := subscriberIDsForTest(rt.ResolveForRun(runB, eventType)); len(got) != 3 {
		t.Fatalf("replacement run B routes = %#v, want three", got)
	}

	rt.patterns = []routePattern{{EventPattern: eventType, Subscriber: global}}
	rt.rebuildLocked()
	if got := subscriberIDsForTest(rt.ResolveForRun(runB, eventType)); len(got) != 1 || got["global-listener"] != 1 {
		t.Fatalf("removed run B routes = %#v, want only authored global owner", got)
	}
}

func TestRouteTableMixedRolesPreserveFullSubscriberIdentity(t *testing.T) {
	sharedNode := testFlowNode(t, "receiver", "shared-node")
	base := Subscriber{
		Recipient:      events.MustNodeDeliveryRecipient(sharedNode),
		Path:           "receiver/instance-a",
		MatchPattern:   "receiver/instance-a/work.ready",
		routeSource:    subscriberRouteSourceSubscription,
		LocalizedEvent: "work.ready",
		handlerNode:    sharedNode,
		targetHandler:  runtimepipeline.MustDeliveryTargetHandler(sharedNode),
	}
	variants := []Subscriber{base}

	differentSource := base
	differentSource.routeSource = subscriberRouteSourceConnectRoutePlan
	variants = append(variants, differentSource)

	differentEvent := base
	differentEvent.LocalizedEvent = "work.audited"
	variants = append(variants, differentEvent)

	differentHandler := base
	differentHandler.handlerNode = testFlowNode(t, "receiver", "other-node")
	differentHandler.targetHandler = runtimepipeline.MustDeliveryTargetHandler(differentHandler.handlerNode)
	variants = append(variants, differentHandler)

	differentConnectHandler := base
	differentConnectHandler.connectHandler = runtimepinrouting.MustConnectReceiverHandler(sharedNode)
	variants = append(variants, differentConnectHandler)

	var got []Subscriber
	for _, subscriber := range variants {
		got = appendUniqueSubscriber(got, subscriber)
	}
	if len(got) != len(variants) {
		t.Fatalf("appendUniqueSubscriber retained %d roles, want %d: %#v", len(got), len(variants), got)
	}
	if deduped := dedupeSubscribers(variants); len(deduped) != len(variants) {
		t.Fatalf("dedupeSubscribers retained %d roles, want %d: %#v", len(deduped), len(variants), deduped)
	}
}

func TestRouteTableMixedRolesExactWildcardConstruction(t *testing.T) {
	const (
		exact    = "receiver/instance-a/work.ready"
		wildcard = "receiver/*/work.ready"
	)
	sharedNode := testFlowNode(t, "receiver", "shared-node")
	base := Subscriber{
		Recipient:      events.MustNodeDeliveryRecipient(sharedNode),
		Path:           "receiver/instance-a",
		routeSource:    subscriberRouteSourceSubscription,
		LocalizedEvent: "work.ready",
		handlerNode:    sharedNode,
		targetHandler:  runtimepipeline.MustDeliveryTargetHandler(sharedNode),
	}
	for _, tc := range []struct {
		name  string
		first string
		last  string
	}{
		{name: "exact then wildcard", first: exact, last: wildcard},
		{name: "wildcard then exact", first: wildcard, last: exact},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := base
			first.MatchPattern = tc.first
			last := base
			last.MatchPattern = tc.last
			got := appendUniqueSubscriber(appendUniqueSubscriber(nil, first), last)
			if len(got) != 1 {
				t.Fatalf("roles = %#v, want one behavioral role", got)
			}
			if got[0].MatchPattern != exact {
				t.Fatalf("retained match evidence = %q, want strongest exact %q", got[0].MatchPattern, exact)
			}

			rt := newRouteTable(nil)
			rt.eventPath[exact] = struct{}{}
			rt.patterns = []routePattern{
				{EventPattern: tc.first, Subscriber: base},
				{EventPattern: tc.last, Subscriber: base},
			}
			rt.rebuildLocked()
			resolved := rt.ResolveForRun(busInternalTestRunID, exact)
			if len(resolved) != 1 || resolved[0].MatchPattern != exact {
				t.Fatalf("Resolve(%s) = %#v, want one role with exact evidence", exact, resolved)
			}
		})
	}
}

func TestPubsubProjectionPreservesDistinctHandlerRoles(t *testing.T) {
	firstNode := testFlowNode(t, "observer", "first-handler")
	secondNode := testFlowNode(t, "observer", "second-handler")
	roles := []Subscriber{
		{Recipient: events.MustNodeDeliveryRecipient(firstNode), Path: "observer", routeSource: subscriberRouteSourceSubscription,
			LocalizedEvent: "work.ready", handlerNode: firstNode, targetHandler: runtimepipeline.MustDeliveryTargetHandler(firstNode)},
		{Recipient: events.MustNodeDeliveryRecipient(secondNode), Path: "observer", routeSource: subscriberRouteSourceSubscription,
			LocalizedEvent: "work.ready", handlerNode: secondNode, targetHandler: runtimepipeline.MustDeliveryTargetHandler(secondNode)},
	}
	for _, reverse := range []bool{false, true} {
		ordered := append([]Subscriber(nil), roles...)
		if reverse {
			ordered[0], ordered[1] = ordered[1], ordered[0]
		}
		got := dedupeSubscribers(append(ordered, ordered...))
		if len(got) != 2 {
			t.Fatalf("distinct handler roles collapsed: %+v", got)
		}
		for _, role := range roles {
			got = appendUniqueSubscriber(got, role)
		}
		if len(got) != 2 {
			t.Fatalf("equal roles were not idempotent: %+v", got)
		}
		if rebuilt := dedupeSubscribers(append(got[:1:1], roles...)); len(rebuilt) != 2 {
			t.Fatalf("repeated projection lost a handler role: %+v", rebuilt)
		}
	}
}

func TestPubsubProjectionPreservesDistinctAgentIdentities(t *testing.T) {
	name, err := agentidentity.DeclaredName("shared-agent", "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	var roles []Subscriber
	for _, id := range []string{"worker-a", "worker-b"} {
		route, err := agentidentity.PresentRoute("workers", id, "workers/"+id)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := agentidentity.NewPlan(name, route)
		if err != nil {
			t.Fatal(err)
		}
		roles = append(roles, Subscriber{Recipient: events.MustAgentDeliveryRecipient("shared-agent"), Path: "workers",
			routeSource: subscriberRouteSourceSubscription, LocalizedEvent: "work.ready", AgentPlan: plan})
	}
	if got := dedupeSubscribers(append(roles, roles...)); len(got) != 2 {
		t.Fatalf("distinct concrete agent identities collapsed: %+v", got)
	}
}

func TestEventBusPublish_UsesRouteTableWildcardSubscriberResolution(t *testing.T) {
	const pattern = "component-scaffold/*/component.scaffolded"
	const eventType = "component-scaffold/component-a/component.scaffolded"
	rt := newRouteTable(nil)
	rt.eventPath[eventType] = struct{}{}
	plan, err := testAgentRouteIdentity(t, "operating-observer", "").Plan()
	if err != nil {
		t.Fatalf("construct wildcard subscriber plan: %v", err)
	}
	rt.patterns = []routePattern{{
		EventPattern: pattern,
		RunID:        busInternalTestRunID,
		Subscriber:   Subscriber{Recipient: events.MustAgentDeliveryRecipient("operating-observer"), AgentPlan: plan, routeSource: subscriberRouteSourceSubscription},
	}}
	rt.rebuildLocked()
	delete(rt.routes, routeResolutionKey{runID: busInternalTestRunID, eventType: eventType})
	eb, err := newScopedTestEventBus(InMemoryEventStore{}, EventBusOptions{RouteTable: rt})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	ch := subscribeTestAgent(t, eb, "operating-observer")
	defer unsubscribeTestAgent(eb, "operating-observer")
	recorder := NewEmittedEventsRecorder()
	ctx := WithEmittedEventsRecorder(context.Background(), recorder)

	if err := eb.Publish(ctx, eventtest.RunCreatingRootIngress("", eventType, "", "", nil, 0, busInternalTestRunID, "", events.EventEnvelope{}, time.Time{})); err != nil {
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
		out[subscriber.Recipient.LocalID()]++
	}
	return out
}
