package bus

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

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
