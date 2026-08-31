package bus

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestMixedPubsubConnectCompositionSameCoordinateDifferentRoles(t *testing.T) {
	consumerNode := testFlowNode(t, "consumer", "shared-node")
	producerNode := testFlowNode(t, "producer", "shared-node")
	consumerRecipient := events.MustNodeDeliveryRecipient(consumerNode)
	producerRecipient := events.MustNodeDeliveryRecipient(producerNode)
	producerTarget := events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: "producer", FlowInstance: "producer", EntityID: "00000000-0000-4000-8000-000000000001",
	})
	consumerTarget := events.MustExistingEntityTarget(events.RouteIdentity{
		FlowID: "consumer", FlowInstance: "consumer", EntityID: "00000000-0000-4000-8000-000000000002",
	})
	connectPlan := newRoutePlan(events.Event{})
	connectPlan.MarkCanonicalRouteMatched(routeIntentProducerConnectRoutePlan)
	connectPlan.AddDeliveryIntents(RoutePlanDeliveryIntent{
		Recipient: consumerRecipient, TargetOwnership: consumerTarget,
		Handler:  runtimepipeline.MustDeliveryTargetHandler(consumerNode).ForEvent("work.accepted"),
		Producer: routeIntentProducerConnectRoutePlan, Persist: true,
	})
	connectPlan.RoutedRecipients = []Subscriber{{
		Recipient: consumerRecipient, Path: "consumer", routeSource: subscriberRouteSourceConnectRoutePlan,
		handlerNode:   consumerNode,
		targetHandler: runtimepipeline.MustDeliveryTargetHandler(consumerNode),
	}}
	localPlan := newRoutePlan(events.Event{})
	localPlan.AddDeliveryIntents(RoutePlanDeliveryIntent{
		Recipient: producerRecipient, TargetOwnership: producerTarget,
		Handler:  runtimepipeline.MustDeliveryTargetHandler(producerNode).ForEvent("work.ready"),
		Producer: routeIntentProducerConcreteNodeRoute, Persist: true,
	})
	localPlan.RoutedRecipients = []Subscriber{{
		Recipient: producerRecipient, Path: "producer", routeSource: subscriberRouteSourceSubscription,
		handlerNode:   producerNode,
		targetHandler: runtimepipeline.MustDeliveryTargetHandler(producerNode),
	}}
	plan := composeIndependentPubsubBranch(connectPlan, localPlan)
	routes := plan.DeliveryRoutes()
	if len(routes) != 2 {
		t.Fatalf("delivery routes = %#v, want two same-ID roles; routed=%#v intents=%#v", routes, plan.RoutedRecipients, plan.DeliveryIntents)
	}
	seenTargets := map[string]bool{}
	for _, route := range routes {
		if route.Recipient.LocalID() != "shared-node" {
			t.Fatalf("route recipient = %#v, want shared-node", route.Recipient)
		}
		seenTargets[route.Target.Route().FlowInstance] = true
	}
	if !seenTargets["producer"] || !seenTargets["consumer"] {
		t.Fatalf("same-ID route targets = %#v, want producer and consumer", routes)
	}
}

func TestMixedPubsubConnectCompositionMultiBoundary(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		name := "forward connect order"
		if reverse {
			name = "reverse connect order"
		}
		t.Run(name, func(t *testing.T) {
			source := mixedFanoutToFanoutSource(reverse)
			owners := mixedStaticOwners("producer", "left", "right", "left-sink", "right-sink")
			store := newTargetRouteMemoryStore()
			store.setTargetOwners(owners...)
			interceptor := &connectRoutePlanNodeInterceptor{}
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle: source, Interceptors: []EventInterceptor{interceptor},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			ownerByFlow := mixedOwnerMap(owners)
			root := mixedStaticSourceEvent("producer", "branch.ready", ownerByFlow["producer"], "root-fanout")
			if err := eb.Publish(context.Background(), root); err != nil {
				t.Fatalf("publish first fan-out: %v", err)
			}
			assertMixedRouteSet(t, store.routes[root.ID()], map[string]string{
				"producer-local": "producer",
				"left-receiver":  "left",
				"right-receiver": "right",
			})
			assertMixedEventProjection(t, store.events[root.ID()], "left", "producer", "right")

			left := mixedStaticSourceEvent("left", "branch.done", ownerByFlow["left"], "left-second-fanout")
			right := mixedStaticSourceEvent("right", "branch.done", ownerByFlow["right"], "right-second-fanout")
			for _, event := range []events.Event{left, right} {
				if err := eb.Publish(context.Background(), event); err != nil {
					t.Fatalf("publish second fan-out %s: %v", event.Type(), err)
				}
			}
			assertMixedRouteSet(t, store.routes[left.ID()], map[string]string{
				"left-local": "left", "left-sink-node": "left-sink",
			})
			assertMixedRouteSet(t, store.routes[right.ID()], map[string]string{
				"right-local": "right", "right-sink-node": "right-sink",
			})
			assertMixedEventProjection(t, store.events[left.ID()], "left", "left-sink")
			assertMixedEventProjection(t, store.events[right.ID()], "right", "right-sink")
			if interceptor.Count() != 7 {
				t.Fatalf("handler executions = %d, want 7 exact mixed routes", interceptor.Count())
			}
		})
	}
}

func TestMixedPubsubConnectCompositionConcurrentSources(t *testing.T) {
	source := mixedFanoutToFanoutSource(false)
	owners := mixedStaticOwners("producer", "left", "right", "left-sink", "right-sink")
	ownerByFlow := mixedOwnerMap(owners)
	store := newTargetRouteMemoryStore()
	store.setTargetOwners(owners...)
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	eventsToPublish := []events.Event{
		mixedStaticSourceEvent("left", "branch.done", ownerByFlow["left"], "concurrent-left"),
		mixedStaticSourceEvent("right", "branch.done", ownerByFlow["right"], "concurrent-right"),
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(eventsToPublish))
	for _, event := range eventsToPublish {
		event := event
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- eb.Publish(context.Background(), event)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent publish: %v", err)
		}
	}
	assertMixedRouteSet(t, store.routes[eventsToPublish[0].ID()], map[string]string{
		"left-local": "left", "left-sink-node": "left-sink",
	})
	assertMixedRouteSet(t, store.routes[eventsToPublish[1].ID()], map[string]string{
		"right-local": "right", "right-sink-node": "right-sink",
	})
}

func TestMixedPubsubConnectCompositionNodeAgentConnect(t *testing.T) {
	const eventName = "deploy.done"
	source := semanticview.Wrap(connectRoutePlanTestBundle([]connectRoutePlanTestFlow{
		{
			id: "producer", mode: runtimecontracts.FlowModeStatic,
			outputs: []runtimecontracts.FlowOutputEventPin{{Event: eventName}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"producer-local": {
					ID: "producer-local", SubscribesTo: []string{eventName},
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{eventName: existingOwnerHandlerFixture()},
				},
			},
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"producer-agent": {ID: "producer-agent", Subscriptions: []string{eventName}},
			},
		},
		{
			id: "consumer", mode: runtimecontracts.FlowModeStatic,
			inputs: []runtimecontracts.FlowInputEventPin{{Event: "deploy.accepted"}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"consumer-node": {
					ID:            "consumer-node",
					EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"deploy.accepted": existingOwnerHandlerFixture()},
				},
			},
		},
	}, []runtimecontracts.FlowPackageConnect{{Event: eventName, From: "producer", To: "consumer", Rename: "deploy.accepted"}}))
	owners := mixedStaticOwners("producer", "consumer")
	store := newTargetRouteMemoryStore()
	store.setTargetOwners(owners...)
	interceptor := &connectRoutePlanNodeInterceptor{}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, Interceptors: []EventInterceptor{interceptor}})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	agentIdentity := connectRoutePlanTestDeclaredAgentIdentity(t, source, "producer", "producer-agent", "producer")
	agentAdmission := testAgentSubscriptionAdmissionForFlow(t, "producer-agent", "producer", events.EventType("producer/"+eventName))
	_ = subscribeTestAgentAdmissionWithIdentity(t, eb, agentAdmission, agentIdentity, owners[0].EntityID)
	defer unsubscribeTestAgent(eb, "producer-agent")

	event := connectRoutePlanStaticProducerEvent(
		uuid.NewString(), events.EventType("producer/"+eventName), "", "", []byte(`{"seed":"node-agent-connect"}`), 0, "", "",
		events.EnvelopeForEntityID(events.EventEnvelope{Source: events.RouteIdentity{
			FlowID: "producer", FlowInstance: "producer", EntityID: owners[0].EntityID,
		}}, owners[0].EntityID),
		time.Now().UTC(),
	)
	plan, err := eb.CheckPublishRecipientPlan(context.Background(), event)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if plan.TargetFailure != "" || len(plan.DeliveryRoutes) != 3 {
		t.Fatalf("mixed node/agent/connect plan = failure:%q routes:%#v", plan.TargetFailure, plan.DeliveryRoutes)
	}
	assertMixedNodeAgentConnectRoutes(t, plan.DeliveryRoutes, agentIdentity)
	internalPlan, err := eb.planSubscribedRoutePlan(context.Background(), event, false)
	if err != nil {
		t.Fatalf("planSubscribedRoutePlan: %v", err)
	}
	if !mixedLiveRecipientsContainAgent(internalPlan.LiveRecipients, agentIdentity) {
		t.Fatalf("mixed live recipients = %#v, want exact local agent", internalPlan.LiveRecipients)
	}
	if err := eb.Publish(context.Background(), event); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	assertMixedNodeAgentConnectRoutes(t, store.routes[event.ID()], agentIdentity)
	assertMixedEventProjection(t, store.events[event.ID()], "consumer", "producer")
	if interceptor.Count() != 2 {
		t.Fatalf("node executions = %d, want local and connect nodes", interceptor.Count())
	}
}

func mixedLiveRecipientsContainAgent(recipients []RoutePlanLiveRecipient, want agentidentity.Identity) bool {
	for _, recipient := range recipients {
		if recipient.Recipient.IsAgent() && recipient.AgentIdentity == want {
			return true
		}
	}
	return false
}

func assertMixedNodeAgentConnectRoutes(t testing.TB, routes []events.DeliveryRoute, agentIdentity agentidentity.Identity) {
	t.Helper()
	if len(routes) != 3 {
		t.Fatalf("mixed node/agent/connect routes = %#v, want three", routes)
	}
	seen := map[string]bool{}
	for _, route := range routes {
		seen[route.Recipient.Code()+":"+route.Recipient.LocalID()] = true
		switch route.Recipient.LocalID() {
		case "producer-local":
			if route.Target.Route().FlowInstance != "producer" || !route.ConnectClaim.Empty() {
				t.Fatalf("local node route = %#v", route)
			}
		case "producer-agent":
			if route.AgentIdentity != agentIdentity || !route.ConnectClaim.Empty() {
				t.Fatalf("local agent route = %#v", route)
			}
		case "consumer-node":
			if route.Target.Route().FlowInstance != "consumer" || route.ConnectClaim.Empty() {
				t.Fatalf("connect node route = %#v", route)
			}
		default:
			t.Fatalf("unexpected mixed route %#v", route)
		}
	}
	for _, key := range []string{"node:producer-local", "agent:producer-agent", "node:consumer-node"} {
		if !seen[key] {
			t.Fatalf("mixed node/agent/connect routes = %#v, missing %s", routes, key)
		}
	}
}

func mixedFanoutToFanoutSource(reverse bool) semanticview.Source {
	localNode := func(id, event string) map[string]runtimecontracts.SystemNodeContract {
		return map[string]runtimecontracts.SystemNodeContract{
			id: {
				ID: id, SubscribesTo: []string{event},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{event: existingOwnerHandlerFixture()},
			},
		}
	}
	receiver := func(id, event string) map[string]runtimecontracts.SystemNodeContract {
		return map[string]runtimecontracts.SystemNodeContract{
			id: {ID: id, EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{event: existingOwnerHandlerFixture()}},
		}
	}
	flows := []connectRoutePlanTestFlow{
		{
			id: "producer", mode: runtimecontracts.FlowModeStatic,
			outputs: []runtimecontracts.FlowOutputEventPin{{Event: "branch.ready"}},
			nodes:   localNode("producer-local", "branch.ready"),
		},
		{
			id: "left", mode: runtimecontracts.FlowModeStatic,
			inputs:  []runtimecontracts.FlowInputEventPin{{Event: "branch.accepted"}},
			outputs: []runtimecontracts.FlowOutputEventPin{{Event: "branch.done"}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"left-receiver": receiver("left-receiver", "branch.accepted")["left-receiver"],
				"left-local":    localNode("left-local", "branch.done")["left-local"],
			},
		},
		{
			id: "right", mode: runtimecontracts.FlowModeStatic,
			inputs:  []runtimecontracts.FlowInputEventPin{{Event: "branch.accepted"}},
			outputs: []runtimecontracts.FlowOutputEventPin{{Event: "branch.done"}},
			nodes: map[string]runtimecontracts.SystemNodeContract{
				"right-receiver": receiver("right-receiver", "branch.accepted")["right-receiver"],
				"right-local":    localNode("right-local", "branch.done")["right-local"],
			},
		},
		{
			id: "left-sink", mode: runtimecontracts.FlowModeStatic,
			inputs: []runtimecontracts.FlowInputEventPin{{Event: "branch.final"}},
			nodes:  receiver("left-sink-node", "branch.final"),
		},
		{
			id: "right-sink", mode: runtimecontracts.FlowModeStatic,
			inputs: []runtimecontracts.FlowInputEventPin{{Event: "branch.final"}},
			nodes:  receiver("right-sink-node", "branch.final"),
		},
	}
	connects := []runtimecontracts.FlowPackageConnect{
		{Event: "branch.ready", From: "producer", To: "left", Rename: "branch.accepted"},
		{Event: "branch.ready", From: "producer", To: "right", Rename: "branch.accepted"},
		{Event: "branch.done", From: "left", To: "left-sink", Rename: "branch.final"},
		{Event: "branch.done", From: "right", To: "right-sink", Rename: "branch.final"},
	}
	if reverse {
		for left, right := 0, len(connects)-1; left < right; left, right = left+1, right-1 {
			connects[left], connects[right] = connects[right], connects[left]
		}
	}
	return semanticview.Wrap(connectRoutePlanTestBundle(flows, connects))
}

func mixedStaticOwners(flowIDs ...string) []ActiveTargetDescriptor {
	out := make([]ActiveTargetDescriptor, 0, len(flowIDs))
	for _, flowID := range flowIDs {
		out = append(out, testSelectedRunTargetOwner(flowID+"-owner", flowID, flowID+"-entity"))
	}
	return out
}

func mixedOwnerMap(owners []ActiveTargetDescriptor) map[string]ActiveTargetDescriptor {
	out := make(map[string]ActiveTargetDescriptor, len(owners))
	for _, owner := range owners {
		out[owner.FlowInstance] = owner
	}
	return out
}

func mixedStaticSourceEvent(flowID, localEvent string, owner ActiveTargetDescriptor, seed string) events.Event {
	return connectRoutePlanStaticProducerEvent(
		uuid.NewString(), events.EventType(flowID+"/"+localEvent), "", "", []byte(fmt.Sprintf(`{"seed":%q}`, seed)), 0, "", "",
		events.EventEnvelope{Source: events.RouteIdentity{
			FlowID: flowID, FlowInstance: flowID, EntityID: owner.EntityID,
		}},
		time.Now().UTC(),
	)
}

func assertMixedRouteSet(t testing.TB, routes []events.DeliveryRoute, want map[string]string) {
	t.Helper()
	if len(routes) != len(want) {
		t.Fatalf("delivery routes = %#v, want %d exact routes", routes, len(want))
	}
	for _, route := range routes {
		flowInstance, ok := want[route.Recipient.LocalID()]
		if !ok {
			t.Fatalf("unexpected delivery route %#v", route)
		}
		if got := route.Target.Route().FlowInstance; got != flowInstance {
			t.Fatalf("route %q flow instance = %q, want %q", route.Recipient.LocalID(), got, flowInstance)
		}
		if route.Target.Route().EntityID == "" {
			t.Fatalf("route %q lost exact entity owner: %#v", route.Recipient.LocalID(), route)
		}
		local := strings.HasSuffix(route.Recipient.LocalID(), "-local") || route.Recipient.LocalID() == "producer-local"
		if local && !route.ConnectClaim.Empty() {
			t.Fatalf("local route %q acquired a connect claim: %#v", route.Recipient.LocalID(), route)
		}
		if !local && route.ConnectClaim.Empty() {
			t.Fatalf("connect route %q lost execution claim: %#v", route.Recipient.LocalID(), route)
		}
	}
}

func assertMixedEventProjection(t testing.TB, event events.Event, wantFlowInstances ...string) {
	t.Helper()
	got := eventDeliveryTargetRoutes(event)
	gotFlowInstances := make([]string, 0, len(got))
	for _, target := range got {
		gotFlowInstances = append(gotFlowInstances, target.FlowInstance)
	}
	sort.Strings(gotFlowInstances)
	want := append([]string(nil), wantFlowInstances...)
	sort.Strings(want)
	if !reflect.DeepEqual(gotFlowInstances, want) {
		t.Fatalf("event target projection = %#v, want flows %#v", got, want)
	}
}

func TestMixedPubsubConnectCompositionReplayUsesCommittedRoutes(t *testing.T) {
	source := mixedPubsubConnectStaticSource()
	routeTable, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	owners := mixedStaticOwners("producer", "consumer")
	store := newTargetRouteMemoryStore()
	store.setTargetOwners(owners...)
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, RouteTable: routeTable})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := mixedStaticSourceEvent("producer", "deploy.done", owners[0], "replay")
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("initial Publish: %v", err)
	}
	wantRoutes := append([]events.DeliveryRoute(nil), store.routes[evt.ID()]...)
	wantEvent := store.events[evt.ID()]

	routeTable.mu.Lock()
	routeTable.routes = map[routeResolutionKey][]Subscriber{}
	routeTable.rootInputRoutes = map[string][]Subscriber{}
	routeTable.patterns = nil
	routeTable.connectGraph = runtimepinrouting.CompiledConnectGraph{}
	routeTable.mu.Unlock()

	restarted, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, RouteTable: routeTable})
	if err != nil {
		t.Fatalf("restart EventBus: %v", err)
	}
	if err := restarted.Publish(context.Background(), evt); err != nil {
		t.Fatalf("duplicate Publish after topology removal: %v", err)
	}
	if got := store.routes[evt.ID()]; !reflect.DeepEqual(got, wantRoutes) {
		t.Fatalf("replayed routes = %#v, want committed %#v", got, wantRoutes)
	}
	if got := store.events[evt.ID()]; !sameEventTargetFacts(got, wantEvent) {
		t.Fatalf("replayed event projection = %#v, want committed %#v", got.NormalizedEnvelope(), wantEvent.NormalizedEnvelope())
	}
}
