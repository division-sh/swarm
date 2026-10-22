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
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

func TestMixedPubsubConnectCompositionMultiBoundary(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		name := "forward connect order"
		if reverse {
			name = "reverse connect order"
		}
		t.Run(name, func(t *testing.T) {
			source := mixedFanoutToFanoutSource(t, reverse)
			owners := mixedStaticOwners(t, source, "producer", "left", "right", "left-sink", "right-sink")
			store := newTargetRouteMemoryStore()
			store.setTargetOwners(owners...)
			installMixedStaticConstructionFixture(t, store, source, owners)
			interceptor := &connectRoutePlanNodeInterceptor{}
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle: source, Interceptors: []EventInterceptor{interceptor},
				Durable: DurableDependencies{RunLifecycle: &publicationRunPreflightTestStore{runID: busInternalTestRunID}},
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
	source := mixedFanoutToFanoutSource(t, false)
	owners := mixedStaticOwners(t, source, "producer", "left", "right", "left-sink", "right-sink")
	ownerByFlow := mixedOwnerMap(owners)
	store := newTargetRouteMemoryStore()
	store.setTargetOwners(owners...)
	installMixedStaticConstructionFixture(t, store, source, owners)
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source,
		Durable: DurableDependencies{RunLifecycle: &publicationRunPreflightTestStore{runID: busInternalTestRunID}},
	})
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
	source := semanticview.Wrap(loadTargetRouteTempBundle(t, map[string]string{
		"manifest.yaml":          "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":            "stages: []\nconnect:\n  - event: deploy.done\n    from: producer\n    to: consumer\n    rename: deploy.accepted\n",
		"producer/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"producer/schema.yaml":   "stages: []\npins:\n  outputs: [deploy.done]\n",
		"producer/events.yaml":   "deploy.done:\n",
		"producer/nodes.yaml":    "producer-local:\n  execution_type: system_node\n  subscribes_to: [deploy.done]\n  event_handlers:\n    deploy.done:\n      guard:\n        id: selected_owner\n        check: _entity.id != \"\"\n",
		"producer/agents.yaml":   "producer-agent:\n  subscriptions: [deploy.done]\n",
		"consumer/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"consumer/schema.yaml":   "stages: []\npins:\n  inputs: [deploy.accepted]\n",
		"consumer/events.yaml":   "deploy.accepted:\n",
		"consumer/nodes.yaml":    "consumer-node:\n  execution_type: system_node\n  event_handlers:\n    deploy.accepted:\n      guard:\n        id: selected_owner\n        check: _entity.id != \"\"\n",
	}))
	owners := mixedStaticOwners(t, source, "producer", "consumer")
	store := newTargetRouteMemoryStore()
	store.setTargetOwners(owners...)
	installMixedStaticConstructionFixture(t, store, source, owners)
	interceptor := &connectRoutePlanNodeInterceptor{}
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, Interceptors: []EventInterceptor{interceptor},
		Durable: DurableDependencies{RunLifecycle: &publicationRunPreflightTestStore{runID: busInternalTestRunID}},
	})
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

func mixedFanoutToFanoutSource(t *testing.T, reverse bool) semanticview.Source {
	t.Helper()
	connects := []map[string]string{
		{"event": "branch.ready", "from": "producer", "to": "left", "rename": "branch.accepted"},
		{"event": "branch.ready", "from": "producer", "to": "right", "rename": "branch.accepted"},
		{"event": "branch.done", "from": "left", "to": "left-sink", "rename": "branch.final"},
		{"event": "branch.done", "from": "right", "to": "right-sink", "rename": "branch.final"},
	}
	if reverse {
		for left, right := 0, len(connects)-1; left < right; left, right = left+1, right-1 {
			connects[left], connects[right] = connects[right], connects[left]
		}
	}
	schema, err := yaml.Marshal(map[string]any{"stages": []any{}, "connect": connects})
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"manifest.yaml":          "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":            string(schema),
		"producer/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"producer/schema.yaml":   "stages: []\npins:\n  outputs: [branch.ready]\n",
		"producer/events.yaml":   "branch.ready:\n",
		"producer/nodes.yaml":    "producer-local:\n  execution_type: system_node\n  subscribes_to: [branch.ready]\n  event_handlers:\n    branch.ready:\n      guard:\n        id: selected_owner\n        check: _entity.id != \"\"\n",
	}
	for _, id := range []string{"left", "right"} {
		files[id+"/manifest.yaml"] = "version: 1.0.0\nplatform_version: '*'\n"
		files[id+"/schema.yaml"] = "stages: []\npins:\n  inputs: [branch.accepted]\n  outputs: [branch.done]\n"
		files[id+"/events.yaml"] = "branch.accepted:\nbranch.done:\n"
		files[id+"/nodes.yaml"] = fmt.Sprintf(`%s-receiver:
  execution_type: system_node
  event_handlers:
    branch.accepted:
      guard: {id: selected_owner, check: _entity.id != ""}
%s-local:
  execution_type: system_node
  subscribes_to: [branch.done]
  event_handlers:
    branch.done:
      guard: {id: selected_owner, check: _entity.id != ""}
`, id, id)
		files[id+"-sink/manifest.yaml"] = "version: 1.0.0\nplatform_version: '*'\n"
		files[id+"-sink/schema.yaml"] = "stages: []\npins:\n  inputs: [branch.final]\n"
		files[id+"-sink/events.yaml"] = "branch.final:\n"
		files[id+"-sink/nodes.yaml"] = fmt.Sprintf(`%s-sink-node:
  execution_type: system_node
  event_handlers:
    branch.final:
      guard: {id: selected_owner, check: _entity.id != ""}
`, id)
	}
	return semanticview.Wrap(loadTargetRouteTempBundle(t, files))
}

func mixedStaticOwners(t testing.TB, source semanticview.Source, flowIDs ...string) []ActiveTargetDescriptor {
	t.Helper()
	out := make([]ActiveTargetDescriptor, 0, len(flowIDs))
	for _, flowID := range flowIDs {
		instance := ConstructedFlowInstanceIdentityFixture(source, flowID, "", busInternalTestRunID)
		out = append(out, ActiveTargetDescriptor{ID: flowID + "-owner", FlowInstance: instance.InstancePath, EntityID: instance.EntityID})
	}
	return out
}

func installMixedStaticConstructionFixture(t testing.TB, store *targetRouteMemoryStore, source semanticview.Source, owners []ActiveTargetDescriptor) {
	t.Helper()
	root := ConstructedFlowInstanceIdentityFixture(source, semanticview.RootExecutionFlowID(source), "", busInternalTestRunID)
	store.installIndexObservation(constructionIndexObservation(t, source, busInternalTestRunID, root, ""))
	for _, owner := range owners {
		instance := StoredFlowInstanceIdentityFixture(source, owner.FlowInstance, "", busInternalTestRunID, owner.EntityID)
		observed := constructionIndexObservation(t, source, busInternalTestRunID, instance, "")
		store.installIndexObservation(observed)
		receipt, native, err := observed.NativeConstruction()
		if err != nil || !native {
			t.Fatalf("mixed fixture lacks native construction: native=%v err=%v", native, err)
		}
		store.installConstructionReceipt(observed.Owner(), receipt)
	}
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
	source := mixedPubsubConnectStaticSource(t)
	routeTable, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatalf("DeriveRouteTable: %v", err)
	}
	owners := mixedStaticOwners(t, source, "producer", "consumer")
	store := newTargetRouteMemoryStore()
	store.setTargetOwners(owners...)
	installMixedStaticConstructionFixture(t, store, source, owners)
	eb, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, RouteTable: routeTable,
		Durable: DurableDependencies{RunLifecycle: &publicationRunPreflightTestStore{runID: busInternalTestRunID}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	evt := mixedStaticSourceEvent("producer", "deploy.done", owners[0], "replay")
	if err := eb.Publish(context.Background(), evt); err != nil {
		t.Fatalf("initial Publish: %v", err)
	}
	wantRoutes := append([]events.DeliveryRoute(nil), store.routes[evt.ID()]...)
	wantEvent := store.events[evt.ID()]

	routeTable.templates = nil
	routeTable.connectGraph = runtimepinrouting.CompiledConnectGraph{}

	restarted, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, RouteTable: routeTable,
		Durable: DurableDependencies{RunLifecycle: &publicationRunPreflightTestStore{runID: busInternalTestRunID}},
	})
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
