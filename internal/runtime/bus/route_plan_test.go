package bus

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
)

func TestRoutePlanDeliveryIntentsCarryTypedProducer(t *testing.T) {
	routes := []plannedDeliveryRoute{{Recipient: events.MustNodeDeliveryRecipient("consumer-node"), Target: events.RouteIdentity{
		FlowInstance: "consumer/inst-1",
		EntityID:     "ent-consumer",
	},
	}}

	intents := routePlanDeliveryIntentsFromRoutes(routes, routeIntentProducerConnectRoutePlan)
	if got, want := len(intents), 1; got != want {
		t.Fatalf("delivery intents = %d, want %d", got, want)
	}
	intent := intents[0]
	if intent.Producer != routeIntentProducerConnectRoutePlan {
		t.Fatalf("intent producer = %q, want %q", routeIntentProducerCode(intent.Producer), routeIntentProducerCode(routeIntentProducerConnectRoutePlan))
	}
	if intent.Producer.Source() != routePlanSourceConnectRoutePlan || intent.Producer.Reason() != routePlanReasonLoweredConnectRoutePlan {
		t.Fatalf("intent producer source/reason = %s/%s, want connect route plan/lowered connect", intent.Producer.Source().code(), intent.Producer.Reason().code())
	}
}

func TestRoutePlanRejectsMalformedOrUnpairedPersistentDeliveryIntent(t *testing.T) {
	identity := agentidentitytest.RootRuntime(t, "agent-a", "route-plan-test")
	plan := RoutePlan{
		LiveRecipients: []RoutePlanLiveRecipient{{Recipient: events.MustAgentDeliveryRecipient("agent-a"), AgentIdentity: identity, PersistAsDelivery: true}},
		DeliveryIntents: []RoutePlanDeliveryIntent{{
			Persist: true,
		}},
	}
	if err := plan.ValidatePersistentDeliveries(); err == nil || !strings.Contains(err.Error(), "unsupported subscriber type") {
		t.Fatalf("malformed durable route validation = %v", err)
	}
	if routes := plan.DeliveryRoutes(); len(routes) != 1 || !routes[0].Recipient.Empty() {
		t.Fatalf("malformed durable intent was silently filtered: %#v", routes)
	}

	plan.DeliveryIntents = []RoutePlanDeliveryIntent{{Recipient: events.MustAgentDeliveryRecipient("agent-b"), AgentIdentity: agentidentitytest.RootRuntime(t, "agent-b", "route-plan-test"),
		Persist: true,
	}}
	if err := plan.ValidatePersistentDeliveries(); err == nil || !strings.Contains(err.Error(), "has no exact durable delivery route") {
		t.Fatalf("live/durable recipient mismatch validation = %v", err)
	}
}

func TestRoutePlanPendingLifecycleAuthorityPersistsWithoutLiveDispatch(t *testing.T) {
	identity := agentidentitytest.Runtime(t, "agent-a", "route-plan-test", "flow", "instance-a", "flow/instance-a")
	targetRoute := events.RouteIdentity{
		FlowID:       "flow",
		FlowInstance: "flow/instance-a",
		EntityID:     "entity-a",
	}
	targetOwner := events.MustExistingEntityTarget(targetRoute)
	nodeRoute := RoutePlanDeliveryIntent{Recipient: events.MustNodeDeliveryRecipient("node-a"), TargetBlueprint: targetRoute, TargetOwnership: targetOwner,
		Persist: true,
	}
	pendingAgentRoute := RoutePlanDeliveryIntent{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity,
		TargetBlueprint:       targetRoute,
		TargetOwnership:       targetOwner,
		Persist:               true,
		PendingAgentLifecycle: true,
	}
	plan := RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{nodeRoute, pendingAgentRoute}}

	if err := plan.ValidatePersistentDeliveries(); err != nil {
		t.Fatalf("validate pending lifecycle authority: %v", err)
	}
	if got, want := len(plan.DeliveryRoutes()), 2; got != want {
		t.Fatalf("durable routes = %d, want %d", got, want)
	}
	dispatchRoutes := plan.liveDispatchDeliveryRoutes()
	if got, want := len(dispatchRoutes), 1; got != want || !dispatchRoutes[0].Recipient.IsNode() {
		t.Fatalf("live dispatch routes = %#v, want only node route", dispatchRoutes)
	}
}

func TestRoutePlanRejectsMalformedPendingLifecycleAuthority(t *testing.T) {
	identity := agentidentitytest.Runtime(t, "agent-a", "route-plan-test", "flow", "instance-a", "flow/instance-a")
	for _, tc := range []struct {
		name   string
		intent RoutePlanDeliveryIntent
		want   string
	}{
		{
			name: "not persistent",
			intent: RoutePlanDeliveryIntent{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity,
				PendingAgentLifecycle: true,
			},
			want: "must be persistent",
		},
		{
			name: "not agent",
			intent: RoutePlanDeliveryIntent{Recipient: events.MustNodeDeliveryRecipient("node-a"), Persist: true,
				PendingAgentLifecycle: true,
			},
			want: "must identify one agent",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := (RoutePlan{DeliveryIntents: []RoutePlanDeliveryIntent{tc.intent}}).ValidatePersistentDeliveries()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validation error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRoutePlanRejectsMalformedDurableRouteBeforePersistence(t *testing.T) {
	identity := agentidentitytest.RootRuntime(t, "agent-a", "route-plan-test")
	plan := RoutePlan{
		LiveRecipients: []RoutePlanLiveRecipient{{
			Recipient:         events.MustAgentDeliveryRecipient("agent-a"),
			AgentIdentity:     identity,
			PersistAsDelivery: true,
		}},
		DeliveryIntents: []RoutePlanDeliveryIntent{{Persist: true}},
	}
	err := plan.ValidatePersistentDeliveries()
	if err == nil || !strings.Contains(err.Error(), "unsupported subscriber type") {
		t.Fatalf("malformed route error = %v", err)
	}
}

func TestRoutePlanProducerHelpersRequireTypedProducer(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(testFile), "route_plan.go"))
	if err != nil {
		t.Fatalf("read route_plan.go: %v", err)
	}
	source := string(raw)
	for _, forbidden := range []string{
		"routePlanLiveRecipientsFromManifest(manifest deliveryRecipientManifest, source, reason string)",
		"routePlanDeliveryIntentsFromRoutes(routes []events.DeliveryRoute, source, reason string)",
		"routePlanFromManifest(evt events.Event, manifest deliveryRecipientManifest, source, reason string)",
		"type routeIntentProducer struct",
		"routeIntentProducer{source:",
		"AuthorityOwner       string",
		"Source            string",
		"Reason            string",
		"Source         string",
		"Reason         string",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("route_plan.go still exposes raw route-intent producer shape %q", forbidden)
		}
	}
}

func TestRoutePlanDoesNotExposeLegacyEventDeliveryPlanCompatibility(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	dir := filepath.Dir(testFile)
	files := []string{
		"eventbus.go",
		"route_plan.go",
		"delivery_planner.go",
		"eventbus_publish.go",
	}
	for _, file := range files {
		raw, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		source := string(raw)
		for _, forbidden := range []string{
			"type eventDeliveryPlan struct",
			"func (p RoutePlan) EventDeliveryPlan()",
			"CanonicalRoutePlan(",
			"WithCanonicalRoutePlan(",
			"routePlanFromLegacyEventDeliveryPlan",
		} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("%s still exposes legacy eventDeliveryPlan compatibility %q", file, forbidden)
			}
		}
	}
}
