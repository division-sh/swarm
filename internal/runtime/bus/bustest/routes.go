package bustest

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type TestingT interface {
	Helper()
	Cleanup(func())
	Fatalf(string, ...any)
}

var routeGeneration atomic.Uint64

func Identity(t TestingT, agentID, flowPath string) agentidentity.Identity {
	t.Helper()
	name, err := agentidentity.RuntimeName(agentID, "bus-test-route")
	if err != nil {
		t.Fatalf("test agent name: %v", err)
	}
	if strings.TrimSpace(flowPath) == "" {
		identity, err := agentidentity.New(name, agentidentity.RootRoute())
		if err != nil {
			t.Fatalf("test root agent identity: %v", err)
		}
		return identity
	}
	flowPath = strings.Trim(strings.TrimSpace(flowPath), "/")
	scopeKey, instanceID := flowPath, flowPath
	if slash := strings.IndexByte(flowPath, '/'); slash >= 0 {
		scopeKey, instanceID = flowPath[:slash], flowPath[slash+1:]
	}
	route, err := agentidentity.PresentRoute(scopeKey, instanceID, flowPath)
	if err != nil {
		t.Fatalf("test agent route: %v", err)
	}
	identity, err := agentidentity.New(name, route)
	if err != nil {
		t.Fatalf("test agent identity: %v", err)
	}
	return identity
}

var routes = struct {
	sync.Mutex
	active map[routeKey]*testRoute
}{active: map[routeKey]*testRoute{}}

type routeKey struct {
	bus      *runtimebus.EventBus
	identity agentidentity.Identity
}

type testRoute struct {
	token runtimeeffects.LifecycleToken
}

func Subscribe(t TestingT, eventBus *runtimebus.EventBus, agentID string, eventTypes ...events.EventType) <-chan *runtimebus.LocalDelivery {
	t.Helper()
	subscriptions := make([]string, 0, len(eventTypes))
	localEvents := make(map[string]struct{}, len(eventTypes))
	for _, eventType := range eventTypes {
		subscription := string(eventType)
		subscriptions = append(subscriptions, subscription)
		if local := eventidentity.LeafName(subscription); local != "" && !strings.Contains(local, "*") {
			localEvents[local] = struct{}{}
		}
	}
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID: agentID, LocalEvents: localEvents, Subscriptions: subscriptions,
	})
	if err != nil {
		t.Fatalf("admit test agent route: %v", err)
	}
	return SubscribeAdmission(t, eventBus, admission)
}

func SubscribeAdmission(t TestingT, eventBus *runtimebus.EventBus, admission semanticview.FlowOwnedAgentSubscriptionAdmission) <-chan *runtimebus.LocalDelivery {
	t.Helper()
	agentID := admission.AgentID()
	return SubscribeIdentity(t, eventBus, Identity(t, agentID, admission.FlowPath()), admission)
}

func SubscribeIdentity(
	t TestingT,
	eventBus *runtimebus.EventBus,
	identity agentidentity.Identity,
	admission semanticview.FlowOwnedAgentSubscriptionAdmission,
) <-chan *runtimebus.LocalDelivery {
	t.Helper()
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		t.Fatalf("validate test agent identity: %v", err)
	}
	if identity.AgentID() != admission.AgentID() || identity.FlowInstance() != admission.FlowPath() {
		t.Fatalf(
			"test route identity %s does not match admitted agent %q at flow instance %q",
			identity,
			admission.AgentID(),
			admission.FlowPath(),
		)
	}
	key := routeKey{bus: eventBus, identity: identity}
	retireCurrentRoute(key)
	token := runtimeeffects.LifecycleToken{
		Identity:     identity,
		RuntimeEpoch: runtimebus.CurrentRuntimeEpoch(), AgentID: identity.AgentID(), Generation: routeGeneration.Add(1),
	}
	source := eventBus.ReplaceAgentRoute(token, admission)
	if source == nil {
		t.Fatalf("install exact test agent route for %s", identity)
	}
	route := &testRoute{token: token}
	routes.Lock()
	routes.active[key] = route
	routes.Unlock()
	t.Cleanup(func() { retireExactRoute(key, route) })
	return source
}

func Unsubscribe(eventBus *runtimebus.EventBus, agentID string) {
	routes.Lock()
	var matched routeKey
	matches := 0
	for key := range routes.active {
		if key.bus == eventBus && key.identity.AgentID() == agentID {
			matched = key
			matches++
		}
	}
	routes.Unlock()
	if matches > 1 {
		panic("ambiguous test agent route unsubscribe; use UnsubscribeIdentity")
	}
	if matches == 1 {
		retireCurrentRoute(matched)
	}
}

func UnsubscribeIdentity(eventBus *runtimebus.EventBus, identity agentidentity.Identity) {
	retireCurrentRoute(routeKey{bus: eventBus, identity: identity.Normalize()})
}

func retireCurrentRoute(key routeKey) {
	routes.Lock()
	route := routes.active[key]
	delete(routes.active, key)
	routes.Unlock()
	if route != nil {
		route.retire(key.bus)
	}
}

func retireExactRoute(key routeKey, route *testRoute) {
	routes.Lock()
	if routes.active[key] == route {
		delete(routes.active, key)
	}
	routes.Unlock()
	route.retire(key.bus)
}

func (r *testRoute) retire(eventBus *runtimebus.EventBus) {
	if r == nil {
		return
	}
	eventBus.RemoveAgentRoute(r.token)
}
