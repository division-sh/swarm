package bustest

import (
	"sync"
	"sync/atomic"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type TestingT interface {
	Helper()
	Cleanup(func())
	Fatalf(string, ...any)
}

var routeGeneration atomic.Uint64

var routes = struct {
	sync.Mutex
	active map[routeKey]*testRoute
}{active: map[routeKey]*testRoute{}}

type routeKey struct {
	bus     *runtimebus.EventBus
	agentID string
}

type testRoute struct {
	token runtimeeffects.LifecycleToken
}

func Subscribe(t TestingT, eventBus *runtimebus.EventBus, agentID string, eventTypes ...events.EventType) <-chan *runtimebus.LocalDelivery {
	t.Helper()
	subscriptions := make([]string, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		subscriptions = append(subscriptions, string(eventType))
	}
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID: agentID, Subscriptions: subscriptions,
	})
	if err != nil {
		t.Fatalf("admit test agent route: %v", err)
	}
	return SubscribeAdmission(t, eventBus, admission)
}

func SubscribeAdmission(t TestingT, eventBus *runtimebus.EventBus, admission semanticview.FlowOwnedAgentSubscriptionAdmission) <-chan *runtimebus.LocalDelivery {
	t.Helper()
	agentID := admission.AgentID()
	key := routeKey{bus: eventBus, agentID: agentID}
	retireCurrentRoute(key)
	token := runtimeeffects.LifecycleToken{
		RuntimeEpoch: runtimebus.CurrentRuntimeEpoch(), AgentID: agentID, Generation: routeGeneration.Add(1),
	}
	source := eventBus.ReplaceAgentRoute(token, admission)
	if source == nil {
		t.Fatalf("install exact test agent route for %q", agentID)
	}
	route := &testRoute{token: token}
	routes.Lock()
	routes.active[key] = route
	routes.Unlock()
	t.Cleanup(func() { retireExactRoute(key, route) })
	return source
}

func Unsubscribe(eventBus *runtimebus.EventBus, agentID string) {
	key := routeKey{bus: eventBus, agentID: agentID}
	retireCurrentRoute(key)
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
