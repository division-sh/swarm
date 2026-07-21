package bus

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

var testAgentRouteGeneration atomic.Uint64

var testAgentRoutes = struct {
	sync.Mutex
	active map[testAgentRouteKey]*testAgentRoute
}{active: map[testAgentRouteKey]*testAgentRoute{}}

type testAgentRouteKey struct {
	bus     *EventBus
	agentID string
}

type testAgentRoute struct {
	token runtimeeffects.LifecycleToken
}

func subscribeTestAgent(t testing.TB, eventBus *EventBus, agentID string, eventTypes ...events.EventType) <-chan *LocalDelivery {
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
	return subscribeTestAgentAdmission(t, eventBus, admission)
}

func subscribeTestAgentAdmission(t testing.TB, eventBus *EventBus, admission semanticview.FlowOwnedAgentSubscriptionAdmission) <-chan *LocalDelivery {
	t.Helper()
	agentID := admission.AgentID()
	key := testAgentRouteKey{bus: eventBus, agentID: agentID}
	retireCurrentTestAgentRoute(key)
	token := runtimeeffects.LifecycleToken{
		RuntimeEpoch: CurrentRuntimeEpoch(), AgentID: agentID, Generation: testAgentRouteGeneration.Add(1),
	}
	source := eventBus.ReplaceAgentRoute(token, admission)
	if source == nil {
		t.Fatalf("install exact test agent route for %q", agentID)
	}
	route := &testAgentRoute{token: token}
	testAgentRoutes.Lock()
	testAgentRoutes.active[key] = route
	testAgentRoutes.Unlock()
	t.Cleanup(func() { retireExactTestAgentRoute(key, route) })
	return source
}

func unsubscribeTestAgent(eventBus *EventBus, agentID string) {
	key := testAgentRouteKey{bus: eventBus, agentID: agentID}
	retireCurrentTestAgentRoute(key)
}

func retireCurrentTestAgentRoute(key testAgentRouteKey) {
	testAgentRoutes.Lock()
	route := testAgentRoutes.active[key]
	delete(testAgentRoutes.active, key)
	testAgentRoutes.Unlock()
	if route != nil {
		route.retire(key.bus)
	}
}

func retireExactTestAgentRoute(key testAgentRouteKey, route *testAgentRoute) {
	testAgentRoutes.Lock()
	if testAgentRoutes.active[key] == route {
		delete(testAgentRoutes.active, key)
	}
	testAgentRoutes.Unlock()
	route.retire(key.bus)
}

func (r *testAgentRoute) retire(eventBus *EventBus) {
	if r == nil {
		return
	}
	eventBus.RemoveAgentRoute(r.token)
}
