package bus

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

var testAgentRouteGeneration atomic.Uint64

const busInternalTestRunID = "99999999-9999-9999-9999-999999999999"

func testRunScopedFlowRoute(route runtimeflowidentity.Route) runtimeflowidentity.RunScopedFlowInstance {
	return testRunScopedFlowRouteForRun(busInternalTestRunID, route)
}

func testRunScopedFlowRouteForRun(runID string, route runtimeflowidentity.Route) runtimeflowidentity.RunScopedFlowInstance {
	identity, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, route)
	if err != nil {
		panic(err)
	}
	return identity
}

func testAgentRouteIdentity(t testing.TB, agentID, flowPath string) agentidentity.Identity {
	return testAgentRouteIdentityForRun(t, busInternalTestRunID, agentID, flowPath)
}

func testAgentRouteIdentityForRun(t testing.TB, runID, agentID, flowPath string) agentidentity.Identity {
	t.Helper()
	if flowPath == "" {
		return agentidentitytest.RootRuntimeForRun(t, runID, agentID, "bus-test-route")
	}
	scopeKey, instanceID := flowPath, flowPath
	if slash := strings.IndexByte(flowPath, '/'); slash >= 0 {
		scopeKey, instanceID = flowPath[:slash], flowPath[slash+1:]
	}
	return agentidentitytest.RuntimeForRun(t, runID, agentID, "bus-test-route", scopeKey, instanceID, flowPath)
}

func testAgentLifecycleToken(t testing.TB, agentID, flowPath string, epoch int64, generation uint64) runtimeeffects.LifecycleToken {
	t.Helper()
	identity := testAgentRouteIdentity(t, agentID, flowPath)
	return runtimeeffects.LifecycleToken{
		Identity: identity, RuntimeEpoch: epoch, AgentID: identity.AgentID(), Generation: generation,
	}
}

var testAgentRoutes = struct {
	sync.Mutex
	active map[testAgentRouteKey]*testAgentRoute
}{active: map[testAgentRouteKey]*testAgentRoute{}}

type testAgentRouteKey struct {
	bus      *EventBus
	identity agentidentity.Identity
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
	return subscribeTestAgentAdmissionWithIdentity(
		t,
		eventBus,
		admission,
		testAgentRouteIdentity(t, agentID, admission.FlowPath()),
		"",
	)
}

func subscribeTestAgentAdmissionWithIdentity(
	t testing.TB,
	eventBus *EventBus,
	admission semanticview.FlowOwnedAgentSubscriptionAdmission,
	identity agentidentity.Identity,
	entityID string,
) <-chan *LocalDelivery {
	t.Helper()
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		t.Fatalf("validate exact test agent identity: %v", err)
	}
	agentID := admission.AgentID()
	if identity.AgentID() != agentID {
		t.Fatalf("test agent identity %q does not match admission %q", identity.AgentID(), agentID)
	}
	key := testAgentRouteKey{bus: eventBus, identity: identity}
	retireCurrentTestAgentRoute(key)
	token := runtimeeffects.LifecycleToken{
		Identity:     identity,
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
	descriptor := ActiveAgentDescriptor{Identity: identity, EntityID: strings.TrimSpace(entityID)}.Normalized()
	eventBus.RegisterRuntimeActiveAgentDescriptor(descriptor)
	t.Cleanup(func() { retireExactTestAgentRoute(key, route) })
	t.Cleanup(func() {
		eventBus.mu.Lock()
		if current, ok := eventBus.runtimeAgentDescriptors[identity]; ok && current == descriptor {
			delete(eventBus.runtimeAgentDescriptors, identity)
		}
		eventBus.mu.Unlock()
	})
	return source
}

func unsubscribeTestAgent(eventBus *EventBus, agentID string) {
	agentID = strings.TrimSpace(agentID)
	testAgentRoutes.Lock()
	matches := make([]testAgentRouteKey, 0, 1)
	for key := range testAgentRoutes.active {
		if key.bus == eventBus && key.identity.AgentID() == agentID {
			matches = append(matches, key)
		}
	}
	testAgentRoutes.Unlock()
	if len(matches) > 1 {
		panic("ambiguous test agent route teardown for " + agentID)
	}
	if len(matches) == 1 {
		retireCurrentTestAgentRoute(matches[0])
	}
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
