package flowidentity

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func TestAgentIdentityRoutePreservesDeclarationScope(t *testing.T) {
	const runID = "11111111-1111-4111-8111-111111111111"
	for _, test := range []struct {
		name string
		flow Route
		want agentidentity.Route
	}{
		{"root", StoredRoute(".", runID, runID), agentidentity.RootRoute()},
		{"static", StoredRoute("child", "child", "child"), mustAgentRoute(t, "child", "child", "child")},
		{"template", StoredRoute("child", "one", "child/one"), mustAgentRoute(t, "child", "one", "child/one")},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.flow.AgentIdentityRoute()
			if err != nil || got != test.want {
				t.Fatalf("agent route=%+v want=%+v err=%v", got, test.want, err)
			}
		})
	}
	if _, err := (Route{ScopeKey: "."}).AgentIdentityRoute(); err == nil {
		t.Fatal("incomplete root flow route admitted an agent route")
	}
	root, err := NewRunScopedFlowInstance(runID, StoredRoute(".", runID, runID))
	if err != nil {
		t.Fatal(err)
	}
	name, err := agentidentity.DeclaredName("reader", "test://root/reader")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agentidentity.NewPlan(name, agentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	agent, err := plan.Live(runID)
	if err != nil {
		t.Fatal(err)
	}
	if !root.MatchesAgentRoute(agent) {
		t.Fatal("root declaration is not bound to its exact constructed run")
	}
	agent.RunID = "22222222-2222-4222-8222-222222222222"
	if root.MatchesAgentRoute(agent) || root.MatchesAgentRoute(agentidentity.Identity{}) {
		t.Fatal("root attachment borrowed a foreign or absent agent route")
	}
}

func mustAgentRoute(t *testing.T, scope, instance, path string) agentidentity.Route {
	t.Helper()
	route, err := agentidentity.PresentRoute(scope, instance, path)
	if err != nil {
		t.Fatal(err)
	}
	return route
}
