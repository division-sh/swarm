package agentidentity

import (
	"testing"
)

func TestIdentityRequiresExplicitRoutePresence(t *testing.T) {
	name, err := DeclaredName("reviewer", "swarm://review/reviewer")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(name, Route{}); err == nil {
		t.Fatal("missing route presence accepted")
	}
	root, err := New(name, RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := root.Route.Fields(); ok {
		t.Fatal("root route reported present")
	}
	present, err := PresentRoute("review", "inst-1", "review/inst-1")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := New(name, present)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, instancePath, ok := identity.Route.Fields(); !ok || instancePath != "review/inst-1" {
		t.Fatalf("instance path = %q, %v", instancePath, ok)
	}
}

func TestIdentityKeepsNameProvenanceSeparate(t *testing.T) {
	declared, err := DeclaredName("reviewer", "swarm://review/reviewer")
	if err != nil {
		t.Fatal(err)
	}
	runtimeCreated, err := RuntimeName("reviewer", "agent_hire")
	if err != nil {
		t.Fatal(err)
	}
	route, err := PresentRoute("review", "inst-1", "review/inst-1")
	if err != nil {
		t.Fatal(err)
	}
	left, err := New(declared, route)
	if err != nil {
		t.Fatal(err)
	}
	right, err := New(runtimeCreated, route)
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatal("declared and runtime-created provenance collapsed")
	}
}
