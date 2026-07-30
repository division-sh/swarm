package agentidentitytest

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func Declared(t testing.TB, agentID, owner, scopeKey, instanceID, instancePath string) agentidentity.Identity {
	t.Helper()
	name, err := agentidentity.DeclaredName(agentID, owner)
	if err != nil {
		t.Fatalf("declared agent identity name: %v", err)
	}
	route, err := agentidentity.PresentRoute(scopeKey, instanceID, instancePath)
	if err != nil {
		t.Fatalf("declared agent identity route: %v", err)
	}
	identity, err := agentidentity.New(name, route)
	if err != nil {
		t.Fatalf("declared agent identity: %v", err)
	}
	return identity
}

func RootDeclared(t testing.TB, agentID, owner string) agentidentity.Identity {
	t.Helper()
	name, err := agentidentity.DeclaredName(agentID, owner)
	if err != nil {
		t.Fatalf("declared root agent identity name: %v", err)
	}
	identity, err := agentidentity.New(name, agentidentity.RootRoute())
	if err != nil {
		t.Fatalf("declared root agent identity: %v", err)
	}
	return identity
}

func Runtime(t testing.TB, agentID, owner, scopeKey, instanceID, instancePath string) agentidentity.Identity {
	t.Helper()
	name, err := agentidentity.RuntimeName(agentID, owner)
	if err != nil {
		t.Fatalf("runtime agent identity name: %v", err)
	}
	route, err := agentidentity.PresentRoute(scopeKey, instanceID, instancePath)
	if err != nil {
		t.Fatalf("runtime agent identity route: %v", err)
	}
	identity, err := agentidentity.New(name, route)
	if err != nil {
		t.Fatalf("runtime agent identity: %v", err)
	}
	return identity
}

func RootRuntime(t testing.TB, agentID, owner string) agentidentity.Identity {
	t.Helper()
	name, err := agentidentity.RuntimeName(agentID, owner)
	if err != nil {
		t.Fatalf("runtime root agent identity name: %v", err)
	}
	identity, err := agentidentity.New(name, agentidentity.RootRoute())
	if err != nil {
		t.Fatalf("runtime root agent identity: %v", err)
	}
	return identity
}
