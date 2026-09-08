package agentidentitytest

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

const DefaultRunID = "00000000-0000-0000-0000-000000000001"

func Declared(t testing.TB, agentID, owner, scopeKey, instanceID, instancePath string) agentidentity.Identity {
	return DeclaredForRun(t, DefaultRunID, agentID, owner, scopeKey, instanceID, instancePath)
}

func DeclaredForRun(t testing.TB, runID, agentID, owner, scopeKey, instanceID, instancePath string) agentidentity.Identity {
	t.Helper()
	name, err := agentidentity.DeclaredName(agentID, owner)
	if err != nil {
		t.Fatalf("declared agent identity name: %v", err)
	}
	route, err := agentidentity.PresentRoute(scopeKey, instanceID, instancePath)
	if err != nil {
		t.Fatalf("declared agent identity route: %v", err)
	}
	identity, err := agentidentity.New(runID, name, route)
	if err != nil {
		t.Fatalf("declared agent identity: %v", err)
	}
	return identity
}

func RootDeclared(t testing.TB, agentID, owner string) agentidentity.Identity {
	return RootDeclaredForRun(t, DefaultRunID, agentID, owner)
}

func RootDeclaredForRun(t testing.TB, runID, agentID, owner string) agentidentity.Identity {
	t.Helper()
	name, err := agentidentity.DeclaredName(agentID, owner)
	if err != nil {
		t.Fatalf("declared root agent identity name: %v", err)
	}
	identity, err := agentidentity.New(runID, name, agentidentity.RootRoute())
	if err != nil {
		t.Fatalf("declared root agent identity: %v", err)
	}
	return identity
}

func Runtime(t testing.TB, agentID, owner, scopeKey, instanceID, instancePath string) agentidentity.Identity {
	return RuntimeForRun(t, DefaultRunID, agentID, owner, scopeKey, instanceID, instancePath)
}

func RuntimeForRun(t testing.TB, runID, agentID, owner, scopeKey, instanceID, instancePath string) agentidentity.Identity {
	t.Helper()
	name, err := agentidentity.RuntimeName(agentID, owner)
	if err != nil {
		t.Fatalf("runtime agent identity name: %v", err)
	}
	route, err := agentidentity.PresentRoute(scopeKey, instanceID, instancePath)
	if err != nil {
		t.Fatalf("runtime agent identity route: %v", err)
	}
	identity, err := agentidentity.New(runID, name, route)
	if err != nil {
		t.Fatalf("runtime agent identity: %v", err)
	}
	return identity
}

func RootRuntime(t testing.TB, agentID, owner string) agentidentity.Identity {
	return RootRuntimeForRun(t, DefaultRunID, agentID, owner)
}

func RootRuntimeForRun(t testing.TB, runID, agentID, owner string) agentidentity.Identity {
	t.Helper()
	name, err := agentidentity.RuntimeName(agentID, owner)
	if err != nil {
		t.Fatalf("runtime root agent identity name: %v", err)
	}
	identity, err := agentidentity.New(runID, name, agentidentity.RootRoute())
	if err != nil {
		t.Fatalf("runtime root agent identity: %v", err)
	}
	return identity
}
