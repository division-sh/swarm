package manager

import (
	"path"
	"strings"
	"testing"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func managerRootAgentConfig(agentID string, subscriptions ...string) runtimeactors.AgentConfig {
	return runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		Identity:      managerAgentIdentity(agentID),
		Subscriptions: append([]string(nil), subscriptions...),
	}
}

func managerScopedRuntimeAgentIdentity(agentID, owner, scopeKey, instanceID, instancePath string) runtimeagentidentity.Identity {
	name, err := runtimeagentidentity.RuntimeName(agentID, owner)
	if err != nil {
		panic(err)
	}
	route, err := runtimeagentidentity.PresentRoute(scopeKey, instanceID, instancePath)
	if err != nil {
		panic(err)
	}
	identity, err := runtimeagentidentity.New(name, route)
	if err != nil {
		panic(err)
	}
	return identity
}

func managerRuntimeAgentIdentityForFlowPath(agentID, instancePath string) runtimeagentidentity.Identity {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	instanceID := path.Base(instancePath)
	scopeKey := strings.TrimSuffix(instancePath, "/"+instanceID)
	return managerScopedRuntimeAgentIdentity(
		agentID,
		"manager.test/"+strings.TrimSpace(agentID),
		scopeKey,
		instanceID,
		instancePath,
	)
}

func testAgentIdentity(t testing.TB, am *AgentManager, agentID, flowInstance string) runtimeagentidentity.Identity {
	t.Helper()
	identity, err := am.lifecycle.resolveAgentTarget(agentID, flowInstance, false)
	if err != nil {
		t.Fatalf("resolve test agent identity %q@%q: %v", agentID, flowInstance, err)
	}
	return identity
}

func testAgentConfig(t testing.TB, am *AgentManager, agentID, flowInstance string) (runtimeactors.AgentConfig, bool) {
	t.Helper()
	identity, err := am.lifecycle.resolveAgentTarget(agentID, flowInstance, false)
	if err != nil {
		return runtimeactors.AgentConfig{}, false
	}
	return am.getAgentConfigIdentity(identity)
}

func testExecutionSnapshot(t testing.TB, am *AgentManager, agentID, flowInstance string) (agentExecutionSnapshot, bool) {
	t.Helper()
	identity, err := am.lifecycle.resolveAgentTarget(agentID, flowInstance, false)
	if err != nil {
		return agentExecutionSnapshot{}, false
	}
	return am.lifecycle.executionSnapshotByIdentity(identity)
}

func testLifecycleCell(t testing.TB, coordinator *agentLifecycleCoordinator, agentID, flowInstance string) (*agentLifecycleCell, bool) {
	t.Helper()
	identity, _, err := coordinator.resolveAgentTargetLockedForTest(agentID, flowInstance, true)
	if err != nil {
		return nil, false
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	cell := coordinator.cells[identity]
	return cell, cell != nil
}

func (c *agentLifecycleCoordinator) resolveAgentTargetLockedForTest(
	agentID,
	flowInstance string,
	includeTerminated bool,
) (runtimeagentidentity.Identity, *agentLifecycleCell, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resolveAgentTargetLocked(agentID, flowInstance, includeTerminated)
}
