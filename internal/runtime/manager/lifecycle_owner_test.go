package manager

import (
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func reconfigureAgentThroughLifecycleForTest(
	t testing.TB,
	am *AgentManager,
	agentID, flowInstance string,
	patch models.AgentConfig,
) error {
	t.Helper()
	current, err := am.ResolveAgentConfig(managerIdentityTestRunID, agentID, flowInstance)
	if err != nil {
		return err
	}
	identity, err := current.ConcreteIdentity()
	if err != nil {
		return err
	}
	candidate := models.MergeAgentConfig(current, patch)
	return am.reconfigureAgentIdentityExactWithTopology(
		am.runtimeContext(),
		am.semanticSource,
		identity,
		candidate,
		nil,
	)
}

func teardownAgentThroughLifecycleForTest(
	t testing.TB,
	am *AgentManager,
	agentID, flowInstance string,
) error {
	t.Helper()
	current, err := am.ResolveAgentConfig(managerIdentityTestRunID, agentID, flowInstance)
	if err != nil {
		return err
	}
	identity, err := current.ConcreteIdentity()
	if err != nil {
		return err
	}
	return am.teardownIdentity(am.runtimeContext(), identity, "test_lifecycle_owner")
}
