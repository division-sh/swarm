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
	// Tests construct a complete candidate, just like production reconfiguration.
	candidate := current
	if patch.Model != "" {
		candidate.Model = patch.Model
	}
	if patch.LLMBackend != "" {
		candidate.LLMBackend = patch.LLMBackend
	}
	if patch.ExecutionMode.Valid() {
		candidate.ExecutionMode = patch.ExecutionMode
	}
	if patch.Tools != nil {
		candidate.Tools = patch.Tools
	}
	if patch.Permissions != nil {
		candidate.Permissions = patch.Permissions
	}
	if patch.Subscriptions != nil {
		candidate.Subscriptions = patch.Subscriptions
	}
	if patch.NativeTools.Any() {
		candidate.NativeTools = patch.NativeTools
	}
	return am.reconfigureAgentIdentityExactWithTopology(
		am.runtimeContext(),
		am.semanticSource,
		identity,
		candidate,
		nil,
	)
}

func reconfigureMemoryThroughLifecycleForTest(t testing.TB, am *AgentManager, agentID, flowInstance string, enabled bool) error {
	t.Helper()
	current, err := am.ResolveAgentConfig(managerIdentityTestRunID, agentID, flowInstance)
	if err != nil {
		return err
	}
	identity, err := current.ConcreteIdentity()
	if err != nil {
		return err
	}
	current.Memory.Enabled = enabled
	return am.reconfigureAgentIdentityExactWithTopology(am.runtimeContext(), am.semanticSource, identity, current, nil)
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
