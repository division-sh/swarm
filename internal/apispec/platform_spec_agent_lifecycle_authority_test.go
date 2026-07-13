package apispec

import "testing"

func TestPlatformSpecOwnsLifecycleSubordinateSessionTransaction(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	authority := mustMappingValue(t, root, "agent_lifecycle_authority")
	transitions := mustMappingValue(t, authority, "transitions")
	subordinate := mustMappingValue(t, transitions, "subordinate_resource_transaction")

	assertScalarContains(t, mustMappingValue(t, mustMappingValue(t, subordinate, "plan"), "identity"), "normalized subordinate plan")
	assertScalarContains(t, mustMappingValue(t, subordinate, "current_set"), "active or suspended")
	assertScalarContains(t, mustMappingValue(t, subordinate, "atomicity"), "complete subordinate mutation set")
	assertScalarContains(t, mustMappingValue(t, subordinate, "replay"), "successor mapping")

	acquire := mustMappingValue(t, authority, "exact_live_session_acquire_hydrate")
	assertScalarContains(t, mustMappingValue(t, acquire, "rule"), "same session's conversation/runtime state")
	assertScalarContains(t, mustMappingValue(t, acquire, "failure"), "before platform.agent_started")

	projection := mustMappingValue(t, authority, "live_session_mutable_projection")
	assertScalarContains(t, mustMappingValue(t, projection, "rule"), "same selected-store transaction")
	assertScalarContains(t, mustMappingValue(t, projection, "turn_evidence_split"), "stale_projection_refused")
}

func TestPlatformSpecOwnsLifecycleCleanupQuiescence(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	authority := mustMappingValue(t, root, "agent_lifecycle_authority")
	cleanup := mustMappingValue(t, authority, "destructive_cleanup_quiescence")

	assertScalarContains(t, mustMappingValue(t, cleanup, "bundle_force"), "before preservation/session cleanup")
	assertScalarContains(t, mustMappingValue(t, cleanup, "runtime_nuke"), "before session cleanup")
	assertScalarContains(t, mustMappingValue(t, cleanup, "runtime_nuke"), "bundle catalog rows are retained")
}
