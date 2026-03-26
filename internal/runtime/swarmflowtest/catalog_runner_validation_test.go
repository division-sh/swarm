package swarmflowtest

import "testing"

func TestValidateCatalogExpectedDocument_AllowsUnsupportedNonExecutableExpectations(t *testing.T) {
	var expected catalogExpectedDocument
	expected.Trigger.Event = "spawn.requested"
	expected.Expected.HandlerOutcome = "success"
	expected.Expected.FlowInstanceCreated = map[string]any{
		"template":    "worker",
		"instance_id": "w-001",
	}

	err := validateCatalogExpectedDocument("tier5-flow-lifecycle/test-create-flow-instance", expected)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if catalogCaseExecutableNowForDir("tier5-flow-lifecycle/test-create-flow-instance", expected) {
		t.Fatal("expected unsupported expectation case to be treated as non-executable")
	}
}

func TestValidateCatalogExpectedDocument_RuntimeOnlyCaseIsNonExecutable(t *testing.T) {
	var expected catalogExpectedDocument
	expected.Trigger.Event = "task.started"
	expected.Expected.RuntimeOnly = true
	expected.Expected.HandlerOutcome = "kill"
	expected.Expected.ChainDepthExceeded = true

	err := validateCatalogExpectedDocument("tier6-event-loop/test-chain-depth-limit", expected)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if catalogCaseExecutableNowForDir("tier6-event-loop/test-chain-depth-limit", expected) {
		t.Fatal("expected runtime-only case to be treated as non-executable")
	}
	if catalogCaseSimpleHarnessEligible(expected) {
		t.Fatal("expected runtime-only case to be ineligible for the simple harness")
	}
}
