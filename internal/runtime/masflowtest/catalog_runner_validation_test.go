package masflowtest

import (
	"strings"
	"testing"
)

func TestValidateCatalogExpectedDocument_RejectsUnsupportedExecutableExpectations(t *testing.T) {
	var expected catalogExpectedDocument
	expected.Trigger.Event = "spawn.requested"
	expected.Expected.HandlerOutcome = "success"
	expected.Expected.FlowInstanceCreated = map[string]any{
		"template":    "worker",
		"instance_id": "w-001",
	}

	err := validateCatalogExpectedDocument("tier5-flow-lifecycle/test-create-flow-instance", expected)
	if err == nil {
		t.Fatal("expected unsupported executable expectation to fail validation")
	}
	if !strings.Contains(err.Error(), "expected.flow_instance_created") {
		t.Fatalf("error = %q, want expected.flow_instance_created", err)
	}
}
