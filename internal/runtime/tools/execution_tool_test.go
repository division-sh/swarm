package tools

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func admittedExecutionToolForTest(t *testing.T, name string, options ...runtimecontracts.ToolSchemaEntryOption) ExecutionTool {
	t.Helper()
	entry, err := runtimecontracts.NewToolSchemaEntry(options...)
	if err != nil {
		t.Fatalf("NewToolSchemaEntry(%s): %v", name, err)
	}
	tool, include := executionToolFromAdmitted(name, entry)
	if !include {
		t.Fatalf("executionToolFromAdmitted(%s) did not produce an executable tool", name)
	}
	return tool
}
