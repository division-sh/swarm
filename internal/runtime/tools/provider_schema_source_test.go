package tools

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestProviderSchemaBuiltinProjectionAdmission(t *testing.T) {
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{})
	for _, actor := range []*models.AgentConfig{nil, {ID: "worker", ExecutionMode: "live"}} {
		entries, err := builtinExecutionTools(source, actor)
		if err != nil || len(entries) == 0 {
			t.Fatalf("builtin admission: entries=%d err=%v", len(entries), err)
		}
		for name, entry := range entries {
			if err := llm.ValidateProviderToolSchema(name, entry.InputSchema()); err != nil {
				t.Fatalf("admitted builtin projection %s: %v", name, err)
			}
		}
	}
	if _, err := admitBuiltinExecutionTool("bad", builtinToolDraft{InputSchema: map[string]any{"type": "unsupported-provider-type"}}); err == nil {
		t.Fatal("malformed builtin draft reached execution projection")
	}
}
