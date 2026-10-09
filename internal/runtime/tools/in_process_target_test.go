package tools

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestInProcessToolRuntimeProjectionKeepsTargetWithoutFallback(t *testing.T) {
	entry := contracts.MustToolSchemaEntry(contracts.WithToolCategory("provider_connector"),
		contracts.WithToolHandler(contracts.ToolHandlerInProcess), contracts.WithToolEffect(contracts.ActivityEffectClassNonIdempotentWrite),
		contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)),
		contracts.WithToolInProcessTarget(contracts.ToolInProcessWhatsAppSendText))
	tool, found := executionToolFromAdmitted("send", entry)
	target, native := tool.InProcess()
	if !found || !native || target != contracts.ToolInProcessWhatsAppSendText {
		t.Fatal("runtime view dropped compiled target")
	}
	if _, http := tool.HTTPExecution(); http {
		t.Fatal("native tool gained an HTTP execution recipe")
	}
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: map[string]contracts.ToolSchemaEntry{"send": entry}})
	if _, err := ValidateToolImplementations(source); err == nil {
		t.Fatal("intent-only declaration advertised an installed execution owner")
	}
	calls := 0
	dispatcher := NewToolDispatcher(nil,
		func(context.Context, actors.AgentConfig, string) (ExecutionTool, bool, error) { return tool, true, nil },
		func(context.Context, actors.AgentConfig, ExecutionTool, any) (any, error) { calls++; return nil, nil },
		nil, nil, nil,
		map[string]ToolHandler{"send": func(context.Context, actors.AgentConfig, any) (any, error) { calls++; return nil, nil }})
	if _, err := dispatcher.Dispatch(context.Background(), actors.AgentConfig{ID: "caller"}, "send", map[string]any{}); err == nil || calls != 0 {
		t.Fatal("uninstalled native operation fell back to HTTP or a named callback", err)
	}
}
