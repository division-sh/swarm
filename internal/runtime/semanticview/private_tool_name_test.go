package semanticview

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestPrivateToolNameRuntimeOverlayAdmission(t *testing.T) {
	tool := contracts.MustToolSchemaEntry(contracts.WithToolCategory("provider_connector"),
		contracts.WithToolHandler(contracts.ToolHandlerInProcess), contracts.WithToolEffect(contracts.ActivityEffectClassNonIdempotentWrite),
		contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)),
		contracts.WithToolInProcessTarget(contracts.ToolInProcessWhatsAppSendText))
	source := Wrap(&contracts.WorkflowContractBundle{})
	for _, id := range []string{"Read", "emit_probe", "mcp__runtime-tools__private.send", "mcp__runtime-tools__mcp__runtime-tools__private.send", " private.send "} {
		t.Run(id, func(t *testing.T) {
			for _, publish := range []func(Source, map[string]contracts.ToolSchemaEntry) (Source, error){WithRuntimeTools, WithChannelRuntimeToolProjection} {
				if out, err := publish(source, map[string]contracts.ToolSchemaEntry{id: tool}); out != nil || err == nil || !strings.Contains(err.Error(), "private tool declaration") {
					t.Fatalf("typed overlay lost the shared name refusal: source=%v error=%v", out, err)
				}
			}
		})
	}
	out, err := WithRuntimeTools(source, map[string]contracts.ToolSchemaEntry{"private.send": tool})
	if err != nil || out.ToolEntries()["private.send"].AgentExposable() || len(source.ToolEntries()) != 0 {
		t.Fatalf("canonical private overlay changed input source or lost intent: %v", err)
	}
}
