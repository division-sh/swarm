package semanticview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type markedToolOverlaySource struct{ Source }

func (markedToolOverlaySource) ConnectorPackImportsApplied() bool  { return true }
func (markedToolOverlaySource) ProviderTriggerEventsApplied() bool { return true }

func TestRuntimeToolOverlayPreservesPackImportMarkers(t *testing.T) {
	base := markedToolOverlaySource{Source: Wrap(&runtimecontracts.WorkflowContractBundle{})}
	overlaid, err := WithRuntimeTools(base, map[string]runtimecontracts.ToolSchemaEntry{
		"channel.ops.deliver": {Category: "channel_operation", HandlerType: "channel"},
	})
	if err != nil {
		t.Fatalf("WithRuntimeTools: %v", err)
	}
	connector, ok := overlaid.(interface{ ConnectorPackImportsApplied() bool })
	if !ok || !connector.ConnectorPackImportsApplied() {
		t.Fatal("runtime tool overlay hid connector-pack import marker")
	}
	trigger, ok := overlaid.(interface{ ProviderTriggerEventsApplied() bool })
	if !ok || !trigger.ProviderTriggerEventsApplied() {
		t.Fatal("runtime tool overlay hid provider-trigger import marker")
	}
}
