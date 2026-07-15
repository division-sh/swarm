package semanticview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type markedToolOverlaySource struct{ Source }

func (markedToolOverlaySource) ConnectorPackImportsApplied() bool { return true }
func (markedToolOverlaySource) ConnectorPackImportSource(toolID string) (string, bool) {
	return "pack://connector/" + toolID, true
}
func (markedToolOverlaySource) ConnectorGenerationSurface(toolID string) (string, bool) {
	return "generation:" + toolID, true
}
func (markedToolOverlaySource) ProviderTriggerEventsApplied() bool { return true }
func (markedToolOverlaySource) ProviderTriggerEventGeneration() string {
	return "trigger-generation"
}
func (markedToolOverlaySource) ProviderTriggerTargetFreeAuthorizations() []string {
	return []string{"target-free"}
}

func TestRuntimeToolOverlayPreservesSemanticSourceCapabilities(t *testing.T) {
	base := markedToolOverlaySource{Source: Wrap(&runtimecontracts.WorkflowContractBundle{})}
	overlaid, err := WithRuntimeTools(base, map[string]runtimecontracts.ToolSchemaEntry{
		"channel.ops.deliver": {Category: "channel_operation", HandlerType: "channel"},
	})
	if err != nil {
		t.Fatalf("WithRuntimeTools: %v", err)
	}
	connector, ok := SourceCapability[interface{ ConnectorPackImportsApplied() bool }](overlaid)
	if !ok || !connector.ConnectorPackImportsApplied() {
		t.Fatal("runtime tool overlay hid connector-pack import marker")
	}
	connectorSource, ok := SourceCapability[interface {
		ConnectorPackImportSource(string) (string, bool)
	}](overlaid)
	if value, exists := connectorSource.ConnectorPackImportSource("deliver"); !ok || !exists || value != "pack://connector/deliver" {
		t.Fatalf("runtime tool overlay hid connector provenance: value=%q exists=%v capability=%v", value, exists, ok)
	}
	connectorGeneration, ok := SourceCapability[interface {
		ConnectorGenerationSurface(string) (string, bool)
	}](overlaid)
	if value, exists := connectorGeneration.ConnectorGenerationSurface("deliver"); !ok || !exists || value != "generation:deliver" {
		t.Fatalf("runtime tool overlay hid connector generation: value=%q exists=%v capability=%v", value, exists, ok)
	}
	trigger, ok := SourceCapability[interface{ ProviderTriggerEventsApplied() bool }](overlaid)
	if !ok || !trigger.ProviderTriggerEventsApplied() {
		t.Fatal("runtime tool overlay hid provider-trigger import marker")
	}
	triggerGeneration, ok := SourceCapability[interface{ ProviderTriggerEventGeneration() string }](overlaid)
	if !ok || triggerGeneration.ProviderTriggerEventGeneration() != "trigger-generation" {
		t.Fatal("runtime tool overlay hid provider-trigger generation")
	}
	targetFree, ok := SourceCapability[interface{ ProviderTriggerTargetFreeAuthorizations() []string }](overlaid)
	if !ok || len(targetFree.ProviderTriggerTargetFreeAuthorizations()) != 1 || targetFree.ProviderTriggerTargetFreeAuthorizations()[0] != "target-free" {
		t.Fatal("runtime tool overlay hid provider-trigger target-free authority")
	}
}
