package semanticview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

type markedToolOverlaySource struct {
	Source
	capabilities Capabilities
}

func (s markedToolOverlaySource) SemanticCapabilities() Capabilities {
	return s.capabilities
}

func TestSemanticSourceCapabilitiesAreCompileVisibleAndComplete(t *testing.T) {
	var _ Source = markedToolOverlaySource{}

	base := Wrap(&runtimecontracts.WorkflowContractBundle{})
	permissions := []ConnectorGenerationPermission{{ID: "messages.write", Note: "owner"}}
	authorizations := []runtimeprovideroutput.Authorization{{Provider: "telegram", Event: "inbound.telegram.message"}}
	triggerGeneration := triggergeneration.FromCanonicalBytes([]byte("trigger-v1"))
	capabilities := Capabilities{}.
		WithConnectorPackImports(
			map[string]ConnectorGenerationSurface{
				"telegram.send": {GeneratorVersion: "v1", Permissions: permissions},
			},
			map[string]string{"telegram.send": "pack://telegram"},
		).
		WithProviderTriggerEvents(base, triggerGeneration, authorizations)
	permissions[0].ID = "caller mutation"
	authorizations[0].Provider = "caller mutation"

	generation, triggerBase, ok := capabilities.ProviderTriggerEvents()
	if !ok || !generation.Equal(triggerGeneration) || triggerBase != base {
		t.Fatalf("provider-trigger capability = (%q, %#v, %v)", generation.Diagnostic(), triggerBase, ok)
	}
	targetFree := capabilities.ProviderTriggerTargetFreeAuthorizations()
	targetFree[0].Provider = "readback mutation"
	if got := capabilities.ProviderTriggerTargetFreeAuthorizations(); len(got) != 1 || got[0].Provider != "telegram" {
		t.Fatalf("provider-trigger authorization leaked mutation: %#v", got)
	}
	connector, ok := capabilities.ConnectorGeneration("telegram.send")
	if !ok || connector.GeneratorVersion != "v1" || len(connector.Permissions) != 1 || connector.Permissions[0].ID != "messages.write" {
		t.Fatalf("connector capability = %#v, exists=%v", connector, ok)
	}
	connector.Permissions[0].ID = "readback mutation"
	if got, _ := capabilities.ConnectorGeneration("telegram.send"); got.Permissions[0].ID != "messages.write" {
		t.Fatalf("connector capability leaked readback mutation: %#v", got)
	}
	if source, ok := capabilities.ConnectorImportSource("telegram.send"); !ok || source != "pack://telegram" {
		t.Fatalf("connector import source = %q, exists=%v", source, ok)
	}
}

func TestSemanticSourceCapabilityCompositionHasOneOwner(t *testing.T) {
	base := Wrap(&runtimecontracts.WorkflowContractBundle{})
	triggerGeneration := triggergeneration.FromCanonicalBytes([]byte("trigger-v1"))
	capabilities := Capabilities{}.
		WithProviderTriggerEvents(base, triggerGeneration, []runtimeprovideroutput.Authorization{{Provider: "telegram"}}).
		WithConnectorPackImports(
			map[string]ConnectorGenerationSurface{"telegram.send": {GeneratorVersion: "connector-v1"}},
			map[string]string{"telegram.send": "pack://telegram"},
		)
	if generation, triggerBase, ok := capabilities.ProviderTriggerEvents(); !ok || !generation.Equal(triggerGeneration) || triggerBase != base {
		t.Fatalf("provider-trigger capability was lost during composition: (%q, %#v, %v)", generation.Diagnostic(), triggerBase, ok)
	}
	if generation, ok := capabilities.ConnectorGeneration("telegram.send"); !ok || generation.GeneratorVersion != "connector-v1" {
		t.Fatalf("connector capability was lost during composition: %#v, exists=%v", generation, ok)
	}
	if generation, base, ok := capabilities.WithProviderTriggerEvents(base, triggergeneration.Generation{}, nil).ProviderTriggerEvents(); ok || generation.Valid() || base != nil {
		t.Fatalf("missing generation acquired provider-trigger capability: (%q, %#v, %v)", generation.Diagnostic(), base, ok)
	}
}

func TestRuntimeToolOverlayPreservesSemanticSourceCapabilities(t *testing.T) {
	baseSource := Wrap(&runtimecontracts.WorkflowContractBundle{})
	triggerGeneration := triggergeneration.FromCanonicalBytes([]byte("trigger-generation"))
	capabilities := Capabilities{}.
		WithConnectorPackImports(
			map[string]ConnectorGenerationSurface{
				"deliver": {GeneratorVersion: "generation:deliver"},
			},
			map[string]string{"deliver": "pack://connector/deliver"},
		).
		WithProviderTriggerEvents(
			baseSource,
			triggerGeneration,
			[]runtimeprovideroutput.Authorization{{Provider: "target-free"}},
		)
	base := markedToolOverlaySource{Source: baseSource, capabilities: capabilities}
	objectSchema := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
	overlaid, err := WithRuntimeTools(base, map[string]runtimecontracts.ToolSchemaEntry{
		"channel.ops.deliver": runtimecontracts.MustToolSchemaEntry(
			runtimecontracts.WithToolCategory("channel_operation"),
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerChannel),
			runtimecontracts.WithToolSchemas(objectSchema, objectSchema),
		),
	})
	if err != nil {
		t.Fatalf("WithRuntimeTools: %v", err)
	}

	got := overlaid.SemanticCapabilities()
	if !got.ConnectorPackImportsApplied() {
		t.Fatal("runtime tool overlay hid connector-pack capabilities")
	}
	if value, exists := got.ConnectorImportSource("deliver"); !exists || value != "pack://connector/deliver" {
		t.Fatalf("connector provenance = %q, exists=%v", value, exists)
	}
	if value, exists := got.ConnectorGeneration("deliver"); !exists || value.GeneratorVersion != "generation:deliver" {
		t.Fatalf("connector generation = %#v, exists=%v", value, exists)
	}
	generation, triggerBase, exists := got.ProviderTriggerEvents()
	if !exists || !generation.Equal(triggerGeneration) || triggerBase != baseSource {
		t.Fatalf("provider trigger capability = generation %q base %#v exists=%v", generation.Diagnostic(), triggerBase, exists)
	}
	targetFree := got.ProviderTriggerTargetFreeAuthorizations()
	if len(targetFree) != 1 || targetFree[0].Provider != "target-free" {
		t.Fatalf("target-free authorizations = %#v", targetFree)
	}
}
