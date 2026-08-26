package semanticview

import (
	"strings"
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
	permissions := []ConnectorGenerationPermission{MustConnectorGenerationPermission("messages.write", "owner")}
	triggerGeneration := triggergeneration.FromCanonicalBytes([]byte("trigger-v1"))
	authorizations := []runtimeprovideroutput.Authorization{runtimeprovideroutput.MustAuthorization(
		"telegram", "inbound.telegram.message", "provider.telegram", "1.0.0",
		"sha256:"+strings.Repeat("a", 64), triggerGeneration,
	)}
	capabilities := Capabilities{}.
		WithConnectorPackImports(
			map[string]ConnectorGenerationSurface{
				"telegram.send": semanticCapabilityGeneration("v1", permissions),
			},
			map[string]ConnectorImportSource{"telegram.send": MustConnectorImportSource("pack://telegram")},
		).
		WithProviderTriggerEvents(base, triggerGeneration, authorizations)
	permissions[0] = MustConnectorGenerationPermission("caller.mutation", "owner")
	authorizations[0] = runtimeprovideroutput.MustAuthorization(
		"caller-mutation", "inbound.telegram.message", "provider.telegram", "1.0.0",
		"sha256:"+strings.Repeat("a", 64), triggerGeneration,
	)

	generation, triggerBase, ok := capabilities.ProviderTriggerEvents()
	if !ok || !generation.Equal(triggerGeneration) || triggerBase != base {
		t.Fatalf("provider-trigger capability = (%q, %#v, %v)", generation.Diagnostic(), triggerBase, ok)
	}
	targetFree := capabilities.ProviderTriggerTargetFreeAuthorizations()
	targetFree[0] = runtimeprovideroutput.Authorization{}
	if got := capabilities.ProviderTriggerTargetFreeAuthorizations(); len(got) != 1 || got[0].Provider() != "telegram" {
		t.Fatalf("provider-trigger authorization leaked mutation: %#v", got)
	}
	connector, ok := capabilities.ConnectorGeneration("telegram.send")
	if !ok || connector.GeneratorVersion() != "v1" || len(connector.Permissions()) != 1 || connector.Permissions()[0].ID() != "messages.write" {
		t.Fatalf("connector capability = %#v, exists=%v", connector, ok)
	}
	readback := connector.Permissions()
	readback[0] = MustConnectorGenerationPermission("readback.mutation", "owner")
	if got, _ := capabilities.ConnectorGeneration("telegram.send"); got.Permissions()[0].ID() != "messages.write" {
		t.Fatalf("connector capability leaked readback mutation: %#v", got)
	}
	if source, ok := capabilities.ConnectorImportSource("telegram.send"); !ok || source.URI() != "pack://telegram" {
		t.Fatalf("connector import source = %q, exists=%v", source, ok)
	}
}

func TestSemanticSourceCapabilityCompositionHasOneOwner(t *testing.T) {
	base := Wrap(&runtimecontracts.WorkflowContractBundle{})
	triggerGeneration := triggergeneration.FromCanonicalBytes([]byte("trigger-v1"))
	authorization := runtimeprovideroutput.MustAuthorization(
		"telegram", "inbound.telegram.message", "provider.telegram", "1.0.0",
		"sha256:"+strings.Repeat("a", 64), triggerGeneration,
	)
	capabilities := Capabilities{}.
		WithProviderTriggerEvents(base, triggerGeneration, []runtimeprovideroutput.Authorization{authorization}).
		WithConnectorPackImports(
			map[string]ConnectorGenerationSurface{"telegram.send": semanticCapabilityGeneration("connector-v1", nil)},
			map[string]ConnectorImportSource{"telegram.send": MustConnectorImportSource("pack://telegram")},
		)
	if generation, triggerBase, ok := capabilities.ProviderTriggerEvents(); !ok || !generation.Equal(triggerGeneration) || triggerBase != base {
		t.Fatalf("provider-trigger capability was lost during composition: (%q, %#v, %v)", generation.Diagnostic(), triggerBase, ok)
	}
	if generation, ok := capabilities.ConnectorGeneration("telegram.send"); !ok || generation.GeneratorVersion() != "connector-v1" {
		t.Fatalf("connector capability was lost during composition: %#v, exists=%v", generation, ok)
	}
	if generation, base, ok := capabilities.WithProviderTriggerEvents(base, triggergeneration.Generation{}, nil).ProviderTriggerEvents(); ok || generation.Valid() || base != nil {
		t.Fatalf("missing generation acquired provider-trigger capability: (%q, %#v, %v)", generation.Diagnostic(), base, ok)
	}
}

func TestRuntimeToolOverlayPreservesSemanticSourceCapabilities(t *testing.T) {
	baseSource := Wrap(&runtimecontracts.WorkflowContractBundle{})
	triggerGeneration := triggergeneration.FromCanonicalBytes([]byte("trigger-generation"))
	authorization := runtimeprovideroutput.MustAuthorization(
		"target-free", "inbound.target_free", "provider.target-free", "1.0.0",
		"sha256:"+strings.Repeat("b", 64), triggerGeneration,
	)
	capabilities := Capabilities{}.
		WithConnectorPackImports(
			map[string]ConnectorGenerationSurface{
				"deliver": semanticCapabilityGeneration("generation:deliver", nil),
			},
			map[string]ConnectorImportSource{"deliver": MustConnectorImportSource("pack://connector/deliver")},
		).
		WithProviderTriggerEvents(
			baseSource,
			triggerGeneration,
			[]runtimeprovideroutput.Authorization{authorization},
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
	if value, exists := got.ConnectorImportSource("deliver"); !exists || value.URI() != "pack://connector/deliver" {
		t.Fatalf("connector provenance = %q, exists=%v", value, exists)
	}
	if value, exists := got.ConnectorGeneration("deliver"); !exists || value.GeneratorVersion() != "generation:deliver" {
		t.Fatalf("connector generation = %#v, exists=%v", value, exists)
	}
	generation, triggerBase, exists := got.ProviderTriggerEvents()
	if !exists || !generation.Equal(triggerGeneration) || triggerBase != baseSource {
		t.Fatalf("provider trigger capability = generation %q base %#v exists=%v", generation.Diagnostic(), triggerBase, exists)
	}
	targetFree := got.ProviderTriggerTargetFreeAuthorizations()
	if len(targetFree) != 1 || targetFree[0].Provider() != "target-free" {
		t.Fatalf("target-free authorizations = %#v", targetFree)
	}
}

func TestChannelRuntimeToolProjectionReplacesOnlyChannelTools(t *testing.T) {
	base := Wrap(&runtimecontracts.WorkflowContractBundle{})
	objectSchema := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)
	channelTool := func() runtimecontracts.ToolSchemaEntry {
		return runtimecontracts.MustToolSchemaEntry(
			runtimecontracts.WithToolCategory("channel_operation"),
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerChannel),
			runtimecontracts.WithToolSchemas(objectSchema, objectSchema),
		)
	}
	platformTool := runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolCategory("platform"),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerPlatformBuiltin),
		runtimecontracts.WithToolSchemas(objectSchema, objectSchema),
	)
	predecessor, err := WithRuntimeTools(base, map[string]runtimecontracts.ToolSchemaEntry{
		"channel.predecessor.deliver": channelTool(),
		"platform.inspect":            platformTool,
	})
	if err != nil {
		t.Fatal(err)
	}
	successor, err := WithChannelRuntimeToolProjection(predecessor, map[string]runtimecontracts.ToolSchemaEntry{
		"channel.successor.deliver": channelTool(),
	})
	if err != nil {
		t.Fatal(err)
	}
	entries := successor.ToolEntries()
	if _, exists := entries["channel.predecessor.deliver"]; exists {
		t.Fatal("successor projection retained predecessor channel authority")
	}
	if _, exists := entries["channel.successor.deliver"]; !exists {
		t.Fatal("successor projection omitted current channel authority")
	}
	if _, exists := entries["platform.inspect"]; !exists {
		t.Fatal("successor projection removed a non-channel tool")
	}

	empty, err := WithChannelRuntimeToolProjection(successor, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := empty.ToolEntries()["channel.successor.deliver"]; exists {
		t.Fatal("empty projection retained successor channel authority")
	}
	if _, err := WithChannelRuntimeToolProjection(predecessor, map[string]runtimecontracts.ToolSchemaEntry{
		"platform.inspect": channelTool(),
	}); err == nil {
		t.Fatal("channel projection replaced a non-channel tool identity")
	}
}

func TestSemanticSourceCapabilitiesRejectIncompletePayloads(t *testing.T) {
	base := Wrap(&runtimecontracts.WorkflowContractBundle{})
	generation := triggergeneration.FromCanonicalBytes([]byte("trigger-generation"))
	capabilities := Capabilities{}.
		WithProviderTriggerEvents(base, generation, []runtimeprovideroutput.Authorization{{}}).
		WithConnectorPackImports(
			map[string]ConnectorGenerationSurface{"deliver": {}},
			map[string]ConnectorImportSource{"deliver": {}},
		)
	if _, _, ok := capabilities.ProviderTriggerEvents(); ok {
		t.Fatal("incomplete provider authorization acquired provider-trigger capability")
	}
	if _, ok := capabilities.ConnectorGeneration("deliver"); ok {
		t.Fatal("incomplete connector generation acquired capability")
	}
	if _, ok := capabilities.ConnectorImportSource("deliver"); ok {
		t.Fatal("incomplete connector import source acquired capability")
	}
	if capabilities.ConnectorPackImportsApplied() {
		t.Fatal("incomplete connector payload acquired imports-applied authority")
	}

	validGeneration := semanticCapabilityGeneration("connector-v1", nil)
	capabilities = Capabilities{}.WithConnectorPackImports(
		map[string]ConnectorGenerationSurface{"orphan": validGeneration},
		map[string]ConnectorImportSource{"deliver": MustConnectorImportSource("pack://deliver")},
	)
	if capabilities.ConnectorPackImportsApplied() {
		t.Fatal("generation without a matching imported tool acquired imports-applied authority")
	}
}

func TestConnectorGenerationSurfaceRejectsNonCanonicalHashes(t *testing.T) {
	permissions := []ConnectorGenerationPermission{MustConnectorGenerationPermission("messages.write", "owner")}
	for _, hash := range []string{
		strings.Repeat("a", 64),
		"sha256:" + strings.Repeat("A", 64),
		" sha256:" + strings.Repeat("a", 64),
	} {
		if _, err := NewConnectorGenerationSurface(
			"v1",
			"catalog/source.yaml",
			hash,
			"catalog/profile.yaml",
			"sha256:"+strings.Repeat("b", 64),
			"sha256:"+strings.Repeat("c", 64),
			"messages.send",
			permissions,
			"fixture",
			"passing",
			"approved",
		); err == nil {
			t.Fatalf("non-canonical source hash %q was admitted", hash)
		}
	}
}

func semanticCapabilityGeneration(version string, permissions []ConnectorGenerationPermission) ConnectorGenerationSurface {
	if len(permissions) == 0 {
		permissions = []ConnectorGenerationPermission{MustConnectorGenerationPermission("messages.write", "owner")}
	}
	return MustConnectorGenerationSurface(
		version,
		"catalog/source.yaml",
		"sha256:"+strings.Repeat("c", 64),
		"catalog/profile.yaml",
		"sha256:"+strings.Repeat("d", 64),
		"sha256:"+strings.Repeat("e", 64),
		"messages.send",
		permissions,
		"fixture",
		"passing",
		"approved",
	)
}
