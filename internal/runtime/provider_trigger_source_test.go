package runtime

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestSourceWithProviderTriggerEventsImportsEffectivePackSchemasWithoutAuthoredOwnership(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram")
	wrapped, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	entry, ok := wrapped.EventEntry("inbound.telegram.text_message")
	if !ok || entry.Source != "provider_trigger_pack_normalized" {
		t.Fatalf("normalized event entry = (%#v, %v)", entry, ok)
	}
	if _, authored := wrapped.AuthoredEventEntries()["inbound.telegram.text_message"]; authored {
		t.Fatal("pack event was misclassified as authored")
	}
	resolved, name, ok := wrapped.ResolveFlowEventCatalogEntry("coordinator", "inbound.telegram.text_message")
	wantFields := []string{"conversation_reference", "external_account_reference", "provider_message_reference", "text"}
	if !ok || name != "inbound.telegram.text_message" || len(resolved.Payload.Properties) != len(wantFields) {
		t.Fatalf("flow catalog resolution = (%#v, %q, %v)", resolved, name, ok)
	}
	for _, field := range wantFields {
		spec, exists := resolved.Payload.Properties[field]
		wantType := "text"
		if field == "provider_message_reference" {
			wantType = "integer"
		}
		if !exists || spec.Type != wantType {
			t.Fatalf("flow catalog field %q = (%#v, %v), want %s", field, spec, exists, wantType)
		}
	}
	if strings.Join(resolved.Payload.Required, ",") != strings.Join(wantFields, ",") {
		t.Fatalf("flow catalog required fields = %q, want %q", resolved.Payload.Required, wantFields)
	}
	if _, err := ResolveStandingTargetDeclarations(wrapped, catalog); err != nil {
		t.Fatalf("standing declarations rejected pack-composed source: %v", err)
	}
}

func TestImportedProviderEventReadbacksUseCanonicalDeepClone(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram.text_message")
	wrapped, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	const eventName = "inbound.telegram.text_message"
	const fieldName = "conversation_reference"
	mutate := func(entry runtimecontracts.EventCatalogEntry) {
		field := entry.Payload.Properties[fieldName]
		if field.ExactSchema == nil {
			t.Fatalf("%s exact schema is missing", fieldName)
		}
		field.ExactSchema.Type = "boolean"
		field.ExactSchema.Pattern = "changed"
		entry.Payload.Properties[fieldName] = field
	}
	assertFresh := func(label string) {
		entry, ok := wrapped.EventEntry(eventName)
		if !ok {
			t.Fatalf("%s: imported event missing", label)
		}
		field := entry.Payload.Properties[fieldName]
		if field.ExactSchema == nil || field.ExactSchema.Type != "string" || field.ExactSchema.Pattern == "changed" {
			t.Fatalf("%s: imported event mutation leaked: %#v", label, field.ExactSchema)
		}
	}

	entry, _ := wrapped.EventEntry(eventName)
	mutate(entry)
	assertFresh("EventEntry")
	entries := wrapped.EventEntries()
	mutate(entries[eventName])
	assertFresh("EventEntries")
	resolvedCatalog := wrapped.ResolvedEventCatalog()
	mutate(resolvedCatalog[eventName])
	assertFresh("ResolvedEventCatalog")
	resolved, _, ok := wrapped.ResolveFlowEventCatalogEntry("coordinator", eventName)
	if !ok {
		t.Fatal("ResolveFlowEventCatalogEntry: imported event missing")
	}
	mutate(resolved)
	assertFresh("ResolveFlowEventCatalogEntry")
	mutatedScope := false
	for _, scope := range wrapped.ProjectScopes() {
		if scoped, exists := scope.Events[eventName]; exists {
			mutate(scoped)
			mutatedScope = true
		}
	}
	if !mutatedScope {
		t.Fatal("ProjectScopes: imported event missing")
	}
	assertFresh("ProjectScopes")
}

func TestSourceWithProviderTriggerEventsRejectsLocalPackEventRedeclaration(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram")
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("bundle source missing")
	}
	bundle.Events["inbound.telegram.text_message"] = runtimecontracts.EventCatalogEntry{
		Source: "events.yaml", Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"chat_id": {Type: "text"}}},
	}
	_, err := SourceWithProviderTriggerEvents(source, catalog)
	if err == nil || !strings.Contains(err.Error(), "collision between events.yaml and trigger pack provider.telegram") || !strings.Contains(err.Error(), "describe pack") {
		t.Fatalf("collision error = %v", err)
	}
}

func TestSourceWithProviderTriggerEvents_HarnessInputIsNotIngress(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram")
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("fixture bundle missing")
	}
	pins := bundle.Semantics.FlowInputEventPins["coordinator"]
	if len(pins) != 1 {
		t.Fatalf("coordinator pins = %#v, want one", pins)
	}
	pins[0].Source = "harness"
	bundle.Semantics.FlowInputEventPins["coordinator"] = pins
	flow, ok := bundle.FlowViewByID("coordinator")
	if !ok || len(flow.Schema.Pins.Inputs.EventPins) != 1 {
		t.Fatal("coordinator typed input pin missing")
	}
	flow.Schema.Pins.Inputs.EventPins[0].Source = "harness"
	schema := bundle.FlowSchemas["coordinator"]
	schema.Pins.Inputs.EventPins[0].Source = "harness"
	bundle.FlowSchemas["coordinator"] = schema

	wrapped, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	if _, err := ResolveStandingTargetDeclarations(wrapped, catalog); err == nil || !strings.Contains(err.Error(), `add an exact external input pin for "inbound.telegram"`) {
		t.Fatalf("standing ingress error = %v, want harness excluded from provider ingress", err)
	}
}

func TestProviderTriggerNormalizedEventLowersThroughExactExternalInputPin(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram.text_message")
	bundle, ok := semanticview.Bundle(source)
	if !ok || len(bundle.PackageTree) == 0 || len(bundle.PackageTree[0].Manifest.Flows) == 0 {
		t.Fatal("fixture bundle flow declaration is unavailable")
	}
	// This unit proof isolates lowering for a non-template receiver. The served
	// proof covers target-free select-or-create materialization.
	bundle.PackageTree[0].Manifest.Flows[0].Mode = "static"
	flow, ok := bundle.FlowViewByID("coordinator")
	if !ok {
		t.Fatal("fixture coordinator flow is unavailable")
	}
	flow.Schema.Mode = runtimecontracts.FlowModeStatic
	wrapped, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	authorized, ok := wrapped.(interface {
		ProviderTriggerTargetFreeAuthorizations() []runtimeprovideroutput.Authorization
	})
	if !ok {
		t.Fatal("provider trigger source does not expose its target-free event authority")
	}
	plans, issues := runtimepinrouting.LowerTargetFreeInputRoutePlans(wrapped, authorized.ProviderTriggerTargetFreeAuthorizations())
	if len(issues) != 0 {
		t.Fatalf("target-free route plan issues = %#v", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("target-free route plans = %#v, want one normalized input plan", plans)
	}
	plan := plans[0]
	if plan.Source.ResolvedEvent != "inbound.telegram.text_message" || plan.Receiver.FlowID != "coordinator" || plan.Receiver.Pin == "" {
		t.Fatalf("target-free normalized route plan = %#v", plan)
	}

	rawPlans, rawIssues := runtimepinrouting.LowerTargetFreeInputRoutePlans(wrapped, []runtimeprovideroutput.Authorization{{
		Provider: "telegram", Event: "inbound.telegram", PackID: "provider.telegram", PackVersion: "1.0.0",
		ManifestHash: "sha256:" + strings.Repeat("a", 64), GenerationID: catalog.GenerationID(),
	}})
	if len(rawIssues) != 0 || len(rawPlans) != 0 {
		t.Fatalf("raw standing event acquired target-free route plans=%#v issues=%#v", rawPlans, rawIssues)
	}
}

func TestSourceWithProviderTriggerEventsRebuildsOnCatalogGenerationChange(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram")
	first, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("first SourceWithProviderTriggerEvents: %v", err)
	}
	entry, ok := catalog.EntryByProvider("telegram")
	if !ok {
		t.Fatal("Telegram catalog entry missing")
	}
	entry.Identity.ManifestHash = "sha256:" + strings.Repeat("a", 64)
	changed, err := providertriggers.NewCatalogSnapshot(entry)
	if err != nil {
		t.Fatalf("NewCatalogSnapshot: %v", err)
	}
	if changed.GenerationID() == catalog.GenerationID() {
		t.Fatal("changed pack identity retained catalog generation")
	}

	second, err := SourceWithProviderTriggerEvents(first, changed)
	if err != nil {
		t.Fatalf("reload SourceWithProviderTriggerEvents: %v", err)
	}
	marker, ok := second.(providerTriggerEventSourceMarker)
	if !ok || marker.ProviderTriggerEventGeneration() != changed.GenerationID() {
		t.Fatalf("reloaded provider trigger source generation = %T %#v", second, marker)
	}
	if _, nested := marker.BaseSemanticSource().(providerTriggerEventSourceMarker); nested {
		t.Fatal("reload stacked a provider trigger wrapper instead of rebuilding from the base source")
	}
	if entry, ok := second.EventEntry("inbound.telegram.text_message"); !ok || entry.Source != "provider_trigger_pack_normalized" {
		t.Fatalf("reloaded normalized event entry = (%#v, %v)", entry, ok)
	}
}

func TestProviderTriggerCapabilitiesRemainVisibleThroughRuntimeToolOverlay(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram.text_message")
	imported, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	overlaid, err := semanticview.WithRuntimeTools(imported, map[string]runtimecontracts.ToolSchemaEntry{
		"channel.ops.deliver": {Category: "channel_operation", HandlerType: "channel"},
	})
	if err != nil {
		t.Fatalf("WithRuntimeTools: %v", err)
	}
	revalidated, err := SourceWithProviderTriggerEvents(overlaid, catalog)
	if err != nil {
		t.Fatalf("same-generation revalidation through overlay: %v", err)
	}
	generation, ok := semanticview.SourceCapability[interface{ ProviderTriggerEventGeneration() string }](revalidated)
	if !ok || generation.ProviderTriggerEventGeneration() != catalog.GenerationID() {
		t.Fatalf("provider trigger generation hidden through overlay: capability=%v generation=%v", ok, generation)
	}
	authorized, ok := semanticview.SourceCapability[interface {
		ProviderTriggerTargetFreeAuthorizations() []runtimeprovideroutput.Authorization
	}](revalidated)
	if !ok || len(authorized.ProviderTriggerTargetFreeAuthorizations()) == 0 {
		t.Fatal("target-free provider authority hidden through overlay")
	}
}
