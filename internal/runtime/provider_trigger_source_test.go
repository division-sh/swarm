package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
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
	wantFields := []string{"conversation_reference", "conversation_scope", "external_account_reference", "provider_message_reference", "text"}
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
	descriptors, err := AuthorActivityEventDescriptors(wrapped)
	if err != nil {
		t.Fatalf("AuthorActivityEventDescriptors: %v", err)
	}
	for _, descriptor := range descriptors {
		if descriptor.EventType != "inbound.telegram.text_message" {
			continue
		}
		if descriptor.Disposition != "different" {
			t.Fatalf("normalized provider event descriptor = %#v, want different", descriptor)
		}
		return
	}
	t.Fatalf("normalized provider event descriptor missing from %#v", descriptors)
}

func TestSourceWithProviderTriggerEventsImportsDeclaredNormalizedSchemaWithoutActivation(t *testing.T) {
	source, catalog := schemaOnlyTelegramDeclarationSource(t)
	wrapper, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	const eventName = "inbound.telegram.text_message"
	entry, ok := wrapper.EventEntry(eventName)
	if !ok || entry.Source != "provider_trigger_pack_normalized" {
		t.Fatalf("normalized event entry = (%#v, %v)", entry, ok)
	}
	if _, authored := wrapper.AuthoredEventEntries()[eventName]; authored {
		t.Fatal("schema-only pack event was misclassified as authored")
	}
	projectVisible := false
	for _, scope := range wrapper.ProjectScopes() {
		if _, exists := scope.Events[eventName]; exists {
			projectVisible = true
		}
	}
	if !projectVisible {
		t.Fatal("schema-only pack event is not visible in its declaring project scope")
	}
	if authorizations := wrapper.SemanticCapabilities().ProviderTriggerTargetFreeAuthorizations(); len(authorizations) != 0 {
		t.Fatalf("schema-only import granted provider-output authorization: %#v", authorizations)
	}
	if targets, err := ResolveStandingTargetDeclarations(wrapper, catalog); err != nil || len(targets) != 0 {
		t.Fatalf("schema-only import standing targets = (%#v, %v), want none", targets, err)
	}
	if subjects, err := EffectiveStandingIngressCapabilitySubjects(context.Background(), wrapper, catalog, nil); err != nil || len(subjects) != 0 {
		t.Fatalf("schema-only import effective ingress subjects = (%#v, %v), want none", subjects, err)
	}
	graph := runtimepinrouting.CompileConnectGraph(wrapper)
	if plans := graph.Plans(); len(plans) != 0 {
		t.Fatalf("schema-only import created executable provider route plans: %#v", plans)
	}
	composed, ok := wrapper.(providerTriggerEventSource)
	if !ok {
		t.Fatalf("effective source = %T, want providerTriggerEventSource", wrapper)
	}
	owner := composed.owners[eventName]
	if owner.provider != "telegram" || owner.event != eventName || owner.kind != providertriggers.OutputKindNormalized ||
		owner.identity.ID != "provider.telegram" || owner.identity.Version == "" || owner.identity.ManifestHash == "" ||
		owner.identity.Provenance == "" || !owner.generation.Equal(catalog.Generation()) {
		t.Fatalf("schema provenance = %#v, generation=%s", owner, owner.generation.Diagnostic())
	}
	provenance := wrapper.SemanticCapabilities().ProviderTriggerEventProvenance()
	if len(provenance) != 1 || provenance[0].Provider != "telegram" || provenance[0].Event != eventName ||
		provenance[0].Kind != "normalized" || provenance[0].PackID != "provider.telegram" ||
		provenance[0].PackVersion == "" || provenance[0].ManifestHash == "" || provenance[0].SourceProvenance == "" ||
		!provenance[0].Generation.Equal(catalog.Generation()) || len(provenance[0].ProjectScopes) != 1 {
		t.Fatalf("provider trigger provenance readback = %#v", provenance)
	}
}

func TestSourceWithProviderTriggerEventsRejectsInvalidSchemaOnlyDeclarations(t *testing.T) {
	tests := []struct {
		name    string
		imports []runtimecontracts.ProviderTriggerEventImport
		catalog bool
		want    string
	}{
		{
			name: "duplicate",
			imports: []runtimecontracts.ProviderTriggerEventImport{
				{Provider: "telegram", Event: "inbound.telegram.text_message"},
				{Provider: "telegram", Event: "inbound.telegram.text_message"},
			},
			catalog: true,
			want:    `duplicates provider "telegram" event "inbound.telegram.text_message"`,
		},
		{
			name:    "unknown provider",
			imports: []runtimecontracts.ProviderTriggerEventImport{{Provider: "telegramm", Event: "inbound.telegram.text_message"}},
			catalog: true,
			want:    `references unknown provider "telegramm"; available providers: github, intercom, shopify, slack, stripe, telegram, twilio, typeform`,
		},
		{
			name:    "unknown event",
			imports: []runtimecontracts.ProviderTriggerEventImport{{Provider: "telegram", Event: "inbound.telegram.message"}},
			catalog: true,
			want:    `references unknown event "inbound.telegram.message" for provider "telegram"`,
		},
		{
			name:    "raw event",
			imports: []runtimecontracts.ProviderTriggerEventImport{{Provider: "telegram", Event: "inbound.telegram"}},
			catalog: true,
			want:    `event "inbound.telegram" is a raw provider event`,
		},
		{
			name:    "catalog unavailable",
			imports: []runtimecontracts.ProviderTriggerEventImport{{Provider: "telegram", Event: "inbound.telegram.text_message"}},
			want:    "requires a verified provider-trigger catalog",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, catalog := schemaOnlyTelegramDeclarationSource(t)
			bundle, ok := semanticview.Bundle(source)
			if !ok || len(bundle.PackageTree) == 0 {
				t.Fatal("fixture package tree missing")
			}
			bundle.PackageTree[0].Manifest.ProviderTriggerEvents.Imports = append([]runtimecontracts.ProviderTriggerEventImport(nil), test.imports...)
			if !test.catalog {
				catalog = nil
			}
			_, err := SourceWithProviderTriggerEvents(source, catalog)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("SourceWithProviderTriggerEvents error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestSourceWithProviderTriggerEventsDeduplicatesMatchingImportAndIngress(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram.text_message")
	bundle, ok := semanticview.Bundle(source)
	if !ok || len(bundle.PackageTree) == 0 {
		t.Fatal("fixture package tree missing")
	}
	bundle.PackageTree[0].Manifest.ProviderTriggerEvents = runtimecontracts.ProviderTriggerEventImports{Imports: []runtimecontracts.ProviderTriggerEventImport{
		{Provider: "telegram", Event: "inbound.telegram.text_message"},
	}}
	wrapper, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	composed := wrapper.(providerTriggerEventSource)
	if _, ok := composed.imported["inbound.telegram.text_message"]; !ok {
		t.Fatal("matching explicit and ingress declarations lost normalized schema")
	}
	authorizations := wrapper.SemanticCapabilities().ProviderTriggerTargetFreeAuthorizations()
	seen := map[string]int{}
	for _, authorization := range authorizations {
		seen[authorization.Event()]++
	}
	if len(authorizations) != 2 || seen["inbound.telegram.text_message"] != 1 || seen["inbound.telegram.callback_action"] != 1 {
		t.Fatalf("ingress authorization = %#v, want one authorization per normalized pack event", authorizations)
	}
}

func TestImportedProviderEventReadbacksAreMutationIsolated(t *testing.T) {
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
		changed := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaBoolean)
		field.ExactSchema = &changed
		entry.Payload.Properties[fieldName] = field
	}
	assertFresh := func(label string) {
		entry, ok := wrapped.EventEntry(eventName)
		if !ok {
			t.Fatalf("%s: imported event missing", label)
		}
		field := entry.Payload.Properties[fieldName]
		if field.ExactSchema == nil || field.ExactSchema.Kind() != runtimecontracts.ToolSchemaString || field.ExactSchema.Pattern() == "changed" {
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

func TestProviderTriggerCatalogRejectsDifferentPackEventOwnership(t *testing.T) {
	_, catalog := schemaOnlyTelegramDeclarationSource(t)
	telegram, ok := catalog.EntryByProvider("telegram")
	if !ok {
		t.Fatal("Telegram catalog entry missing")
	}
	competing := telegram
	competing.Identity.ID = "provider.telegram_alt"
	competing.Identity.ManifestHash = "sha256:" + strings.Repeat("c", 64)
	competing.Identity.Provenance = "test:provider.telegram_alt"
	competing.Source = "test competing pack provider.telegram_alt"
	_, err := providertriggers.NewCatalogSnapshot(telegram, competing)
	if err == nil || !strings.Contains(err.Error(), `duplicate provider trigger manifest for "telegram"`) ||
		!strings.Contains(err.Error(), "test competing pack provider.telegram_alt") {
		t.Fatalf("cross-pack collision error = %v", err)
	}
}

func TestSourceWithProviderTriggerEvents_HarnessInputIsNotIngress(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram")
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("fixture bundle missing")
	}
	flow, ok := bundle.FlowViewByID("coordinator")
	if !ok || len(flow.Schema.Pins.Inputs.EventPins) != 1 {
		t.Fatal("coordinator typed input pin missing")
	}
	flow.Schema.Pins.Inputs.EventPins[0].Source = runtimecontracts.FlowInputPinSourceHarness
	schema := bundle.FlowSchemas["coordinator"]
	schema.Pins.Inputs.EventPins[0].Source = runtimecontracts.FlowInputPinSourceHarness
	bundle.FlowSchemas["coordinator"] = schema
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatalf("compile harness input semantics: %v", err)
	}

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
	authorized := wrapped.SemanticCapabilities().ProviderTriggerTargetFreeAuthorizations()
	if len(authorized) == 0 {
		t.Fatal("provider trigger source does not expose its target-free event authority")
	}
	graph := runtimepinrouting.CompileConnectGraph(wrapped)
	plans, issues := graph.Plans(), graph.Issues()
	if len(issues) != 0 {
		t.Fatalf("target-free route plan issues = %#v", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("target-free route plans = %#v, want one normalized input plan", plans)
	}
	plan := plans[0]
	if plan.SourceEndpoint().Readback().ResolvedEvent != "inbound.telegram.text_message" || plan.ReceiverEndpoint().Readback().FlowID != "coordinator" || plan.ReceiverEndpoint().Readback().Pin == "" {
		t.Fatalf("target-free normalized route plan = %#v", plan)
	}

	routingSource, err := events.NewExternalIngressRoutingSource("coordinator", "provider-event", events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	rawEvent, err := runtimepinrouting.AdmitSourceEvent("inbound.telegram", routingSource)
	if err != nil {
		t.Fatal(err)
	}
	if rawPlans := graph.MatchingSourceEvent(rawEvent); len(rawPlans) != 0 {
		t.Fatalf("raw standing event acquired target-free route plans=%#v", rawPlans)
	}
}

func TestW2ProviderTriggerImportBindsCompiledInputPinOnce(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram.text_message")
	basePin, ok := source.FlowInputEventPin("coordinator", "inbound.telegram.text_message")
	if !ok {
		t.Fatal("base provider input pin is unavailable")
	}
	if _, ownsSchema := basePin.ProducerEventSchema(); ownsSchema {
		t.Fatal("provider input pin owned a schema before the provider catalog was admitted")
	}

	wrapped, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	bound, ok := wrapped.FlowInputEventPin("coordinator", "inbound.telegram.text_message")
	if !ok {
		t.Fatal("provider input pin is unavailable after catalog admission")
	}
	schema, ownsSchema := bound.ProducerEventSchema()
	if !ownsSchema || schema.Classification() != runtimecontracts.CompiledEventSchemaImported || schema.EventName() != bound.EventType() {
		t.Fatalf("bound provider input schema = (%#v, %v), pin=%#v", schema, ownsSchema, bound)
	}
	resolution := semanticview.ResolveEventSchema(wrapped, "coordinator", "inbound.telegram.text_message")
	if !resolution.HasStructural || !resolution.HasCompiled || !resolution.HasClassification || resolution.Classification != runtimecontracts.CompiledEventSchemaImported {
		t.Fatalf("imported payload reader schema resolution = %#v", resolution)
	}
	globalResolution := semanticview.ResolveEventSchema(wrapped, "", "inbound.telegram.text_message")
	if !globalResolution.HasStructural || !globalResolution.HasCompiled || !globalResolution.HasClassification || globalResolution.Classification != runtimecontracts.CompiledEventSchemaImported {
		t.Fatalf("target-free imported payload schema resolution = %#v", globalResolution)
	}
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(payloadAdmissionTestBundleHash)
	if err != nil {
		t.Fatalf("bundle source fact: %v", err)
	}
	payload := []byte(`{"conversation_reference":"12345","conversation_scope":"direct","external_account_reference":"67890","provider_message_reference":1,"text":"hello"}`)
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), "inbound.telegram.text_message", "telegram", "", payload, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	admission, err := NewRuntimePayloadAdmitter(nil, wrapped, fact)(context.Background(), event, "telegram-ingress")
	if err != nil {
		t.Fatalf("admit imported provider payload: %v", err)
	}
	if admission.Binding().SchemaClass() != events.PayloadSchemaImported {
		t.Fatalf("imported provider payload schema class = %q", admission.Binding().SchemaClass())
	}
	if bound.Digest() == "" || bound.Digest() == basePin.Digest() {
		t.Fatalf("bound provider input digest = %q, base=%q", bound.Digest(), basePin.Digest())
	}

	pins := wrapped.FlowInputEventPins("coordinator")
	pins[0] = runtimecontracts.CompiledFlowInputPin{}
	again, ok := wrapped.FlowInputEventPin("coordinator", "inbound.telegram.text_message")
	if !ok || again.Digest() != bound.Digest() {
		t.Fatalf("compiled provider input readback was mutable: (%#v, %v)", again, ok)
	}
	acceptance := schema.AcceptanceSchema()
	acceptance["required"] = []string{"changed"}
	againSchema, _ := again.ProducerEventSchema()
	required, ok := againSchema.AcceptanceSchema()["required"].([]string)
	if !ok || strings.Join(required, ",") == "changed" {
		t.Fatal("compiled provider schema readback mutation leaked into the owner")
	}
}

func TestW2ProviderTriggerImportBindsIntrinsicProjectionAfterProducerSchema(t *testing.T) {
	for _, tc := range []struct {
		name string
		mint canonicalrouting.CreateMint
	}{
		{name: "generated_uuid", mint: canonicalrouting.CreateMintUUID},
		{name: "event_id", mint: canonicalrouting.CreateMintEventID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, catalog := importedSyntheticProjectionSource(t, tc.mint, false)
			flowID, basePin := importedSyntheticProjectionPin(t, source)
			if _, present := basePin.ProducerEventSchema(); present {
				t.Fatal("schema-only provider pin bound producer schema before catalog composition")
			}
			if _, present := basePin.ReceiverEventSchema(); present {
				t.Fatal("schema-only provider pin derived receiver schema before producer binding")
			}
			projection, present := basePin.Projection()
			if !present || projection.Field != "chat_id" {
				t.Fatalf("staged intrinsic projection = (%#v, %t)", projection, present)
			}

			wrapped, err := SourceWithProviderTriggerEvents(source, catalog)
			if err != nil {
				t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
			}
			bound, ok := wrapped.FlowInputEventPin(flowID, "inbound.telegram.text_message")
			if !ok {
				t.Fatal("bound schema-only provider input pin is unavailable")
			}
			producer, producerOK := bound.ProducerEventSchema()
			receiver, receiverOK := bound.ReceiverEventSchema()
			if !producerOK || !receiverOK || producer.Classification() != runtimecontracts.CompiledEventSchemaImported {
				t.Fatalf("bound schema roles = producer:(%#v,%t) receiver:(%#v,%t)", producer, producerOK, receiver, receiverOK)
			}
			if producer.AcceptanceSchemaDigest() == receiver.AcceptanceSchemaDigest() || !compiledSchemaHasField(receiver, "chat_id") || compiledSchemaHasField(producer, "chat_id") {
				t.Fatalf("bound schema roles did not preserve producer and projected receiver: producer=%#v receiver=%#v", producer.Fields(), receiver.Fields())
			}
			graph := runtimepinrouting.CompileConnectGraph(wrapped)
			if issues := graph.Issues(); len(issues) != 0 {
				t.Fatalf("provider target-free route issues = %#v", issues)
			}
			var matched *runtimepinrouting.ConnectRoutePlanReadback
			for _, plan := range graph.Plans() {
				readback := plan.Readback()
				if readback.Receiver.FlowID == flowID && readback.Receiver.LocalEvent == "inbound.telegram.text_message" {
					matched = &readback
					break
				}
			}
			if matched == nil || matched.ProducerEvent == nil || matched.ReceiverEvent == nil || matched.ProducerEvent.AcceptanceSchemaDigest == matched.ReceiverEvent.AcceptanceSchemaDigest {
				t.Fatalf("schema-only provider target-free plan = %#v, want distinct producer/receiver evidence", matched)
			}
		})
	}
}

func TestW2ProviderTriggerImportRejectsIntrinsicProjectionCollisionAtBinding(t *testing.T) {
	for _, tc := range []struct {
		name string
		mint canonicalrouting.CreateMint
	}{
		{name: "generated_uuid", mint: canonicalrouting.CreateMintUUID},
		{name: "event_id", mint: canonicalrouting.CreateMintEventID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, catalog := importedSyntheticProjectionSource(t, tc.mint, true)
			if _, err := SourceWithProviderTriggerEvents(source, catalog); err == nil || !strings.Contains(err.Error(), "field conversation_reference conflicts with receiver-owned resolution projection") {
				t.Fatalf("SourceWithProviderTriggerEvents error = %v, want imported projection collision", err)
			}
		})
	}
}

func importedSyntheticProjectionSource(t testing.TB, mint canonicalrouting.CreateMint, collision bool) (semanticview.Source, *providertriggers.CatalogSnapshot) {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyTelegramAgentImportedSyntheticProjection(t, mint, collision)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(
		repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot),
		runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory},
	)
	if err != nil {
		t.Fatalf("load schema-only imported projection fixture: %v", err)
	}
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		t.Fatalf("load provider-trigger catalog: %v", err)
	}
	return semanticview.Wrap(bundle), projection.ProviderTriggers
}

func importedSyntheticProjectionPin(t testing.TB, source semanticview.Source) (string, runtimecontracts.CompiledFlowInputPin) {
	t.Helper()
	for _, scope := range source.FlowScopes() {
		if pin, ok := source.FlowInputEventPin(scope.ID, "inbound.telegram.text_message"); ok {
			return scope.ID, pin
		}
	}
	t.Fatal("schema-only provider input pin is unavailable")
	return "", runtimecontracts.CompiledFlowInputPin{}
}

func compiledSchemaHasField(schema runtimecontracts.CompiledEventSchema, field string) bool {
	for _, candidate := range schema.Fields() {
		if candidate.Name() == field {
			return true
		}
	}
	return false
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
	if changed.Generation().Equal(catalog.Generation()) {
		t.Fatal("changed pack identity retained catalog generation")
	}

	second, err := SourceWithProviderTriggerEvents(first, changed)
	if err != nil {
		t.Fatalf("reload SourceWithProviderTriggerEvents: %v", err)
	}
	generation, base, ok := second.SemanticCapabilities().ProviderTriggerEvents()
	if !ok || !generation.Equal(changed.Generation()) {
		t.Fatalf("reloaded provider trigger source generation = %T %q", second, generation.Diagnostic())
	}
	if _, _, nested := base.SemanticCapabilities().ProviderTriggerEvents(); nested {
		t.Fatal("reload stacked a provider trigger wrapper instead of rebuilding from the base source")
	}
	if entry, ok := second.EventEntry("inbound.telegram.text_message"); !ok || entry.Source != "provider_trigger_pack_normalized" {
		t.Fatalf("reloaded normalized event entry = (%#v, %v)", entry, ok)
	}
}

func TestSchemaOnlyProviderTriggerImportRebuildsOnCatalogGenerationChange(t *testing.T) {
	source, catalog := schemaOnlyTelegramDeclarationSource(t)
	first, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("first SourceWithProviderTriggerEvents: %v", err)
	}
	entry, ok := catalog.EntryByProvider("telegram")
	if !ok {
		t.Fatal("Telegram catalog entry missing")
	}
	entry.Identity.ManifestHash = "sha256:" + strings.Repeat("b", 64)
	changed, err := providertriggers.NewCatalogSnapshot(entry)
	if err != nil {
		t.Fatalf("NewCatalogSnapshot: %v", err)
	}
	second, err := SourceWithProviderTriggerEvents(first, changed)
	if err != nil {
		t.Fatalf("reload SourceWithProviderTriggerEvents: %v", err)
	}
	provenance := second.SemanticCapabilities().ProviderTriggerEventProvenance()
	if len(provenance) != 1 || provenance[0].ManifestHash != entry.Identity.ManifestHash || !provenance[0].Generation.Equal(changed.Generation()) {
		t.Fatalf("reloaded schema-only provenance = %#v", provenance)
	}
	if authorizations := second.SemanticCapabilities().ProviderTriggerTargetFreeAuthorizations(); len(authorizations) != 0 {
		t.Fatalf("reloaded schema-only import granted provider authority: %#v", authorizations)
	}
}

func TestProviderTriggerCapabilitiesRemainVisibleThroughRuntimeToolOverlay(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram.text_message")
	imported, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	overlaid, err := semanticview.WithRuntimeTools(imported, map[string]runtimecontracts.ToolSchemaEntry{
		"channel.ops.deliver": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory("channel_operation"), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("channel")), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject))),
	})
	if err != nil {
		t.Fatalf("WithRuntimeTools: %v", err)
	}
	revalidated, err := SourceWithProviderTriggerEvents(overlaid, catalog)
	if err != nil {
		t.Fatalf("same-generation revalidation through overlay: %v", err)
	}
	generation, _, ok := revalidated.SemanticCapabilities().ProviderTriggerEvents()
	if !ok || !generation.Equal(catalog.Generation()) {
		t.Fatalf("provider trigger generation hidden through overlay: capability=%v generation=%v", ok, generation.Diagnostic())
	}
	authorized := revalidated.SemanticCapabilities().ProviderTriggerTargetFreeAuthorizations()
	if len(authorized) == 0 {
		t.Fatal("target-free provider authority hidden through overlay")
	}
}

func schemaOnlyTelegramDeclarationSource(t testing.TB) (semanticview.Source, *providertriggers.CatalogSnapshot) {
	t.Helper()
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram.text_message")
	bundle, ok := semanticview.Bundle(source)
	if !ok || len(bundle.PackageTree) == 0 {
		t.Fatal("fixture package tree missing")
	}
	for packageIndex := range bundle.PackageTree {
		pkg := &bundle.PackageTree[packageIndex]
		for flowIndex := range pkg.Manifest.Flows {
			pkg.Manifest.Flows[flowIndex].Ingress = nil
			pkg.Manifest.Flows[flowIndex].Activation = ""
			pkg.Manifest.Flows[flowIndex].Mode = runtimecontracts.FlowModeTemplate
		}
	}
	if flow, exists := bundle.FlowViewByID("coordinator"); exists {
		flow.Schema.Mode = runtimecontracts.FlowModeTemplate
	}
	if schema, exists := bundle.FlowSchemas["coordinator"]; exists {
		schema.Mode = runtimecontracts.FlowModeTemplate
		bundle.FlowSchemas["coordinator"] = schema
	}
	bundle.PackageTree[0].Manifest.ProviderTriggerEvents = runtimecontracts.ProviderTriggerEventImports{Imports: []runtimecontracts.ProviderTriggerEventImport{
		{Provider: "telegram", Event: "inbound.telegram.text_message"},
	}}
	return source, catalog
}
