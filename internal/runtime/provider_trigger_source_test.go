package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packmodel"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"

	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"

	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

func TestSourceWithProviderTriggerEventsImportsEffectivePackSchemasWithoutAuthoredOwnership(t *testing.T) {
	members := map[string][]byte{
		"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":               []byte("stages: []\n"),
		"coordinator/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"coordinator/schema.yaml":   []byte("stages: []\npins: {inputs: [inbound.telegram.raw]}\n"),
	}
	for _, entry := range packfixture.EmbeddedInventory(t).Entries() {
		if entry.ID() != "telegram" {
			continue
		}
		members["coordinator/telegram/manifest.yaml"] = entry.ManifestBody()
		for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
			if body, present := entry.Provider().Body(role); present {
				members["coordinator/telegram/"+role.FileName()] = body.Bytes()
			}
		}
		break
	}
	if members["coordinator/telegram/manifest.yaml"] == nil {
		t.Fatal("embedded Telegram provider is unavailable")
	}
	captured, _, _ := loadS1EffectiveProviderScope(t, members)
	projection, err := packadmission.FromBundle(captured)
	if err != nil {
		t.Fatal(err)
	}
	source, catalog := semanticview.Wrap(captured), projection.ProviderTriggers
	wrapped, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	entry, ok := wrapped.EventEntry("inbound.telegram.text_message")
	if !ok || entry.Payload.Properties["text"].Type != "text" {
		t.Fatalf("normalized event entry = (%#v, %v)", entry, ok)
	}
	if _, authored := wrapped.AuthoredEventEntries()["inbound.telegram.text_message"]; authored {
		t.Fatal("pack event was misclassified as authored")
	}
	resolved, name, ok := wrapped.ResolveFlowEventCatalogEntry("coordinator", "inbound.telegram.text_message")
	wantFields := []string{"command_invocation", "conversation_reference", "conversation_scope", "external_account_reference", "provider_message_reference", "reply_to_message_reference", "text"}
	if !ok || name != "inbound.telegram.text_message" || len(resolved.Payload.Properties) != len(wantFields) {
		t.Fatalf("flow catalog resolution = (%#v, %q, %v)", resolved, name, ok)
	}
	for _, field := range wantFields {
		spec, exists := resolved.Payload.Properties[field]
		wantType := "text"
		if field == "provider_message_reference" || field == "reply_to_message_reference" {
			wantType = "integer"
		} else if field == "command_invocation" {
			wantType = "object"
		}
		if !exists || spec.Type != wantType {
			t.Fatalf("flow catalog field %q = (%#v, %v), want %s", field, spec, exists, wantType)
		}
	}
	wantRequired := []string{"conversation_reference", "conversation_scope", "external_account_reference", "provider_message_reference", "text"}
	if strings.Join(resolved.Payload.Required, ",") != strings.Join(wantRequired, ",") {
		t.Fatalf("flow catalog required fields = %q, want %q", resolved.Payload.Required, wantRequired)
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

func TestProviderTriggerLoopRevisionUsesEffectiveImportedSchema(t *testing.T) {
	members := map[string][]byte{
		"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":               []byte("stages: []\n"),
		"coordinator/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"coordinator/schema.yaml":   []byte("stages: []\n"),
	}
	for _, entry := range packfixture.EmbeddedInventory(t).Entries() {
		if entry.ID() != "telegram" {
			continue
		}
		members["coordinator/telegram/manifest.yaml"] = entry.ManifestBody()
		for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
			if body, present := entry.Provider().Body(role); present {
				members["coordinator/telegram/"+role.FileName()] = body.Bytes()
			}
		}
		break
	}
	if members["coordinator/telegram/manifest.yaml"] == nil {
		t.Fatal("embedded Telegram provider is unavailable")
	}
	captured, _, _ := loadS1EffectiveProviderScope(t, members)
	projection, err := packadmission.FromBundle(captured)
	if err != nil {
		t.Fatal(err)
	}
	source, catalog := semanticview.Wrap(captured), projection.ProviderTriggers
	imported, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatal(err)
	}
	const event = "inbound.telegram.text_message"
	bundle, ok := semanticview.Bundle(imported)
	if !ok {
		t.Fatal("imported source lost its bundle")
	}
	if _, found := runtimecontracts.ResolveEventFieldType(bundle, "coordinator", event, "text"); found {
		t.Fatal("provider import unexpectedly resides in the underlying bundle")
	}
	resolution := semanticview.ResolveEventSchema(imported, "coordinator", event)
	if !resolution.HasCompiled || resolution.Classification != runtimecontracts.CompiledEventSchemaImported {
		t.Fatalf("effective schema is not an admitted provider import: %#v", resolution)
	}
	if !runtimecontracts.RequiresLoopRevision(resolution.Schema, "text") {
		t.Fatal("required imported text field was refused as loop revision evidence")
	}
	for _, field := range []string{"provider_message_reference", "reply_to_message_reference", "command_invocation", "missing"} {
		if runtimecontracts.RequiresLoopRevision(resolution.Schema, field) {
			t.Fatalf("non-text, optional or absent imported field %q admitted", field)
		}
	}
}

func TestProviderSchemaOnlyImportRetainsBindingWithoutInputPin(t *testing.T) {
	members := map[string][]byte{
		"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":               []byte("stages: []\n"),
		"coordinator/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"coordinator/schema.yaml":   []byte("stages: []\n"),
	}
	for _, entry := range packfixture.EmbeddedInventory(t).Entries() {
		if entry.ID() != "telegram" {
			continue
		}
		members["coordinator/telegram/manifest.yaml"] = entry.ManifestBody()
		for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
			if body, present := entry.Provider().Body(role); present {
				members["coordinator/telegram/"+role.FileName()] = body.Bytes()
			}
		}
		break
	}
	if members["coordinator/telegram/manifest.yaml"] == nil {
		t.Fatal("embedded Telegram provider is unavailable")
	}
	captured, _, _ := loadS1EffectiveProviderScope(t, members)
	projection, err := packadmission.FromBundle(captured)
	if err != nil {
		t.Fatal(err)
	}
	source, catalog := semanticview.Wrap(captured), projection.ProviderTriggers
	wrapped, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatal(err)
	}
	const eventName = "inbound.telegram.text_message"
	if _, found := wrapped.FlowInputEventPin("coordinator", eventName); found {
		t.Fatal("schema-only import invented an input pin")
	}
	compiled, found, err := wrapped.ResolveEffectiveCompiledFlowEventSchema("coordinator", eventName)
	if err != nil || !found || compiled.Classification() != runtimecontracts.CompiledEventSchemaImported {
		t.Fatalf("schema-only import lost exact declaration evidence: found=%v err=%v", found, err)
	}
	if _, found, err := wrapped.ResolveEffectiveCompiledFlowEventSchema("sibling", eventName); err != nil || found {
		t.Fatalf("schema-only import crossed declaration scope: found=%v err=%v", found, err)
	}
	if len(wrapped.SemanticCapabilities().ProviderTriggerOutputBindings()) != 0 {
		t.Fatal("schema-only import granted provider publication authority")
	}
}

func TestProviderCompiledSchemaReadbackDoesNotGrantReceiverOwnership(t *testing.T) {
	members := map[string][]byte{
		"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":               []byte("stages: []\n"),
		"coordinator/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"coordinator/schema.yaml":   []byte("stages: []\npins: {inputs: [inbound.telegram.text_message]}\n"),
	}
	for _, entry := range packfixture.EmbeddedInventory(t).Entries() {
		if entry.ID() != "telegram" {
			continue
		}
		members["coordinator/telegram/manifest.yaml"] = entry.ManifestBody()
		for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
			if body, present := entry.Provider().Body(role); present {
				members["coordinator/telegram/"+role.FileName()] = body.Bytes()
			}
		}
		break
	}
	if members["coordinator/telegram/manifest.yaml"] == nil {
		t.Fatal("embedded Telegram provider is unavailable")
	}
	captured, _, _ := loadS1EffectiveProviderScope(t, members)
	projection, err := packadmission.FromBundle(captured)
	if err != nil {
		t.Fatal(err)
	}
	source, catalog := semanticview.Wrap(captured), projection.ProviderTriggers
	wrapped, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatal(err)
	}
	const eventName = "inbound.telegram.text_message"
	compiled, ok, err := wrapped.ResolveEffectiveCompiledFlowEventSchema("coordinator", eventName)
	if err != nil || !ok || compiled.Classification() != runtimecontracts.CompiledEventSchemaImported {
		t.Fatalf("imported receiver binding: found=%v err=%v", ok, err)
	}
	classify := func(flow string) semanticview.AuthoredSubscriptionAdmission {
		return semanticview.ClassifyAuthoredSubscription(wrapped, semanticview.AuthoredSubscriptionRequest{
			ConsumerKind: semanticview.AuthoredSubscriptionConsumerNode, ConsumerID: "reader",
			FlowID: flow, FlowPath: flow, Authored: eventName,
			LocalEvents: map[string]struct{}{eventName: {}}, InputEvents: []string{eventName},
		})
	}
	if admission := classify("coordinator"); !admission.Admitted() {
		t.Fatalf("exact imported receiver rejected: %#v", admission)
	}
	for _, flow := range []string{"", ".", "sibling", "coordinator/unknown"} {
		if _, found, err := wrapped.ResolveEffectiveCompiledFlowEventSchema(flow, eventName); err != nil || found {
			t.Fatalf("undeclaring flow %q acquired imported receiver binding: found=%v err=%v", flow, found, err)
		}
		if admission := classify(flow); admission.Admitted() {
			t.Fatalf("undeclaring flow %q borrowed imported receiver authority: %#v", flow, admission)
		}
	}
	// Target-free catalog metadata is not execution-coordinate admission.
	readbackSchema, readbackFound := wrapped.SemanticCapabilities().ProviderTriggerSchemaReadback(eventName)
	_, structural := readbackSchema.StructuralType()
	if !readbackFound || !structural || readbackSchema.Classification() != runtimecontracts.CompiledEventSchemaImported {
		t.Fatalf("target-free provider catalog readback lost compiled evidence: %#v", readbackSchema)
	}
	rootSchema := semanticview.ResolveEventSchema(wrapped, ".", eventName)
	if rootSchema.HasSchema || rootSchema.HasCompiled || rootSchema.HasStructural {
		t.Fatalf("explicit root borrowed descendant-only imported schema: %#v", rootSchema)
	}
	// Neither a mutable diagnostic catalog nor returned schema maps may change
	// the retained acceptance binding or turn an import into authored data.
	composed := wrapped.(providerTriggerEventSource)
	delete(composed.byFlow["coordinator"], eventName)
	delete(composed.imported, eventName)
	readback := compiled.AcceptanceSchema()
	delete(readback, "properties")
	after, found, err := wrapped.ResolveEffectiveCompiledFlowEventSchema("coordinator", eventName)
	if err != nil || !found || after.AcceptanceSchemaDigest() != compiled.AcceptanceSchemaDigest() {
		t.Fatalf("diagnostic mutation changed receiver binding: found=%v err=%v", found, err)
	}
	if after.Classification() != runtimecontracts.CompiledEventSchemaImported {
		t.Fatal("provider schema acquired authored classification")
	}
	if admission := classify("coordinator"); !admission.Admitted() {
		t.Fatalf("diagnostic mutation revoked exact compiled receiver: %#v", admission)
	}
}

func TestImportedProviderEventReadbacksAreMutationIsolated(t *testing.T) {
	members := map[string][]byte{
		"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":               []byte("stages: []\n"),
		"coordinator/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"coordinator/schema.yaml":   []byte("stages: []\npins: {inputs: [inbound.telegram.text_message]}\n"),
	}
	for _, entry := range packfixture.EmbeddedInventory(t).Entries() {
		if entry.ID() != "telegram" {
			continue
		}
		members["coordinator/telegram/manifest.yaml"] = entry.ManifestBody()
		for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
			if body, present := entry.Provider().Body(role); present {
				members["coordinator/telegram/"+role.FileName()] = body.Bytes()
			}
		}
		break
	}
	if members["coordinator/telegram/manifest.yaml"] == nil {
		t.Fatal("embedded Telegram provider is unavailable")
	}
	captured, _, _ := loadS1EffectiveProviderScope(t, members)
	projection, err := packadmission.FromBundle(captured)
	if err != nil {
		t.Fatal(err)
	}
	source, catalog := semanticview.Wrap(captured), projection.ProviderTriggers
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
	for _, scope := range wrapped.FlowScopes() {
		if scoped, exists := scope.Events[eventName]; exists {
			mutate(scoped)
			mutatedScope = true
		}
	}
	if !mutatedScope {
		t.Fatal("FlowScopes: imported event missing")
	}
	assertFresh("FlowScopes")
}

func TestProviderTriggerInputWithoutConnectionDoesNotAcquireCrossFlowPlan(t *testing.T) {
	members := map[string][]byte{
		"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":               []byte("stages: []\n"),
		"coordinator/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"coordinator/schema.yaml":   []byte("stages: []\npins: {inputs: [inbound.telegram.text_message]}\n"),
	}
	for _, entry := range packfixture.EmbeddedInventory(t).Entries() {
		if entry.ID() != "telegram" {
			continue
		}
		members["coordinator/telegram/manifest.yaml"] = entry.ManifestBody()
		for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
			if body, present := entry.Provider().Body(role); present {
				members["coordinator/telegram/"+role.FileName()] = body.Bytes()
			}
		}
		break
	}
	if members["coordinator/telegram/manifest.yaml"] == nil {
		t.Fatal("embedded Telegram provider is unavailable")
	}
	captured, _, _ := loadS1EffectiveProviderScope(t, members)
	projection, err := packadmission.FromBundle(captured)
	if err != nil {
		t.Fatal(err)
	}
	source, catalog := semanticview.Wrap(captured), projection.ProviderTriggers
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("fixture bundle flow declaration is unavailable")
	}
	// This unit proof isolates lowering for a non-template receiver. The served
	// proof covers target-free select-or-create materialization.
	flow, ok := bundle.FlowViewByID("coordinator")
	if !ok {
		t.Fatal("fixture coordinator flow is unavailable")
	}
	flow.Schema.Instance = runtimecontracts.TemplateInstanceField{}
	wrapped, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	authorized := wrapped.SemanticCapabilities().ProviderTriggerOutputBindings()["coordinator"]
	if len(authorized) == 0 {
		t.Fatal("provider trigger source does not expose its target-free event authority")
	}
	graph := runtimepinrouting.CompileConnectGraph(wrapped)
	plans, issues := graph.Plans(), graph.Issues()
	if len(issues) != 0 {
		t.Fatalf("target-free route plan issues = %#v", issues)
	}
	if len(plans) != 0 {
		t.Fatalf("unconnected provider input acquired executable plans: %#v", plans)
	}

	routingSource, err := events.NewExternalIngressRoutingSource("coordinator", events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	rawEvent, err := runtimepinrouting.AdmitSourceEvent("inbound.telegram.raw", routingSource)
	if err != nil {
		t.Fatal(err)
	}
	if rawPlans := graph.MatchingSourceEvent(rawEvent); len(rawPlans) != 0 {
		t.Fatalf("raw standing event acquired target-free route plans=%#v", rawPlans)
	}
}

func TestW2ProviderTriggerImportBindsCompiledInputPinOnce(t *testing.T) {
	members := map[string][]byte{
		"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":               []byte("stages: []\n"),
		"coordinator/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"coordinator/schema.yaml":   []byte("stages: []\npins: {inputs: [inbound.telegram.text_message]}\n"),
	}
	for _, entry := range packfixture.EmbeddedInventory(t).Entries() {
		if entry.ID() != "telegram" {
			continue
		}
		members["coordinator/telegram/manifest.yaml"] = entry.ManifestBody()
		for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
			if body, present := entry.Provider().Body(role); present {
				members["coordinator/telegram/"+role.FileName()] = body.Bytes()
			}
		}
		break
	}
	if members["coordinator/telegram/manifest.yaml"] == nil {
		t.Fatal("embedded Telegram provider is unavailable")
	}
	captured, _, _ := loadS1EffectiveProviderScope(t, members)
	projection, err := packadmission.FromBundle(captured)
	if err != nil {
		t.Fatal(err)
	}
	source, catalog := semanticview.Wrap(captured), projection.ProviderTriggers
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
	if globalResolution.HasSchema || globalResolution.HasStructural || globalResolution.HasCompiled || globalResolution.HasClassification {
		t.Fatalf("empty coordinate gained an imported payload schema = %#v", globalResolution)
	}
	metadata, found := wrapped.SemanticCapabilities().ProviderTriggerSchemaReadback("inbound.telegram.text_message")
	_, structural := metadata.StructuralType()
	if !found || !structural || metadata.Classification() != runtimecontracts.CompiledEventSchemaImported || metadata.EventName() != schema.EventName() || metadata.AcceptanceSchemaDigest() != schema.AcceptanceSchemaDigest() {
		t.Fatalf("target-free provider metadata lost its bound producer schema: found=%t structural=%t", found, structural)
	}
	const producerFlow = "coordinator"
	authorization, authorized := wrapped.SemanticCapabilities().ProviderTriggerOutputAuthorization(producerFlow, "inbound.telegram.text_message")
	if !authorized || authorization.Provider() != "telegram" || authorization.Event() != "inbound.telegram.text_message" {
		t.Fatal("fixture has no exact admitted provider/event output authority")
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle.SourceArtifact == nil {
		t.Fatal("provider fixture requires its admitted source artifact")
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatalf("bundle source fact: %v", err)
	}
	payload := []byte(`{"conversation_reference":"12345","conversation_scope":"direct","external_account_reference":"67890","provider_message_reference":1.0,"text":"hello"}`)
	producer, err := events.NewExternalIngressRoutingSource(producerFlow, events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.RunCreatingRootIngressWithRoutingSource(uuid.NewString(), "inbound.telegram.text_message", "telegram", "", payload, 0, uuid.NewString(), "", events.EventEnvelope{}, producer, time.Now().UTC())
	// The declaration owns the producer coordinate; no receiver target is supplied.
	admit := NewRuntimePayloadAdmitter(nil, wrapped, fact)
	if absent, err := admit(context.Background(), event, events.AuthoredPayloadProducer("")); err == nil || absent.Valid() {
		t.Fatal("empty producer coordinate gained imported payload admission")
	}
	admission, err := admit(context.Background(), event, events.AuthoredPayloadProducer(producerFlow))
	if err != nil {
		t.Fatalf("admit imported provider payload: %v", err)
	}
	if admission.Binding().SchemaClass() != events.PayloadSchemaImported {
		t.Fatalf("imported provider payload schema class = %q", admission.Binding().SchemaClass())
	}
	var executionPayload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(admission.Payload(), &executionPayload); err != nil {
		t.Fatal(err)
	}
	if executionPayload["provider_message_reference"] != json.Number("1.0") {
		t.Fatalf("imported schema coerced execution kind: %s", admission.Payload())
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

func compiledSchemaHasField(schema runtimecontracts.CompiledEventSchema, field string) bool {
	for _, candidate := range schema.Fields() {
		if candidate.Name() == field {
			return true
		}
	}
	return false
}

func TestProviderTriggerCapabilitiesRemainVisibleThroughRuntimeToolOverlay(t *testing.T) {
	members := map[string][]byte{
		"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":               []byte("stages: []\n"),
		"coordinator/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"coordinator/schema.yaml":   []byte("stages: []\npins: {inputs: [inbound.telegram.text_message]}\n"),
	}
	for _, entry := range packfixture.EmbeddedInventory(t).Entries() {
		if entry.ID() != "telegram" {
			continue
		}
		members["coordinator/telegram/manifest.yaml"] = entry.ManifestBody()
		for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
			if body, present := entry.Provider().Body(role); present {
				members["coordinator/telegram/"+role.FileName()] = body.Bytes()
			}
		}
		break
	}
	if members["coordinator/telegram/manifest.yaml"] == nil {
		t.Fatal("embedded Telegram provider is unavailable")
	}
	captured, _, _ := loadS1EffectiveProviderScope(t, members)
	projection, err := packadmission.FromBundle(captured)
	if err != nil {
		t.Fatal(err)
	}
	source, catalog := semanticview.Wrap(captured), projection.ProviderTriggers
	imported, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatalf("SourceWithProviderTriggerEvents: %v", err)
	}
	expectedGeneration, _, present := imported.SemanticCapabilities().ProviderTriggerEvents()
	if !present {
		t.Fatal("effective source has no provider trigger generation before overlay")
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
	if !ok || !generation.Equal(expectedGeneration) {
		t.Fatalf("provider trigger generation hidden through overlay: capability=%v generation=%v", ok, generation.Diagnostic())
	}
	authorized := revalidated.SemanticCapabilities().ProviderTriggerOutputBindings()["coordinator"]
	if len(authorized) == 0 {
		t.Fatal("target-free provider authority hidden through overlay")
	}
}
