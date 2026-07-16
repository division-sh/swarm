package providertriggers

import (
	"encoding/json"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"gopkg.in/yaml.v3"
)

func TestNormalizedEventManifestPublishesRawAndTypedFlatEvent(t *testing.T) {
	manifest := normalizedEventTestManifest()
	if err := manifest.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	delivery, err := manifest.Accept(Request{
		Target: Target{EntityID: "entity-1"},
		Payload: map[string]any{
			"update_id": json.Number("123"),
			"message": map[string]any{
				"message_id": json.Number("7"),
				"chat":       map[string]any{"id": json.Number("9007199254740993")},
				"text":       "hello",
			},
		},
	})
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if len(delivery.Events) != 2 {
		t.Fatalf("events = %#v, want raw plus normalized", delivery.Events)
	}
	if delivery.Events[0].Kind != OutputKindRaw || delivery.Events[0].Name != "inbound.telegram" {
		t.Fatalf("raw event = %#v", delivery.Events[0])
	}
	normalized := delivery.Events[1]
	if normalized.Kind != OutputKindNormalized || normalized.Name != "inbound.telegram.text_message" {
		t.Fatalf("normalized event = %#v", normalized)
	}
	if got := normalized.Payload["chat_id"]; got != "9007199254740993" {
		t.Fatalf("chat_id = %#v, want lossless text", got)
	}
	if got := normalized.Payload["message_id"]; got != json.Number("7") {
		t.Fatalf("message_id = %#v, want JSON number", got)
	}
	if got := normalized.Payload["text"]; got != "hello" {
		t.Fatalf("text = %#v", got)
	}
}

func TestCompiledPackPlanOwnsNormalizedOutputAuthorization(t *testing.T) {
	manifest := normalizedEventTestManifest()
	identity := PackIdentity{
		ID: "provider.telegram", Version: "1.0.0",
		ManifestHash: "sha256:" + strings.Repeat("c", 64), Provenance: "platform",
	}
	catalog, err := NewCatalogSnapshot(CatalogEntry{Manifest: manifest, Identity: identity, Source: "test"})
	if err != nil {
		t.Fatalf("NewCatalogSnapshot: %v", err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{
		Alias: "chat", Provider: "telegram",
		Declaration: AdmissionDeclaration{Acknowledge: UnsignedWebhookAcknowledgement},
	})
	if err != nil {
		t.Fatalf("CompileAdmission: %v", err)
	}
	delivery, err := plan.Accept(Request{
		Target: Target{EntityID: "entity-1"},
		Payload: map[string]any{
			"update_id": json.Number("123"),
			"message": map[string]any{
				"message_id": json.Number("7"),
				"chat":       map[string]any{"id": json.Number("42")},
				"text":       "hello",
			},
		},
	})
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if len(delivery.Events) != 2 || !delivery.Events[0].Authorization.Empty() {
		t.Fatalf("raw output authorization = %#v, want empty", delivery.Events)
	}
	got := delivery.Events[1].Authorization
	if !got.Valid() || got.Provider != "telegram" || got.Event != "inbound.telegram.text_message" ||
		got.PackID != identity.ID || got.PackVersion != identity.Version ||
		got.ManifestHash != identity.ManifestHash || got.GenerationID != catalog.GenerationID() {
		t.Fatalf("normalized output authorization = %#v, want compiled pack identity/generation", got)
	}
	if err := catalog.VerifyProviderOutputAuthorization(got); err != nil {
		t.Fatalf("VerifyProviderOutputAuthorization: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*runtimeprovideroutput.Authorization)
	}{
		{name: "provider", mutate: func(a *runtimeprovideroutput.Authorization) { a.Provider = "telegram-stale" }},
		{name: "event", mutate: func(a *runtimeprovideroutput.Authorization) { a.Event = "inbound.telegram.edited_message" }},
		{name: "pack id", mutate: func(a *runtimeprovideroutput.Authorization) { a.PackID = "provider.telegram.stale" }},
		{name: "pack version", mutate: func(a *runtimeprovideroutput.Authorization) { a.PackVersion = "0.9.0" }},
		{name: "manifest hash", mutate: func(a *runtimeprovideroutput.Authorization) { a.ManifestHash = "sha256:" + strings.Repeat("d", 64) }},
		{name: "catalog generation", mutate: func(a *runtimeprovideroutput.Authorization) { a.GenerationID = "generation-stale" }},
	} {
		t.Run("rejects "+tc.name+" mismatch", func(t *testing.T) {
			stale := got
			tc.mutate(&stale)
			if err := catalog.VerifyProviderOutputAuthorization(stale); err == nil {
				t.Fatalf("VerifyProviderOutputAuthorization(%s mismatch) error = nil", tc.name)
			}
		})
	}
}

func TestNormalizedEventManifestUnmatchedPayloadPublishesRawOnly(t *testing.T) {
	manifest := normalizedEventTestManifest()
	delivery, err := manifest.Accept(Request{
		Target:  Target{EntityID: "entity-1"},
		Payload: map[string]any{"update_id": json.Number("123"), "callback_query": map[string]any{"id": "callback-1"}},
	})
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if len(delivery.Events) != 1 || delivery.Events[0].Kind != OutputKindRaw {
		t.Fatalf("events = %#v, want raw only", delivery.Events)
	}
}

func TestNormalizedEventManifestRejectsOverlappingBranchesAtLoad(t *testing.T) {
	manifest := normalizedEventTestManifest()
	manifest.NormalizedEvents = append(manifest.NormalizedEvents, NormalizedEventManifest{
		Event: "inbound.telegram.message_copy",
		Fields: map[string]NormalizedEventFieldProjection{
			"text": {From: "message.text", Schema: runtimecontracts.ToolInputSchema{Type: "string"}},
		},
	})
	err := manifest.Validate()
	if err == nil || !strings.Contains(err.Error(), "can match the same payload") || !strings.Contains(err.Error(), "when.absent") {
		t.Fatalf("Validate error = %v, want overlap teaching error", err)
	}
	manifest.NormalizedEvents[1].When.Absent = []string{"message.chat"}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("mutually exclusive manifest rejected: %v", err)
	}
}

func TestNormalizedEventPlanRejectsForcedRuntimeMultiMatch(t *testing.T) {
	manifest := normalizedEventTestManifest()
	manifest.NormalizedEvents = append(manifest.NormalizedEvents, NormalizedEventManifest{
		Event: "inbound.telegram.message_copy",
		Fields: map[string]NormalizedEventFieldProjection{
			"text": {From: "message.text", Schema: runtimecontracts.ToolInputSchema{Type: "string"}},
		},
	})
	_, err := manifest.Accept(Request{
		Target: Target{EntityID: "entity-1"},
		Payload: map[string]any{
			"update_id": json.Number("123"),
			"message": map[string]any{
				"message_id": json.Number("7"), "chat": map[string]any{"id": json.Number("42")}, "text": "hello",
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "matched multiple branches") {
		t.Fatalf("Accept error = %v, want forced runtime multi-match rejection", err)
	}
}

func TestAdmittedSemanticDigestRetainsRedactedConsumedValues(t *testing.T) {
	manifest := normalizedEventTestManifest()
	manifest.RedactKeys = []string{"text"}
	catalog, err := NewCatalogSnapshot(CatalogEntry{
		Manifest: manifest,
		Identity: PackIdentity{
			ID: "provider.telegram", Version: "1.0.0",
			ManifestHash: "sha256:" + strings.Repeat("c", 64), Provenance: "platform",
		},
		Source: "test",
	})
	if err != nil {
		t.Fatalf("NewCatalogSnapshot: %v", err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{
		Alias: "chat", Provider: "telegram",
		Declaration: AdmissionDeclaration{Acknowledge: UnsignedWebhookAcknowledgement},
	})
	if err != nil {
		t.Fatalf("CompileAdmission: %v", err)
	}
	request := Request{
		Target: Target{EntityID: "entity-1"},
		Payload: map[string]any{
			"update_id": json.Number("123"),
			"message": map[string]any{
				"message_id": json.Number("7"), "chat": map[string]any{"id": json.Number("42")}, "text": "first",
			},
		},
	}
	first, err := plan.AdmitRequest(request)
	if err != nil {
		t.Fatalf("AdmitRequest(first): %v", err)
	}
	request.Payload.(map[string]any)["message"].(map[string]any)["text"] = "second"
	second, err := plan.AdmitRequest(request)
	if err != nil {
		t.Fatalf("AdmitRequest(second): %v", err)
	}
	if first.SemanticContentDigest == second.SemanticContentDigest {
		t.Fatal("semantic digest collapsed a changed redacted value consumed by normalized projection")
	}
}

func TestNormalizedEventManifestRejectsHostileProjectionSyntax(t *testing.T) {
	for _, path := range []string{"$.message.text", "message..text", "payload.message.text", "entity.id", "message[0].text", "message.text == 'x'"} {
		t.Run(path, func(t *testing.T) {
			manifest := normalizedEventTestManifest()
			field := manifest.NormalizedEvents[0].Fields["text"]
			field.From = path
			manifest.NormalizedEvents[0].Fields["text"] = field
			if err := manifest.Validate(); err == nil {
				t.Fatalf("Validate accepted hostile path %q", path)
			}
		})
	}
}

func TestNormalizedEventManifestRejectsRetiredTypeSpelling(t *testing.T) {
	_, err := parseManifestStrict([]byte(`
provider: telegram
normalized_events:
  - event: inbound.telegram.text_message
    fields:
      text:
        from: message.text
        type: text
`))
	if err == nil || !strings.Contains(err.Error(), "field type not found") {
		t.Fatalf("parseManifestStrict error = %v, want retired type spelling rejection", err)
	}
}

func TestNormalizedEventManifestRejectsMalformedEventNamesAtLoad(t *testing.T) {
	for _, eventName := range []string{
		"inbound.telegram.",
		"inbound.telegram.TextMessage",
		"inbound.telegram.text-message",
		"inbound.telegram..text_message",
		"inbound.telegram.text message",
	} {
		t.Run(eventName, func(t *testing.T) {
			manifest := normalizedEventTestManifest()
			manifest.NormalizedEvents[0].Event = eventName
			if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), "valid canonical event name") {
				t.Fatalf("Validate(%q) error = %v, want canonical event-name rejection", eventName, err)
			}
		})
	}
}

func TestNormalizedEventManifestRejectsRawTemplateCollision(t *testing.T) {
	manifest := normalizedEventTestManifest()
	manifest.Provider = "github"
	manifest.EventName = EventNameManifest{Template: "inbound.github.{event_type}"}
	manifest.NormalizedEvents[0].Event = "inbound.github.push"
	err := manifest.Validate()
	if err == nil || !strings.Contains(err.Error(), "collides with the raw event-name policy") {
		t.Fatalf("Validate error = %v, want raw-template collision rejection", err)
	}
}

func TestNormalizedEventManifestRejectsNonCanonicalFieldNames(t *testing.T) {
	manifest := normalizedEventTestManifest()
	manifest.NormalizedEvents[0].Fields[" text "] = manifest.NormalizedEvents[0].Fields["text"]
	delete(manifest.NormalizedEvents[0].Fields, "text")
	err := manifest.Validate()
	if err == nil || !strings.Contains(err.Error(), "field name") || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("Validate error = %v, want non-canonical field-name rejection", err)
	}
}

func TestNormalizedEventPlanRejectsCompositeSchemaMismatchesWithPackProvenance(t *testing.T) {
	for _, tc := range []struct {
		name     string
		schema   runtimecontracts.ToolInputSchema
		value    any
		wantPart string
	}{
		{name: "array", schema: runtimecontracts.ToolInputSchema{Type: "array", Items: &runtimecontracts.ToolInputSchema{Type: "string"}}, value: []any{"ok", json.Number("2")}, wantPart: "$[1] must be string"},
		{name: "object", schema: runtimecontracts.ToolInputSchema{Type: "object", Properties: map[string]runtimecontracts.ToolInputSchema{"id": {Type: "integer"}}, Required: []string{"id"}}, value: map[string]any{"id": "not-an-integer"}, wantPart: "$.id must be integer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest := normalizedEventTestManifest()
			field := manifest.NormalizedEvents[0].Fields["raw"]
			field.Schema = tc.schema
			field.From = "message.raw"
			field.Optional = false
			manifest.NormalizedEvents[0].Fields["raw"] = field
			entry := CatalogEntry{
				Manifest: manifest,
				Identity: PackIdentity{
					ID: "provider.telegram", Version: "1.0.0",
					ManifestHash: "sha256:" + strings.Repeat("a", 64), Provenance: "platform",
				},
				Source: "test",
			}
			catalog, err := NewCatalogSnapshot(entry)
			if err != nil {
				t.Fatalf("NewCatalogSnapshot: %v", err)
			}
			plan, err := catalog.CompileAdmission(CompileAdmissionRequest{
				Alias: "chat", Provider: "telegram",
				Declaration: AdmissionDeclaration{Acknowledge: UnsignedWebhookAcknowledgement},
			})
			if err != nil {
				t.Fatalf("CompileAdmission: %v", err)
			}
			_, err = plan.Accept(Request{
				Target: Target{EntityID: "entity-1"},
				Payload: map[string]any{
					"update_id": json.Number("123"),
					"message": map[string]any{
						"message_id": json.Number("7"),
						"chat":       map[string]any{"id": json.Number("42")},
						"text":       "hello", "raw": tc.value,
					},
				},
			})
			for _, want := range []string{"provider.telegram", "version=1.0.0", "manifest_hash=sha256:", "inbound.telegram.text_message", "path \"message.raw\"", tc.wantPart} {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("Accept error = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestNormalizedEventManifestRejectsMissingOutputSchemaWithPackProvenance(t *testing.T) {
	manifest := normalizedEventTestManifest()
	field := manifest.NormalizedEvents[0].Fields["text"]
	field.Schema = runtimecontracts.ToolInputSchema{}
	manifest.NormalizedEvents[0].Fields["text"] = field
	_, err := NewCatalogSnapshot(CatalogEntry{
		Manifest: manifest,
		Identity: PackIdentity{
			ID: "provider.telegram", Version: "1.0.0",
			ManifestHash: "sha256:" + strings.Repeat("b", 64), Provenance: "platform",
		},
		Source: "test",
	})
	for _, want := range []string{"provider.telegram", "version=1.0.0", "manifest_hash=sha256:", "requires an explicit supported JSON type"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("NewCatalogSnapshot error = %v, want %q", err, want)
		}
	}
}

func TestNormalizedEventManifestRejectsImplicitAndUnknownConversions(t *testing.T) {
	manifest := normalizedEventTestManifest()
	field := manifest.NormalizedEvents[0].Fields["chat_id"]
	field.Convert = "stringify"
	manifest.NormalizedEvents[0].Fields["chat_id"] = field
	if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), "unsupported conversion") {
		t.Fatalf("Validate error = %v, want closed conversion rejection", err)
	}

	manifest = normalizedEventTestManifest()
	field = manifest.NormalizedEvents[0].Fields["chat_id"]
	field.Convert = ""
	manifest.NormalizedEvents[0].Fields["chat_id"] = field
	_, err := manifest.Accept(Request{
		Target: Target{EntityID: "entity-1"},
		Payload: map[string]any{"message": map[string]any{
			"message_id": json.Number("7"), "chat": map[string]any{"id": json.Number("42")}, "text": "hello",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "must be string") {
		t.Fatalf("Accept error = %v, want implicit conversion rejection", err)
	}
}

func TestNormalizedEventCatalogDerivesSchemaAndCapabilities(t *testing.T) {
	manifest := normalizedEventTestManifest()
	entry := manifest.EventCatalogEntries()["inbound.telegram.text_message"]
	if entry.Source != "provider_trigger_pack_normalized" || entry.Payload.Properties["chat_id"].Type != "text" {
		t.Fatalf("catalog entry = %#v", entry)
	}
	if got := strings.Join(entry.Required, ","); got != "chat_id,message_id,text" {
		t.Fatalf("required = %q", got)
	}
	capabilities := DerivedCapabilities(manifest)
	if got := strings.Join(capabilities.Can.EmitEvents, ","); got != "inbound.telegram,inbound.telegram.text_message" {
		t.Fatalf("emit events = %q", got)
	}
}

func TestNormalizedEventSchemaRemainsExactThroughAdmissionCatalogAndRuntime(t *testing.T) {
	var exact runtimecontracts.ToolInputSchema
	if err := yaml.Unmarshal([]byte(`
type: object
required: [status, attempts]
properties:
  status:
    type: string
    pattern: ' approved $'
    enum: [' approved ']
  attempts:
    type: array
    minItems: 1
    items: {type: integer}
additionalProperties:
  type: boolean
`), &exact); err != nil {
		t.Fatalf("decode exact schema: %v", err)
	}
	manifest := normalizedEventTestManifest()
	raw := manifest.NormalizedEvents[0].Fields["raw"]
	raw.From = "message.raw"
	raw.Schema = exact
	raw.Optional = false
	manifest.NormalizedEvents[0].Fields["raw"] = raw
	if err := manifest.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	catalogEntries := manifest.EventCatalogEntries()
	field := catalogEntries["inbound.telegram.text_message"].Payload.Properties["raw"]
	if field.ExactSchema == nil || field.ExactSchema.Properties["status"].Pattern != " approved $" || len(field.ExactSchema.Properties["attempts"].Enum) != 0 {
		t.Fatalf("catalog flattened exact schema: %#v", field)
	}
	registry := runtimecontracts.EventSchemaRegistryFromCatalog(catalogEntries)
	properties := registry["inbound.telegram.text_message"].Schema["properties"].(map[string]any)
	rawSchema := properties["raw"].(map[string]any)
	valid := map[string]any{"status": " approved ", "attempts": []any{json.Number("1")}, "urgent": true}
	if err := eventschema.ValidateValueAgainstSchema(rawSchema, valid); err != nil {
		t.Fatalf("runtime registry rejected exact value: %v", err)
	}
	for _, tc := range []struct {
		name  string
		value map[string]any
		want  string
	}{
		{name: "trimmed enum", value: map[string]any{"status": "approved", "attempts": []any{json.Number("1")}}, want: "invalid enum"},
		{name: "array item", value: map[string]any{"status": " approved ", "attempts": []any{"1"}}, want: "must be integer"},
		{name: "additional property", value: map[string]any{"status": " approved ", "attempts": []any{json.Number("1")}, "urgent": "true"}, want: "must be boolean"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := eventschema.ValidateValueAgainstSchema(rawSchema, tc.value); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateValueAgainstSchema error = %v, want %q", err, tc.want)
			}
		})
	}

	entry := CatalogEntry{
		Manifest: manifest,
		Identity: PackIdentity{ID: "provider.telegram", Version: "1.0.0", ManifestHash: "sha256:" + strings.Repeat("c", 64), Provenance: "platform"},
		Source:   "test",
	}
	catalog, err := NewCatalogSnapshot(entry)
	if err != nil {
		t.Fatalf("NewCatalogSnapshot: %v", err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{
		Alias: "chat", Provider: "telegram", Declaration: AdmissionDeclaration{Acknowledge: UnsignedWebhookAcknowledgement},
	})
	if err != nil {
		t.Fatalf("CompileAdmission: %v", err)
	}
	mutateExactStatusEnum := func(schema *runtimecontracts.ToolInputSchema) {
		status := schema.Properties["status"]
		normalizedEventEnumScalar(t, &status.Enum[0].Node).Value = "mutated"
		schema.Properties["status"] = status
	}
	descriptors := catalog.PackDescriptors()
	descriptorEvent := descriptors[0].Events["inbound.telegram.text_message"]
	descriptorField := descriptorEvent.Fields["raw"]
	mutateExactStatusEnum(&descriptorField.Schema)
	descriptorEvent.Fields["raw"] = descriptorField
	descriptors[0].Events["inbound.telegram.text_message"] = descriptorEvent
	if got := normalizedEventEnumText(t, catalog.PackDescriptors()[0].Events["inbound.telegram.text_message"].Fields["raw"].Schema.Properties["status"]); got != " approved " {
		t.Fatalf("PackDescriptors shared enum node: %q", got)
	}
	outputs := plan.Outputs()
	for index := range outputs {
		if outputs[index].Event != "inbound.telegram.text_message" {
			continue
		}
		outputField := outputs[index].Fields["raw"]
		mutateExactStatusEnum(&outputField.Schema)
		outputs[index].Fields["raw"] = outputField
	}
	for _, output := range plan.Outputs() {
		if output.Event == "inbound.telegram.text_message" {
			if got := normalizedEventEnumText(t, output.Fields["raw"].Schema.Properties["status"]); got != " approved " {
				t.Fatalf("InboundAdmissionPlan.Outputs shared enum node: %q", got)
			}
		}
	}

	request := Request{Target: Target{EntityID: "entity-1"}, Payload: map[string]any{
		"update_id": json.Number("123"),
		"message": map[string]any{
			"message_id": json.Number("7"), "chat": map[string]any{"id": json.Number("42")}, "text": "hello", "raw": valid,
		},
	}}
	if _, err := plan.Accept(request); err != nil {
		t.Fatalf("Accept exact normalized value: %v", err)
	}
	request.Payload.(map[string]any)["message"].(map[string]any)["raw"] = map[string]any{"status": "approved", "attempts": []any{json.Number("1")}}
	if _, err := plan.Accept(request); err == nil || !strings.Contains(err.Error(), "invalid enum") {
		t.Fatalf("Accept trimmed enum error = %v", err)
	}
}

func normalizedEventEnumText(t *testing.T, schema runtimecontracts.ToolInputSchema) string {
	t.Helper()
	var value string
	if err := schema.Enum[0].Node.Decode(&value); err != nil {
		t.Fatalf("decode schema enum: %v", err)
	}
	return value
}

func normalizedEventEnumScalar(t *testing.T, node *yaml.Node) *yaml.Node {
	t.Helper()
	if node == nil {
		t.Fatal("schema enum node is nil")
	}
	if node.Kind == yaml.ScalarNode {
		return node
	}
	for _, child := range node.Content {
		if child != nil {
			return normalizedEventEnumScalar(t, child)
		}
	}
	if node.Alias != nil {
		return normalizedEventEnumScalar(t, node.Alias)
	}
	t.Fatalf("schema enum node kind %d has no scalar", node.Kind)
	return nil
}

func normalizedEventTestManifest() Manifest {
	return Manifest{
		Provider:              "telegram",
		PayloadObjectRequired: true,
		DeliveryID:            ValueSource{Literal: "delivery-1", Required: true},
		EventType:             ValueSource{Literal: "update", Required: true},
		EventName:             EventNameManifest{Literal: "inbound.telegram"},
		NormalizedEvents: []NormalizedEventManifest{{
			Event: "inbound.telegram.text_message",
			Fields: map[string]NormalizedEventFieldProjection{
				"chat_id":    {From: "message.chat.id", Schema: runtimecontracts.ToolInputSchema{Type: "string"}, Convert: runtimecontracts.FieldProjectionConvertNumberToText},
				"text":       {From: "message.text", Schema: runtimecontracts.ToolInputSchema{Type: "string"}},
				"message_id": {From: "message.message_id", Schema: runtimecontracts.ToolInputSchema{Type: "integer"}},
				"raw":        {From: "message", Schema: runtimecontracts.ToolInputSchema{Type: "object"}, Optional: true},
			},
		}},
	}
}
