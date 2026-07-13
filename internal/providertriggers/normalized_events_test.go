package providertriggers

import (
	"encoding/json"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
		Fields: map[string]runtimecontracts.FieldProjection{
			"text": {From: "message.text", Type: "text"},
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
	if err == nil || !strings.Contains(err.Error(), "implicit conversion is forbidden") {
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

func normalizedEventTestManifest() Manifest {
	return Manifest{
		Provider:              "telegram",
		PayloadObjectRequired: true,
		DeliveryID:            ValueSource{Literal: "delivery-1", Required: true},
		EventType:             ValueSource{Literal: "update", Required: true},
		EventName:             EventNameManifest{Literal: "inbound.telegram"},
		NormalizedEvents: []NormalizedEventManifest{{
			Event: "inbound.telegram.text_message",
			Fields: map[string]runtimecontracts.FieldProjection{
				"chat_id":    {From: "message.chat.id", Type: "text", Convert: runtimecontracts.FieldProjectionConvertNumberToText},
				"text":       {From: "message.text", Type: "text"},
				"message_id": {From: "message.message_id", Type: "integer"},
				"raw":        {From: "message", Type: "json", Optional: true},
			},
		}},
	}
}
