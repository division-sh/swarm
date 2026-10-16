package contracts

import (
	"strings"
	"testing"
)

func TestEventMetadataRetirementRejectsPresenceBeforeValueInterpretation(t *testing.T) {
	// Keep this list independent of the admission map so deleting a rejection is
	// detected, rather than deleting its test at the same time.
	for _, field := range []string{
		"swarm", "emitter", "emitter_type", "producer", "_producer",
		"alternate_emitters", "consumer", "_consumer", "consumer_type",
		"_consumer_type", "_source", "_status", "_note", "intercepted",
		"passthrough", "runtime_handling", "owning_node", "delivery_channel",
		"author_summary_field",
	} {
		metadata := strings.HasPrefix(field, "_")
		for _, value := range []string{"null", "''", "false", "{}", "[]", "text"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				entry, err := admitEventCatalogEntryForTest(t, field+": "+value)
				if !metadata && value == "text" {
					if err != nil || entry.Payload.Properties[field].Type != "text" {
						t.Fatalf("ordinary field consumed as retired metadata: %v", err)
					}
					return
				}
				if err == nil {
					_, err = newCompiledEventSchema(".", "test.event", entry, TypeCatalogDocument{}, "", CompiledEventSchemaSource{})
				}
				if err == nil || (metadata && !strings.Contains(err.Error(), "event metadata")) {
					t.Fatalf("invalid field/metadata admitted: %v", err)
				}
			})
		}
		for _, shape := range []string{
			"<<: &retired\n  " + field + ": null\nvalue: text",
			"<<: [&retired {" + field + ": null}]\nvalue: text",
			"value: &value text\n" + field + ": *value",
			field + ": null\n" + field + ": {}",
		} {
			t.Run(field+"/indirect/"+shape, func(t *testing.T) {
				entry, err := admitEventCatalogEntryForTest(t, shape)
				if !metadata && strings.Contains(shape, "*value") {
					if err != nil || entry.Payload.Properties[field].Type != "text" {
						t.Fatalf("ordinary aliased field consumed as metadata: %v", err)
					}
				} else if err == nil {
					t.Fatalf("malformed indirect or duplicate declaration admitted: %v", err)
				}
			})
		}
	}
}

func TestEventMetadataRetirementPreservesBusinessPayloadNames(t *testing.T) {
	entry, err := admitEventCatalogEntryForTest(t, "source: text\nstatus: text\nnote: text\nvalue:\n  type: text\n  description: author_summary_field\n")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"source", "status", "note"} {
		if entry.Payload.Properties[name].Type != "text" {
			t.Fatalf("business field %s was consumed as metadata: %#v", name, entry.Payload)
		}
	}
	if entry.Payload.Properties["value"].Description != "author_summary_field" {
		t.Fatal("retired spelling inside payload description must remain prose")
	}
}
