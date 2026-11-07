package contracts

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestEventMetadataRetirementRejectsPresenceBeforeValueInterpretation(t *testing.T) {
	// Keep this list independent of the admission map so deleting a rejection is
	// detected, rather than deleting its test at the same time.
	for _, field := range []string{
		"_producer", "_consumer", "_consumer_type", "_source", "_status", "_note",
	} {
		for _, value := range []string{"null", "''", "false", "{}", "[]", "text"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				_, err := admitEventCatalogEntryForTest(t, field+": "+value)
				if err == nil || !strings.Contains(err.Error(), `event field name "`+field+`"`) {
					t.Fatalf("retired field presence admitted: %v", err)
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
				_, err := admitEventCatalogEntryForTest(t, shape)
				if err == nil || !strings.Contains(err.Error(), `event field name "`+field+`"`) {
					t.Fatalf("indirect retired declaration admitted: %v", err)
				}
			})
		}
	}
}

func TestEventMetadataRetirementPreservesBusinessPayloadNames(t *testing.T) {
	names := []string{"source", "status", "note", "swarm", "emitter", "emitter_type", "producer", "alternate_emitters", "consumer", "consumer_type", "intercepted", "passthrough", "runtime_handling", "owning_node", "delivery_channel", "author_summary_field", "required", "key"}
	var body strings.Builder
	for _, name := range names {
		body.WriteString(name + ": text\n")
	}
	entry, err := admitEventCatalogEntryForTest(t, body.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if entry.Payload.Properties[name].Type != "text" {
			t.Fatalf("business field %s was consumed as metadata: %#v", name, entry.Payload)
		}
	}
	if entry.platformAuthority != (PlatformEventAuthority{}) || entry.BusinessKeyField != "" {
		t.Fatal("business payload acquired platform authority or key metadata")
	}
	types, err := yamlsource.Load([]byte("Business:\n  " + strings.ReplaceAll(strings.TrimSuffix(body.String(), "\n"), "\n", "\n  ") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := projectTypeCatalogDocument(types.Document("types.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	events, err := yamlsource.Load([]byte("test.event:\n  _embed: [Business]\n"))
	if err != nil {
		t.Fatal(err)
	}
	expanded, err := admitEventCatalogDocument(events.Document("events.yaml"), catalog)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(expanded["test.event"])
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("business names acquired metadata semantics under embedding: %s, %v", got, err)
	}
}
