package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestS1ReservedInboundDeclarationDiskAndRetainedSource(t *testing.T) {
	for _, tc := range []struct {
		declaration string
		matched     string
	}{
		{"inbound.telegram.message", "inbound.telegram.message"},
		{"inbound.telegram", "inbound.telegram"},
		{"inbound.*", "inbound.telegram.message"},
		{"inbound.telegram.*", "inbound.telegram.message"},
		{"*.completed", "inbound.telegram.completed"},
		{"*.inbound.*", "inbound.inbound.message"},
		{"*", "inbound.telegram.message"},
		{"**", "inbound.telegram.message"},
		{"child/inbound.telegram.message", "child/inbound.telegram.message"},
		{"child/*", "child/inbound.telegram.message"},
		{"child/**", "child/inbound.telegram.message"},
		{"**/*.completed", "child/inbound.telegram.completed"},
	} {
		t.Run(tc.declaration, func(t *testing.T) {
			if !eventidentity.IsCanonicalDeclaration(tc.declaration) || !eventidentity.MatchPattern(tc.declaration, tc.matched) {
				t.Fatal("control is not a canonical declaration matching the provider namespace")
			}
			root := t.TempDir()
			file := filepath.Join(root, "events.yaml")
			if err := os.WriteFile(file, []byte(fmt.Sprintf("%q:\n", tc.declaration)), 0o600); err != nil {
				t.Fatal(err)
			}
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			restored, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			for _, loader := range []struct {
				name string
				load func() (map[string]EventCatalogEntry, error)
			}{
				{"disk", func() (map[string]EventCatalogEntry, error) { return loadOptionalEventCatalog(file) }},
				{"retained", func() (map[string]EventCatalogEntry, error) {
					return loadOptionalEventCatalogFromSource(restored, "events.yaml")
				}},
			} {
				t.Run(loader.name, func(t *testing.T) {
					entries, err := loader.load()
					if err == nil || len(entries) != 0 {
						t.Fatalf("flow catalog gained provider declaration authority: entries=%v error=%v", entries, err)
					}
					for _, want := range []string{tc.declaration, "events.yaml:1:", "reserved provider namespace inbound.*", "trigger.yaml", "introduced at", "resolved at"} {
						if !strings.Contains(err.Error(), want) {
							t.Fatalf("namespace refusal lost %q: %v", want, err)
						}
					}
				})
			}
		})
	}
}

func TestS1ReservedInboundDeclarationPreservesYAMLIntroductions(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		locations  []string
	}{
		{"merge", "<<: &catalog\n  inbound.telegram.message:\n", []string{"introduced at events.yaml:1:", "events.yaml:2:"}},
		{"key_alias", "business.event:\n  value:\n    type: text\n    description: &reserved inbound.telegram.message\n? *reserved\n:\n", []string{"events.yaml:5:"}},
		{"value_alias", "business.event: &payload\n  value: text\ninbound.telegram.message: *payload\n", []string{"events.yaml:3:", "resolved at events.yaml:1:"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			entries, err := admitEventCatalogDocument(snapshot.Document("events.yaml"))
			if err == nil || len(entries) != 0 || !strings.Contains(err.Error(), "reserved provider namespace inbound.*") {
				t.Fatalf("expanded declaration bypassed namespace admission: %v %v", entries, err)
			}
			for _, location := range tc.locations {
				if !strings.Contains(err.Error(), location) {
					t.Fatalf("lost YAML occurrence %q: %v", location, err)
				}
			}
		})
	}
}

func TestS1ProviderReferencesDoNotDeclareInboundNamespace(t *testing.T) {
	const body = "pins:\n  inputs: [inbound.telegram.message]\n  outputs: [inbound.telegram.message]\nconnect:\n  - {event: inbound.telegram.message, from: ., to: child, resolution: select}\n"
	snapshot, err := yamlsource.Load([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := AdmitFlowSchemaValue(snapshot.Document("schema.yaml").Root())
	if err != nil {
		t.Fatalf("lawful provider reference refused by source grammar: %v", err)
	}
	if len(schema.Pins.Inputs.EventPins) != 1 || len(schema.Pins.Outputs.EventPins) != 1 || len(schema.Connect) != 1 {
		t.Fatalf("provider references were erased: %+v", schema)
	}
	if got := schema.Pins.Inputs.EventPins[0].Event; got != "inbound.telegram.message" {
		t.Fatalf("provider input rewritten to %q", got)
	}
	if schema.Pins.Outputs.EventPins[0].Event != "inbound.telegram.message" || schema.Connect[0].Event != "inbound.telegram.message" {
		t.Fatal("provider output or connection reference changed")
	}

	const catalog = "business.event:\ninbound:\ninboundx.event:\n'business.*':\n'*/business.event':\n'**/business.event':\n"
	snapshot, err = yamlsource.Load([]byte(catalog))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := admitEventCatalogDocument(snapshot.Document("events.yaml"))
	if err != nil {
		t.Fatalf("business declarations refused: %v", err)
	}
	if got, want := len(entries), 6; got != want {
		t.Fatalf("business declaration count=%d, want %d", got, want)
	}
}
