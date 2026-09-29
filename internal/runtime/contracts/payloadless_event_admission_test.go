package contracts

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestPayloadlessEventCanonicalGrammarDiskAndReconstructedSource(t *testing.T) {
	const event = "investigation.timed_out"
	for _, variant := range []string{"", "{}", `""`, "~", "null"} {
		name := variant
		if name == "" {
			name = "bare"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			body := event + ":"
			if variant != "" {
				body += " " + variant
			}
			body += "\n"
			path := filepath.Join(root, "events.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
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
				{"disk", func() (map[string]EventCatalogEntry, error) { return loadOptionalEventCatalog(path) }},
				{"reconstructed_source", func() (map[string]EventCatalogEntry, error) {
					return loadOptionalEventCatalogFromSource(restored, "events.yaml")
				}},
			} {
				t.Run(loader.name, func(t *testing.T) {
					entries, err := loader.load()
					if variant == "" {
						if err != nil {
							t.Fatalf("bare declaration refused: %v", err)
						}
						entry, ok := entries[event]
						if !ok || len(entries) != 1 || len(entry.Payload.Properties) != 0 || len(entry.Payload.Required) != 0 {
							t.Fatalf("bare declaration = %#v", entries)
						}
						if entry.admissionProvenance["declaration"].SourcePresence != "null" {
							t.Fatal("bare source presence lost")
						}
						return
					}
					want := fmt.Sprintf("an event with no fields is declared bare \u2014 write `%s:` (remove the `%s`).", event, variant)
					if err == nil {
						t.Fatalf("accepted noncanonical %q", variant)
					}
					for errors.Unwrap(err) != nil {
						err = errors.Unwrap(err)
					}
					if err.Error() != want {
						t.Fatalf("teaching error = %q, want %q", err, want)
					}
				})
			}
		})
	}
}

func TestPayloadlessEventRejectsAlternateNullSpellings(t *testing.T) {
	for _, tc := range []struct {
		name, source, variant string
	}{
		{"single_quotes", "test.event: ''\n", "''"},
		{"anchor", "test.event: &empty\n", "&empty"},
		{"null_tag", "test.event: !!null\n", "!!null"},
		{"metadata_only", "test.event:\n  description: No fields\n", "mapping"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(tc.source))
			if err != nil {
				t.Fatal(err)
			}
			_, err = admitEventCatalogDocument(snapshot.Document("events.yaml"))
			if err == nil || !strings.Contains(err.Error(), "(remove the `"+tc.variant+"`).") {
				t.Fatalf("non-bare spelling accepted or wrong diagnostic: %v", err)
			}
		})
	}
}

func TestPayloadlessEventRejectsNullAliasAtUse(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("test.anchor: &empty\ntest.event: *empty\n"))
	if err != nil {
		t.Fatal(err)
	}
	declarations, err := snapshot.Document("events.yaml").Root().Mapping()
	if err != nil {
		t.Fatal(err)
	}
	_, err = admitEventCatalogEntry("test.event", declarations[1])
	if err == nil || !strings.Contains(err.Error(), "(remove the `*empty`).") {
		t.Fatalf("alias use bypassed bare-only admission: %v", err)
	}
}

func TestPayloadlessEventMissingDeclarationStaysMissing(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "events.yaml")
	if err := os.WriteFile(path, []byte("other.event:\n  id: text\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	disk, err := loadOptionalEventCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	reconstructed, err := loadOptionalEventCatalogFromSource(artifact, "events.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, catalog := range []map[string]EventCatalogEntry{disk, reconstructed} {
		if _, exists := catalog["investigation.timed_out"]; exists || len(catalog) != 1 {
			t.Fatalf("missing declaration was coerced to a bare event: %#v", catalog)
		}
	}
}
