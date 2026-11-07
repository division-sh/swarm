package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestEventCatalogFactoringConsumers(t *testing.T) {
	t.Run("artifact_and_disk", func(t *testing.T) {
		root := t.TempDir()
		writeFixtureFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
		path := filepath.Join(root, "events.yaml")
		const body = "test.event:\n  id: uuid\n  note: text?\n"
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
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
		actual, err := loadOptionalEventCatalogFromSource(artifact, "events.yaml")
		if err != nil {
			t.Fatal(err)
		}
		diskPayload, err := json.Marshal(disk["test.event"].Payload)
		if err != nil {
			t.Fatal(err)
		}
		artifactPayload, err := json.Marshal(actual["test.event"].Payload)
		if err != nil || !bytes.Equal(diskPayload, artifactPayload) || !reflect.DeepEqual(actual["test.event"].Payload.Required, []string{"id"}) {
			t.Fatal("disk/artifact admission changed")
		}
		diskOrigin := disk["test.event"].Payload.Properties["id"].origin.definition
		artifactOrigin := actual["test.event"].Payload.Properties["id"].origin.definition
		if diskOrigin.File != path || artifactOrigin.File != "events.yaml" || diskOrigin.Line != 2 || artifactOrigin.Line != 2 || diskOrigin.Column != artifactOrigin.Column {
			t.Fatalf("disk/artifact source coordinates changed: %+v / %+v", diskOrigin, artifactOrigin)
		}
		merged := &WorkflowContractBundle{
			Events: map[string]EventCatalogEntry{}, scopedEvents: map[string]EventCatalogEntry{},
			eventSources: map[string]ContractItemSource{}, scopedEventSources: map[string]ContractItemSource{},
			ambiguousEventAliases: map[string]struct{}{},
		}
		source := ContractItemSource{FlowPath: ".", Family: "events", File: "events.yaml"}
		for _, entries := range []map[string]EventCatalogEntry{disk, actual} {
			if err := mergeEventContracts(merged, entries, source); err != nil {
				t.Fatalf("source coordinates changed duplicate admission: %v", err)
			}
		}
		for _, change := range []func(*EventCatalogEntry){
			func(e *EventCatalogEntry) { e.Payload.Properties["id"] = EventFieldSpec{Type: "text"} },
			func(e *EventCatalogEntry) { e.Payload.Required = []string{"id", "note"} },
			func(e *EventCatalogEntry) { e.BusinessKeyField = "id" },
			func(e *EventCatalogEntry) { e.platformAuthority = PlatformEventAuthority{declared: true} },
		} {
			changed := cloneEventCatalogEntry(actual["test.event"])
			change(&changed)
			if err := mergeEventContracts(merged, map[string]EventCatalogEntry{"test.event": changed}, source); err == nil {
				t.Fatal("provenance-independent equality lost a semantic or authority refusal")
			}
		}
		if disk["test.event"].Payload.Properties["id"].origin.definition != diskOrigin || actual["test.event"].Payload.Properties["id"].origin.definition != artifactOrigin {
			t.Fatal("declaration equality mutated source provenance")
		}
		for _, label := range []string{"", "missing.yaml"} {
			_, err := loadOptionalEventCatalogFromSource(artifact, label)
			if (label != "") != (err != nil) {
				t.Fatalf("optional artifact label %q: %v", label, err)
			}
		}
		absent, err := loadOptionalEventCatalog("")
		if err != nil || absent == nil || len(absent) != 0 {
			t.Fatalf("missing disk declaration: %v %v", absent, err)
		}
		if _, err := loadOptionalEventCatalog(filepath.Join(root, "absent.yaml")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("explicit missing disk path did not refuse: %v", err)
		}
		if err := os.WriteFile(filepath.Join(root, "empty.yaml"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadOptionalEventCatalog(filepath.Join(root, "empty.yaml")); err == nil {
			t.Fatal("present empty declaration became absent")
		}
		persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0))
		if err != nil {
			t.Fatal(err)
		}
		rebuilt, err := persisted.Decode()
		if err != nil {
			t.Fatal(err)
		}
		actual["test.event"].Payload.Properties["id"] = EventFieldSpec{Type: "text"}
		fresh, err := loadOptionalEventCatalogFromSource(rebuilt, "events.yaml")
		if err != nil || fresh["test.event"].Payload.Properties["id"].Type != "uuid" || !bytes.Equal(artifact.LogicalBlob(), rebuilt.LogicalBlob()) {
			t.Fatalf("independent reconstruction/source nonmutation: %v", err)
		}
	})
	for _, text := range []string{"text?", "[uuid]?", "map[text]uuid?"} {
		kind, optional, err := admitEventFieldTypeMarker(text, "shared")
		if err != nil || !optional || strings.Contains(kind, "?") {
			t.Fatalf("shared marker %q: %q %t %v", text, kind, optional, err)
		}
	}
}
