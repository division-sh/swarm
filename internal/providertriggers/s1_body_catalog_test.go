package providertriggers

import (
	"strings"
	"testing"
)

func TestS1CapturedTriggerCatalogRejectsUnboundAndByteOnlyManifests(t *testing.T) {
	byteOnly, err := ParseManifest([]byte("provider: team-bot\nevent_name: {literal: inbound.team-bot.raw}\n"))
	if err != nil {
		t.Fatal(err)
	}
	for _, manifest := range []Manifest{{}, byteOnly} {
		if catalog, err := NewCatalogSnapshotFromAdmittedBodies(manifest); err == nil || catalog != nil {
			t.Fatal("unbound or byte-only policy minted captured catalog evidence")
		}
	}
	body := s1AdmittedTriggerBody(t, "team-bot", "literal: raw", "text_message")
	manifest, err := AdmitBody(body)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalogSnapshotFromAdmittedBodies(manifest)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := catalog.EntryByProvider("team-bot")
	if !ok || entry.Identity.ID != "team-bot" || entry.Identity.Version != body.Metadata().Version || entry.Identity.ManifestHash != body.Hash() || entry.Identity.Provenance != "" || entry.SourcePath != body.Label() || entry.Source != body.Label() {
		t.Fatalf("captured catalog invented identity or provenance: %+v", entry)
	}
	if _, err := NewCatalogSnapshotFromAdmittedBodies(manifest, manifest); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatal("same scoped provider was merged twice")
	}
	repeated, err := NewCatalogSnapshotFromAdmittedBodies(manifest)
	if err != nil || !repeated.Generation().Equal(catalog.Generation()) {
		t.Fatal("same frozen BODY changed catalog generation")
	}
}

func TestS1CapturedTriggerCatalogDoesNotBorrowAnotherNamespace(t *testing.T) {
	var manifests []Manifest
	for _, key := range []string{"team-bot", "team_bot"} {
		manifest, err := AdmitBody(s1AdmittedTriggerBody(t, key, "literal: raw", "text_message"))
		if err != nil {
			t.Fatal(err)
		}
		manifests = append(manifests, manifest)
	}
	catalog, err := NewCatalogSnapshotFromAdmittedBodies(manifests...)
	if err != nil {
		t.Fatal(err)
	}
	descriptors := catalog.PackDescriptors()
	if len(descriptors) != 2 {
		t.Fatal("distinct directory keys collapsed")
	}
	for i, key := range []string{"team-bot", "team_bot"} {
		descriptor := descriptors[i]
		if descriptor.Provider != key || descriptor.Identity.ID() != key || descriptor.Identity.Source().Provenance() != "" {
			t.Fatalf("descriptor does not retain its directory key: %+v", descriptor)
		}
		name := "inbound." + key + ".text_message"
		if _, exists := descriptor.Events[name]; !exists {
			t.Fatal("typed normalized schema lost its directory namespace")
		}
		delete(descriptor.Events, name)
		if _, exists := catalog.PackDescriptors()[i].Events[name]; !exists {
			t.Fatal("descriptor readback mutated canonical captured catalog")
		}
	}
}
