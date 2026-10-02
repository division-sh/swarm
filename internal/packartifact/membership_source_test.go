package packartifact

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestPackMembershipAdmissionRequiresIntegerVersion(t *testing.T) {
	for _, version := range []string{"1.5", "1.0", "'1'", "true", "null", "[]", "{}", "''", "2"} {
		t.Run(version, func(t *testing.T) {
			inventory := fstest.MapFS{InventoryManifestFileName: {Data: []byte("version: " + version + "\npacks: [{id: provider.demo, type: trigger, path: demo}]\n")}}
			if _, err := LoadInventoryManifest(inventory, InventoryManifestFileName); err == nil || !strings.Contains(err.Error(), "version") {
				t.Fatalf("invalid inventory version admitted: %v", err)
			}
			if _, err := ParseProjectPackManifest([]byte("version: " + version + "\nimports: []\n")); err == nil || !strings.Contains(err.Error(), "version") {
				t.Fatalf("invalid project version admitted: %v", err)
			}
		})
	}
}

func TestPackMembershipAdmissionRejectsMalformedRowsAndOrigins(t *testing.T) {
	for _, row := range []string{
		"null", "[]", "7", "{}", "{id: true, type: trigger, path: demo}",
		"{id: provider.demo, type: trigger, path: 7}",
		"{id: provider.demo, type: trigger, path: demo, unknown: false}",
		"{id: provider.demo, id: provider.other, type: trigger, path: demo}",
	} {
		t.Run(row, func(t *testing.T) {
			inventory := fstest.MapFS{InventoryManifestFileName: {Data: []byte("version: 1\npacks: [" + row + "]\n")}}
			_, err := LoadInventoryManifest(inventory, InventoryManifestFileName)
			if err == nil || !strings.Contains(err.Error(), "inventory.yaml:") {
				t.Fatalf("malformed row admitted or source lost: %v", err)
			}
		})
	}
	for _, origin := range []string{"null", "{}", "[]", "{source: embedded}", "{source: true, id: provider.demo, version: 0.1.0, manifest_hash: hash, envelope_hash: hash}"} {
		_, err := ParseProjectPackManifest([]byte("version: 1\nimports: [{id: provider.demo, type: trigger, path: demo, origin: " + origin + "}]\n"))
		if err == nil || !strings.Contains(err.Error(), "origin") || !strings.Contains(err.Error(), "packs/manifest.yaml:") {
			t.Fatalf("malformed origin %s admitted or source lost: %v", origin, err)
		}
	}
}

func TestAdmittedPackHandoffPreservesEvidenceAndDefensiveCopies(t *testing.T) {
	fsys := testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", "platform", []byte("provider: demo\n"))
	fsys["provider-triggers/demo/pack.yaml"].Data = append([]byte("# exact authored envelope\n"), fsys["provider-triggers/demo/pack.yaml"].Data...)
	inventory, err := LoadPlatformPackInventoryFS(fsys, InventoryManifestFileName, testPlatformVersion, SelectionEmbedded)
	if err != nil {
		t.Fatal(err)
	}
	entry := mustLookupEntry(t, inventory, "provider.demo")
	loaded, err := entry.Loaded(testPlatformVersion)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(entry.EnvelopeBody()), "# exact authored envelope") {
		t.Fatal("original envelope bytes lost")
	}
	if loaded.Envelope.SourceValue().Location().File != "provider-triggers/demo/pack.yaml" || inventory.SourceValue().Location().File != InventoryManifestFileName {
		t.Fatal("admitted handoff lost original source")
	}
	loaded.ManifestBody[0] = '!'
	loaded.Envelope.Tests[0] = "mutated"
	before := inventory.Digest()
	again, err := entry.Loaded(testPlatformVersion)
	if err != nil {
		t.Fatal(err)
	}
	if string(again.ManifestBody) != "provider: demo\n" || again.Envelope.Tests[0] == "mutated" || inventory.Digest() != before {
		t.Fatal("admitted handoff mutates inventory")
	}
	if _, err := entry.Loaded("1.0.0"); err == nil {
		t.Fatal("handoff ignored running version")
	}
}
