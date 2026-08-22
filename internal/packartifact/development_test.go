package packartifact

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDevelopmentOverrideAndProjectPrecedenceAreFailClosed(t *testing.T) {
	embedded, err := LoadPlatformPackInventoryFS(
		testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", ProvenancePlatform, []byte("provider: demo\nrevision: embedded\n")),
		InventoryManifestFileName,
		testPlatformVersion,
		SelectionEmbedded,
	)
	if err != nil {
		t.Fatal(err)
	}
	embeddedEntry, _ := embedded.Lookup("provider.demo")
	if embedded.SelectionMode() != SelectionEmbedded || embeddedEntry.Source() != ProvenanceEmbedded {
		t.Fatalf("embedded selection = mode:%s source:%s", embedded.SelectionMode(), embeddedEntry.Source())
	}

	developmentDirs := materializeDevelopmentInventory(t, embedded, map[string][]byte{
		"provider.demo": []byte("provider: demo\nrevision: development\n"),
	})
	development, err := LoadDevelopmentPlatformPackInventory(testPlatformVersion, developmentDirs, embedded)
	if err != nil {
		t.Fatalf("load complete development override: %v", err)
	}
	developmentEntry, ok := development.Lookup("provider.demo")
	if !ok || development.SelectionMode() != SelectionDevelopmentOverride || developmentEntry.Source() != ProvenanceDevelopmentOverride {
		t.Fatalf("development selection = inventory:%#v entry:%#v present:%t", development, developmentEntry, ok)
	}
	if string(developmentEntry.ManifestBody()) != "provider: demo\nrevision: development\n" {
		t.Fatalf("development body = %q", developmentEntry.ManifestBody())
	}

	projectBody := []byte("provider: demo\nrevision: project\n")
	projectEnvelope := embeddedEntry.Envelope()
	projectEnvelope.Provenance.Source = ProvenanceProject
	projectEnvelope.ManifestHash = ManifestHashDerived
	projectEnvelopeBody, err := yaml.Marshal(projectEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	effective, err := NewEffectivePackInventory(development, []ProjectPackSource{{
		Path: "provider.demo", EnvelopeBody: projectEnvelopeBody, ManifestBody: projectBody,
		Origin: ImportOrigin{
			Source: ProvenanceEmbedded, ID: embeddedEntry.ID(), Version: embeddedEntry.Version(), ManifestHash: embeddedEntry.ManifestHash(),
			EnvelopeHash: importedEnvelopeHash(projectEnvelopeBody),
		},
	}})
	if err != nil {
		t.Fatalf("project over development inventory: %v", err)
	}
	projectEntry, ok := effective.Lookup("provider.demo")
	if !ok || projectEntry.Source() != ProvenanceProject || !projectEntry.ShadowsBase() || effective.BaseSelectionMode() != SelectionDevelopmentOverride {
		t.Fatalf("project precedence = inventory:%#v entry:%#v present:%t", effective, projectEntry, ok)
	}
	if !projectEntry.Modified() || string(projectEntry.ManifestBody()) != string(projectBody) {
		t.Fatalf("project entry = modified:%t body:%q", projectEntry.Modified(), projectEntry.ManifestBody())
	}

	if _, err := LoadDevelopmentPlatformPackInventory(testPlatformVersion, nil, embedded); err == nil {
		t.Fatal("empty development override fell back to embedded inventory")
	}
	tampered := developmentDirs[0]
	manifestPath := filepath.Join(tampered, TriggerManifestFileName)
	if err := os.WriteFile(manifestPath, []byte("provider: demo\nrevision: tampered\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDevelopmentPlatformPackInventory(testPlatformVersion, developmentDirs, embedded); err == nil || !strings.Contains(err.Error(), "manifest_hash mismatch") {
		t.Fatalf("tampered development override error = %v, want manifest hash rejection without fallback", err)
	}
}

func TestDevelopmentOverrideDigestIsIndependentOfDirectoryOrderAndRejectsIncompleteReplacement(t *testing.T) {
	embedded, err := LoadEmbeddedPlatformPackInventory(testPlatformVersion)
	if err != nil {
		t.Fatal(err)
	}
	dirs := materializeDevelopmentInventory(t, embedded, nil)
	forward, err := LoadDevelopmentPlatformPackInventory(testPlatformVersion, dirs, embedded)
	if err != nil {
		t.Fatalf("load forward development inventory: %v", err)
	}
	reversedDirs := append([]string(nil), dirs...)
	slices.Reverse(reversedDirs)
	reversed, err := LoadDevelopmentPlatformPackInventory(testPlatformVersion, reversedDirs, embedded)
	if err != nil {
		t.Fatalf("load reversed development inventory: %v", err)
	}
	if forward.Digest() != reversed.Digest() {
		t.Fatalf("development digest depends on directory order: %s != %s", forward.Digest(), reversed.Digest())
	}
	if strings.Join(forward.SourceDirectories(), "\n") != strings.Join(reversed.SourceDirectories(), "\n") {
		t.Fatalf("development source ordering differs:\n%v\n%v", forward.SourceDirectories(), reversed.SourceDirectories())
	}
	if _, err := LoadDevelopmentPlatformPackInventory(testPlatformVersion, dirs[:len(dirs)-1], embedded); err == nil || !strings.Contains(err.Error(), "development override replaces the embedded inventory") {
		t.Fatalf("incomplete development override error = %v", err)
	}
}

func TestDevelopmentOverrideEntriesRetainTheirConfiguredDirectories(t *testing.T) {
	embedded, err := LoadEmbeddedPlatformPackInventory(testPlatformVersion)
	if err != nil {
		t.Fatal(err)
	}
	dirs := materializeDevelopmentInventory(t, embedded, nil)
	wantByID := make(map[string]string, len(dirs))
	for _, dir := range dirs {
		envelopeBody, err := os.ReadFile(filepath.Join(dir, EnvelopeFileName))
		if err != nil {
			t.Fatal(err)
		}
		var envelope Envelope
		if err := yaml.Unmarshal(envelopeBody, &envelope); err != nil {
			t.Fatal(err)
		}
		wantByID[envelope.ID] = dir
	}
	slices.Reverse(dirs)

	development, err := LoadDevelopmentPlatformPackInventory(testPlatformVersion, dirs, embedded)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range development.Entries() {
		if got, want := entry.Directory(), wantByID[entry.ID()]; got != want {
			t.Errorf("pack %q directory = %q, want configured directory %q", entry.ID(), got, want)
		}
	}
}

func TestDevelopmentOverrideRejectsSymlinkedConfiguredRoot(t *testing.T) {
	embedded, err := LoadEmbeddedPlatformPackInventory(testPlatformVersion)
	if err != nil {
		t.Fatal(err)
	}
	dirs := materializeDevelopmentInventory(t, embedded, nil)
	target := dirs[0]
	symlink := filepath.Join(t.TempDir(), "pack-root")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	dirs[0] = symlink
	if _, err := LoadDevelopmentPlatformPackInventory(testPlatformVersion, dirs, embedded); err == nil || !strings.Contains(err.Error(), "must be a real directory") {
		t.Fatalf("symlinked development root error = %v", err)
	}
}

func materializeDevelopmentInventory(t *testing.T, inventory *PlatformPackInventory, replacements map[string][]byte) []string {
	t.Helper()
	root := t.TempDir()
	dirs := make([]string, 0, len(inventory.Entries()))
	for _, entry := range inventory.Entries() {
		directory := filepath.Join(root, entry.ID())
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		envelope := entry.Envelope()
		manifestBody := entry.ManifestBody()
		if replacement, ok := replacements[entry.ID()]; ok {
			manifestBody = replacement
			var err error
			envelope, _, err = StampEnvelope(envelope, manifestBody)
			if err != nil {
				t.Fatal(err)
			}
		}
		envelopeBody, err := yaml.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, EnvelopeFileName), envelopeBody, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, ManifestFileNameForType(entry.Type())), manifestBody, 0o644); err != nil {
			t.Fatal(err)
		}
		dirs = append(dirs, directory)
	}
	return dirs
}
