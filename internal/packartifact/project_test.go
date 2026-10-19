package packartifact

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

const testPlatformVersion = "0.7.0"

func TestImportEmbeddedPackIsIdempotentAndPreservesEditedConflict(t *testing.T) {
	members := s1ProviderMembers()
	files := fstest.MapFS{InventoryManifestFileName: {Data: []byte("version: 1\npacks:\n  - path: providers/demo\n")}}
	for name, body := range members {
		files["providers/demo/"+name] = &fstest.MapFile{Data: body}
	}
	base, err := LoadPlatformPackInventoryFS(files, InventoryManifestFileName, testPlatformVersion, SelectionEmbedded)
	if err != nil {
		t.Fatal(err)
	}
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, "manifest.yaml"), []byte("version: 1.0.0\nplatform_version: '*'\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err := ImportEmbeddedPack(project, "demo", base)
	if err != nil || !changed {
		t.Fatalf("first import changed=%v error=%v", changed, err)
	}
	for name, want := range members {
		got, err := os.ReadFile(filepath.Join(project, "demo", name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("copied member %s = %q, error=%v", name, got, err)
		}
	}
	changed, err = ImportEmbeddedPack(project, "demo", base)
	if err != nil || changed {
		t.Fatalf("identical import changed=%v error=%v", changed, err)
	}
	bodyPath := filepath.Join(project, "demo", "trigger.yaml")
	edited := []byte("event_name: {literal: edited}\n")
	if err := os.WriteFile(bodyPath, edited, 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err = ImportEmbeddedPack(project, "demo", base)
	if err == nil || changed || !strings.Contains(err.Error(), "will not overwrite") {
		t.Fatalf("edited import changed=%v error=%v", changed, err)
	}
	if got, err := os.ReadFile(bodyPath); err != nil || !bytes.Equal(got, edited) {
		t.Fatalf("edited member overwritten: %q, error=%v", got, err)
	}
}
