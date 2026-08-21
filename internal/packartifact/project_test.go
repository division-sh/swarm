package packartifact

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestImportEmbeddedPackIsIdempotentAndPreservesEditedConflict(t *testing.T) {
	base, err := LoadPlatformPackInventoryFS(
		testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", ProvenancePlatform, []byte("provider: demo\n")),
		InventoryManifestFileName,
		testPlatformVersion,
		SelectionEmbedded,
	)
	if err != nil {
		t.Fatal(err)
	}
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, "package.yaml"), []byte("name: demo\nversion: 1.0.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	changed, err := ImportEmbeddedPack(project, "provider.demo", base)
	if err != nil || !changed {
		t.Fatalf("first import changed=%v error=%v", changed, err)
	}
	set, err := LoadProjectPackSet(project)
	if err != nil {
		t.Fatalf("LoadProjectPackSet: %v", err)
	}
	if len(set.Sources) != 1 || !bytes.Equal(set.Sources[0].ManifestBody, []byte("provider: demo\n")) {
		t.Fatalf("project sources = %#v", set.Sources)
	}
	if !strings.Contains(string(set.Sources[0].EnvelopeBody), "source: project") || !strings.Contains(string(set.Sources[0].EnvelopeBody), "manifest_hash: derived") {
		t.Fatalf("project envelope = %s", set.Sources[0].EnvelopeBody)
	}
	changed, err = ImportEmbeddedPack(project, "provider.demo", base)
	if err != nil || changed {
		t.Fatalf("identical import changed=%v error=%v", changed, err)
	}

	bodyPath := filepath.Join(project, "packs", "provider.demo", TriggerManifestFileName)
	if err := os.WriteFile(bodyPath, []byte("provider: demo\nrevision: edited\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err = ImportEmbeddedPack(project, "provider.demo", base)
	if err == nil || changed || !strings.Contains(err.Error(), "will not overwrite") {
		t.Fatalf("edited import changed=%v error=%v", changed, err)
	}
}

func TestProjectPackAdmissionRejectsHostileMembership(t *testing.T) {
	base, err := LoadPlatformPackInventoryFS(
		testInventoryFS(t, "provider.demo", TypeTrigger, "provider-triggers/demo", ProvenancePlatform, []byte("provider: demo\n")),
		InventoryManifestFileName,
		testPlatformVersion,
		SelectionEmbedded,
	)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(t *testing.T, project string)
	}{
		{name: "missing manifest", mutate: func(t *testing.T, project string) {
			t.Helper()
			if err := os.Remove(filepath.Join(project, "packs", ProjectPackManifestFileName)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "manifest symlink", mutate: func(t *testing.T, project string) {
			t.Helper()
			manifest := filepath.Join(project, "packs", ProjectPackManifestFileName)
			if err := os.Remove(manifest); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(project, "package.yaml"), manifest); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "malformed manifest", mutate: func(t *testing.T, project string) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(project, "packs", ProjectPackManifestFileName), []byte("version: 1\nimports: [\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "manifest trailing document", mutate: func(t *testing.T, project string) {
			t.Helper()
			manifest := filepath.Join(project, "packs", ProjectPackManifestFileName)
			body, err := os.ReadFile(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(manifest, append(body, []byte("---\nversion: 1\n")...), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unlisted file", mutate: func(t *testing.T, project string) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(project, "packs", "notes.yaml"), []byte("hidden: true\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unlisted directory", mutate: func(t *testing.T, project string) {
			t.Helper()
			if err := os.Mkdir(filepath.Join(project, "packs", "hidden"), 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "body symlink", mutate: func(t *testing.T, project string) {
			t.Helper()
			body := filepath.Join(project, "packs", "provider.demo", TriggerManifestFileName)
			if err := os.Remove(body); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(project, "package.yaml"), body); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "body non regular", mutate: func(t *testing.T, project string) {
			t.Helper()
			body := filepath.Join(project, "packs", "provider.demo", TriggerManifestFileName)
			if err := os.Remove(body); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(body, 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing body", mutate: func(t *testing.T, project string) {
			t.Helper()
			if err := os.Remove(filepath.Join(project, "packs", "provider.demo", TriggerManifestFileName)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "pack directory symlink", mutate: func(t *testing.T, project string) {
			t.Helper()
			directory := filepath.Join(project, "packs", "provider.demo")
			if err := os.RemoveAll(directory); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(project, directory); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing envelope", mutate: func(t *testing.T, project string) {
			t.Helper()
			if err := os.Remove(filepath.Join(project, "packs", "provider.demo", EnvelopeFileName)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "malformed envelope", mutate: func(t *testing.T, project string) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(project, "packs", "provider.demo", EnvelopeFileName), []byte("id: [\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "envelope trailing document", mutate: func(t *testing.T, project string) {
			t.Helper()
			rewriteProjectPackEnvelope(t, project, func(body string) string { return body + "---\nid: provider.second\n" })
		}},
		{name: "wrong provenance", mutate: func(t *testing.T, project string) {
			t.Helper()
			rewriteProjectPackEnvelope(t, project, func(body string) string { return strings.Replace(body, "source: project", "source: external", 1) })
		}},
		{name: "stale manifest hash", mutate: func(t *testing.T, project string) {
			t.Helper()
			rewriteProjectPackEnvelope(t, project, func(body string) string {
				return strings.Replace(body, "manifest_hash: derived", "manifest_hash: sha256:"+strings.Repeat("a", 64), 1)
			})
		}},
		{name: "type contradiction", mutate: func(t *testing.T, project string) {
			t.Helper()
			rewriteProjectPackEnvelope(t, project, func(body string) string { return strings.Replace(body, "type: trigger", "type: connector", 1) })
		}},
		{name: "incompatible platform", mutate: func(t *testing.T, project string) {
			t.Helper()
			rewriteProjectPackEnvelope(t, project, func(body string) string { return strings.Replace(body, ">=0.7.0 <0.8.0", ">=99.0.0 <100.0.0", 1) })
		}},
		{name: "duplicate id", mutate: func(t *testing.T, project string) {
			t.Helper()
			mutateProjectPackManifest(t, project, func(manifest *ProjectPackManifest) {
				duplicate := manifest.Imports[0]
				duplicate.Path = "provider.other"
				manifest.Imports = append(manifest.Imports, duplicate)
			})
		}},
		{name: "duplicate path", mutate: func(t *testing.T, project string) {
			t.Helper()
			mutateProjectPackManifest(t, project, func(manifest *ProjectPackManifest) {
				duplicate := manifest.Imports[0]
				duplicate.ID = "provider.other"
				duplicate.Origin.ID = "provider.other"
				manifest.Imports = append(manifest.Imports, duplicate)
			})
		}},
		{name: "invalid origin", mutate: func(t *testing.T, project string) {
			t.Helper()
			mutateProjectPackManifest(t, project, func(manifest *ProjectPackManifest) {
				manifest.Imports[0].Origin.Source = ProvenanceExternal
			})
		}},
		{name: "manifest traversal", mutate: func(t *testing.T, project string) {
			t.Helper()
			manifestPath := filepath.Join(project, "packs", ProjectPackManifestFileName)
			body, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			var manifest ProjectPackManifest
			if err := yaml.Unmarshal(body, &manifest); err != nil {
				t.Fatal(err)
			}
			manifest.Imports[0].Path = "../provider.demo"
			body, err = yaml.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(manifestPath, body, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			project := t.TempDir()
			if err := os.WriteFile(filepath.Join(project, "package.yaml"), []byte("name: demo\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := ImportEmbeddedPack(project, "provider.demo", base); err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, project)
			set, err := LoadProjectPackSet(project)
			if err != nil {
				return
			}
			if _, err := NewEffectivePackInventory(base, set.Sources); err == nil {
				t.Fatal("project pack load and effective admission succeeded for hostile project inventory")
			}
		})
	}
}

func rewriteProjectPackEnvelope(t *testing.T, project string, mutate func(string) string) {
	t.Helper()
	path := filepath.Join(project, "packs", "provider.demo", EnvelopeFileName)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(mutate(string(body))), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mutateProjectPackManifest(t *testing.T, project string, mutate func(*ProjectPackManifest)) {
	t.Helper()
	path := filepath.Join(project, "packs", ProjectPackManifestFileName)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest ProjectPackManifest
	if err := yaml.Unmarshal(body, &manifest); err != nil {
		t.Fatal(err)
	}
	mutate(&manifest)
	body, err = yaml.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
}
