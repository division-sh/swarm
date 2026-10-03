package cataloge2e

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestStaticDataInvocationGoldenConsumesAdmittedIdentity(t *testing.T) {
	repo := repoRootFromCatalogE2E(t)
	root := filepath.Join(repo, "internal/releasee2e/testdata/static_data_invocation")
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	var expected struct {
		Readback map[string]map[string]any `json:"readback"`
	}
	raw, err := os.ReadFile(root + ".expected.json")
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&expected); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		t.Fatalf("static oracle has trailing data: %v", err)
	}
	if bundle.SourceArtifact.BundleHash() == "" {
		t.Fatal("fixture has no canonical admitted identity")
	}
	source := semanticview.Wrap(bundle)
	observed := map[string]map[string]any{}
	for flow, event := range map[string]string{".": "read.completed", "registry": "registry/child.completed"} {
		data := source.StaticDataForAgent(flow, "reader")
		if len(data) != 1 {
			t.Fatalf("flow %s admitted data=%v, want one exact file", flow, data)
		}
		observed[event] = map[string]any{"static_id": string(data[0].StaticID), "content": string(data[0].Content)}
	}
	if os.Getenv("SWARM_UPDATE_STATIC_DATA_INVOCATION_GOLDEN") == "1" {
		expected.Readback = observed
		encoded, err := json.MarshalIndent(expected, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(root+".expected.json", append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(observed, expected.Readback) {
		t.Fatalf("golden is not the exact admitted static-data identity/content: got=%v want=%v", observed, expected.Readback)
	}
	for _, field := range []string{"static_id", "content"} {
		t.Run("corrupt_expected_"+field, func(t *testing.T) {
			var corrupted struct {
				Readback map[string]map[string]any `json:"readback"`
			}
			if err := json.Unmarshal(raw, &corrupted); err != nil {
				t.Fatal(err)
			}
			corrupted.Readback["read.completed"][field] = "foreign"
			if reflect.DeepEqual(observed, corrupted.Readback) {
				t.Fatal("corrupt oracle matched independent admission")
			}
		})
	}
	t.Run("changed_fixture_bytes", func(t *testing.T) {
		copyRoot := t.TempDir()
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			target := filepath.Join(copyRoot, rel)
			if entry.IsDir() {
				return os.MkdirAll(target, 0700)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			return os.WriteFile(target, data, 0600)
		}); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(copyRoot, "data/resume.md"), []byte("different fixture bytes\n"), 0600); err != nil {
			t.Fatal(err)
		}
		changed, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, copyRoot, contracts.DefaultPlatformSpecFile(repo))
		if err != nil {
			t.Fatal(err)
		}
		data := semanticview.Wrap(changed).StaticDataForAgent(".", "reader")
		if changed.SourceArtifact.BundleHash() == bundle.SourceArtifact.BundleHash() || len(data) != 1 || string(data[0].Content) == observed["read.completed"]["content"] {
			t.Fatal("fixture mutation was hidden by derived identity")
		}
	})
}
