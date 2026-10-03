package cataloge2e

import (
	"encoding/json"
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
		BundleHash string                    `json:"bundle_hash"`
		Readback   map[string]map[string]any `json:"readback"`
	}
	raw, err := os.ReadFile(root + ".expected.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
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
		expected.BundleHash, expected.Readback = bundle.SourceArtifact.BundleHash(), observed
		encoded, err := json.MarshalIndent(expected, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(root+".expected.json", append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if got := bundle.SourceArtifact.BundleHash(); got != expected.BundleHash {
		t.Fatalf("golden source identity=%s, admitted=%s", expected.BundleHash, got)
	}
	if !reflect.DeepEqual(observed, expected.Readback) {
		t.Fatalf("golden is not the exact admitted static-data identity/content: got=%v want=%v", observed, expected.Readback)
	}
}
