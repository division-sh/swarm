package semanticview

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type s1InventedProviderScope struct{ Source }

func (s1InventedProviderScope) ProviderDirectoriesInScope(string) ([]sourceartifact.ProviderDirectory, error) {
	return []sourceartifact.ProviderDirectory{{}}, nil
}

func TestS1ProviderScopeConsumesFrozenCompilerMembership(t *testing.T) {
	root := t.TempDir()
	for label, body := range map[string]string{
		"schema.yaml":         "stages: []\n",
		"team/manifest.yaml":  "version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [message]}, cannot: [execute code]}\n",
		"team/trigger.yaml":   "{}\n",
		"child/schema.yaml":   "stages: []\n",
		"sibling/schema.yaml": "stages: []\n",
	} {
		file := filepath.Join(root, label)
		if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(filepath.Clean("../../.."), sourceartifactfixture.CapturedRoot(t, artifact), "", contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source := s1InventedProviderScope{Wrap(bundle)}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	bundle.FlowSources = map[string]contracts.FlowSource{}
	bundle.FlowTree = contracts.FlowTree{}
	providers, err := ProviderDirectoriesInScope(source, "child")
	if err != nil || len(providers) != 1 || providers[0].Path() != "team" {
		t.Fatalf("mutable source indexes or caller override replaced admitted membership: %v %v", providers, err)
	}
	if providers, err := ProviderDirectoriesInScope(source, "invented"); err == nil || providers != nil {
		t.Fatalf("mutable semantic view invented a flow: %v %v", providers, err)
	}
	copy, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	bundle.SourceArtifact = copy
	if providers, err := ProviderDirectoriesInScope(source, "."); err == nil || providers != nil {
		t.Fatalf("substituted content borrowed compiler authority: %v %v", providers, err)
	}
	for _, invalid := range []Source{nil, s1InventedProviderScope{Wrap(&contracts.WorkflowContractBundle{SourceArtifact: artifact})}} {
		if providers, err := ProviderDirectoriesInScope(invalid, "."); err == nil || providers != nil {
			t.Fatalf("programmatic source minted rooted scope: %v %v", providers, err)
		}
	}
}
