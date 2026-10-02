package contracts

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func Test2376NestedManifestPreservesSemanticScope(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := canonicalrouting.CopyConnectedRootInput(t)
	load := func() *WorkflowContractBundle {
		t.Helper()
		bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
		if err != nil {
			t.Fatal(err)
		}
		return bundle
	}
	before := load()
	for _, member := range []struct{ path, body string }{
		{"manifest.yaml", "name: Not the directory name\nversion: 1.2.3\nplatform_version: '>=0.7.0 <0.8.0'\n"},
		{"producer/manifest.yaml", "name: Also free text\nversion: 4.5.6\nplatform_version: '>=9.0.0'\n"},
	} {
		if err := os.WriteFile(filepath.Join(root, member.path), []byte(member.body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	after := load()
	if before.SourceArtifact.BundleHash() == after.SourceArtifact.BundleHash() {
		t.Fatal("metadata bytes did not change source identity")
	}
	for _, row := range []struct {
		name          string
		before, after any
	}{
		{"flow paths", before.SourceArtifact.Root().Children()[0].Path(), after.SourceArtifact.Root().Children()[0].Path()},
		{"scoped events", before.ScopedEventEntries(), after.ScopedEventEntries()},
		{"input pins", before.Semantics.flowInputEventPins, after.Semantics.flowInputEventPins},
		{"output pins", before.Semantics.flowOutputEventPins, after.Semantics.flowOutputEventPins},
		{"connections", before.CompositionConnects(), after.CompositionConnects()},
	} {
		if !reflect.DeepEqual(row.before, row.after) {
			t.Fatalf("manifest changed %s", row.name)
		}
	}
	stored, err := sourceartifact.DecodeLogical(after.SourceArtifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadWorkflowContractBundleFromArtifact(repo, stored, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{}); err != nil {
		t.Fatalf("stored compile enforced nested range: %v", err)
	}
	if _, err := LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(root, "producer"), DefaultPlatformSpecFile(repo)); err == nil || !strings.Contains(err.Error(), "platform_version") {
		t.Fatalf("selected nested root did not enforce its own range: %v", err)
	}
}
