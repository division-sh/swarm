package runtime

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// These census inputs lack a named semantic-admission proof. The served-reporter
// overlay already has TestFanOutExecutionSourcesLoadExactOverlays with its owner.
func TestClosureNonCatalogBundleAdmission2287(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	for _, fixture := range []struct {
		root         string
		declarations []string
	}{
		{"internal/releasee2e/testdata/claude_cli_managed_lifecycle", []string{"worker/nodes.yaml"}},
		{"internal/releasee2e/testdata/claude_resource_read", []string{"worker/nodes.yaml"}},
		{"internal/releasee2e/testdata/golden_agent_workload", []string{"nodes.yaml", "candidate/nodes.yaml", "scout/nodes.yaml"}},
		{"internal/releasee2e/testdata/node_identity", []string{"nodes.yaml", "left/nodes.yaml", "left/nested/nodes.yaml", "right/nodes.yaml"}},
		{"internal/releasee2e/testdata/static_data_invocation", []string{"nodes.yaml", "registry/nodes.yaml"}},
		{"internal/releasee2e/testdata/workspace_mcp", []string{"nodes.yaml"}},
		{"internal/runtime/cataloge2e/testdata/scatter-gather-safety", []string{"nodes.yaml", "collector/nodes.yaml", "workers/nodes.yaml"}},
		{"internal/runtime/conformance/testdata/stage-lifecycle-identity", []string{"scout/nodes.yaml"}},
		{"internal/runtime/pipeline/testdata/entity-requirement-execution", []string{"nodes.yaml"}},
		{"internal/runtime/testdata/generic-swarm-bundle", []string{"delivery/nodes.yaml", "intake/nodes.yaml", "processing/nodes.yaml"}},
		{"internal/runtime/testfixtures/canonicalrouting/testdata/clock-deployment", []string{"finite/schema.yaml", "nested/clock/schema.yaml"}},
		{"internal/runtime/testfixtures/canonicalrouting/testdata/issue2564/final-entry-completion", []string{"work/nodes.yaml"}},
		{"internal/runtime/testfixtures/canonicalrouting/testdata/issue2564/h1", []string{"hub/nodes.yaml", "hub/agents.yaml"}},
		{"internal/runtime/testfixtures/canonicalrouting/testdata/issue2564/h2", []string{"hub/nodes.yaml"}},
		{"internal/runtime/testfixtures/canonicalrouting/testdata/issue2564/h3", []string{"hub/nodes.yaml", "hub/agents.yaml"}},
		{"internal/runtime/testfixtures/canonicalrouting/testdata/issue2564/m33", []string{"hub/nodes.yaml", "hub/agents.yaml"}},
		{"internal/serveapp/testdata/verify-run-jobflow-registry", []string{"registry/nodes.yaml"}},
		{"tests/conformance/entity-progressive-presence", []string{"nodes.yaml"}},
	} {
		t.Run(fixture.root, func(t *testing.T) {
			root := filepath.Join(repo, filepath.FromSlash(fixture.root))
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatalf("semantic bundle admission: %v", err)
			}
			for _, label := range fixture.declarations {
				assertClosureDeclaration2287(t, bundle, root, label)
			}
		})
	}
}

func TestClosureTerminalActivityLoopVariantsAdmission2287(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	for _, variant := range []string{"activity_loop", "activity_loop_rule", "activity_loop_failure"} {
		t.Run(variant, func(t *testing.T) {
			root := canonicalrouting.CopySelectedForkReadiness(t, 1, variant)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatalf("assembled terminal-loop semantic admission: %v", err)
			}
			assertClosureDeclaration2287(t, bundle, root, "worker-flow/nodes.yaml")
		})
	}
}

func assertClosureDeclaration2287(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, root, label string) {
	t.Helper()
	used := false
	for _, source := range bundle.FlowSources {
		if source.DeclarationLabel(filepath.Base(label)) == label {
			used = true
		}
	}
	if !used {
		t.Fatalf("%s was not consumed as a semantic declaration", label)
	}
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(label)))
	if err != nil {
		t.Fatal(err)
	}
	entry, found := bundle.SourceArtifact.Entry(label)
	if !found || !bytes.Equal(raw, entry.Bytes()) {
		t.Fatalf("admitted declaration %s differs from its actual source", label)
	}
	t.Logf("semantic_declaration=%s sha256=%x bundle=%s", label, sha256.Sum256(raw), bundle.SourceArtifact.BundleHash())
}
