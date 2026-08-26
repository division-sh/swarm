package bootverify

import (
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestWorkspaceClassFindingsCensusAmbiguousScopedDeclarations(t *testing.T) {
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: scoped-workspace-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: packages/project-a
  - path: packages/project-b
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: scoped-workspace-validation\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), `
workspace_classes:
  dedicated:
    workspace_scope: per-agent
`)
	for _, project := range []string{"project-a", "project-b"} {
		dir := filepath.Join(root, "packages", project)
		writeBootverifyFixtureFile(t, filepath.Join(dir, "package.yaml"), "name: "+project+"\nversion: \"1.0.0\"\nflows: []\n")
		writeBootverifyFixtureFile(t, filepath.Join(dir, "agents.yaml"), `
shared-worker:
  id: shared-worker
  model: regular
  memory: false
  intent:
    inline: Validate workspace-class ownership for this scoped worker.
  workspace_class: missing
`)
	}

	repoRoot := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	source := semanticview.Wrap(bundle)
	joined := findingsText(workspaceClassFindings(source))
	for _, want := range []string{
		`project packages/project-a agent shared-worker references undefined workspace_class "missing"`,
		`project packages/project-b agent shared-worker references undefined workspace_class "missing"`,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("workspace findings = %q, want %q", joined, want)
		}
	}
}

func findingsText(findings []Finding) string {
	parts := make([]string, 0, len(findings))
	for _, finding := range findings {
		parts = append(parts, finding.Message)
	}
	return strings.Join(parts, "\n")
}
