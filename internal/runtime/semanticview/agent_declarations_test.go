package semanticview

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestAgentDeclarationsResolvesUniqueRootOnlyProjectOwner(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: root-only-agent\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "root-agent:\n  id: root-agent\n  model: regular\n  memory: false\n  intent:\n    inline: Exercise root declaration ownership.\n")

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	declarations := AgentDeclarations(Wrap(bundle))
	if len(declarations) != 1 || declarations[0].OwnerURI == "" || declarations[0].OwnerFlowID != "." {
		t.Fatalf("declarations = %#v, want one root flow declaration with an exact owner", declarations)
	}
	if owner, ok := ScopedAgentDeclarationOwner(Wrap(bundle), declarations[0]); !ok || owner != declarations[0].OwnerURI {
		t.Fatalf("owner = %q, ok=%v, want %q", owner, ok, declarations[0].OwnerURI)
	}
}
