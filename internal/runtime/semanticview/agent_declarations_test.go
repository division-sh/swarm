package semanticview

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestAgentDeclarationsDeduplicatesPackageBackedFlowProjectionAndResolvesOwner(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()

	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: agent-declaration-census
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: agent-declaration-census\n")
	for _, file := range []string{"agents.yaml", "events.yaml", "nodes.yaml", "policy.yaml", "tools.yaml"} {
		writeSemanticviewFixtureFile(t, filepath.Join(root, file), "{}\n")
	}
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "package.yaml"), "name: support\nversion: \"1.0.0\"\nflows: []\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), "name: support\nmode: static\ninitial_state: active\nstates: [active]\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), "flow-agent:\n  id: flow-agent\n  model: regular\n  memory: false\n  intent:\n    inline: Exercise package-backed declaration ownership.\n")
	for _, file := range []string{"events.yaml", "nodes.yaml", "policy.yaml", "tools.yaml"} {
		writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", file), "{}\n")
	}

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := Wrap(bundle)
	rawOccurrences := 0
	for _, scope := range source.ProjectScopes() {
		if _, ok := scope.Agents["flow-agent"]; ok {
			rawOccurrences++
		}
	}
	for _, scope := range source.FlowScopes() {
		if _, ok := scope.Agents["flow-agent"]; ok {
			rawOccurrences++
		}
	}
	if rawOccurrences < 2 {
		t.Fatalf("raw scoped occurrences = %d, want duplicate loader projections", rawOccurrences)
	}

	declarations := AgentDeclarations(source)
	if len(declarations) != 1 {
		t.Fatalf("declarations = %#v, want one canonical flow declaration", declarations)
	}
	declaration := declarations[0]
	if declaration.ScopeKind != "project" || declaration.ScopeID != "flows/support" || declaration.OwnerFlowID != "support" || declaration.LocalID != "flow-agent" {
		t.Fatalf("declaration = %#v, want canonical package-backed support agent", declaration)
	}
	owner, ok := ScopedAgentDeclarationOwner(source, declaration)
	if !ok || !strings.Contains(owner, "flow-agent") {
		t.Fatalf("owner = %q, ok=%v, refs=%#v, want exact declaration URI", owner, ok, bundle.URIRegistry.Agents)
	}
	wrongScope := declaration
	wrongScope.ScopeID = "flows/other"
	if owner, ok := ScopedAgentDeclarationOwner(source, wrongScope); ok {
		t.Fatalf("wrong-scope declaration unexpectedly resolved owner %q", owner)
	}
	delete(bundle.URIRegistry.ByURI, declaration.OwnerURI)
	missingOwnerDeclaration := AgentDeclarations(Wrap(bundle))[0]
	if owner, ok := ScopedAgentDeclarationOwner(Wrap(bundle), missingOwnerDeclaration); ok {
		t.Fatalf("declaration with an unregistered scoped URI unexpectedly resolved owner %q", owner)
	}
}

func TestAgentDeclarationsResolvesUniqueRootOnlyProjectOwner(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()
	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), "name: root-only-agent\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: root-only-agent\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "root-agent:\n  id: root-agent\n  model: regular\n  memory: false\n  intent:\n    inline: Exercise root declaration ownership.\n")
	for _, file := range []string{"events.yaml", "nodes.yaml", "policy.yaml", "tools.yaml"} {
		writeSemanticviewFixtureFile(t, filepath.Join(root, file), "{}\n")
	}

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	declarations := AgentDeclarations(Wrap(bundle))
	if len(declarations) != 1 || declarations[0].OwnerURI == "" {
		t.Fatalf("declarations = %#v, want one root project declaration with an exact owner", declarations)
	}
	if owner, ok := ScopedAgentDeclarationOwner(Wrap(bundle), declarations[0]); !ok || owner != declarations[0].OwnerURI {
		t.Fatalf("owner = %q, ok=%v, want %q", owner, ok, declarations[0].OwnerURI)
	}
}
