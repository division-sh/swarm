package semanticview

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	canonicalrouting "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestProjectScopes_PackageBackedScopeCarriesOwningFlowID(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/semanticview/scopes_test.go:TestProjectScopes_PackageBackedScopeCarriesOwningFlowID"))
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()

	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "package.yaml"), `
name: support
version: "1.0.0"
flows: []
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
  - done
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), `
flow-agent:
  id: flow-agent
  model: regular
  mode: session
  subscriptions:
    - support/item.created
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := Wrap(bundle)

	var packageScope ProjectScope
	var found bool
	for _, scope := range source.ProjectScopes() {
		if scope.Key == "flows/support" && len(scope.Agents) > 0 {
			packageScope = scope
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected package-backed support project scope, got %#v", source.ProjectScopes())
	}
	if packageScope.OwningFlowID != "support" {
		t.Fatalf("OwningFlowID = %q, want support", packageScope.OwningFlowID)
	}
}

func TestProjectScopes_SoleParentFlowCarriesOwningFlowIDOutsideFlowDir(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/semanticview/scopes_test.go:TestProjectScopes_PackageBackedScopeCarriesOwningFlowID"))
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()

	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: extras
flows:
  - id: support
    flow: support
    mode: static
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
  - done
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "extras", "package.yaml"), `
name: extras
version: "1.0.0"
flows: []
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "extras", "agents.yaml"), `
flow-agent:
  id: flow-agent
  model: regular
  mode: session
  subscriptions:
    - support/item.created
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := Wrap(bundle)

	var packageScope ProjectScope
	var found bool
	for _, scope := range source.ProjectScopes() {
		if scope.Key == "extras" && len(scope.Agents) > 0 {
			packageScope = scope
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected extras project scope, got %#v", source.ProjectScopes())
	}
	if packageScope.OwningFlowID != "support" {
		t.Fatalf("OwningFlowID = %q, want support", packageScope.OwningFlowID)
	}
}

func TestResolveAgentSessionScopeProof_PackageBackedAgentCarriesFlowPath(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/semanticview/scopes_test.go:TestProjectScopes_PackageBackedScopeCarriesOwningFlowID"))
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()

	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "package.yaml"), `
name: support
version: "1.0.0"
flows: []
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
  - done
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), `
backend:
  id: backend-{vertical_id}
  model: regular
  mode: session
  subscriptions:
    - support/item.created
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := Wrap(bundle)

	proof := ResolveAgentSessionScopeProof(source, AgentSessionScopeLocator{
		AgentID:         "backend",
		ProjectScopeKey: "flows/support",
	})
	if proof.OwningFlowID != "support" {
		t.Fatalf("OwningFlowID = %q, want support", proof.OwningFlowID)
	}
	if proof.FlowPath != "support" {
		t.Fatalf("FlowPath = %q, want support", proof.FlowPath)
	}
}

func TestResolveAgentSessionScopeProof_FlowScopedAgentCarriesFlowPath(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/semanticview/scopes_test.go:TestProjectScopes_PackageBackedScopeCarriesOwningFlowID"))
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", ".."))
	root := t.TempDir()

	writeSemanticviewFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
  - done
`)
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), "{}\n")
	writeSemanticviewFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), `
backend:
  id: backend-{flow_id}
  model: regular
  mode: session
  subscriptions:
    - item.created
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := Wrap(bundle)

	proof := ResolveAgentSessionScopeProof(source, AgentSessionScopeLocator{
		AgentID: "backend",
		FlowID:  "support",
	})
	if proof.OwningFlowID != "support" {
		t.Fatalf("OwningFlowID = %q, want support", proof.OwningFlowID)
	}
	if proof.FlowPath != "support" {
		t.Fatalf("FlowPath = %q, want support", proof.FlowPath)
	}
}

func writeSemanticviewFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
