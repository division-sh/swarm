package semanticview

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestProjectScopes_PackageBackedScopeCarriesOwningFlowID(t *testing.T) {
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
  intent: {inline: "Exercise package-backed scope."}
  model: regular
  memory: true
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

func TestRootExecutionCoordinateBindsAuthoredRootAndExactRun(t *testing.T) {
	root := runtimecontracts.FlowContractView{
		Path:  "authored-root",
		Paths: runtimecontracts.FlowContractPaths{ID: "authored-root", Flow: "authored-root"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Name: "authored-root",
		},
	}
	source := Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "display-workflow"},
		FlowTree: runtimecontracts.FlowTree{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{"authored-root": &root},
		},
	})
	const runID = "11111111-1111-1111-1111-111111111111"

	coordinate, err := AdmitRootExecutionCoordinate(source, runID)
	if err != nil {
		t.Fatalf("AdmitRootExecutionCoordinate: %v", err)
	}
	if coordinate.FlowID() != "authored-root" || coordinate.RunID() != runID {
		t.Fatalf("coordinate = (%q, %q), want exact authored root and run", coordinate.FlowID(), coordinate.RunID())
	}
	if source.WorkflowName() != "display-workflow" || coordinate.FlowID() == source.WorkflowName() {
		t.Fatalf("test requires distinct display and authored root identities: display=%q authored=%q", source.WorkflowName(), coordinate.FlowID())
	}
	if !coordinate.Matches("authored-root", runID) {
		t.Fatal("exact root coordinate did not match")
	}
	for _, hostile := range []struct {
		flowID string
		runID  string
	}{
		{flowID: "display-workflow", runID: runID},
		{flowID: "authored-root", runID: "22222222-2222-2222-2222-222222222222"},
		{flowID: "display-workflow", runID: "22222222-2222-2222-2222-222222222222"},
	} {
		if coordinate.Matches(hostile.flowID, hostile.runID) {
			t.Fatalf("hostile coordinate (%q, %q) matched exact root owner", hostile.flowID, hostile.runID)
		}
	}
	if _, err := AdmitRootExecutionCoordinate(source, ""); err == nil {
		t.Fatal("missing run identity was admitted")
	}
	if _, err := AdmitRootExecutionCoordinate(nil, runID); err == nil {
		t.Fatal("missing semantic source was admitted")
	}
}

func TestProjectScopes_SoleParentFlowDoesNotOwnUnrelatedSiblingPackage(t *testing.T) {
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
  intent: {inline: "Exercise sole-parent scope."}
  model: regular
  memory: true
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
	if packageScope.OwningFlowID != "" {
		t.Fatalf("OwningFlowID = %q, want explicit root ownership", packageScope.OwningFlowID)
	}
	declarations := AgentDeclarations(source)
	if len(declarations) != 1 || declarations[0].OwnerFlowID != "" {
		t.Fatalf("declarations = %#v, want unrelated package declaration to remain root-owned", declarations)
	}
}

func TestResolveAgentMemoryProof_PackageBackedFlowProjectionUsesCanonicalFlowSource(t *testing.T) {
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
  intent: {inline: "Exercise package-backed memory proof."}
  model: regular
  memory: true
  subscriptions:
    - support/item.created
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := Wrap(bundle)

	proof := ResolveAgentMemoryProof(source, AgentMemoryLocator{
		AgentID: "backend",
		FlowID:  "support",
	})
	if proof.OwningFlowID != "support" {
		t.Fatalf("OwningFlowID = %q, want support; declarations = %#v", proof.OwningFlowID, AgentDeclarations(source))
	}
	if proof.ProjectScopeKey != "." || proof.ContractSource.Layer != "flow" || proof.ContractSource.FlowID != "support" {
		t.Fatalf("memory proof source = %#v, want root-package flow-owned source", proof)
	}
	if proof.FlowPath != "support" {
		t.Fatalf("FlowPath = %q, want support", proof.FlowPath)
	}
}

func TestResolveAgentMemoryProof_FlowScopedAgentCarriesFlowPath(t *testing.T) {
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
  intent: {inline: "Exercise flow-scoped memory proof."}
  model: regular
  memory: true
  subscriptions:
    - item.created
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := Wrap(bundle)

	proof := ResolveAgentMemoryProof(source, AgentMemoryLocator{
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
