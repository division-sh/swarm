package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDiscoverProjectPackagePathsIncludesNestedFlowPackages(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	workflowDir := writeLayer3FlowTreeFixture(t)

	paths := ResolveWorkflowContractPathsWithOverrides(repoRoot, workflowDir, "")

	if len(paths.ProjectPackages) < 2 {
		t.Fatalf("expected nested flow package discovery, got %d packages", len(paths.ProjectPackages))
	}
	var found bool
	for _, pkg := range paths.ProjectPackages {
		if pkg.Key == filepath.Clean(filepath.Join("flows", "parent")) {
			found = true
			if pkg.ParentKey != "." {
				t.Fatalf("expected nested flow package parent '.'; got %q", pkg.ParentKey)
			}
		}
	}
	if !found {
		t.Fatalf("expected nested flow package %q in discovered package tree", filepath.Clean(filepath.Join("flows", "parent")))
	}
}

func TestLoadWorkflowContractBundleBuildsRecursiveFlowTree(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	workflowDir := writeLayer3FlowTreeFixture(t)

	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, workflowDir, "")
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	if bundle.FlowTree.Root == nil {
		t.Fatal("expected populated FlowTree.Root")
	}
	if len(bundle.FlowTree.ByPath) == 0 {
		t.Fatal("expected populated FlowTree.ByPath")
	}
	parent, ok := bundle.FlowTree.ByPath["parent"]
	if !ok {
		t.Fatalf("expected flow path %q in FlowTree.ByPath", "parent")
	}
	child, ok := bundle.FlowTree.ByPath["parent/child"]
	if !ok {
		t.Fatalf("expected flow path %q in FlowTree.ByPath", "parent/child")
	}
	if got := strings.TrimSpace(parent.Paths.ID); got != "parent" {
		t.Fatalf("expected parent flow id %q, got %q", "parent", got)
	}
	if got := strings.TrimSpace(child.Paths.ID); got != "child" {
		t.Fatalf("expected child flow id %q, got %q", "child", got)
	}
	if got := strings.TrimSpace(parent.URI); got != "root-platform://parent" {
		t.Fatalf("expected parent flow uri %q, got %q", "root-platform://parent", got)
	}
	if got := strings.TrimSpace(child.URI); got != "root-platform://parent/child" {
		t.Fatalf("expected child flow uri %q, got %q", "root-platform://parent/child", got)
	}
	if child.Parent == nil {
		t.Fatal("expected child flow parent pointer to be set")
	}
	if got := strings.TrimSpace(child.Parent.Paths.ID); got != "parent" {
		t.Fatalf("expected child parent flow id %q, got %q", "parent", got)
	}
	if got := parent.NodeURIs["parent-node"]; got != "root-platform://parent/parent-node" {
		t.Fatalf("expected parent node uri, got %q", got)
	}
	if got := child.AgentURIs["child-agent"]; got != "root-platform://parent/child/child-agent" {
		t.Fatalf("expected child agent uri, got %q", got)
	}
	if got := child.EventURIs["child.completed"]; got != "root-platform://parent/child/child.completed" {
		t.Fatalf("expected child event uri, got %q", got)
	}
	if ref, ok := bundle.URIRegistry.ByURI["root-platform://parent/child/child.completed"]; !ok {
		t.Fatal("expected full URI in registry")
	} else if ref.Kind != "event" || ref.FlowID != "child" {
		t.Fatalf("unexpected URI registry ref: %+v", ref)
	}
	policy := bundle.ResolvedPolicyForFlow("child")
	if got := policy.Values["root_policy"].Value; got != "root" {
		t.Fatalf("expected root policy value %q, got %#v", "root", got)
	}
	if got := policy.Values["package_policy"].Value; got != "nested" {
		t.Fatalf("expected package policy value %q, got %#v", "nested", got)
	}
	if got := policy.Values["shared"].Value; got != "child" {
		t.Fatalf("expected child flow override %q, got %#v", "child", got)
	}
}

func repoRootForContractsTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

func writeLayer3FlowTreeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: root-platform
version: "1.0.0"
flows:
  - id: parent
    flow: parent
    mode: static
`)
	writeFixtureFile(t, filepath.Join(root, "policy.yaml"), `
root_policy:
  value: root
shared:
  value: root
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "schema.yaml"), `
name: parent
mode: static
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "nodes.yaml"), `
parent-node:
  id: parent-node
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "events.yaml"), `
parent.started:
  payload:
    entity_id: string
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "agents.yaml"), `
parent-agent:
  id: parent-agent
  role: parent-agent
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "package.yaml"), `
name: parent
version: "1.0.0"
flows:
  - id: child
    flow: child
    mode: static
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "policy.yaml"), `
package_policy:
  value: nested
shared:
  value: package
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "flows", "child", "schema.yaml"), `
name: child
mode: static
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "flows", "child", "nodes.yaml"), `
child-node:
  id: child-node
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "flows", "child", "events.yaml"), `
child.completed:
  payload:
    entity_id: string
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "flows", "child", "agents.yaml"), `
child-agent:
  id: child-agent
  role: child-agent
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "flows", "child", "policy.yaml"), `
shared:
  value: child
`)

	return root
}

func writeFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestToolInputSchema_TypedEnumAndAdditionalProperties(t *testing.T) {
	var schema ToolInputSchema
	if err := loadYAMLBytes([]byte(`
type: object
properties:
  mode:
    type: string
    enum: [one, two]
  metadata:
    type: object
    additionalProperties:
      type: string
additionalProperties: false
`), &schema); err != nil {
		t.Fatalf("load tool schema: %v", err)
	}
	if len(schema.Properties["mode"].Enum) != 2 {
		t.Fatalf("expected typed enum values, got %+v", schema.Properties["mode"].Enum)
	}
	if schema.AdditionalProperties.Allowed == nil || *schema.AdditionalProperties.Allowed {
		t.Fatalf("expected additionalProperties=false, got %+v", schema.AdditionalProperties)
	}
	if schema.Properties["metadata"].AdditionalProperties.Schema == nil {
		t.Fatalf("expected nested additionalProperties schema, got %+v", schema.Properties["metadata"].AdditionalProperties)
	}
	if got := strings.TrimSpace(schema.Properties["metadata"].AdditionalProperties.Schema.Type); got != "string" {
		t.Fatalf("expected nested additionalProperties schema type string, got %q", got)
	}
}

func loadYAMLBytes(raw []byte, target any) error {
	return yaml.Unmarshal(raw, target)
}
