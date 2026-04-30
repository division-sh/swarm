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
	if got := bundle.RootRequiredAgents(); len(got) != 1 || strings.TrimSpace(got[0].Role) != "root-agent" {
		t.Fatalf("expected root required agent role root-agent, got %#v", got)
	}
}

func TestNodeEventHandler_LocalizesCrossFlowQualifiedInputEventToLocalHandler(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: cross-flow-localization
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: scoring
    flow: scoring
    mode: static
  - id: validation
    flow: validation
    mode: static
`)
	writeFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: cross-flow-localization\n")
	writeFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	writeFixtureFile(t, filepath.Join(root, "flows", "scoring", "schema.yaml"), `
name: scoring
initial_state: discovered
terminal_states: [done]
states: [discovered, done]
pins:
  inputs:
    events: []
  outputs:
    events:
      - vertical.shortlisted
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "scoring", "policy.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "flows", "scoring", "events.yaml"), `
vertical.shortlisted:
  entity_id: string
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "scoring", "nodes.yaml"), `
scoring-node:
  id: scoring-node
  execution_type: system_node
  subscribes_to:
    - score.ready
  produces:
    - vertical.shortlisted
  event_handlers:
    score.ready:
      emit: vertical.shortlisted
`)

	writeFixtureFile(t, filepath.Join(root, "flows", "validation", "schema.yaml"), `
name: validation
initial_state: researching
terminal_states: [done]
states: [researching, done]
pins:
  inputs:
    events:
      - vertical.shortlisted
  outputs:
    events:
      - validation.started
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "validation", "policy.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "flows", "validation", "events.yaml"), `
validation.started:
  entity_id: string
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "validation", "nodes.yaml"), `
validation-orchestrator:
  id: validation-orchestrator
  execution_type: system_node
  produces:
    - validation.started
  event_handlers:
    vertical.shortlisted:
      create_entity: true
      emit: validation.started
`)

	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, "")
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	handler, ok := bundle.NodeEventHandler("validation-orchestrator", "scoring/vertical.shortlisted")
	if !ok {
		t.Fatal("expected cross-flow qualified input event to resolve to local handler")
	}
	if got := strings.TrimSpace(handler.Emit.EventType()); got != "validation/validation.started" {
		t.Fatalf("handler emits = %q, want %q", got, "validation/validation.started")
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

func currentWorkflowContractsDirForTest(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: contract-test-bundle
version: "1.0.0"
platform_version: ">=1.0.0"
flows: []
`)
	writeFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
  status: string
`)
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), `
initial_state: idle
terminal_states:
  - done
states:
  - idle
  - done
pins:
  inputs:
    events:
      - item.created
  outputs:
    events:
      - evidence.recorded
`)
	writeFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "agents.yaml"), `
control-plane:
  id: control-plane
  role: control-plane
  subscriptions:
    - evidence.recorded
`)
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  entity_id: string
  item_id: string
evidence.recorded:
  entity_id: string
  note: string
`)
	writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
audit-node:
  id: audit-node
  execution_type: system_node
  subscribes_to:
    - item.created
  produces:
    - evidence.recorded
  event_handlers:
    item.created:
      action: record_evidence
      evidence_target: item.audit
      emit: evidence.recorded
`)
	return root
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
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: root-platform
required_agents:
  - role: root-agent
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
  execution_type: system_node
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "events.yaml"), `
parent.started:
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
  execution_type: system_node
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "parent", "flows", "child", "events.yaml"), `
child.completed:
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

func TestSystemNodeContract_GateStateSupportsShorthandMap(t *testing.T) {
	var node SystemNodeContract
	if err := loadYAMLBytes([]byte(`
id: build-orchestrator
gate_state:
  g_product_spec: PM completed product spec
  g_tech_spec: CTO completed tech spec
`), &node); err != nil {
		t.Fatalf("load system node: %v", err)
	}
	if got := len(node.GateState.Gates); got != 2 {
		t.Fatalf("expected 2 gates, got %d", got)
	}
	if got := node.GateState.Gates[0].Name; got != "g_product_spec" {
		t.Fatalf("expected first gate g_product_spec, got %q", got)
	}
	if got := node.GateState.Gates[0].Description; got != "PM completed product spec" {
		t.Fatalf("expected first gate description, got %q", got)
	}
}

func TestSystemNodeContract_GateStateSupportsStructuredForm(t *testing.T) {
	var node SystemNodeContract
	if err := loadYAMLBytes([]byte(`
id: validation-orchestrator
gate_state:
  description: Tracks 4 validation gates per vertical
  gates:
    - g1_research
    - g2_spec
  storage: validation_pipelines.gate_state JSONB
`), &node); err != nil {
		t.Fatalf("load system node: %v", err)
	}
	if got := strings.TrimSpace(node.GateState.Description); got != "Tracks 4 validation gates per vertical" {
		t.Fatalf("expected gate_state description, got %q", got)
	}
	if got := len(node.GateState.Gates); got != 2 {
		t.Fatalf("expected 2 gates, got %d", got)
	}
	if got := node.GateState.Gates[1].Name; got != "g2_spec" {
		t.Fatalf("expected second gate g2_spec, got %q", got)
	}
	if got := strings.TrimSpace(node.GateState.Storage); got != "validation_pipelines.gate_state JSONB" {
		t.Fatalf("expected gate_state storage, got %q", got)
	}
}

func TestEventCatalogEntry_SwarmMetadataOwnsTopologyAndLifecycle(t *testing.T) {
	var entry EventCatalogEntry
	if err := loadYAMLBytes([]byte(`
swarm:
  source: external (human board interface)
  producer: mailbox_human
  consumer: mailbox_system (external UI, not agent-subscribed)
  status: planned
  note: Human board handoff
consumer_type: external_ui
entity_id: string
`), &entry); err != nil {
		t.Fatalf("load event catalog entry: %v", err)
	}
	if got := entry.SwarmSource(); got != "external (human board interface)" {
		t.Fatalf("expected source annotation preserved, got %q", got)
	}
	if got := entry.SwarmConsumer(); len(got) != 1 || strings.TrimSpace(got[0]) == "" {
		t.Fatalf("expected swarm.consumer to populate canonical consumer, got %#v", got)
	}
	if got := entry.ConsumerType; len(got) != 1 || strings.TrimSpace(got[0]) != "external_ui" {
		t.Fatalf("expected sibling consumer_type to remain runtime delivery metadata, got %#v", got)
	}
	if got := entry.SwarmProducer(); len(got) != 1 || strings.TrimSpace(got[0]) != "mailbox_human" {
		t.Fatalf("expected swarm.producer to populate canonical producer, got %#v", got)
	}
	if got := entry.SwarmStatus(); got != "planned" {
		t.Fatalf("expected swarm.status preserved, got %q", got)
	}
	if got := entry.SwarmNote(); got != "Human board handoff" {
		t.Fatalf("expected swarm.note preserved, got %q", got)
	}
	if _, ok := entry.Payload.Properties["source"]; ok {
		t.Fatalf("did not expect metadata source to become a payload field")
	}
}

func TestEventCatalogEntry_LegacyMetadataNormalizesIntoSwarmOwner(t *testing.T) {
	var entry EventCatalogEntry
	if err := loadYAMLBytes([]byte(`
_source: external (human board interface)
_producer: mailbox_human
_consumer: mailbox_system (external UI, not agent-subscribed)
_consumer_type: external_ui
_status: planned
_note: Human board handoff
source: text
`), &entry); err != nil {
		t.Fatalf("load event catalog entry: %v", err)
	}
	if got := entry.SwarmSource(); got != "external (human board interface)" {
		t.Fatalf("expected legacy _source to normalize into swarm.source, got %q", got)
	}
	if got := entry.SwarmProducer(); len(got) != 1 || got[0] != "mailbox_human" {
		t.Fatalf("expected legacy _producer to normalize into swarm.producer, got %#v", got)
	}
	if got := entry.SwarmConsumer(); len(got) != 1 || got[0] == "" {
		t.Fatalf("expected legacy _consumer to normalize into swarm.consumer, got %#v", got)
	}
	if got := entry.ConsumerType; len(got) != 1 || got[0] != "external_ui" {
		t.Fatalf("expected legacy _consumer_type to normalize into sibling consumer_type, got %#v", got)
	}
	if got := entry.SwarmStatus(); got != "planned" {
		t.Fatalf("expected legacy _status to normalize into swarm.status, got %q", got)
	}
	if got := entry.SwarmNote(); got != "Human board handoff" {
		t.Fatalf("expected legacy _note to normalize into swarm.note, got %q", got)
	}
	if _, ok := entry.Payload.Properties["source"]; !ok {
		t.Fatalf("expected bare source to remain payload field, not metadata")
	}
}

func TestEventCatalogEntry_ConflictingSwarmAndLegacyMetadataFailsClosed(t *testing.T) {
	var entry EventCatalogEntry
	err := loadYAMLBytes([]byte(`
swarm:
  source: external (operator)
_source: platform (timer)
entity_id: string
`), &entry)
	if err == nil || !strings.Contains(err.Error(), "swarm.source") || !strings.Contains(err.Error(), "_source") {
		t.Fatalf("load event catalog entry error = %v, want swarm/_source conflict", err)
	}
}

func loadYAMLBytes(raw []byte, target any) error {
	return yaml.Unmarshal(raw, target)
}
