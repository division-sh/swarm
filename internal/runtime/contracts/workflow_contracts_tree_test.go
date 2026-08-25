package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
		if strings.Contains(pkg.Key, `\`) {
			t.Fatalf("package key %q uses an OS-native separator; want portable slash identity", pkg.Key)
		}
		if pkg.Key == "flows/parent" {
			found = true
			if pkg.ParentKey != "." {
				t.Fatalf("expected nested flow package parent '.'; got %q", pkg.ParentKey)
			}
		}
	}
	if !found {
		t.Fatal("expected nested flow package \"flows/parent\" in discovered package tree")
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
	records := bundle.ScopedNodeRecords()
	wantNodes := map[string]bool{
		identitytest.ExecutableNode(t, "flows/parent", "parent", "parent-node").Key(): false,
		identitytest.ExecutableNode(t, "flows/parent", "child", "child-node").Key():   false,
	}
	for _, record := range records {
		node, identityErr := record.Identity()
		if identityErr != nil {
			t.Fatalf("scoped node identity: %v", identityErr)
		}
		if _, exists := wantNodes[node.Key()]; exists {
			wantNodes[node.Key()] = true
		}
	}
	for node, found := range wantNodes {
		if !found {
			t.Fatalf("scoped node records %#v missing exact imported package node %q", records, node)
		}
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

func TestBuildFlowTreeRootOnlyPackageIndexesAgentIdentityWithoutCopyingCatalogs(t *testing.T) {
	paths := ProjectPackagePaths{Key: ".", Dir: "."}
	bundle := &WorkflowContractBundle{
		Package: ProjectPackageDocument{Name: "root-platform"},
		PackageTree: []LoadedProjectPackage{{
			Key:   ".",
			Paths: paths,
		}},
		projectContracts: map[string]ProjectContractView{
			".": {
				Paths:  paths,
				Nodes:  map[string]SystemNodeContract{"root-node": {}},
				Events: map[string]EventCatalogEntry{"root.event": {}},
				Agents: map[string]AgentRegistryEntry{"root-agent": {}},
			},
		},
	}

	if err := buildFlowTree(bundle, nil); err != nil {
		t.Fatalf("buildFlowTree: %v", err)
	}
	if bundle.FlowTree.Root != nil || len(bundle.FlowTree.ByPath) != 0 || len(bundle.FlowTree.ByID) != 0 {
		t.Fatalf("root-only package unexpectedly materialized a flow tree: %#v", bundle.FlowTree)
	}
	if _, ok := bundle.URIRegistry.Agents["root-agent"]; !ok {
		t.Fatalf("root agent identity was not indexed: %#v", bundle.URIRegistry.Agents)
	}
	if len(bundle.URIRegistry.Nodes) != 0 || len(bundle.URIRegistry.Events) != 0 {
		t.Fatalf(
			"root-only package copied project catalogs into the identity registry: nodes=%#v events=%#v",
			bundle.URIRegistry.Nodes,
			bundle.URIRegistry.Events,
		)
	}
}

func TestAgentDeclarationRejectsArbitraryIDInterpolation(t *testing.T) {
	for _, id := range []string{"account-worker-{instance_id}", "account-worker-{unknown_business_key}"} {
		t.Run(id, func(t *testing.T) {
			_, err := normalizeAgentRegistryEntries(map[string]AgentRegistryEntry{
				"account-worker": {ID: id},
			}, "flows/account/agents.yaml")
			if err == nil {
				t.Fatalf("agent declaration accepted retired interpolation %q", id)
			}
			for _, want := range []string{
				"flows/account/agents.yaml",
				`agent "account-worker"`,
				"contains interpolation",
				"move the discriminator to instance.by",
				"declaration x instance route",
			} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %q, want teaching fragment %q", err, want)
				}
			}
		})
	}
}

func TestAgentDeclarationIDPresenceMatrix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		field   string
		wantErr bool
	}{
		{name: "omitted"},
		{name: "quoted empty", field: `  id: ""`, wantErr: true},
		{name: "whitespace", field: `  id: "   "`, wantErr: true},
		{name: "null", field: "  id: null", wantErr: true},
		{name: "bare", field: "  id:", wantErr: true},
		{name: "tilde", field: "  id: ~", wantErr: true},
		{name: "literal override", field: "  id: public-worker"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := "worker:\n  role: worker\n"
			if tc.field != "" {
				source = "worker:\n" + tc.field + "\n  role: worker\n"
			}
			var entries map[string]AgentRegistryEntry
			if err := yaml.Unmarshal([]byte(source), &entries); err != nil {
				t.Fatal(err)
			}
			normalized, err := normalizeAgentRegistryEntries(entries, "agents.yaml")
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "id is authored but empty") {
					t.Fatalf("normalize error = %v, want authored-empty rejection", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := DeclaredAgentID("worker", normalized["worker"])
			if err != nil {
				t.Fatal(err)
			}
			want := "worker"
			if tc.name == "literal override" {
				want = "public-worker"
			}
			if got != want {
				t.Fatalf("effective id = %q, want %q", got, want)
			}
		})
	}
}

func TestAgentDeclarationRejectsDuplicateEffectiveNames(t *testing.T) {
	for _, source := range []string{
		"worker:\n  role: worker\nalias:\n  id: worker\n  role: alias\n",
		"first:\n  id: shared\n  role: first\nsecond:\n  id: shared\n  role: second\n",
	} {
		var entries map[string]AgentRegistryEntry
		if err := yaml.Unmarshal([]byte(source), &entries); err != nil {
			t.Fatal(err)
		}
		_, err := normalizeAgentRegistryEntries(entries, "agents.yaml")
		if err == nil || !strings.Contains(err.Error(), "derive the same effective agent id") {
			t.Fatalf("normalize error = %v, want duplicate effective-name rejection", err)
		}
	}
}

func TestWorkflowContractBundleEffectiveRequiredAgentsInferWhenOmitted(t *testing.T) {
	flowView := &FlowContractView{
		Paths: FlowContractPaths{ID: "analysis", SchemaFile: "flows/analysis/schema.yaml", AgentsFile: "flows/analysis/agents.yaml"},
		Agents: map[string]AgentRegistryEntry{
			"analyzer": {
				Subscriptions: []string{"analysis.requested"},
				EmitEvents:    []string{"analysis.done"},
			},
		},
	}
	bundle := &WorkflowContractBundle{
		Paths:      ContractPaths{RootSchemaFile: "schema.yaml", ProjectAgentsFile: "agents.yaml"},
		RootSchema: &FlowSchemaDocument{},
		Agents: map[string]AgentRegistryEntry{
			"root-agent": {
				Subscriptions: []string{"root.requested"},
				EmitEvents:    []string{"root.done"},
			},
		},
		FlowSchemas: map[string]FlowSchemaDocument{
			"analysis": {},
		},
		FlowTree: FlowTree{ByID: map[string]*FlowContractView{"analysis": flowView}},
	}
	bundle.FlowTree.Root = flowView
	bundle.projectContracts = map[string]ProjectContractView{
		".": {
			Paths:     ProjectPackagePaths{Key: ".", ProjectAgentsFile: "agents.yaml"},
			Agents:    bundle.Agents,
			AgentURIs: map[string]string{"root-agent": "swarm://root/agent/root-agent"},
		},
	}

	rootFacts := bundle.RootRequiredAgentFacts()
	if len(rootFacts) != 1 || rootFacts[0].Role != "root-agent" || rootFacts[0].Source != RequiredAgentSourceInferred {
		t.Fatalf("root required agent facts = %#v, want inferred root-agent", rootFacts)
	}
	flowFacts := bundle.FlowRequiredAgentFacts("analysis")
	if len(flowFacts) != 1 || flowFacts[0].Role != "analyzer" || flowFacts[0].Source != RequiredAgentSourceInferred {
		t.Fatalf("flow required agent facts = %#v, want inferred analyzer", flowFacts)
	}
	if got := bundle.FlowRequiredAgents("analysis"); len(got) != 1 || got[0].SubscribesTo[0] != "analysis.requested" || got[0].Emits[0] != "analysis.done" {
		t.Fatalf("effective flow required agents = %#v, want inferred subscriptions/emits", got)
	}
}

func TestWorkflowContractBundleEffectiveRequiredAgentsPreserveExplicitEmpty(t *testing.T) {
	flowView := &FlowContractView{
		Paths: FlowContractPaths{ID: "analysis", SchemaFile: "flows/analysis/schema.yaml", AgentsFile: "flows/analysis/agents.yaml"},
		Agents: map[string]AgentRegistryEntry{
			"analyzer": {Subscriptions: []string{"analysis.requested"}},
		},
	}
	bundle := &WorkflowContractBundle{
		Paths: ContractPaths{RootSchemaFile: "schema.yaml", ProjectAgentsFile: "agents.yaml"},
		RootSchema: &FlowSchemaDocument{
			RequiredAgentsDeclared: true,
		},
		Agents: map[string]AgentRegistryEntry{
			"root-agent": {Subscriptions: []string{"root.requested"}},
		},
		FlowSchemas: map[string]FlowSchemaDocument{
			"analysis": {RequiredAgentsDeclared: true},
		},
		FlowTree: FlowTree{ByID: map[string]*FlowContractView{"analysis": flowView}},
	}

	if got := bundle.RootRequiredAgents(); len(got) != 0 {
		t.Fatalf("root required agents = %#v, want explicit empty boundary", got)
	}
	if got := bundle.FlowRequiredAgents("analysis"); len(got) != 0 {
		t.Fatalf("flow required agents = %#v, want explicit empty boundary", got)
	}
}

func TestLoadWorkflowContractBundleLoadsCanonicalToolSchemasFromRootAndFlowTools(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: canonical-tool-schema
version: "1.0.0"
flows:
  - id: worker
    flow: worker
    mode: static
`)
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: canonical-tool-schema\n")
	writeFixtureFile(t, filepath.Join(root, "tools.yaml"), `
root_lookup:
  description: Root-level lookup tool.
  handler_type: http
  http:
    method: GET
    url: https://example.test/root
  input_schema:
    type: object
    required: [query]
    properties:
      query:
        type: string
  output_schema:
    type: object
    properties:
      result:
        type: string
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), `
name: worker
mode: static
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "worker", "tools.yaml"), `
flow_lookup:
  description: Flow-level lookup tool.
  handler_type: http
  http:
    method: GET
    url: https://example.test/flow
  input_schema:
    type: object
    required: [flow_id]
    properties:
      flow_id:
        type: string
  output_schema:
    type: object
    properties:
      accepted:
        type: boolean
`)

	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, "")
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	rootTool, ok := bundle.Tools["root_lookup"]
	if !ok {
		t.Fatalf("expected root tool to load, got keys %#v", sortedContractKeys(bundle.Tools))
	}
	rootInput := rootTool.InputSchema()
	if got := rootInput.Kind(); got != ToolSchemaObject {
		t.Fatalf("root input schema type = %q, want object", got)
	}
	if _, ok := rootInput.Property("query"); !ok {
		t.Fatalf("root input schema properties = %#v, want query", rootInput.PropertyNames())
	}
	if _, ok := rootTool.OutputSchema().Property("result"); !ok {
		t.Fatalf("root output schema properties = %#v, want result", rootTool.OutputSchema().PropertyNames())
	}
	flowTool, ok := bundle.Tools["flow_lookup"]
	if !ok {
		t.Fatalf("expected flow tool to load, got keys %#v", sortedContractKeys(bundle.Tools))
	}
	if _, ok := flowTool.InputSchema().Property("flow_id"); !ok {
		t.Fatalf("flow input schema properties = %#v, want flow_id", flowTool.InputSchema().PropertyNames())
	}
	if _, ok := flowTool.OutputSchema().Property("accepted"); !ok {
		t.Fatalf("flow output schema properties = %#v, want accepted", flowTool.OutputSchema().PropertyNames())
	}
}

func TestMigratedToolFixturesPreserveQueryInputSchema(t *testing.T) {
	repoRoot := repoRootForContractsTest(t)
	paths := []string{
		filepath.Join("tests", "tier8-boot-verification", "test-boot-permission-tool-mismatch", "tools.yaml"),
		filepath.Join("tests", "tier11-flow-composition", "test-child-flow-tool-inherit", "tools.yaml"),
		filepath.Join("tests", "tier11-flow-composition", "test-tool-override", "tools.yaml"),
		filepath.Join("tests", "tier11-flow-composition", "test-tool-override", "flows", "child", "tools.yaml"),
	}
	for _, rel := range paths {
		t.Run(rel, func(t *testing.T) {
			var tools map[string]ToolSchemaEntry
			if err := loadYAMLFile(filepath.Join(repoRoot, rel), &tools); err != nil {
				t.Fatalf("load %s: %v", rel, err)
			}
			entry, ok := tools["lookup_data"]
			if !ok {
				t.Fatalf("lookup_data missing from %s: %#v", rel, tools)
			}
			input := entry.InputSchema()
			if got := input.Kind(); got != ToolSchemaObject {
				t.Fatalf("%s input_schema.type = %q, want object", rel, got)
			}
			query, ok := input.Property("query")
			if !ok {
				t.Fatalf("%s input_schema properties = %#v, want query", rel, input.PropertyNames())
			}
			if got := query.Kind(); got != ToolSchemaString {
				t.Fatalf("%s query.type = %q, want string", rel, got)
			}
		})
	}
}

func TestNodeEventHandler_LocalizesCrossFlowQualifiedInputEventToLocalHandler(t *testing.T) {

	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: cross-flow-localization
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
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
	node := identitytest.FlowNode(t, "validation", "validation-orchestrator")
	handler, ok := bundle.Semantics.NodeHandlers[node.Key()]["vertical.shortlisted"]
	handler = bundle.ExternalizeExecutableNodeHandler(node, handler)
	if !ok {
		t.Fatal("expected cross-flow qualified input event to resolve to local handler")
	}
	if got := strings.TrimSpace(handler.Emit.EventType()); got != "validation/validation.started" {
		t.Fatalf("handler emits = %q, want %q", got, "validation/validation.started")
	}
}

func TestNodeEventHandler_ExternalizesOnSuccessEmitWithRules(t *testing.T) {

	repoRoot := repoRootForContractsTest(t)
	root := t.TempDir()

	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: cross-flow-on-success
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
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
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: cross-flow-on-success\n")
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
      - validation.rule
      - validation.started
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "validation", "policy.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "flows", "validation", "events.yaml"), `
validation.rule:
  entity_id: string
validation.started:
  entity_id: string
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "validation", "nodes.yaml"), `
validation-orchestrator:
  id: validation-orchestrator
  execution_type: system_node
  produces:
    - validation.rule
    - validation.started
  event_handlers:
    vertical.shortlisted:
      rules:
        accepted:
          element_id: 00000000-0000-4000-8000-000000000001
          condition: "else"
          emit: validation.rule
      on_success:
        emit: validation.started
`)

	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, "")
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	node := identitytest.FlowNode(t, "validation", "validation-orchestrator")
	handler, ok := bundle.Semantics.NodeHandlers[node.Key()]["vertical.shortlisted"]
	handler = bundle.ExternalizeExecutableNodeHandler(node, handler)
	if !ok {
		t.Fatal("expected cross-flow qualified input event to resolve to local handler")
	}
	if got := len(handler.Rules); got != 1 {
		t.Fatalf("handler rules len = %d, want 1", got)
	}
	if got := strings.TrimSpace(handler.Rules[0].Emit.EventType()); got != "validation/validation.rule" {
		t.Fatalf("handler rule emit = %q, want %q", got, "validation/validation.rule")
	}
	if got := strings.TrimSpace(handler.OnSuccess.Emit.EventType()); got != "validation/validation.started" {
		t.Fatalf("handler on_success emit = %q, want %q", got, "validation/validation.started")
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
platform_version: ">=0.7.0 <0.8.0"
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
  intent: {inline: "Coordinate contract-test evidence events."}
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
  intent: {inline: "Coordinate the parent flow."}
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
  intent: {inline: "Execute the child flow."}
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
	mode, ok := schema.Property("mode")
	if !ok {
		t.Fatal("mode property missing")
	}
	enum, declared := mode.EnumValues()
	if !declared || len(enum) != 2 {
		t.Fatalf("expected typed enum values, got %+v", enum)
	}
	allowed, declared := schema.AdditionalPropertiesAllowed()
	if !declared || allowed {
		t.Fatalf("expected additionalProperties=false, got declared=%v allowed=%v", declared, allowed)
	}
	metadata, ok := schema.Property("metadata")
	if !ok {
		t.Fatal("metadata property missing")
	}
	additional, ok := metadata.AdditionalPropertiesSchema()
	if !ok || additional.Kind() != ToolSchemaString {
		t.Fatalf("expected nested additionalProperties schema type string, got %#v", additional)
	}
	raw, err := yaml.Marshal(schema)
	if err != nil {
		t.Fatalf("marshal tool schema: %v", err)
	}
	var roundTrip ToolInputSchema
	if err := loadYAMLBytes(raw, &roundTrip); err != nil {
		t.Fatalf("reload marshaled tool schema: %v\n%s", err, raw)
	}
	allowed, declared = roundTrip.AdditionalPropertiesAllowed()
	if !declared || allowed {
		t.Fatalf("round-trip additionalProperties declared=%v allowed=%v, want false", declared, allowed)
	}
	metadata, _ = roundTrip.Property("metadata")
	nested, ok := metadata.AdditionalPropertiesSchema()
	if !ok || nested.Kind() != ToolSchemaString {
		t.Fatalf("round-trip nested additionalProperties = %+v, want string schema", nested)
	}
}

func TestToolSchemaEntryDecode_RejectsRetiredSchemaAliases(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		want     string
		wantNext string
	}{
		{
			name: "parameters",
			body: `
lookup:
  description: Lookup data.
  parameters:
    type: object
`,
			want:     "parameters",
			wantNext: "input_schema",
		},
		{
			name: "returns",
			body: `
lookup:
  description: Lookup data.
  returns:
    type: object
`,
			want:     "returns",
			wantNext: "output_schema",
		},
		{
			name: "canonical plus alias",
			body: `
lookup:
  description: Lookup data.
  input_schema:
    type: object
  parameters:
    type: object
`,
			want:     "parameters",
			wantNext: "input_schema",
		},
		{
			name: "endpoint",
			body: `
lookup:
  description: Lookup data.
  endpoint: /api/v1/lookup
`,
			want:     "endpoint",
			wantNext: "http.url",
		},
		{
			name: "type",
			body: `
lookup:
  description: Lookup data.
  type: custom
`,
			want:     "type",
			wantNext: "handler_type",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tools map[string]ToolSchemaEntry
			err := loadYAMLBytes([]byte(tc.body), &tools)
			if err == nil ||
				!strings.Contains(err.Error(), "RETIRED") ||
				!strings.Contains(err.Error(), tc.want) ||
				!strings.Contains(err.Error(), tc.wantNext) {
				t.Fatalf("load tool schema error = %v, want RETIRED %s -> %s", err, tc.want, tc.wantNext)
			}
		})
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
	snippet := canonicalrouting.EventCatalogMetadataParserSnippet(t, canonicalrouting.CanonicalExternalEventMetadata)
	entry, err := admitEventCatalogParserSnippet(t, snippet)
	if err != nil {
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

func TestEventCatalogEntry_LegacyMetadataFieldsFailClosed(t *testing.T) {
	snippet := canonicalrouting.EventCatalogMetadataParserSnippet(t, canonicalrouting.RetiredExternalEventMetadata)
	if _, err := admitEventCatalogParserSnippet(t, snippet); err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "metadata field") || !strings.Contains(err.Error(), "swarm.") {
		t.Fatalf("load event catalog entry error = %v, want retired metadata failure", err)
	}
}

func TestEventCatalogEntry_TopLevelProducerConsumerMetadataFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		yaml      string
		wantField string
	}{
		{
			name: "producer",
			yaml: `
producer: mailbox_human
entity_id: string
`,
			wantField: "producer",
		},
		{
			name: "consumer",
			yaml: `
consumer: dashboard
entity_id: string
`,
			wantField: "consumer",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := admitEventCatalogEntryForTest(t, tc.yaml)
			if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), tc.wantField) || !strings.Contains(err.Error(), "swarm.") {
				t.Fatalf("load event catalog entry error = %v, want retired %s failure", err, tc.wantField)
			}
		})
	}
}

func TestEventCatalogEntry_ConflictingSwarmAndLegacyMetadataFailsClosed(t *testing.T) {
	snippet := canonicalrouting.EventCatalogMetadataParserSnippet(t, canonicalrouting.ConflictingEventMetadata)
	_, err := admitEventCatalogParserSnippet(t, snippet)
	if err == nil || !strings.Contains(err.Error(), "swarm.source") || !strings.Contains(err.Error(), "_source") {
		t.Fatalf("load event catalog entry error = %v, want swarm/_source conflict", err)
	}
}

func admitEventCatalogParserSnippet(t testing.TB, snippet canonicalrouting.ParserSnippet) (EventCatalogEntry, error) {
	t.Helper()
	raw, err := snippet.SourceBytes()
	if err != nil {
		return EventCatalogEntry{}, err
	}
	return admitEventCatalogEntryForTest(t, string(raw))
}

func loadYAMLBytes(raw []byte, target any) error {
	return yaml.Unmarshal(raw, target)
}
