package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveWorkflowContractPaths_PrefersWorkflowScopedLayout(t *testing.T) {
	repoRoot := t.TempDir()
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "workflow-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "hooks-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "nodes-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "events-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "agents-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "tools-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "policy-empire.yaml"))
	if err := os.MkdirAll(filepath.Join(repoRoot, "contracts", "empire", "prompts"), 0o755); err != nil {
		t.Fatalf("mkdir prompts dir: %v", err)
	}

	paths := ResolveWorkflowContractPaths(repoRoot)
	if want := filepath.Join(repoRoot, "contracts", "empire", "workflow-empire.yaml"); paths.WorkflowSchemaFile != want {
		t.Fatalf("workflow schema path = %s, want %s", paths.WorkflowSchemaFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "empire", "hooks-empire.yaml"); paths.GuardRegistryFile != want {
		t.Fatalf("guard registry path = %s, want %s", paths.GuardRegistryFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "empire", "prompts"); paths.PromptsDir != want {
		t.Fatalf("prompts dir = %s, want %s", paths.PromptsDir, want)
	}
}

func TestResolveWorkflowContractPaths_FallsBackToLegacyLayout(t *testing.T) {
	repoRoot := t.TempDir()
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"))
	if err := os.MkdirAll(filepath.Join(repoRoot, "contracts", "prompts"), 0o755); err != nil {
		t.Fatalf("mkdir legacy prompts dir: %v", err)
	}

	paths := ResolveWorkflowContractPaths(repoRoot)
	if want := filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"); paths.WorkflowSchemaFile != want {
		t.Fatalf("workflow schema path = %s, want %s", paths.WorkflowSchemaFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"); paths.GuardRegistryFile != want {
		t.Fatalf("guard registry path = %s, want %s", paths.GuardRegistryFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "prompts"); paths.PromptsDir != want {
		t.Fatalf("prompts dir = %s, want %s", paths.PromptsDir, want)
	}
}

func TestLoadWorkflowContractBundle_LoadsCurrentRootFields(t *testing.T) {
	repoRoot := projectRootFromContractsTest(t)
	tmp := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmp, "contracts"), 0o755); err != nil {
		t.Fatalf("mkdir contracts dir: %v", err)
	}
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"), filepath.Join(tmp, "contracts", "workflow-schema.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"), filepath.Join(tmp, "contracts", "guard-action-registry.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"), filepath.Join(tmp, "contracts", "system-nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"), filepath.Join(tmp, "contracts", "event-catalog.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"), filepath.Join(tmp, "contracts", "agent-tools.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"), filepath.Join(tmp, "contracts", "tool-schemas.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"), filepath.Join(tmp, "contracts", "prompt-variables.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"), filepath.Join(tmp, "contracts", "platform", "platform-spec.yaml"))

	bundle, err := LoadWorkflowContractBundle(tmp)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	if got := bundle.Workflow.Workflow.Version; got == "" {
		t.Fatal("expected workflow version to load")
	}
	if bundle.Workflow.Workflow.EntitySchema == nil {
		t.Fatal("expected entity_schema to load")
	}
	foundAccumulation := false
	for _, tr := range bundle.Workflow.Workflow.Transitions {
		if tr.ID != "discovered_to_scoring" {
			continue
		}
		foundAccumulation = true
		if len(tr.DataAccumulation.Writes) == 0 || tr.DataAccumulation.SourceEvent != "vertical.discovered" {
			t.Fatalf("expected data_accumulation on discovered_to_scoring, got %+v", tr.DataAccumulation)
		}
	}
	if !foundAccumulation {
		t.Fatal("expected discovered_to_scoring transition in v2.2.0 workflow")
	}
	node, ok := bundle.Nodes["scan-orchestrator"]
	if !ok {
		t.Fatal("expected scan-orchestrator node")
	}
	if len(node.EventHandlers) == 0 {
		t.Fatal("expected event_handlers on scan-orchestrator")
	}
	if node.StateSchema == nil {
		t.Fatal("expected state_schema on scan-orchestrator")
	}
	event, ok := bundle.Events["scan.requested"]
	if !ok {
		t.Fatal("expected scan.requested event")
	}
	if event.RuntimeHandling != "consuming" {
		t.Fatalf("scan.requested runtime_handling = %q, want consuming", event.RuntimeHandling)
	}
	if event.OwningNode != "scan-orchestrator" {
		t.Fatalf("scan.requested owning_node = %q, want scan-orchestrator", event.OwningNode)
	}
}

func TestResolveWorkflowContractPaths_DiscoversPackageLayout(t *testing.T) {
	repoRoot := t.TempDir()
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"))
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "package.yaml"), `
name: empire
version: 2.6.0
flows:
  - id: discovery
    flow: discovery
    namespace: vertical
  - id: scoring
    flow: scoring
    namespace: vertical
runtime_contracts:
  nodes: runtime/nodes.yaml
  events: runtime/events.yaml
  agents: runtime/agents.yaml
`)
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "events.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "agents.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "runtime", "nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "runtime", "events.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "runtime", "agents.yaml"))
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "schema.yaml"), `
name: discovery
pins:
  inputs:
    events: [scan.requested]
    reads: []
  outputs:
    events: [scan.completed]
    writes: []
`)
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "events.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "agents.yaml"))
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "scoring", "schema.yaml"), `
name: scoring
initial_state: discovered
pins:
  inputs:
    events: [vertical.discovered]
    reads: []
  outputs:
    events: [vertical.scored]
    writes: [scores]
`)
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "scoring", "nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "scoring", "events.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "scoring", "agents.yaml"))

	paths := ResolveWorkflowContractPaths(repoRoot)
	if want := filepath.Join(repoRoot, "contracts", "empire", "package.yaml"); paths.ProjectPackageFile != want {
		t.Fatalf("project package path = %s, want %s", paths.ProjectPackageFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "empire", "runtime", "nodes.yaml"); paths.RuntimeBridge.NodesFile != want {
		t.Fatalf("runtime bridge nodes path = %s, want %s", paths.RuntimeBridge.NodesFile, want)
	}
	if paths.SystemNodesFile != paths.RuntimeBridge.NodesFile {
		t.Fatalf("active nodes path = %s, want runtime bridge %s", paths.SystemNodesFile, paths.RuntimeBridge.NodesFile)
	}
	if paths.EventCatalogFile != paths.RuntimeBridge.EventsFile {
		t.Fatalf("active events path = %s, want runtime bridge %s", paths.EventCatalogFile, paths.RuntimeBridge.EventsFile)
	}
	if paths.AgentRegistryFile != paths.RuntimeBridge.AgentsFile {
		t.Fatalf("active agents path = %s, want runtime bridge %s", paths.AgentRegistryFile, paths.RuntimeBridge.AgentsFile)
	}
	if got := len(paths.Flows); got != 2 {
		t.Fatalf("flow count = %d, want 2", got)
	}
	if paths.Flows[0].ID != "discovery" || paths.Flows[1].ID != "scoring" {
		t.Fatalf("flow ids = %#v", []string{paths.Flows[0].ID, paths.Flows[1].ID})
	}
}

func TestResolveWorkflowContractPaths_DiscoversNestedPackageLayout(t *testing.T) {
	repoRoot := t.TempDir()
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"))
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "package.yaml"), `
name: empire
version: 2.6.0
flows:
  - id: discovery
    flow: discovery
    namespace: vertical
packages:
  - path: packages/operating-pack
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "operating-pack", "package.yaml"), `
name: operating-pack
version: 2.6.0
flows:
  - id: operating
    flow: operating
    namespace: opco
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "schema.yaml"), `
name: discovery
pins:
  inputs:
    events: [scan.requested]
    reads: []
  outputs:
    events: [scan.completed]
    writes: []
`)
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "events.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "agents.yaml"))
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "operating-pack", "flows", "operating", "schema.yaml"), `
name: operating
pins:
  inputs:
    events: [opco.spinup_requested]
    reads: []
  outputs:
    events: [opco.steady_state_reached]
    writes: [live_url]
`)
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "operating-pack", "flows", "operating", "nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "operating-pack", "flows", "operating", "events.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "operating-pack", "flows", "operating", "agents.yaml"))

	paths := ResolveWorkflowContractPaths(repoRoot)
	if got := len(paths.ProjectPackages); got != 2 {
		t.Fatalf("package count = %d, want 2", got)
	}
	if paths.ProjectPackages[0].Key != "." || paths.ProjectPackages[1].Key != "packages/operating-pack" {
		t.Fatalf("package keys = %#v", []string{paths.ProjectPackages[0].Key, paths.ProjectPackages[1].Key})
	}
	if got := len(paths.Flows); got != 2 {
		t.Fatalf("flow count = %d, want 2", got)
	}
	if paths.Flows[0].PackageKey != "." || paths.Flows[1].PackageKey != "packages/operating-pack" {
		t.Fatalf("flow package keys = %#v", []string{paths.Flows[0].PackageKey, paths.Flows[1].PackageKey})
	}
}

func TestLoadWorkflowContractBundle_LoadsPackageAndFlowSchemas(t *testing.T) {
	repoRoot := projectRootFromContractsTest(t)
	specRoot := currentV260ContractsRoot(t, repoRoot)
	tmp := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmp, "contracts"), 0o755); err != nil {
		t.Fatalf("mkdir contracts dir: %v", err)
	}
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"), filepath.Join(tmp, "contracts", "workflow-schema.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"), filepath.Join(tmp, "contracts", "guard-action-registry.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"), filepath.Join(tmp, "contracts", "system-nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"), filepath.Join(tmp, "contracts", "event-catalog.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"), filepath.Join(tmp, "contracts", "agent-tools.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"), filepath.Join(tmp, "contracts", "tool-schemas.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"), filepath.Join(tmp, "contracts", "prompt-variables.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"), filepath.Join(tmp, "contracts", "platform", "platform-spec.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "package.yaml"), filepath.Join(tmp, "contracts", "empire", "package.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "events.yaml"), filepath.Join(tmp, "contracts", "empire", "events.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "agents.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "policy.yaml"), filepath.Join(tmp, "contracts", "empire", "policy.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "tools.yaml"), filepath.Join(tmp, "contracts", "empire", "tools.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "runtime", "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "runtime", "events.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "events.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "runtime", "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "agents.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "runtime", "policy.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "policy.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "runtime", "tools.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "tools.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "discovery", "schema.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "discovery", "schema.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "discovery", "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "discovery", "nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "discovery", "events.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "discovery", "events.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "discovery", "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "discovery", "agents.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "discovery", "policy.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "discovery", "policy.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "scoring", "schema.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "scoring", "schema.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "scoring", "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "scoring", "nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "scoring", "events.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "scoring", "events.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "scoring", "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "scoring", "agents.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "scoring", "policy.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "scoring", "policy.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "validation", "schema.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "validation", "schema.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "validation", "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "validation", "nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "validation", "events.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "validation", "events.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "validation", "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "validation", "agents.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "validation", "policy.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "validation", "policy.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "validation", "tools.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "validation", "tools.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "operating", "schema.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "operating", "schema.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "operating", "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "operating", "nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "operating", "events.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "operating", "events.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "operating", "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "operating", "agents.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "operating", "policy.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "operating", "policy.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "operating", "tools.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "operating", "tools.yaml"))

	bundle, err := LoadWorkflowContractBundle(tmp)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	if got := bundle.Package.Version; got != "2.6.0" {
		t.Fatalf("package version = %q, want 2.6.0", got)
	}
	if got := bundle.WorkflowName(); got == "" {
		t.Fatal("expected semantic workflow name to be populated")
	}
	if got := bundle.WorkflowVersion(); got != "2.6.0" {
		t.Fatalf("semantic workflow version = %q, want 2.6.0", got)
	}
	if len(bundle.WorkflowTransitions()) == 0 {
		t.Fatal("expected semantic workflow transitions to be populated")
	}
	if _, ok := bundle.ActionEntryByID("emit_validation_package_ready"); !ok {
		t.Fatal("expected semantic action lookup to be populated")
	}
	if got := len(bundle.FlowSchemas); got != 4 {
		t.Fatalf("flow schema count = %d, want 4", got)
	}
	if got := bundle.FlowInitialStage("scoring"); got != "discovered" {
		t.Fatalf("semantic scoring initial_state = %q, want discovered", got)
	}
	if got := bundle.FlowNamespace("operating"); got != "opco" {
		t.Fatalf("semantic operating namespace = %q, want opco", got)
	}
	if got := bundle.FlowNamespacePrefix("validation"); got != "vertical" {
		t.Fatalf("semantic validation namespace_prefix = %q, want vertical", got)
	}
	if got := bundle.FlowNamespaceRule("validation"); got == "" {
		t.Fatal("expected semantic validation namespace_rule to be populated")
	}
	if got := bundle.FlowInputEvents("validation"); len(got) == 0 || got[0] != "brand.candidates_ready" {
		t.Fatalf("semantic validation input events = %v, want populated flow inputs", got)
	}
	if got := bundle.FlowOutputEvents("operating"); len(got) == 0 || got[0] != "opco.spinup_requested" {
		t.Fatalf("semantic operating output events = %v, want populated flow outputs", got)
	}
	if got := bundle.FlowWritePins("validation"); len(got) == 0 || got[0] != "brand" {
		t.Fatalf("semantic validation write pins = %v, want populated write pins", got)
	}
	if got := bundle.FlowRequiredAgents("scoring"); len(got) != 1 || got[0].Role != "analyst" {
		t.Fatalf("semantic scoring required_agents = %v, want analyst", got)
	}
	if owners := bundle.WritePinOwners("validation_kit"); len(owners) != 1 || owners[0] != "validation" {
		t.Fatalf("semantic write pin owners for validation_kit = %v, want validation", owners)
	}
	if got := bundle.FlowTerminalStages("validation"); len(got) != 1 || got[0] != "killed" {
		t.Fatalf("semantic validation terminal_states = %v, want [killed]", got)
	}
	if got := bundle.FlowStates("operating"); len(got) == 0 {
		t.Fatal("expected semantic operating states to be populated")
	}
	if schema, ok := bundle.FlowSchemas["scoring"]; !ok {
		t.Fatal("expected scoring flow schema")
	} else if got := schema.InitialState; got != "discovered" {
		t.Fatalf("scoring initial_state = %q, want discovered", got)
	}
	if handler, ok := bundle.NodeEventHandler("validation-orchestrator", "spec.approved"); !ok {
		t.Fatal("expected semantic validation handler lookup for spec.approved")
	} else {
		if got := handler.AdvancesTo; got != "cto_spec_review" {
			t.Fatalf("semantic handler advances_to = %q, want cto_spec_review", got)
		}
		if got := handler.DataAccumulation.SourceEvent; got != "spec.approved" {
			t.Fatalf("semantic handler data_accumulation.source_event = %q, want spec.approved", got)
		}
	}
	if derived, ok := bundle.DerivedHandlerTransition("validation-orchestrator", "spec.approved"); !ok {
		t.Fatal("expected derived semantic transition for validation-orchestrator/spec.approved")
	} else {
		if got := derived.Action; got != "set_gate" {
			t.Fatalf("derived semantic action = %q, want set_gate", got)
		}
		if got := derived.AdvancesTo; got != "cto_spec_review" {
			t.Fatalf("derived semantic advances_to = %q, want cto_spec_review", got)
		}
		if got := derived.DataAccumulation.SourceEvent; got != "spec.approved" {
			t.Fatalf("derived semantic source_event = %q, want spec.approved", got)
		}
		if got := derived.FlowID; got != "validation" {
			t.Fatalf("derived semantic flow = %q, want validation", got)
		}
		if got := derived.CompletionRule; got != "" {
			t.Fatalf("derived semantic completion_rule = %q, want empty", got)
		}
	}
	if owners := bundle.RuntimeEventOwners("opco.ceo_ready"); len(owners) == 0 || owners[0] != "lifecycle-orchestrator" {
		t.Fatalf("semantic event owners for opco.ceo_ready = %v, want lifecycle-orchestrator", owners)
	}
	if handler, ok := bundle.NodeEventHandler("scan-orchestrator", "scanner.google_maps.scan_complete"); !ok {
		t.Fatal("expected wildcard semantic handler lookup for scanner.google_maps.scan_complete")
	} else if got := handler.Action; got != "accumulate_scan" {
		t.Fatalf("wildcard semantic handler action = %q, want accumulate_scan", got)
	}
	if owners := bundle.RuntimeEventOwners("scanner.google_maps.scan_complete"); len(owners) == 0 || owners[0] != "scan-orchestrator" {
		t.Fatalf("semantic event owners for scanner.google_maps.scan_complete = %v, want scan-orchestrator", owners)
	}
	if want := filepath.Join(tmp, "contracts", "empire", "runtime", "nodes.yaml"); bundle.Paths.RuntimeBridge.NodesFile != want {
		t.Fatalf("runtime bridge nodes path = %s, want %s", bundle.Paths.RuntimeBridge.NodesFile, want)
	}
	if got := len(bundle.ProjectContracts); got != 1 {
		t.Fatalf("project contract count = %d, want 1", got)
	}
	if got := len(bundle.FlowContracts); got != 4 {
		t.Fatalf("flow contract count = %d, want 4", got)
	}
	if _, ok := bundle.MergedNodes["portfolio-node"]; !ok {
		t.Fatal("expected merged project node portfolio-node")
	}
	if source := bundle.NodeSources["portfolio-node"]; source.Layer != "project" || source.PackageKey != "." {
		t.Fatalf("portfolio-node source = %+v, want root project provenance", source)
	}
	if _, ok := bundle.MergedNodes["scan-orchestrator"]; !ok {
		t.Fatal("expected merged flow node scan-orchestrator")
	}
	if source := bundle.NodeSources["scan-orchestrator"]; source.Layer != "flow" || source.FlowID != "discovery" {
		t.Fatalf("scan-orchestrator source = %+v, want discovery flow provenance", source)
	}
	if _, ok := bundle.MergedAgents["analysis-agent"]; !ok {
		t.Fatal("expected merged scoring flow agent analysis-agent")
	}
	if source := bundle.AgentSources["analysis-agent"]; source.FlowID != "scoring" {
		t.Fatalf("analysis-agent source = %+v, want scoring flow provenance", source)
	}
	if got, ok := bundle.MergedPolicy["composite_shortlist"]; !ok || got == nil {
		t.Fatalf("expected merged policy to include scoring flow policy, got %#v", got)
	}
}

func TestPopulateWorkflowSemantics_DerivesStagesAndTerminalStagesFromFlowsWhenWorkflowDocMissingThem(t *testing.T) {
	repoRoot := projectRootFromContractsTest(t)
	specRoot := currentV260ContractsRoot(t, repoRoot)
	tmp := t.TempDir()
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"), filepath.Join(tmp, "contracts", "workflow-schema.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"), filepath.Join(tmp, "contracts", "guard-action-registry.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"), filepath.Join(tmp, "contracts", "system-nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"), filepath.Join(tmp, "contracts", "event-catalog.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"), filepath.Join(tmp, "contracts", "agent-tools.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"), filepath.Join(tmp, "contracts", "tool-schemas.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"), filepath.Join(tmp, "contracts", "prompt-variables.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"), filepath.Join(tmp, "contracts", "platform", "platform-spec.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "package.yaml"), filepath.Join(tmp, "contracts", "empire", "package.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "runtime", "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "runtime", "events.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "events.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "runtime", "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "agents.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "discovery", "schema.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "discovery", "schema.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "scoring", "schema.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "scoring", "schema.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "validation", "schema.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "validation", "schema.yaml"))
	copyContractFileForTest(t, filepath.Join(specRoot, "flows", "operating", "schema.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", "operating", "schema.yaml"))

	bundle, err := LoadWorkflowContractBundle(tmp)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	bundle.Workflow.Workflow.Stages = nil
	bundle.Workflow.Workflow.TerminalStages = nil
	populateWorkflowSemantics(bundle)

	stages := bundle.WorkflowStages()
	if len(stages) == 0 {
		t.Fatal("expected derived stages from flow schemas")
	}
	stageByID := make(map[string]WorkflowStageContract, len(stages))
	for _, stage := range stages {
		stageByID[strings.TrimSpace(stage.ID)] = stage
	}
	if got := stageByID["discovered"].Phase; got != "scoring" {
		t.Fatalf("derived stage discovered phase = %q, want scoring", got)
	}
	if got := stageByID["approved"].Phase; got != "operating" {
		t.Fatalf("derived stage approved phase = %q, want operating", got)
	}
	terminals := bundle.WorkflowTerminalStages()
	if len(terminals) == 0 {
		t.Fatal("expected derived terminal stages from flow schemas")
	}
	foundKilled := false
	for _, stage := range terminals {
		if stage == "killed" {
			foundKilled = true
			break
		}
	}
	if !foundKilled {
		t.Fatalf("derived terminal stages = %v, want killed included", terminals)
	}
}

func TestLoadWorkflowContractBundle_PrefersRuntimeBridgeContracts(t *testing.T) {
	repoRoot := t.TempDir()
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"), `
workflow:
  name: test
  version: "1.0.0"
  entity: vertical
  entity_table: verticals
  state_field: state
  initial_stage: discovered
  stages:
    - id: discovered
      phase: discovery
  transitions: []
  timers: []
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"), `
guards: []
actions: []
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"), `
platform:
  name: test
  version: "2.6.0"
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"), `
emit:
  category: output
  description: emit
  input_schema: {}
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"), `
foo: bar
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "package.yaml"), `
name: empire
version: 2.6.0
flows:
  - id: discovery
    flow: discovery
    namespace: vertical
runtime_contracts:
  nodes: runtime/nodes.yaml
  events: runtime/events.yaml
  agents: runtime/agents.yaml
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "nodes.yaml"), `
project-node:
  id: project-node
  execution_type: workflow_node
  subscribes_to: [project.event]
  produces: []
  event_handlers: {}
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "events.yaml"), `
project.event:
  payload:
    id: uuid
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "agents.yaml"), `
project-agent:
  id: project-agent
  model_tier: standard
  conversation_mode: task
  subscriptions: [project.event]
  emit_events: []
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "runtime", "nodes.yaml"), `
runtime-node:
  id: runtime-node
  execution_type: workflow_node
  subscribes_to: [runtime.event]
  produces: []
  event_handlers: {}
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "runtime", "events.yaml"), `
runtime.event:
  payload:
    id: uuid
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "runtime", "agents.yaml"), `
runtime-agent:
  id: runtime-agent
  model_tier: standard
  conversation_mode: task
  subscriptions: [runtime.event]
  emit_events: []
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "schema.yaml"), `
name: discovery
pins:
  inputs:
    events: [scan.requested]
    reads: []
  outputs:
    events: [scan.completed]
    writes: []
`)
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "events.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "agents.yaml"))

	bundle, err := LoadWorkflowContractBundle(repoRoot)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	if _, ok := bundle.Nodes["runtime-node"]; !ok {
		t.Fatal("expected runtime bridge nodes to load")
	}
	if _, ok := bundle.Nodes["project-node"]; ok {
		t.Fatal("did not expect project-level nodes.yaml to override runtime bridge nodes")
	}
	if _, ok := bundle.Events["runtime.event"]; !ok {
		t.Fatal("expected runtime bridge events to load")
	}
	if _, ok := bundle.Events["project.event"]; ok {
		t.Fatal("did not expect project-level events.yaml to override runtime bridge events")
	}
	if _, ok := bundle.Agents["runtime-agent"]; !ok {
		t.Fatal("expected runtime bridge agents to load")
	}
	if _, ok := bundle.Agents["project-agent"]; ok {
		t.Fatal("did not expect project-level agents.yaml to override runtime bridge agents")
	}
}

func TestLoadWorkflowContractBundle_ErrorsOnDuplicateMergedNodeIDs(t *testing.T) {
	repoRoot := t.TempDir()
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"), `
workflow:
  name: test
  version: "1.0.0"
  entity: vertical
  entity_table: verticals
  state_field: state
  initial_stage: discovered
  stages:
    - id: discovered
      phase: discovery
  transitions: []
  timers: []
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"), "guards: []\nactions: []\n")
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"), "platform:\n  name: test\n  version: \"2.6.0\"\n")
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"), "{}\n")
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"), "{}\n")
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"), "{}\n")
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"), "{}\n")
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"), "{}\n")
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "package.yaml"), `
name: empire
version: 2.6.0
flows:
  - id: discovery
    flow: discovery
    namespace: vertical
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "nodes.yaml"), `
dup-node:
  id: dup-node
  execution_type: system_node
  subscribes_to: []
  produces: []
  event_handlers: {}
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "schema.yaml"), `
name: discovery
pins:
  inputs: {events: [], reads: []}
  outputs: {events: [], writes: []}
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "nodes.yaml"), `
dup-node:
  id: dup-node
  execution_type: system_node
  subscribes_to: [scan.requested]
  produces: []
  event_handlers: {}
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "events.yaml"), "{}\n")
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "agents.yaml"), "{}\n")

	if _, err := LoadWorkflowContractBundle(repoRoot); err == nil {
		t.Fatal("expected duplicate merged node id error")
	}
}

func TestLoadWorkflowContractBundle_LoadsNestedPackageTree(t *testing.T) {
	currentRepo := projectRootFromContractsTest(t)
	repoRoot := t.TempDir()
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "workflow-schema.yaml"), filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "guard-action-registry.yaml"), filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "system-nodes.yaml"), filepath.Join(repoRoot, "contracts", "system-nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "event-catalog.yaml"), filepath.Join(repoRoot, "contracts", "event-catalog.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "agent-tools.yaml"), filepath.Join(repoRoot, "contracts", "agent-tools.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "tool-schemas.yaml"), filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "prompt-variables.yaml"), filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "platform", "platform-spec.yaml"), filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"))
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "package.yaml"), `
name: empire
version: 2.6.0
flows:
  - id: discovery
    flow: discovery
    namespace: vertical
children:
  - path: packages/operating-pack
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "operating-pack", "package.yaml"), `
name: operating-pack
version: 2.6.0
flows:
  - id: operating
    flow: operating
    namespace: opco
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "schema.yaml"), `
name: discovery
pins:
  inputs:
    events: [scan.requested]
    reads: []
  outputs:
    events: [scan.completed]
    writes: []
`)
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "events.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "agents.yaml"))
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "operating-pack", "flows", "operating", "schema.yaml"), `
name: operating
initial_state: approved
pins:
  inputs:
    events: [opco.spinup_requested]
    reads: []
  outputs:
    events: [opco.steady_state_reached]
    writes: [live_url]
`)
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "operating-pack", "flows", "operating", "nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "operating-pack", "flows", "operating", "events.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "operating-pack", "flows", "operating", "agents.yaml"))

	bundle, err := LoadWorkflowContractBundle(repoRoot)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	if got := len(bundle.PackageTree); got != 2 {
		t.Fatalf("package tree count = %d, want 2", got)
	}
	if got := len(bundle.FlowSchemas); got != 2 {
		t.Fatalf("flow schema count = %d, want 2", got)
	}
	if _, ok := bundle.FlowSchemas["operating"]; !ok {
		t.Fatal("expected nested operating flow schema")
	}
}

func TestLoadWorkflowContractBundle_ErrorsOnDuplicateFlowIDsAcrossPackageTree(t *testing.T) {
	currentRepo := projectRootFromContractsTest(t)
	repoRoot := t.TempDir()
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "workflow-schema.yaml"), filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "guard-action-registry.yaml"), filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "system-nodes.yaml"), filepath.Join(repoRoot, "contracts", "system-nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "event-catalog.yaml"), filepath.Join(repoRoot, "contracts", "event-catalog.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "agent-tools.yaml"), filepath.Join(repoRoot, "contracts", "agent-tools.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "tool-schemas.yaml"), filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "prompt-variables.yaml"), filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"))
	copyContractFileForTest(t, filepath.Join(currentRepo, "contracts", "platform", "platform-spec.yaml"), filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"))
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "package.yaml"), `
name: empire
version: 2.6.0
flows:
  - id: shared
    flow: discovery
    namespace: vertical
packages:
  - path: packages/dup-pack
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "dup-pack", "package.yaml"), `
name: dup-pack
version: 2.6.0
flows:
  - id: shared
    flow: scoring
    namespace: vertical
`)
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "schema.yaml"), `
name: discovery
pins:
  inputs:
    events: [scan.requested]
    reads: []
  outputs:
    events: [scan.completed]
    writes: []
`)
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "events.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "flows", "discovery", "agents.yaml"))
	writeContractsTestYAML(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "dup-pack", "flows", "scoring", "schema.yaml"), `
name: scoring
pins:
  inputs:
    events: [vertical.discovered]
    reads: []
  outputs:
    events: [vertical.scored]
    writes: [scores]
`)
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "dup-pack", "flows", "scoring", "nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "dup-pack", "flows", "scoring", "events.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "packages", "dup-pack", "flows", "scoring", "agents.yaml"))

	if _, err := LoadWorkflowContractBundle(repoRoot); err == nil || err.Error() == "" {
		t.Fatal("expected duplicate flow id error")
	}
}

func mustWriteTestFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func writeContractsTestYAML(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func copyContractFileForTest(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(dst), err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

func projectRootFromContractsTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

func currentV260ContractsRoot(t *testing.T, repoRoot string) string {
	t.Helper()
	candidates := []string{
		filepath.Join(repoRoot, "docs", "specs", "mas-platform", "empire", "contracts"),
		filepath.Join(repoRoot, "docs", "specs", "empireai-v2_6_0", "contracts-v250", "empire"),
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(filepath.Join(candidate, "package.yaml")); err == nil {
			return candidate
		}
	}
	t.Fatalf("could not locate current v2.6 contract fixture root")
	return ""
}
