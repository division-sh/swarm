package pipeline

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestWorkflowFlowInputProducerAliases_IncludeProducerScopedAlias(t *testing.T) {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "producer",
	}
	discovery := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "discovery",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID:           "scan-orchestrator",
				SubscribesTo: []string{"scan.requested"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, discovery}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer":  &root.Children[0],
				"discovery": &root.Children[1],
			},
		},
	}

	aliases := workflowFlowInputProducerAliases(semanticview.Wrap(bundle), "discovery", "scan.requested")
	for _, candidate := range aliases {
		if candidate == "producer/scan.requested" {
			return
		}
	}
	t.Fatalf("aliases = %#v, want producer/scan.requested", aliases)
}

func TestWorkflowFlowInputProducerAliases_AutoWireCrossFlowInputPinsToProducerScopedEvent(t *testing.T) {
	scoring := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "scoring", Flow: "scoring"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"vertical.shortlisted"}},
			},
		},
		Path: "scoring",
	}
	validation := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"vertical.shortlisted"}},
			},
		},
		Path: "validation",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"validation-orchestrator": {
				ID:           "validation-orchestrator",
				SubscribesTo: []string{"vertical.shortlisted"},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{scoring, validation}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"scoring":    &root.Children[0],
				"validation": &root.Children[1],
			},
		},
	}

	aliases := workflowFlowInputProducerAliases(semanticview.Wrap(bundle), "validation", "vertical.shortlisted")
	for _, alias := range aliases {
		if alias == "scoring/vertical.shortlisted" {
			return
		}
	}
	t.Fatalf("aliases = %#v, want scoring/vertical.shortlisted", aliases)
}

func TestWorkflowNodeExternalEventType_ExternalizesLocalFlowOutputs(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-child-flow-pin-wiring")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	if got := workflowNodeExternalEventType(semanticview.Wrap(bundle), "child-worker", "work.completed"); got != "child/work.completed" {
		t.Fatalf("workflowNodeExternalEventType = %q, want child/work.completed", got)
	}
}

func TestLoadWorkflowNodes_UsesHandlerKeysForCrossFlowPinAutoWire(t *testing.T) {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "producer",
	}
	consumer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "consumer", Flow: "consumer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "consumer",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"consumer-node": {
				ID:       "consumer-node",
				Produces: []string{"scan.completed"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"scan.requested": {
						Emit: runtimecontracts.EmitSpec{Event: "scan.completed"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.completed": {OwningNode: "consumer-node"},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, consumer}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer": &root.Children[0],
				"consumer": &root.Children[1],
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"consumer-node": {
					"scan.requested": {},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"consumer-node": consumer.Nodes["consumer-node"],
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"consumer/scan.completed": {OwningNode: "consumer-node"},
		},
	}

	nodes, err := LoadWorkflowNodes(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("LoadWorkflowNodes returned %d nodes, want 1", len(nodes))
	}
	for _, subscription := range nodes[0].Subscriptions {
		if string(subscription) == "producer/scan.requested" {
			return
		}
	}
	t.Fatalf("Subscriptions = %#v, want producer/scan.requested", nodes[0].Subscriptions)
}

func TestLoadWorkflowNodes_UsesEffectiveFactsForMinimizedSystemNode(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"task.start": {
						Emit: runtimecontracts.EmitSpec{Event: "task.done"},
					},
				},
			},
		},
	}

	nodes, err := LoadWorkflowNodes(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	worker := workflowNodeByIDForTest(nodes, "worker")
	if worker == nil {
		t.Fatalf("worker missing from %#v", nodes)
	}
	if got, want := worker.ExecutionType, runtimecontracts.SystemNodeExecutionType; got != want {
		t.Fatalf("execution type = %q, want %q", got, want)
	}
	if !workflowNodeHasSubscriptionForTest(*worker, "task.start") {
		t.Fatalf("subscriptions = %#v, want task.start", worker.Subscriptions)
	}
	if !workflowNodeHasProducesForTest(*worker, "task.done") {
		t.Fatalf("produces = %#v, want task.done", worker.Produces)
	}
}

func TestLoadWorkflowNodes_UsesImportBoundaryInputAlias(t *testing.T) {
	source := loadPipelineImportBoundaryAliasSource(t)
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	worker := workflowNodeByIDForTest(nodes, "worker-node")
	if worker == nil {
		t.Fatalf("worker-node missing from %#v", nodes)
	}
	if !workflowNodeHasSubscriptionForTest(*worker, "parent.lead_captured") {
		t.Fatalf("worker-node subscriptions = %#v, want parent.lead_captured", worker.Subscriptions)
	}
	if workflowNodeHasSubscriptionForTest(*worker, "work.requested") || workflowNodeHasSubscriptionForTest(*worker, "worker/work.requested") {
		t.Fatalf("worker-node subscriptions = %#v, should not preserve raw required-import input fallback", worker.Subscriptions)
	}
	evt := eventtest.RootIngress("", "parent.lead_captured", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "worker-node", evt)
	if !resolved.Matched {
		t.Fatal("expected worker-node handler to resolve through input alias")
	}
	if got := resolved.HandlerEventKey; got != "work.requested" {
		t.Fatalf("handler event key = %q, want work.requested", got)
	}
}

func TestLoadWorkflowNodes_UsesImportBoundaryOutputAliasForParentHandler(t *testing.T) {
	source := loadPipelineImportBoundaryAliasSource(t)
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	parent := workflowNodeByIDForTest(nodes, "parent-listener")
	if parent == nil {
		t.Fatalf("parent-listener missing from %#v", nodes)
	}
	if !workflowNodeHasSubscriptionForTest(*parent, "worker/work.completed") {
		t.Fatalf("parent-listener subscriptions = %#v, want worker/work.completed output alias", parent.Subscriptions)
	}
	evt := eventtest.RootIngress("", "worker/work.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "parent-listener", evt)
	if !resolved.Matched {
		t.Fatal("expected parent-listener handler to resolve through output alias")
	}
	if got := resolved.HandlerEventKey; got != "parent.lead_enriched" {
		t.Fatalf("handler event key = %q, want parent.lead_enriched", got)
	}
}

func TestLoadWorkflowNodes_UsesImportBoundaryOutputAliasForWildcardParentSubscription(t *testing.T) {
	source := loadPipelineImportBoundaryAliasSourceWithParentSubscription(t, "parent.*")
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	parent := workflowNodeByIDForTest(nodes, "parent-listener")
	if parent == nil {
		t.Fatalf("parent-listener missing from %#v", nodes)
	}
	if !workflowNodeHasSubscriptionForTest(*parent, "worker/work.completed") {
		t.Fatalf("parent-listener subscriptions = %#v, want worker/work.completed output alias for wildcard parent subscription", parent.Subscriptions)
	}
	evt := eventtest.RootIngress("", "worker/work.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "parent-listener", evt)
	if !resolved.Matched {
		t.Fatal("expected parent-listener handler to resolve through output alias")
	}
	if got := resolved.HandlerEventKey; got != "parent.lead_enriched" {
		t.Fatalf("handler event key = %q, want parent.lead_enriched", got)
	}
}

func TestWorkflowNodeHandlerResolution_DeniesImportBoundaryWildcardRawFallback(t *testing.T) {
	source := loadPipelineImportBoundaryWildcardSource(t, "")
	evt := eventtest.RootIngress("", "producer/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "worker-listener", evt)
	if resolved.Matched {
		t.Fatalf("worker-listener matched ungranted sibling event through raw wildcard fallback: %#v", resolved)
	}
	if _, ok := source.NodeEventHandler("worker-listener", "producer/task.done"); ok {
		t.Fatal("semantic source NodeEventHandler matched ungranted sibling event")
	}
}

func TestWorkflowNodeHandlerResolution_AllowsGrantedImportBoundaryWildcard(t *testing.T) {
	source := loadPipelineImportBoundaryWildcardSource(t, "      observe:\n        - source: producer\n          events: [task.done]\n")
	evt := eventtest.RootIngress("", "producer/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "worker-listener", evt)
	if !resolved.Matched {
		t.Fatal("worker-listener did not match granted sibling event")
	}
	if got := resolved.HandlerEventKey; got != "**/task.done" {
		t.Fatalf("handler event key = %q, want **/task.done", got)
	}
}

func loadPipelineImportBoundaryAliasSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadPipelineImportBoundaryAliasSourceWithParentSubscription(t, "parent.lead_enriched")
}

func loadPipelineImportBoundaryAliasSourceWithParentSubscription(t *testing.T, parentSubscription string) semanticview.Source {
	t.Helper()
	root := writePipelineImportBoundaryAliasFixture(t, parentSubscription)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(contractComplianceRepoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(contractComplianceRepoRoot(t)))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writePipelineImportBoundaryAliasFixture(t *testing.T, parentSubscription string) string {
	t.Helper()
	root := t.TempDir()
	writePipelineFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: pipeline-import-boundary-alias
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: worker
    flow: worker
    mode: static
    bind:
      inputs:
        work.requested: parent.lead_captured
      outputs:
        work.completed: parent.lead_enriched
`)
	writePipelineFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: pipeline-import-boundary-alias\n")
	writePipelineFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "events.yaml"), `
parent.lead_captured: {}
parent.lead_enriched: {}
`)
	writePipelineFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
parent-listener:
  id: parent-listener
  execution_type: system_node
  subscribes_to: [`+parentSubscription+`]
  event_handlers:
    parent.lead_enriched: {}
`)
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "worker", "package.yaml"), `
name: worker
version: "1.0.0"
requires:
  inputs: [work.requested]
  outputs: [work.completed]
`)
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), `
name: worker
mode: static
pins:
  inputs:
    events: [work.requested]
  outputs:
    events: [work.completed]
`)
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "worker", "policy.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "worker", "agents.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), "work.completed: {}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "worker", "nodes.yaml"), `
worker-node:
  id: worker-node
  execution_type: system_node
  subscribes_to: [work.requested]
  produces: [work.completed]
  event_handlers:
    work.requested:
      emit: work.completed
`)
	return root
}

func loadPipelineImportBoundaryWildcardSource(t *testing.T, observeGrant string) semanticview.Source {
	t.Helper()
	bundle := loadPipelineImportBoundaryWildcardBundle(t, observeGrant)
	return semanticview.Wrap(bundle)
}

func loadPipelineImportBoundaryWildcardBundle(t *testing.T, observeGrant string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := writePipelineImportBoundaryWildcardFixture(t, observeGrant)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(contractComplianceRepoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(contractComplianceRepoRoot(t)))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func writePipelineImportBoundaryWildcardFixture(t *testing.T, observeGrant string) string {
	t.Helper()
	root := t.TempDir()
	workerBind := ""
	if strings.TrimSpace(observeGrant) != "" {
		workerBind = "    bind:\n" + observeGrant
	}
	writePipelineFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: pipeline-import-boundary-wildcard
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: worker
    flow: worker
    mode: static
`+workerBind+`  - id: producer
    flow: producer
    mode: static
`)
	writePipelineFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: pipeline-import-boundary-wildcard\n")
	writePipelineFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "worker", "package.yaml"), "name: worker\nversion: \"1.0.0\"\n")
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), `
name: worker
mode: static
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  outputs:
    events: [task.done]
`)
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "worker", "policy.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "worker", "agents.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), "task.done: {}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "worker", "nodes.yaml"), `
worker-listener:
  id: worker-listener
  execution_type: system_node
  subscribes_to: ["**/task.done"]
  event_handlers:
    "**/task.done":
      clear_gates: [sibling_gate]
`)
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "producer", "package.yaml"), "name: producer\nversion: \"1.0.0\"\n")
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  outputs:
    events: [task.done]
`)
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), "task.done: {}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "{}\n")
	return root
}

func workflowNodeByIDForTest(nodes []WorkflowNode, id string) *WorkflowNode {
	for i := range nodes {
		if nodes[i].ID == id {
			return &nodes[i]
		}
	}
	return nil
}

func workflowNodeHasSubscriptionForTest(node WorkflowNode, eventType string) bool {
	for _, subscription := range node.Subscriptions {
		if string(subscription) == eventType {
			return true
		}
	}
	return false
}

func workflowNodeHasProducesForTest(node WorkflowNode, eventType string) bool {
	for _, produced := range node.Produces {
		if string(produced) == eventType {
			return true
		}
	}
	return false
}
