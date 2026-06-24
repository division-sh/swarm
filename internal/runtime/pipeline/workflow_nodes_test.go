package pipeline

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
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
	evt := events.NewProjectionEvent("", "parent.lead_captured", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
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
	evt := events.NewProjectionEvent("", "worker/work.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "parent-listener", evt)
	if !resolved.Matched {
		t.Fatal("expected parent-listener handler to resolve through output alias")
	}
	if got := resolved.HandlerEventKey; got != "parent.lead_enriched" {
		t.Fatalf("handler event key = %q, want parent.lead_enriched", got)
	}
}

func loadPipelineImportBoundaryAliasSource(t *testing.T) semanticview.Source {
	t.Helper()
	root := writePipelineImportBoundaryAliasFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(contractComplianceRepoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(contractComplianceRepoRoot(t)))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writePipelineImportBoundaryAliasFixture(t *testing.T) string {
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
  subscribes_to: [parent.lead_enriched]
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
