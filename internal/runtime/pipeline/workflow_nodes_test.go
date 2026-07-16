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
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestWorkflowFlowInputProducerAliases_DoNotInferSiblingProducerAlias(t *testing.T) {
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
	if len(aliases) != 0 {
		t.Fatalf("aliases = %#v, want none for retired sibling auto-wire", aliases)
	}
}

func TestWorkflowFlowInputProducerAliases_DoNotAutoWireCrossFlowInputPinsToProducerScopedEvent(t *testing.T) {
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
	if len(aliases) != 0 {
		t.Fatalf("aliases = %#v, want none for retired sibling auto-wire", aliases)
	}
}

func TestWorkflowFlowInputProducerAliases_HarnessSourceAddsNoAlias(t *testing.T) {
	source := loadHarnessInjectionPipelineSource(t, canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection))
	if aliases := workflowFlowInputProducerAliases(source, "worker", "work.requested"); len(aliases) != 0 {
		t.Fatalf("producer aliases = %#v, want none for harness source", aliases)
	}
}

func TestWorkflowNodeHarnessInputKeepsOnlyAuthoredLocalSubscription(t *testing.T) {
	harness := loadHarnessInjectionPipelineSource(t, canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection))
	withoutSource := loadHarnessInjectionPipelineSource(t, canonicalrouting.CopyHarnessInjectionWithoutSource(t))

	got := workflowNodeSubscriptionAliases(harness, "worker-node", "work.requested")
	want := workflowNodeSubscriptionAliases(withoutSource, "worker-node", "work.requested")
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("harness subscriptions = %#v, undeclared subscriptions = %#v", got, want)
	}
	if strings.Join(got, ",") != "worker/work.requested,work.requested" {
		t.Fatalf("subscriptions = %#v, want only ordinary authored local aliases", got)
	}
}

func loadHarnessInjectionPipelineSource(t *testing.T, root string) semanticview.Source {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load harness injection fixture: %v", err)
	}
	return semanticview.Wrap(bundle)
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

func TestLoadWorkflowNodes_DoesNotUseSiblingOutputForCrossFlowPinAutoWire(t *testing.T) {
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
			t.Fatalf("Subscriptions = %#v, want no retired sibling auto-wire alias", nodes[0].Subscriptions)
		}
	}
	if !workflowNodeHasSubscriptionForTest(nodes[0], "scan.requested") {
		t.Fatalf("Subscriptions = %#v, want local scan.requested", nodes[0].Subscriptions)
	}
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

func TestLoadWorkflowNodes_ImportInputBindingRequiresEffectiveConnect(t *testing.T) {
	source := loadPipelineImportBoundaryAliasSource(t)
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	worker := workflowNodeByIDForTest(nodes, "worker-node")
	if worker == nil {
		t.Fatalf("worker-node missing from %#v", nodes)
	}
	if !workflowNodeHasSubscriptionForTest(*worker, "work.requested") {
		t.Fatalf("worker-node subscriptions = %#v, want receiver-local work.requested", worker.Subscriptions)
	}
	if workflowNodeHasSubscriptionForTest(*worker, "parent.lead_captured") {
		t.Fatalf("worker-node subscriptions = %#v, bind must not add producer authority", worker.Subscriptions)
	}
	evt := eventtest.RootIngress("", "parent.lead_captured", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "worker-node", evt)
	if resolved.Matched {
		t.Fatalf("bind-only producer event resolved handler: %#v", resolved)
	}
}

func TestLoadWorkflowNodes_ImportOutputBindingRequiresEffectiveConnect(t *testing.T) {
	source := loadPipelineImportBoundaryAliasSource(t)
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	parent := workflowNodeByIDForTest(nodes, "parent-listener")
	if parent == nil {
		t.Fatalf("parent-listener missing from %#v", nodes)
	}
	if !workflowNodeHasSubscriptionForTest(*parent, "parent.lead_enriched") {
		t.Fatalf("parent-listener subscriptions = %#v, want receiver-local parent.lead_enriched", parent.Subscriptions)
	}
	evt := eventtest.RootIngress("", "worker/work.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "parent-listener", evt)
	if resolved.Matched {
		t.Fatalf("bind-only child event resolved parent handler: %#v", resolved)
	}
}

func TestLoadWorkflowNodes_ImportOutputWildcardRequiresEffectiveConnect(t *testing.T) {
	source := loadPipelineImportBoundaryAliasSourceWithParentSubscription(t, "parent.*")
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	parent := workflowNodeByIDForTest(nodes, "parent-listener")
	if parent == nil {
		t.Fatalf("parent-listener missing from %#v", nodes)
	}
	if workflowNodeHasSubscriptionForTest(*parent, "worker/work.completed") {
		t.Fatalf("parent-listener subscriptions = %#v, bind must not authorize child output", parent.Subscriptions)
	}
	evt := eventtest.RootIngress("", "worker/work.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "parent-listener", evt)
	if resolved.Matched {
		t.Fatalf("bind-only child event resolved wildcard parent handler: %#v", resolved)
	}
}

func TestWorkflowNodeHandlerResolution_ConnectConsumesImportBindings(t *testing.T) {
	source := loadPipelineImportBoundaryConnectedSource(t)
	for _, tc := range []struct {
		nodeID    string
		eventType events.EventType
		wantKey   string
	}{
		{nodeID: "worker-node", eventType: "parent.lead_captured", wantKey: "work.requested"},
		{nodeID: "parent-listener", eventType: "worker/work.completed", wantKey: "parent.lead_enriched"},
	} {
		evt := eventtest.RootIngress("", tc.eventType, "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
		resolved := workflowNodeEventHandlerResolutionForDelivery(source, tc.nodeID, evt)
		if !resolved.Matched || resolved.HandlerEventKey != tc.wantKey {
			t.Fatalf("handler resolution for %s = %#v, want %s", tc.eventType, resolved, tc.wantKey)
		}
	}
}

func TestWorkflowNodeConnectedInputEventHandlerResolution_ConsumesLoweredPackageRootConnect(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")
	producerEvent := source.ResolveFlowEventReference("grandchild", "micro.done")
	receiverEvent := source.ResolveFlowEventReference("child", "micro.done")
	if producerEvent == receiverEvent {
		t.Fatalf("producer event %q must differ from receiver event %q for this proof", producerEvent, receiverEvent)
	}

	resolved := workflowNodeConnectedInputEventHandlerResolution(source, "child-relay", producerEvent)
	if !resolved.Matched || resolved.HandlerEventKey != "micro.done" {
		t.Fatalf("package-root connect handler resolution = %#v, want child-relay micro.done", resolved)
	}
}

func TestWorkflowNodeHandlerResolution_LocalizesProducerScopedEventThroughTargetRoute(t *testing.T) {
	accountCase := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "account_case", Flow: "account_case"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "template",
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{
					Name:  "account_ready",
					Event: "account.ready",
				}}},
			},
		},
		Path: "account_case",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"account-case-worker": {
				ID:           "account-case-worker",
				SubscribesTo: []string{"account.ready"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"account.ready": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{accountCase}}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"account-case-worker": {
					"account.ready": {},
				},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"account_case": &root.Children[0],
			},
		},
	})
	evt := eventtest.RootIngress("", "intake/account.ready", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	evt = eventtest.TargetRouted(evt, events.RouteIdentity{
		FlowID:       "account_case",
		FlowInstance: "account_case/ti-1",
		EntityID:     "entity-1",
	})
	if got := workflowNodeTargetRouteLocalEventType("account_case", "account_case", []string{"account.ready"}, "intake/account.ready", evt.TargetRoute()); got != "account.ready" {
		t.Fatalf("target-route localized event = %q, want account.ready", got)
	}

	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "account-case-worker", evt)
	if !resolved.Matched {
		t.Fatal("expected account-case-worker handler to resolve through target route")
	}
	if got := resolved.HandlerEventKey; got != "account.ready" {
		t.Fatalf("handler event key = %q, want account.ready", got)
	}
}

func TestWorkflowNodeHandlerResolution_PreservesAuthoredKeyForCanonicalCrossFlowEvent(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := semanticview.Wrap(bundle)
	evt := eventtest.RootIngress("", "operating/operating.reported", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	evt = eventtest.TargetRouted(evt, events.RouteIdentity{
		FlowID:       "portfolio",
		FlowInstance: "portfolio",
		EntityID:     FlowInstanceEntityID("portfolio"),
	})

	resolved := workflowNodeEventHandlerResolutionForDelivery(source, "portfolio-collector", evt)
	if !resolved.Matched {
		t.Fatal("expected portfolio handler to resolve through the canonical cross-flow event")
	}
	if got := resolved.HandlerEventKey; got != "operating.reported" {
		t.Fatalf("handler event key = %q, want authored operating.reported", got)
	}
}

func TestWorkflowNodeHandlerResolution_DeniesImportBoundaryWildcardRawFallback(t *testing.T) {
	source := loadPipelineImportBoundaryWildcardSource(t, canonicalrouting.ImportBoundaryWildcardDenied)
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
	source := loadPipelineImportBoundaryWildcardSource(t, canonicalrouting.ImportBoundaryWildcardObserveGranted)
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
	return loadPipelineImportBoundaryAliasVariant(t, canonicalrouting.ImportBoundaryAliasBindOnly)
}

func loadPipelineImportBoundaryAliasSourceWithParentSubscription(t *testing.T, parentSubscription string) semanticview.Source {
	t.Helper()
	if parentSubscription != "parent.*" {
		t.Fatalf("unsupported import-boundary parent subscription %q", parentSubscription)
	}
	return loadPipelineImportBoundaryAliasVariant(t, canonicalrouting.ImportBoundaryAliasBindOnlyWildcardOutput)
}

func loadPipelineImportBoundaryConnectedSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadPipelineImportBoundaryAliasVariant(t, canonicalrouting.ImportBoundaryAliasConnected)
}

func loadPipelineImportBoundaryAliasVariant(t *testing.T, variant canonicalrouting.ImportBoundaryAliasVariant) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyImportBoundaryAlias(t, variant)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(contractComplianceRepoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(contractComplianceRepoRoot(t)))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}
func loadPipelineImportBoundaryWildcardSource(t *testing.T, variant canonicalrouting.ImportBoundaryWildcardVariant) semanticview.Source {
	t.Helper()
	bundle := loadPipelineImportBoundaryWildcardBundle(t, variant)
	return semanticview.Wrap(bundle)
}

func loadPipelineImportBoundaryWildcardBundle(t *testing.T, variant canonicalrouting.ImportBoundaryWildcardVariant) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := canonicalrouting.CopyImportBoundaryWildcard(t, variant)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(contractComplianceRepoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(contractComplianceRepoRoot(t)))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
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
