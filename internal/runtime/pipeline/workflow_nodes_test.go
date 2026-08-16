package pipeline

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
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

	workerNode := pipelinePackageNode(t, "flows/worker", "worker", "worker-node")
	got, err := workflowNodeSubscriptionAliases(harness, workerNode, "work.requested")
	if err != nil {
		t.Fatalf("harness subscription aliases: %v", err)
	}
	want, err := workflowNodeSubscriptionAliases(withoutSource, workerNode, "work.requested")
	if err != nil {
		t.Fatalf("plain subscription aliases: %v", err)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("harness subscriptions = %#v, undeclared subscriptions = %#v", got, want)
	}
	if strings.Join(got, ",") != "worker/work.requested,work.requested" {
		t.Fatalf("subscriptions = %#v, want only ordinary authored local aliases", got)
	}
}

func TestWorkflowNodeProjectionRejectsQualifiedExactHandlerBeforeExecution(t *testing.T) {
	for _, authored := range []string{"child/task.done", "missing/task.done", "child/grandchild/task.done", "swarm://child/task.done"} {
		t.Run(strings.ReplaceAll(authored, "/", "_"), func(t *testing.T) {
			source := workflowNodeExactSubscriptionSource(authored)
			nodeRef := pipelinePackageNode(t, "flows/child", "child", "listener")
			if aliases, err := workflowNodeSubscriptionAliases(source, nodeRef, authored); err == nil || aliases != nil {
				t.Fatalf("subscription aliases = %#v error = %v, want typed rejection", aliases, err)
			}
			if _, ok := source.ExecutableNodeEventHandler(nodeRef, "child/task.done"); ok {
				t.Fatal("invalid authored handler resolved through the semantic source")
			}
			if resolved := workflowNodeEventHandlerResolutionForEventType(source, nodeRef, "child/task.done"); resolved.Matched {
				t.Fatalf("invalid authored handler reached execution projection: %#v", resolved)
			}
			record, ok := source.ExecutableNode(nodeRef)
			if !ok {
				t.Fatal("listener executable node missing")
			}
			node := NewNode(nodeRef, record.Entry, source, nil, nil)
			if subscriptions := node.Subscriptions(); len(subscriptions) != 0 {
				t.Fatalf("invalid authored subscription reached node executor: %#v", subscriptions)
			}
			if _, err := LoadWorkflowNodes(source); err == nil || !strings.Contains(err.Error(), "must use a local event name") {
				t.Fatalf("LoadWorkflowNodes error = %v, want typed exact rejection", err)
			}
		})
	}
}

func TestWorkflowNodeProjectionPreservesLocalExactHandler(t *testing.T) {
	source := workflowNodeExactSubscriptionSource("task.done")
	node := pipelinePackageNode(t, "flows/child", "child", "listener")
	aliases, err := workflowNodeSubscriptionAliases(source, node, "task.done")
	if err != nil {
		t.Fatalf("workflowNodeSubscriptionAliases: %v", err)
	}
	if got := strings.Join(aliases, ","); got != "child/task.done,task.done" {
		t.Fatalf("aliases = %q, want canonical and local exact forms", got)
	}
	if _, ok := source.ExecutableNodeEventHandler(node, "child/task.done"); !ok {
		t.Fatal("local authored handler did not resolve from its canonical runtime event")
	}
	if resolved := workflowNodeEventHandlerResolutionForEventType(source, node, "child/task.done"); !resolved.Matched || resolved.HandlerEventKey != "task.done" {
		t.Fatalf("handler resolution = %#v, want local exact handler", resolved)
	}
}

func workflowNodeExactSubscriptionSource(authored string) semanticview.Source {
	node := runtimecontracts.SystemNodeContract{
		ID:            "listener",
		SubscribesTo:  []string{authored},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{authored: {}},
	}
	flow := runtimecontracts.FlowContractView{
		Path:   "child",
		Paths:  runtimecontracts.FlowContractPaths{ID: "child", Flow: "child", PackageKey: "flows/child"},
		Events: map[string]runtimecontracts.EventCatalogEntry{"task.done": {}},
		Nodes:  map[string]runtimecontracts.SystemNodeContract{"listener": node},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{"child": &root.Children[0]},
		},
	})
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

	source := semanticview.Wrap(bundle)
	if got := workflowNodeExternalEventType(source, pipelineSourceNode(t, source, "child", "child-worker"), "work.completed"); got != "child/work.completed" {
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
	evt := eventtest.RunCreatingRootIngress("", "parent.lead_captured", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, pipelinePackageNode(t, "flows/worker", "worker", "worker-node"), evt)
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
	evt := eventtest.RunCreatingRootIngress("", "worker/work.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, pipelineNode(t, "", "parent-listener"), evt)
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
	evt := eventtest.RunCreatingRootIngress("", "worker/work.completed", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, pipelineNode(t, "", "parent-listener"), evt)
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
		evt := eventtest.RunCreatingRootIngress("", tc.eventType, "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
		route := workflowNodeStampedConnectRouteForHandlerEvent(t, source, tc.wantKey, tc.nodeID)
		node, ok := route.Recipient.Node()
		if !ok {
			t.Fatal("stamped connect route lacks exact node recipient")
		}
		resolved := workflowNodeEventHandlerResolutionForDeliveryContext(withWorkflowNodeDeliveryRoute(context.Background(), route), source, node, evt)
		if !resolved.Matched || resolved.HandlerEventKey != tc.wantKey {
			t.Fatalf("handler resolution for %s = %#v, want %s", tc.eventType, resolved, tc.wantKey)
		}
	}
}

func TestWorkflowNodeConnectedInputEventHandlerResolution_ConsumesStampedPackageRootConnect(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")
	producerEvent := source.ResolveFlowEventReference("grandchild", "micro.done")
	receiverEvent := source.ResolveFlowEventReference("child", "micro.done")
	if producerEvent == receiverEvent {
		t.Fatalf("producer event %q must differ from receiver event %q for this proof", producerEvent, receiverEvent)
	}

	evt := eventtest.RunCreatingRootIngress("", events.EventType(producerEvent), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	route := workflowNodeStampedConnectRoute(t, source, "child", "micro_done", "child-relay")
	resolved := workflowNodeEventHandlerResolutionForDeliveryContext(withWorkflowNodeDeliveryRoute(context.Background(), route), source, pipelineSourceNode(t, source, "child", "child-relay"), evt)
	if !resolved.Matched || resolved.HandlerEventKey != "micro.done" {
		t.Fatalf("package-root connect handler resolution = %#v, want child-relay micro.done", resolved)
	}
}

func TestWorkflowNodeConnectedInputEventHandlerResolution_ConsumesStampedRootReceiverConnect(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")
	producerEvent := source.ResolveFlowEventReference("child", "micro.relayed")
	receiverEvent := source.ResolveFlowEventReference("", "micro.done")
	if producerEvent == receiverEvent {
		t.Fatalf("producer event %q must differ from receiver event %q for this proof", producerEvent, receiverEvent)
	}

	evt := eventtest.RunCreatingRootIngress("", events.EventType(producerEvent), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	route := workflowNodeStampedConnectRoute(t, source, "", "micro_done", "root-collector")
	node := pipelineSourceNode(t, source, "", "root-collector")
	loaded, err := LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	if !slices.ContainsFunc(loaded, func(candidate WorkflowNode) bool { return candidate.Node.Equal(node) }) {
		t.Fatalf("loaded workflow nodes = %#v, want exact root node %s", loaded, node.Key())
	}
	resolved := workflowNodeEventHandlerResolutionForDeliveryContext(withWorkflowNodeDeliveryRoute(context.Background(), route), source, node, evt)
	if !resolved.Matched || resolved.HandlerEventKey != "micro.done" {
		t.Fatalf("root receiver connect handler resolution = %#v, want root-collector micro.done", resolved)
	}
}

func TestWorkflowNodeConnectedInputEventHandlerResolution_DoesNotInferClaimFromFlowInstanceShape(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-nested-three-levels")
	producerEvent := source.ResolveFlowEventReference("grandchild", "micro.done")
	for _, tc := range []struct {
		name         string
		flowInstance string
	}{
		{name: "child context", flowInstance: "child/inst-1"},
		{name: "UUID root context", flowInstance: "11111111-1111-4111-8111-111111111111"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evt := eventtest.RunCreatingRootIngress("", events.EventType(producerEvent), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{
				FlowInstance: tc.flowInstance,
			}, time.Unix(1, 0).UTC())
			childRelay := pipelineNode(t, "child", "child-relay")
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(childRelay)}
			resolved := workflowNodeEventHandlerResolutionForDeliveryContext(withWorkflowNodeDeliveryRoute(context.Background(), route), source, childRelay, evt)
			if resolved.Matched {
				t.Fatalf("connected-input resolution = %#v, want no authority without a stamped claim", resolved)
			}
		})
	}
}

func TestWorkflowNodeConnectedInputHandlerMatchesConcreteTemplateProducer(t *testing.T) {
	source := testWorkflowNodeConnectedInputSource("template")
	evt := eventtest.RunCreatingRootIngress("", "producer/inst-1/deploy.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{
		EntityID:     "receiver-entity",
		FlowInstance: "receiver",
		Source:       events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: "producer-entity"},
		Target:       events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver", EntityID: "receiver-entity"},
	}, time.Unix(1, 0).UTC())

	route := workflowNodeStampedConnectRoute(t, source, "receiver", "deploy_requested", "receiver-node")
	resolved := workflowNodeEventHandlerResolutionForDeliveryContext(withWorkflowNodeDeliveryRoute(context.Background(), route), source, pipelineSourceNode(t, source, "receiver", "receiver-node"), evt)
	if !resolved.Matched || resolved.HandlerEventKey != "deploy.requested" {
		t.Fatalf("concrete template connect handler resolution = %#v, want receiver deploy.requested", resolved)
	}
}

func TestWorkflowNodeConnectedInputHandlerEnforcesProducerMode(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      string
		eventType string
		source    events.RouteIdentity
		want      bool
	}{
		{name: "static exact scope", mode: "static", eventType: "producer/deploy.done", source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer"}, want: true},
		{name: "static descendant", mode: "static", eventType: "producer/inst-1/deploy.done", source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1"}},
		{name: "template concrete instance", mode: "template", eventType: "producer/inst-1/deploy.done", source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1"}, want: true},
		{name: "template base scope", mode: "template", eventType: "producer/deploy.done", source: events.RouteIdentity{FlowID: "producer", FlowInstance: "producer"}},
		{name: "template concrete name without route", mode: "template", eventType: "producer/inst-1/deploy.done"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := testWorkflowNodeConnectedInputSource(tc.mode)
			sourceRoute := tc.source
			if !sourceRoute.Empty() {
				sourceRoute.EntityID = "producer-entity"
			}
			evt := eventtest.RunCreatingRootIngress("", events.EventType(tc.eventType), "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{
				EntityID:     "receiver-entity",
				FlowInstance: "receiver",
				Source:       sourceRoute,
				Target:       events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver", EntityID: "receiver-entity"},
			}, time.Unix(1, 0).UTC())
			ctx := context.Background()
			if tc.want {
				route := workflowNodeStampedConnectRoute(t, source, "receiver", "deploy_requested", "receiver-node")
				ctx = withWorkflowNodeDeliveryRoute(ctx, route)
			}
			resolved := workflowNodeEventHandlerResolutionForDeliveryContext(ctx, source, pipelineSourceNode(t, source, "receiver", "receiver-node"), evt)
			if resolved.Matched != tc.want {
				t.Fatalf("handler resolution = %#v, want matched %v", resolved, tc.want)
			}
		})
	}
}

func TestWorkflowNodeConnectedInputHandlerUsesExactStampedReceiverPin(t *testing.T) {
	source := testWorkflowNodeConnectedInputCollisionSource()
	evt := eventtest.RunCreatingRootIngress("", "producer/deploy.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{
		EntityID:     "receiver-entity",
		FlowInstance: "receiver",
		Source:       events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: "producer-entity"},
		Target:       events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver", EntityID: "receiver-entity"},
	}, time.Unix(1, 0).UTC())

	route := workflowNodeStampedConnectRoute(t, source, "receiver", "deploy_accepted", "receiver-node")
	ctx := withWorkflowNodeDeliveryRoute(context.Background(), route)
	resolved := workflowNodeEventHandlerResolutionForDeliveryContext(ctx, source, pipelineSourceNode(t, source, "receiver", "receiver-node"), evt)
	if !resolved.Matched || resolved.HandlerEventKey != "deploy.accepted" {
		t.Fatalf("stamped connected-input resolution = %#v, want deploy.accepted", resolved)
	}

}

func workflowNodeStampedConnectRouteForHandlerEvent(t testing.TB, source semanticview.Source, handlerEvent, nodeID string) events.DeliveryRoute {
	t.Helper()
	for _, plan := range runtimepinrouting.CompileConnectGraph(source).Plans() {
		if plan.ReceiverLocalEvent() != events.EventType(eventidentity.Normalize(handlerEvent)) {
			continue
		}
		receiver := plan.ReceiverEndpoint().Readback()
		flowID := receiver.FlowID
		node := pipelineSourceNode(t, source, flowID, nodeID)
		target := plan.ReceiverRoute(receiver.FlowPath, "receiver-entity")
		if target.FlowID == "" {
			target.FlowID = flowID
			if target.FlowID == "" {
				target.FlowID = semanticview.RootExecutionFlowID(source)
			}
		}
		if target.FlowInstance == "" {
			target.FlowInstance = target.FlowID
		}
		blueprint := runtimepinrouting.ConnectDeliveryRoute{
			Recipient: events.MustNodeDeliveryRecipient(node), Target: target,
			Handler: runtimepinrouting.MustConnectReceiverHandler(node),
		}
		claim, err := runtimepinrouting.ConnectExecutionClaim(plan, blueprint)
		if err != nil {
			t.Fatalf("build stamped connect claim: %v", err)
		}
		return events.DeliveryRoute{Recipient: blueprint.Recipient, Target: events.MustExistingEntityTarget(target), ConnectClaim: claim}
	}
	t.Fatalf("compiled graph has no receiver event %s", handlerEvent)
	return events.DeliveryRoute{}
}

func workflowNodeStampedConnectRoute(t testing.TB, source semanticview.Source, receiverFlow, receiverPin, nodeID string) events.DeliveryRoute {
	t.Helper()
	for _, plan := range runtimepinrouting.CompileConnectGraph(source).Plans() {
		receiver := plan.ReceiverEndpoint().Readback()
		if receiver.FlowID != receiverFlow || receiver.Pin != receiverPin {
			continue
		}
		target := plan.ReceiverRoute(receiver.FlowPath, "receiver-entity")
		if target.FlowID == "" {
			target.FlowID = semanticview.RootExecutionFlowID(source)
		}
		if target.FlowInstance == "" {
			target.FlowInstance = target.FlowID
		}
		node := pipelineSourceNode(t, source, receiver.FlowID, nodeID)
		blueprint := runtimepinrouting.ConnectDeliveryRoute{
			Recipient: events.MustNodeDeliveryRecipient(node), Target: target,
			Handler: runtimepinrouting.MustConnectReceiverHandler(node),
		}
		claim, err := runtimepinrouting.ConnectExecutionClaim(plan, blueprint)
		if err != nil {
			t.Fatalf("build stamped connect claim: %v", err)
		}
		return events.DeliveryRoute{Recipient: blueprint.Recipient, Target: events.MustExistingEntityTarget(target), ConnectClaim: claim}
	}
	t.Fatalf("compiled graph has no receiver %s.%s", receiverFlow, receiverPin)
	return events.DeliveryRoute{}
}

func testWorkflowNodeConnectedInputSource(producerMode string) semanticview.Source {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: producerMode,
			Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{
				Name: "deploy_done", Event: "deploy.done",
			}}}},
		},
		Path: "producer",
	}
	receiver := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "receiver", Flow: "receiver"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{
				Name: "deploy_requested", Event: "deploy.requested",
			}}}},
		},
		Path: "receiver",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"receiver-node": {
				ID:           "receiver-node",
				SubscribesTo: []string{"deploy.requested"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"deploy.requested": {},
				},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, receiver}}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowOutputEventPins: map[string][]runtimecontracts.FlowOutputEventPin{
				"producer": {{Name: "deploy_done", Event: "deploy.done"}},
			},
			FlowInputEventPins: map[string][]runtimecontracts.FlowInputEventPin{
				"receiver": {{Name: "deploy_requested", Event: "deploy.requested"}},
			},
			CompositionConnects: []runtimecontracts.FlowPackageConnect{{
				SourceFile: "package.yaml", SourceLine: 1, From: "producer.deploy_done", To: "receiver.deploy_requested", Adapter: "deploy_done_to_deploy_requested",
			}},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"receiver-node": {"deploy.requested": {}},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer": &root.Children[0],
				"receiver": &root.Children[1],
			},
		},
	})
}

func testWorkflowNodeConnectedInputCollisionSource() semanticview.Source {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "static",
			Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Name: "deploy_done", Event: "deploy.done"}}}},
		},
		Path: "producer",
	}
	receiverInputs := []runtimecontracts.FlowInputEventPin{
		{Name: "deploy_accepted", Event: "deploy.accepted"},
		{Name: "deploy_audited", Event: "deploy.audited"},
	}
	receiverHandlers := map[string]runtimecontracts.SystemNodeEventHandler{
		"deploy.accepted": {},
		"deploy.audited":  {},
	}
	receiver := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "receiver", Flow: "receiver"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Mode: "static",
			Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{EventPins: receiverInputs}},
		},
		Path: "receiver",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"receiver-node": {ID: "receiver-node", EventHandlers: receiverHandlers},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, receiver}}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowOutputEventPins: map[string][]runtimecontracts.FlowOutputEventPin{
				"producer": {{Name: "deploy_done", Event: "deploy.done"}},
			},
			FlowInputEventPins: map[string][]runtimecontracts.FlowInputEventPin{"receiver": receiverInputs},
			CompositionConnects: []runtimecontracts.FlowPackageConnect{
				{SourceFile: "package.yaml", SourceLine: 1, From: "producer.deploy_done", To: "receiver.deploy_accepted", Adapter: "deploy_done_to_deploy_accepted"},
				{SourceFile: "package.yaml", SourceLine: 2, From: "producer.deploy_done", To: "receiver.deploy_audited", Adapter: "deploy_done_to_deploy_audited"},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{"receiver-node": receiverHandlers},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer": &root.Children[0],
				"receiver": &root.Children[1],
			},
		},
	})
}

func TestWorkflowNodeHandlerResolution_TargetRouteDoesNotAuthorizeProducerScopedEvent(t *testing.T) {
	source := workflowNodeDirectTemplateDeliverySource()
	evt := eventtest.RunCreatingRootIngress("", "intake/account.ready", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	evt = eventtest.TargetRouted(evt, events.RouteIdentity{
		FlowID:       "account_case",
		FlowInstance: "account_case/ti-1",
		EntityID:     "entity-1",
	})
	accountNode := pipelineNode(t, "account_case", "account-case-worker")
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(accountNode), Target: events.MustExistingEntityTarget(evt.TargetRoute())}
	resolved := workflowNodeEventHandlerResolutionForDeliveryContext(withWorkflowNodeDeliveryRoute(context.Background(), route), source, accountNode, evt)
	if resolved.Matched || !strings.Contains(resolved.Failure, "stamped connect claim") {
		t.Fatalf("target-route-only resolution = %#v, want fail-closed stamped-claim error", resolved)
	}
}

func TestWorkflowNodeHandlerResolution_DirectConcreteDeliveryConsumesExactTargetOwner(t *testing.T) {
	source := workflowNodeDirectTemplateDeliverySource()
	producerRoute := events.RouteIdentity{FlowID: "account_case", FlowInstance: "account_case/ti-1", EntityID: "entity-1"}
	evt := eventtest.RunCreatingRootIngressWithRoutingSource(
		"", "account_case/ti-1/account.ready", "", "", []byte(`{}`), 0, "", "",
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, producerRoute),
		eventtest.ConcreteTemplateRoutingSource(producerRoute.FlowID, producerRoute.FlowInstance, producerRoute.EntityID),
		time.Unix(1, 0).UTC(),
	)
	for _, tc := range []struct {
		name   string
		target events.RouteIdentity
		want   bool
	}{
		{name: "exact instance", target: producerRoute, want: true},
		{name: "stamped owner entity", target: events.RouteIdentity{FlowID: "account_case", FlowInstance: "account_case/ti-1", EntityID: "entity-2"}, want: true},
		{name: "unrelated flow", target: events.RouteIdentity{FlowID: "intake", FlowInstance: "account_case/ti-1", EntityID: "entity-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accountNode := pipelineNode(t, "account_case", "account-case-worker")
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(accountNode), Target: events.MustExistingEntityTarget(tc.target)}
			resolved := workflowNodeEventHandlerResolutionForDeliveryContext(withWorkflowNodeDeliveryRoute(context.Background(), route), source, accountNode, evt)
			if resolved.Matched != tc.want {
				t.Fatalf("direct concrete resolution = %#v, want matched=%t", resolved, tc.want)
			}
			if tc.want && resolved.HandlerEventKey != "account.ready" {
				t.Fatalf("handler event key = %q, want account.ready", resolved.HandlerEventKey)
			}
		})
	}
}

func workflowNodeDirectTemplateDeliverySource() semanticview.Source {
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
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
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
	evt := eventtest.RunCreatingRootIngress("", "operating/operating.reported", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	evt = eventtest.TargetRouted(evt, events.RouteIdentity{
		FlowID:       "portfolio",
		FlowInstance: "portfolio",
		EntityID:     FlowInstanceEntityID("portfolio"),
	})

	route := workflowNodeStampedConnectRouteForHandlerEvent(t, source, "operating.reported", "portfolio-collector")
	route.Target = events.MustExistingEntityTarget(evt.TargetRoute())
	resolved := workflowNodeEventHandlerResolutionForDeliveryContext(withWorkflowNodeDeliveryRoute(context.Background(), route), source, pipelineSourceNode(t, source, "portfolio", "portfolio-collector"), evt)
	if !resolved.Matched {
		t.Fatal("expected portfolio handler to resolve through the canonical cross-flow event")
	}
	if got := resolved.HandlerEventKey; got != "operating.reported" {
		t.Fatalf("handler event key = %q, want authored operating.reported", got)
	}
}

func TestWorkflowNodeHandlerResolution_DeniesImportBoundaryWildcardRawFallback(t *testing.T) {
	source := loadPipelineImportBoundaryWildcardSource(t, canonicalrouting.ImportBoundaryWildcardDenied)
	evt := eventtest.RunCreatingRootIngress("", "producer/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	workerNode := pipelineSourceNode(t, source, "worker", "worker-listener")
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, workerNode, evt)
	if resolved.Matched {
		t.Fatalf("worker-listener matched ungranted sibling event through raw wildcard fallback: %#v", resolved)
	}
	if _, ok := source.ExecutableNodeEventHandler(workerNode, "producer/task.done"); ok {
		t.Fatal("semantic source NodeEventHandler matched ungranted sibling event")
	}
}

func TestWorkflowNodeHandlerResolution_AllowsGrantedImportBoundaryWildcard(t *testing.T) {
	source := loadPipelineImportBoundaryWildcardSource(t, canonicalrouting.ImportBoundaryWildcardObserveGranted)
	evt := eventtest.RunCreatingRootIngress("", "producer/task.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	node := pipelineSourceNode(t, source, "worker", "worker-listener")
	resolved := workflowNodeEventHandlerResolutionForDelivery(source, node, evt)
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
		if nodes[i].Node.NodeID() == id {
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
