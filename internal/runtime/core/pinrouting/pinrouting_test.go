package pinrouting

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestResolveTargetsCompleteParentRouteForPinDeclaredOutput(t *testing.T) {
	parent := events.RouteIdentity{FlowID: "root", FlowInstance: "root/inst-1", EntityID: "parent-ent"}
	result := Resolve(ResolutionInput{
		Source: testPinRoutingSource(runtimecontracts.FlowOutputSinkNone, nil), FlowID: "child", EventType: "child.done",
		StructuralParent: ClassifyPersistedStructuralParent(parent),
	}, eventtest.RunCreatingRootIngress("", "child.done", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if !result.Failure.Empty() || result.Target != parent || result.Event.TargetRoute() != parent {
		t.Fatalf("resolution = %#v, want exact parent route", result)
	}
}

func TestResolveFailsClosedWithoutCanonicalConsumer(t *testing.T) {
	result := Resolve(ResolutionInput{
		Source: testRootPinRoutingSource(runtimecontracts.FlowOutputSinkNone, nil), EventType: "root.ready",
	}, eventtest.RunCreatingRootIngress("", "root.ready", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if result.Failure != FailureTargetRequiredMissing {
		t.Fatalf("Failure = %q, want %q", result.Failure, FailureTargetRequiredMissing)
	}
}

func TestResolveAllowsAcceptedExternalConsumerWithoutInventingRoute(t *testing.T) {
	entry := runtimecontracts.EventCatalogEntry{}
	entry.Swarm.Consumer = []string{"external"}
	result := Resolve(ResolutionInput{
		Source: testRootPinRoutingSource(runtimecontracts.FlowOutputSinkNone, map[string]runtimecontracts.EventCatalogEntry{"root.ready": entry}), EventType: "root.ready",
	}, eventtest.RunCreatingRootIngress("", "root.ready", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if !result.Failure.Empty() || !result.Event.TargetRoute().Empty() || len(result.Event.TargetRoutes()) != 0 {
		t.Fatalf("resolution = %#v, want targetless accepted external observation", result)
	}
}

func TestResolveRejectsUnregisteredExternalConsumerMetadata(t *testing.T) {
	for _, consumer := range []string{"external_catalog_harness", "externl", "webhook"} {
		t.Run(consumer, func(t *testing.T) {
			entry := runtimecontracts.EventCatalogEntry{}
			entry.Swarm.Consumer = []string{consumer}
			result := Resolve(ResolutionInput{
				Source: testRootPinRoutingSource(runtimecontracts.FlowOutputSinkNone, map[string]runtimecontracts.EventCatalogEntry{"root.ready": entry}), EventType: "root.ready",
			}, eventtest.RunCreatingRootIngress("", "root.ready", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
			if result.Failure != FailureTargetRequiredMissing {
				t.Fatalf("Failure = %q, want %q", result.Failure, FailureTargetRequiredMissing)
			}
		})
	}
}

func TestResolveAllowsTypedSameFlowConsumerWithoutInventingRoute(t *testing.T) {
	source := testRootPinRoutingSource(runtimecontracts.FlowOutputSinkNone, nil)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("bundle source missing")
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"consumer": {
			ID:            "consumer",
			ExecutionType: "system_node",
			SubscribesTo:  []string{"root.ready"},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"root.ready": {}},
		},
	}
	result := Resolve(ResolutionInput{Source: source, EventType: "root.ready"}, eventtest.RunCreatingRootIngress("", "root.ready", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if !result.Failure.Empty() || !result.Event.TargetRoute().Empty() {
		t.Fatalf("resolution = %#v, want targetless same-flow delivery", result)
	}
}

func TestResolveHarnessSinkCreatesNoRuntimeRoute(t *testing.T) {
	source := testRootPinRoutingSource(runtimecontracts.FlowOutputSinkHarness, nil)
	if !OutputHarnessSink(source, "", "root.ready") {
		t.Fatal("typed harness sink not found")
	}
	result := Resolve(ResolutionInput{Source: source, EventType: "root.ready"}, eventtest.RunCreatingRootIngress("", "root.ready", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if !result.Failure.Empty() || !result.Event.TargetRoute().Empty() || len(result.Event.TargetRoutes()) != 0 {
		t.Fatalf("resolution = %#v, want targetless validation observation", result)
	}
}

func TestResolveFailsClosedOnIncompleteParentRoute(t *testing.T) {
	result := Resolve(ResolutionInput{
		Source: testPinRoutingSource(runtimecontracts.FlowOutputSinkNone, nil), FlowID: "child", EventType: "child.done",
		StructuralParent: ClassifyPersistedStructuralParent(events.RouteIdentity{FlowID: "root", EntityID: "parent-ent"}),
	}, eventtest.RunCreatingRootIngress("", "child.done", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if result.Failure != FailureParentRouteIncomplete {
		t.Fatalf("Failure = %q, want %q", result.Failure, FailureParentRouteIncomplete)
	}
}

func TestAdmitNodeExecutionRoutingSourcePreservesEntitylessSelectedRun(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"root-node": {ID: "root-node"},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "root-workflow"},
	})
	route := events.RouteIdentity{FlowID: "root-workflow", FlowInstance: "run-one"}

	node := identitytest.RootNode(t, "root-node")
	got, err := AdmitNodeExecutionRoutingSource(source, node, "root-workflow", route)
	if err != nil {
		t.Fatalf("AdmitNodeExecutionRoutingSource: %v", err)
	}
	if got.Kind() != events.RoutingSourceStaticFlow || got.Route() != route {
		t.Fatalf("routing source = %s %#v, want entityless static flow %#v", got.Kind().StorageCode(), got.Route(), route)
	}

	if _, err := AdmitNodeExecutionRoutingSource(source, node, "root-workflow", events.RouteIdentity{FlowID: "root-workflow"}); err == nil || !strings.Contains(err.Error(), "exact selected-run flow route") {
		t.Fatalf("incomplete entityless source error = %v", err)
	}
}

func TestAdmitNodeExecutionRoutingSourceUsesNestedPackageOwningFlow(t *testing.T) {
	flow := runtimecontracts.FlowContractView{
		Path: "orders", Paths: runtimecontracts.FlowContractPaths{ID: "orders", PackageKey: "root/flows/orders"},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate},
		Children: []runtimecontracts.FlowContractView{{
			Paths: runtimecontracts.FlowContractPaths{PackageKey: "packages/a", NodesFile: "packages/a/nodes.yaml"},
			Nodes: map[string]runtimecontracts.SystemNodeContract{"shared": {ID: "shared"}},
		}},
	}
	root := runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{PackageKey: "root"}, Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{FlowTree: runtimecontracts.FlowTree{
		Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{"orders": &root.Children[0]},
	}}
	source := semanticview.Wrap(bundle)
	node := identitytest.ExecutableNode(t, "packages/a", "orders", "shared")
	owner, ok := source.ExecutableNodeSource(node)
	if !ok || owner.Layer != "project" || owner.FlowID != "orders" {
		t.Fatalf("nested package owner = %#v, ok=%v", owner, ok)
	}
	route := events.RouteIdentity{FlowID: "orders", FlowInstance: "orders/one", EntityID: "entity-one"}
	got, err := AdmitNodeExecutionRoutingSource(source, node, "orders", route)
	if err != nil {
		t.Fatalf("AdmitNodeExecutionRoutingSource: %v", err)
	}
	if got.Kind() != events.RoutingSourceConcreteTemplateInstance || got.Route() != route {
		t.Fatalf("nested package routing source = %s %#v, want concrete route %#v", got.Kind().StorageCode(), got.Route(), route)
	}
	hostile := identitytest.ExecutableNode(t, "packages/b", "orders", "shared")
	if _, err := AdmitNodeExecutionRoutingSource(source, hostile, "orders", route); err == nil || !strings.Contains(err.Error(), "requires declared node") {
		t.Fatalf("undeclared package sibling error = %v", err)
	}
}

func TestAdmitAgentExecutionRoutingSourceSeparatesImportedDeclarationFromRuntimePath(t *testing.T) {
	const owner = "test://pinrouting/bot/telegram-chat/phrase-bot"
	flow := runtimecontracts.FlowContractView{
		Path: "telegram-ingress/telegram-chat",
		Paths: runtimecontracts.FlowContractPaths{
			ID: "telegram-chat", Flow: "telegram-chat", PackageKey: "bot", AgentsFile: "/contracts/bot/flows/telegram-chat/agents.yaml",
		},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"phrase-bot": runtimecontracts.EffectiveAgentRegistryEntry("phrase-bot", runtimecontracts.AgentRegistryEntry{ID: "phrase-bot", Role: "phrase-bot"}),
		},
		AgentURIs: map[string]string{"phrase-bot": owner},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{"telegram-chat": &root.Children[0]}},
		URIRegistry: runtimecontracts.ContractURIRegistry{ByURI: map[string]runtimecontracts.ContractURIRef{
			owner: {Kind: "agent", FlowID: "telegram-chat", LocalID: "phrase-bot", Full: owner},
		}},
	})
	actor := models.AgentConfig{
		ID:       "phrase-bot",
		FlowID:   "telegram-chat",
		FlowPath: "telegram-ingress/telegram-chat/chat-1",
		Identity: agentidentitytest.Declared(t, "phrase-bot", owner, "telegram-ingress/telegram-chat", "chat-1", "telegram-ingress/telegram-chat/chat-1"),
	}
	got, err := AdmitAgentExecutionRoutingSource(source, actor, "chat-entity")
	if err != nil {
		t.Fatalf("AdmitAgentExecutionRoutingSource: %v", err)
	}
	wantRoute := events.RouteIdentity{FlowID: "telegram-chat", FlowInstance: actor.FlowPath, EntityID: "chat-entity"}
	if got.Kind() != events.RoutingSourceConcreteTemplateInstance || got.Route() != wantRoute {
		t.Fatalf("routing source = %s %#v, want exact declaration flow plus runtime path %#v", got.Kind().StorageCode(), got.Route(), wantRoute)
	}

	hostile := actor
	hostile.Identity = agentidentitytest.Runtime(t, "phrase-bot", owner, "telegram-ingress/telegram-chat", "chat-1", actor.FlowPath)
	if _, err := AdmitAgentExecutionRoutingSource(source, hostile, "chat-entity"); err == nil || !strings.Contains(err.Error(), "declared agent identity") {
		t.Fatalf("runtime-created declaration collision error = %v", err)
	}
}

func TestPinDeclaredOutputRecognizesExactRootOutputOnly(t *testing.T) {
	source := testRootPinRoutingSource(runtimecontracts.FlowOutputSinkNone, nil)
	if !PinDeclaredOutput(source, "", "root.ready") {
		t.Fatal("root output pin was not recognized")
	}
	if PinDeclaredOutput(source, "", "worker/root.ready") {
		t.Fatal("namespaced event matched root output pin by leaf")
	}
}

func testPinRoutingSource(sink runtimecontracts.FlowOutputSink, events map[string]runtimecontracts.EventCatalogEntry) semanticview.Source {
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: "template", Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{
			EventPins: []runtimecontracts.FlowOutputEventPin{{Name: "child.done", Event: "child.done", Sink: sink}},
		}}},
		Path: "child",
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events:      events,
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"child": child.Schema},
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowOutputs:         map[string][]string{"child": {"child.done"}},
			FlowOutputEventPins: map[string][]runtimecontracts.FlowOutputEventPin{"child": child.Schema.Pins.Outputs.EventPins},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}},
			ByID: map[string]*runtimecontracts.FlowContractView{"child": &child},
		},
	})
}

func testRootPinRoutingSource(sink runtimecontracts.FlowOutputSink, catalog map[string]runtimecontracts.EventCatalogEntry) semanticview.Source {
	if catalog == nil {
		catalog = map[string]runtimecontracts.EventCatalogEntry{"root.ready": {}}
	}
	pin := runtimecontracts.FlowOutputEventPin{Name: "root.ready", Event: "root.ready", Sink: sink}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{pin}}}},
		Events:     catalog,
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowOutputs:         map[string][]string{"": {"root.ready"}},
			FlowOutputEventPins: map[string][]runtimecontracts.FlowOutputEventPin{"": {pin}},
		},
	})
}
