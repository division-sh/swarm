package pinrouting

import (
	"os"
	"path/filepath"
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
		Source: testRootPinRoutingSource(runtimecontracts.FlowOutputSinkNone, nil), FlowID: ".", EventType: "root.ready",
	}, eventtest.RunCreatingRootIngress("", "root.ready", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if result.Failure != FailureTargetRequiredMissing {
		t.Fatalf("Failure = %q, want %q", result.Failure, FailureTargetRequiredMissing)
	}
}

func TestResolveAllowsAcceptedExternalConsumerWithoutInventingRoute(t *testing.T) {
	entry := runtimecontracts.EventCatalogEntry{}
	entry.Swarm.Consumer = []string{"external"}
	result := Resolve(ResolutionInput{
		Source: testRootPinRoutingSource(runtimecontracts.FlowOutputSinkNone, map[string]runtimecontracts.EventCatalogEntry{"root.ready": entry}), FlowID: ".", EventType: "root.ready",
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
				Source: testRootPinRoutingSource(runtimecontracts.FlowOutputSinkNone, map[string]runtimecontracts.EventCatalogEntry{"root.ready": entry}), FlowID: ".", EventType: "root.ready",
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
	bundle.FlowTree.Root.Nodes = bundle.Nodes
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatalf("CompileWorkflowSemantics: %v", err)
	}
	result := Resolve(ResolutionInput{Source: source, FlowID: ".", EventType: "root.ready"}, eventtest.RunCreatingRootIngress("", "root.ready", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if !result.Failure.Empty() || !result.Event.TargetRoute().Empty() {
		t.Fatalf("resolution = %#v, want targetless same-flow delivery", result)
	}
}

func TestResolveHarnessSinkCreatesNoRuntimeRoute(t *testing.T) {
	source := testRootPinRoutingSource(runtimecontracts.FlowOutputSinkHarness, nil)
	if !OutputHarnessSink(source, ".", "root.ready") {
		t.Fatal("typed harness sink not found")
	}
	result := Resolve(ResolutionInput{Source: source, FlowID: ".", EventType: "root.ready"}, eventtest.RunCreatingRootIngress("", "root.ready", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
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
	root := runtimecontracts.FlowContractView{
		Path:  ".",
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "."},
		Nodes: map[string]runtimecontracts.SystemNodeContract{"root-node": {ID: "root-node"}},
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{FlowTree: runtimecontracts.FlowTree{
		Root: &root, ByPath: map[string]*runtimecontracts.FlowContractView{".": &root}, ByID: map[string]*runtimecontracts.FlowContractView{".": &root},
	}})
	route := events.RouteIdentity{FlowID: ".", FlowInstance: "run-one"}

	node := identitytest.RootNode(t, "root-node")
	got, err := AdmitNodeExecutionRoutingSource(source, node, ".", route)
	if err != nil {
		t.Fatalf("AdmitNodeExecutionRoutingSource: %v", err)
	}
	if got.Kind() != events.RoutingSourceStaticFlow || got.Route() != route {
		t.Fatalf("routing source = %s %#v, want entityless static flow %#v", got.Kind().StorageCode(), got.Route(), route)
	}

	if _, err := AdmitNodeExecutionRoutingSource(source, node, ".", events.RouteIdentity{FlowID: "."}); err == nil || !strings.Contains(err.Error(), "exact selected-run flow route") {
		t.Fatalf("incomplete entityless source error = %v", err)
	}
}

func TestAdmitNodeExecutionRoutingSourceUsesSelectedRootEntityAuthority(t *testing.T) {
	root := runtimecontracts.FlowContractView{
		Path:  ".",
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "."},
		Nodes: map[string]runtimecontracts.SystemNodeContract{"root-node": {ID: "root-node"}},
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{FlowTree: runtimecontracts.FlowTree{
		Root: &root, ByPath: map[string]*runtimecontracts.FlowContractView{".": &root}, ByID: map[string]*runtimecontracts.FlowContractView{".": &root},
	}})
	route := events.RouteIdentity{FlowID: ".", FlowInstance: "run-one", EntityID: "entity-one"}
	got, err := AdmitNodeExecutionRoutingSource(source, identitytest.RootNode(t, "root-node"), ".", route)
	if err != nil {
		t.Fatalf("AdmitNodeExecutionRoutingSource: %v", err)
	}
	if got.Kind() != events.RoutingSourceRoot || got.Route() != (events.RouteIdentity{EntityID: "entity-one"}) {
		t.Fatalf("routing source = %s %#v, want selected-root entity authority", got.Kind().StorageCode(), got.Route())
	}
}

func TestAdmitNodeExecutionRoutingSourceUsesNestedFilesystemFlowOwner(t *testing.T) {
	flow := runtimecontracts.FlowContractView{
		Path: "orders/reconciliation", Paths: runtimecontracts.FlowContractPaths{FlowPath: "orders/reconciliation", NodesFile: "orders/reconciliation/nodes.yaml"},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate},
		Nodes:  map[string]runtimecontracts.SystemNodeContract{"shared": {ID: "shared"}},
	}
	root := runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{FlowTree: runtimecontracts.FlowTree{
		Root:   &root,
		ByID:   map[string]*runtimecontracts.FlowContractView{".": &root, "orders/reconciliation": &root.Children[0]},
		ByPath: map[string]*runtimecontracts.FlowContractView{".": &root, "orders/reconciliation": &root.Children[0]},
	}}
	source := semanticview.Wrap(bundle)
	node := identitytest.ExecutableNode(t, "orders/reconciliation", "shared")
	owner, ok := source.ExecutableNodeSource(node)
	if !ok || owner.FlowPath != "orders/reconciliation" || owner.Family != "nodes" {
		t.Fatalf("nested filesystem owner = %#v, ok=%v", owner, ok)
	}
	route := events.RouteIdentity{FlowID: "orders/reconciliation", FlowInstance: "orders/reconciliation/one", EntityID: "entity-one"}
	got, err := AdmitNodeExecutionRoutingSource(source, node, "orders/reconciliation", route)
	if err != nil {
		t.Fatalf("AdmitNodeExecutionRoutingSource: %v", err)
	}
	if got.Kind() != events.RoutingSourceConcreteTemplateInstance || got.Route() != route {
		t.Fatalf("nested filesystem routing source = %s %#v, want concrete route %#v", got.Kind().StorageCode(), got.Route(), route)
	}
	hostile := identitytest.ExecutableNode(t, "orders/other", "shared")
	if _, err := AdmitNodeExecutionRoutingSource(source, hostile, "orders/reconciliation", route); err == nil || !strings.Contains(err.Error(), "requires declared node") {
		t.Fatalf("undeclared filesystem sibling error = %v", err)
	}
}

func TestAdmitAgentExecutionRoutingSourceSeparatesFilesystemDeclarationFromRuntimePath(t *testing.T) {
	const owner = "test://pinrouting/bot/telegram-chat/phrase-bot"
	flow := runtimecontracts.FlowContractView{
		Path: "telegram-ingress/telegram-chat",
		Paths: runtimecontracts.FlowContractPaths{
			FlowPath: "telegram-ingress/telegram-chat", AgentsFile: "telegram-ingress/telegram-chat/agents.yaml",
		},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeTemplate},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"phrase-bot": runtimecontracts.EffectiveAgentRegistryEntry("phrase-bot", runtimecontracts.AgentRegistryEntry{ID: "phrase-bot", Role: "phrase-bot"}),
		},
		AgentURIs: map[string]string{"phrase-bot": owner},
	}
	root := runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Children: []runtimecontracts.FlowContractView{flow}}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root:   &root,
			ByID:   map[string]*runtimecontracts.FlowContractView{".": &root, "telegram-ingress/telegram-chat": &root.Children[0]},
			ByPath: map[string]*runtimecontracts.FlowContractView{".": &root, "telegram-ingress/telegram-chat": &root.Children[0]},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{ByURI: map[string]runtimecontracts.ContractURIRef{
			owner: {Kind: "agent", FlowID: "telegram-ingress/telegram-chat", LocalID: "phrase-bot", Full: owner},
		}},
	})
	actor := models.AgentConfig{
		ID:       "phrase-bot",
		FlowID:   "telegram-ingress/telegram-chat",
		FlowPath: "telegram-ingress/telegram-chat/chat-1",
		Identity: agentidentitytest.Declared(t, "phrase-bot", owner, "telegram-ingress/telegram-chat", "chat-1", "telegram-ingress/telegram-chat/chat-1"),
	}
	got, err := AdmitAgentExecutionRoutingSource(source, actor, "chat-entity")
	if err != nil {
		t.Fatalf("AdmitAgentExecutionRoutingSource: %v", err)
	}
	wantRoute := events.RouteIdentity{FlowID: "telegram-ingress/telegram-chat", FlowInstance: actor.FlowPath, EntityID: "chat-entity"}
	if got.Kind() != events.RoutingSourceConcreteTemplateInstance || got.Route() != wantRoute {
		t.Fatalf("routing source = %s %#v, want exact declaration flow plus runtime path %#v", got.Kind().StorageCode(), got.Route(), wantRoute)
	}

	hostile := actor
	hostile.Identity = agentidentitytest.Runtime(t, "phrase-bot", owner, "telegram-ingress/telegram-chat", "chat-1", actor.FlowPath)
	if _, err := AdmitAgentExecutionRoutingSource(source, hostile, "chat-entity"); err == nil || !strings.Contains(err.Error(), "declared agent identity") {
		t.Fatalf("runtime-created declaration collision error = %v", err)
	}
}

func TestAdmitAgentExecutionRoutingSourceUsesFilesystemDeclarationOwningFlow(t *testing.T) {
	for _, tc := range []struct {
		name         string
		mode         string
		instanceID   string
		instancePath string
		entityID     string
		wantKind     events.RoutingSourceKind
	}{
		{name: "template", mode: runtimecontracts.FlowModeTemplate, instanceID: "inst-1", instancePath: "support/inst-1", entityID: "entity-one", wantKind: events.RoutingSourceConcreteTemplateInstance},
		{name: "entityless_template", mode: runtimecontracts.FlowModeTemplate, instanceID: "inst-1", instancePath: "support/inst-1", wantKind: events.RoutingSourceConcreteTemplateInstance},
		{name: "static", mode: runtimecontracts.FlowModeStatic, instanceID: "support", instancePath: "support", entityID: "entity-one", wantKind: events.RoutingSourceStaticFlow},
		{name: "singleton", mode: runtimecontracts.FlowModeSingleton, instanceID: "support", instancePath: "support", entityID: "entity-one", wantKind: events.RoutingSourceStaticFlow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadFilesystemAgentOwnedByFlowSource(t, tc.mode)
			declarations := semanticview.AgentDeclarations(source)
			if len(declarations) != 1 || declarations[0].Source.FlowPath != "support" || declarations[0].OwnerFlowID != "support" {
				t.Fatalf("declarations = %#v, want one filesystem declaration owned by support", declarations)
			}
			plan, err := semanticview.ScopedAgentNamePlan(source, declarations[0])
			if err != nil {
				t.Fatalf("ScopedAgentNamePlan: %v", err)
			}
			actor := models.AgentConfig{
				ID:       "backend",
				FlowID:   "support",
				FlowPath: tc.instancePath,
				Identity: agentidentitytest.Declared(t, "backend", plan.OwnerURI, "support", tc.instanceID, tc.instancePath),
			}
			got, err := AdmitAgentExecutionRoutingSource(source, actor, tc.entityID)
			if err != nil {
				t.Fatalf("AdmitAgentExecutionRoutingSource: %v", err)
			}
			wantRoute := events.RouteIdentity{FlowID: "support", FlowInstance: tc.instancePath, EntityID: tc.entityID}
			if got.Kind() != tc.wantKind || got.Route() != wantRoute {
				t.Fatalf("routing source = %s %#v, want %s %#v", got.Kind().StorageCode(), got.Route(), tc.wantKind.StorageCode(), wantRoute)
			}
		})
	}
}

func loadFilesystemAgentOwnedByFlowSource(t *testing.T, mode string) semanticview.Source {
	t.Helper()
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := t.TempDir()
	write := func(path, contents string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	write(filepath.Join(root, "schema.yaml"), "name: filesystem-flow-agent\n")
	write(filepath.Join(root, "support", "schema.yaml"), "name: support\nmode: "+mode+"\ninitial_state: waiting\nstates: [waiting, done]\n")
	write(filepath.Join(root, "support", "agents.yaml"), "backend:\n  type: generic\n  role: backend\n  intent: {inline: \"Handle backend work.\"}\n  model: regular\n")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func TestPinDeclaredOutputRecognizesExactRootOutputOnly(t *testing.T) {
	source := testRootPinRoutingSource(runtimecontracts.FlowOutputSinkNone, nil)
	if !PinDeclaredOutput(source, ".", "root.ready") {
		t.Fatal("root output pin was not recognized")
	}
	if PinDeclaredOutput(source, ".", "worker/root.ready") {
		t.Fatal("namespaced event matched root output pin by leaf")
	}
}

func testPinRoutingSource(sink runtimecontracts.FlowOutputSink, events map[string]runtimecontracts.EventCatalogEntry) semanticview.Source {
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "child", SchemaFile: "child/schema.yaml"},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: "template", Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{
			EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "child.done", Sink: sink}},
		}}},
		Path: "child",
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events:      events,
		RootSchema:  &runtimecontracts.FlowSchemaDocument{},
		FlowSources: map[string]runtimecontracts.FlowSource{".": {FlowPath: ".", Schema: "schema.yaml", Children: []string{"child"}}, "child": {FlowPath: "child", Schema: "child/schema.yaml"}},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"child": child.Schema},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Children: []runtimecontracts.FlowContractView{child}},
			ByID: map[string]*runtimecontracts.FlowContractView{"child": &child}, ByPath: map[string]*runtimecontracts.FlowContractView{"child": &child},
		},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		panic(err)
	}
	return semanticview.Wrap(bundle)
}

func testRootPinRoutingSource(sink runtimecontracts.FlowOutputSink, catalog map[string]runtimecontracts.EventCatalogEntry) semanticview.Source {
	if catalog == nil {
		catalog = map[string]runtimecontracts.EventCatalogEntry{"root.ready": {}}
	}
	pin := runtimecontracts.FlowOutputEventPin{Event: "root.ready", Sink: sink}
	rootSchema := runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{pin}}}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema:  &rootSchema,
		FlowSources: map[string]runtimecontracts.FlowSource{".": {FlowPath: ".", Schema: "schema.yaml"}},
		Events:      catalog,
	}
	root := &runtimecontracts.FlowContractView{
		Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: ".", SchemaFile: "schema.yaml"},
		Schema: rootSchema, Events: catalog,
	}
	bundle.FlowTree = runtimecontracts.FlowTree{
		Root: root, ByID: map[string]*runtimecontracts.FlowContractView{".": root}, ByPath: map[string]*runtimecontracts.FlowContractView{".": root},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		panic(err)
	}
	return semanticview.Wrap(bundle)
}
