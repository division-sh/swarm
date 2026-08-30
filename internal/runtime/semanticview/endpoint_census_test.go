package semanticview

import (
	"errors"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestAuthoredEventEndpointCensusEnumeratesExecutableFactsAndAssertions(t *testing.T) {
	source := endpointCensusFixture(t, []runtimecontracts.FlowInputEventPin{{
		Event: "work.requested",
		Resolution: runtimecontracts.FlowInputPinResolution{
			Mode:        runtimecontracts.FlowInputResolutionModeFanIn,
			Aggregation: "stream",
			Window:      "5m",
			DedupBy:     []string{"work_id"},
		},
	}})

	census := BuildAuthoredEventEndpointCensus(source)
	if got := endpointCount(census.Producers(), EventEndpointNodeHandler, "worker", "work.completed"); got != 1 {
		t.Fatalf("node handler producers = %d, want 1: %#v", got, census.Producers())
	}
	if got := endpointCount(census.Consumers(), EventEndpointNodeHandler, "worker", "work.requested"); got != 1 {
		t.Fatalf("node handler consumers = %d, want 1: %#v", got, census.Consumers())
	}
	if got := endpointCount(census.InputPins(), EventEndpointFlowInputPin, "worker", "work.requested"); got != 1 {
		t.Fatalf("input endpoints = %d, want 1: %#v", got, census.InputPins())
	}
	assertions := census.ProducerAssertions()
	if len(assertions) != 1 || assertions[0].NodeID != "worker-node" || !assertions[0].Declared || len(assertions[0].EventTypes) != 0 {
		t.Fatalf("producer assertions = %#v, want explicit empty worker-node assertion", assertions)
	}

	producers := census.Producers()
	producers[0].NodeID = "mutated"
	if got := census.Producers()[0].NodeID; got == "mutated" {
		t.Fatal("census exposed mutable producer storage")
	}
}

func TestAuthoredEventEndpointCensusReportsHarnessSinkWithoutConsumer(t *testing.T) {
	bundle := endpointCensusBundle(nil)
	schema := bundle.FlowSchemas["worker"]
	schema.Pins.Outputs.EventPins = []runtimecontracts.FlowOutputEventPin{{
		Event: "work.completed", Sink: runtimecontracts.FlowOutputSinkHarness,
	}}
	bundle.FlowSchemas["worker"] = schema
	bundle.FlowTree.ByID["worker"].Schema = schema

	source := withCompiledTestPins(t, Wrap(bundle), nil, map[string][]runtimecontracts.FlowOutputEventPin{"worker": {{Event: "work.completed", Sink: runtimecontracts.FlowOutputSinkHarness}}})
	census := BuildAuthoredEventEndpointCensus(source)
	outputs := census.OutputPins()
	if len(outputs) != 1 || outputs[0].Sink != "harness" {
		t.Fatalf("output endpoints = %#v, want harness sink readback", outputs)
	}
	if got := census.MatchingConsumers("worker", "work.completed"); len(got) != 0 {
		t.Fatalf("matching consumers = %#v, want harness to create none", got)
	}
}

func TestAuthoredEventEndpointCensusIncludesCompiledHandlersOutsideEffectiveSubscriptions(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {ID: "worker", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.requested": {}}},
		},
	}

	census := BuildAuthoredEventEndpointCensus(Wrap(bundle))
	if got := endpointCount(census.Consumers(), EventEndpointNodeHandler, ".", "work.requested"); got != 1 {
		t.Fatalf("compiled handler consumers = %d, want 1: %#v", got, census.Consumers())
	}
}

func TestAuthoredEventEndpointCensusClassifiesInternalStageTimerAsPlatform(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		Timers: []runtimecontracts.WorkflowTimerContract{{
			ID:         "active.timed_out",
			Event:      runtimecontracts.WorkflowStageTimerInternalEvent,
			StageOwned: true,
			AdvancesTo: "timed_out",
		}},
	}}
	producers := BuildAuthoredEventEndpointCensus(Wrap(bundle)).Producers()
	if got := endpointCount(producers, EventEndpointPlatform, "", runtimecontracts.WorkflowStageTimerInternalEvent); got != 1 {
		t.Fatalf("internal stage timer producers = %#v, want one platform endpoint", producers)
	}
	if got := endpointCount(producers, EventEndpointTimer, "", runtimecontracts.WorkflowStageTimerInternalEvent); got != 0 {
		t.Fatalf("internal stage timer was classified as authored timer: %#v", producers)
	}
}

func TestAuthoredEventEndpointCensusEnumeratesEveryProducerConsumerFamily(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{Event: "flow.created"},
			Pins: runtimecontracts.FlowPins{
				Inputs:  runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "flow.started"}}},
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "flow.completed"}}},
			},
			RequiredAgents: []runtimecontracts.FlowRequiredAgent{{Role: "reviewer", SubscribesTo: []string{"review.requested"}, Emits: []string{"review.completed"}}},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {ID: "worker", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.requested": {Emit: runtimecontracts.EmitSpec{Event: "work.completed"}}}},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"analyst": {ID: "analyst", Role: "analyst", Subscriptions: []string{"analysis.requested"}, EmitEvents: []string{"analysis.completed"}},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{
				"analyst": {Kind: "agent", LocalID: "analyst", Full: "test://endpoint-census/analyst"},
			},
			ByURI: map[string]runtimecontracts.ContractURIRef{
				"test://endpoint-census/analyst": {Kind: "agent", LocalID: "analyst", Full: "test://endpoint-census/analyst"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"external.received": {Swarm: runtimecontracts.EventSwarmMetadata{Source: "external", Consumer: []string{"external"}}},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Timers: []runtimecontracts.WorkflowTimerContract{{ID: "reminder", Event: "timer.fired", StartOn: "event:timer.started"}},
		},
	}
	root := runtimecontracts.FlowContractView{
		Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Schema: *bundle.RootSchema,
		Nodes: bundle.Nodes, Agents: bundle.Agents,
		AgentURIs: map[string]string{"analyst": "test://endpoint-census/analyst"},
	}
	bundle.FlowTree = runtimecontracts.FlowTree{
		Root:   &root,
		ByPath: map[string]*runtimecontracts.FlowContractView{".": &root},
		ByID:   map[string]*runtimecontracts.FlowContractView{".": &root},
	}
	base := withCompiledTestPins(t, Wrap(bundle), map[string][]runtimecontracts.FlowInputEventPin{".": {{Event: "flow.started"}}}, map[string][]runtimecontracts.FlowOutputEventPin{".": {{Event: "flow.completed"}}})
	census := BuildAuthoredEventEndpointCensus(base)
	producerKinds := endpointKindSet(census.Producers())
	for _, kind := range []EventEndpointKind{EventEndpointNodeHandler, EventEndpointAgent, EventEndpointRequiredAgentRole, EventEndpointTimer, EventEndpointAutoEmit, EventEndpointExternal} {
		if !producerKinds[kind] {
			t.Fatalf("producer kinds = %#v, missing %s", producerKinds, kind)
		}
	}
	consumerKinds := endpointKindSet(census.Consumers())
	for _, kind := range []EventEndpointKind{EventEndpointNodeHandler, EventEndpointAgent, EventEndpointRequiredAgentRole, EventEndpointTimer, EventEndpointExternal} {
		if !consumerKinds[kind] {
			t.Fatalf("consumer kinds = %#v, missing %s", consumerKinds, kind)
		}
	}
	if len(census.InputPins()) != 1 || len(census.OutputPins()) != 1 {
		t.Fatalf("interface endpoints = inputs %#v outputs %#v", census.InputPins(), census.OutputPins())
	}
}

func TestResolveDeclaredInputEndpointUsesAllDeclaredIdentitiesAndFailsClosed(t *testing.T) {
	source := endpointCensusFixture(t, []runtimecontracts.FlowInputEventPin{{
		Event: "work.requested",
	}})
	census := BuildAuthoredEventEndpointCensus(source)

	for _, identity := range []string{"work.requested"} {
		result := census.ResolveDeclaredInputEndpoint("worker", identity)
		endpoint, ok := result.Endpoint()
		if !ok || endpoint.PinName != "work.requested" {
			t.Fatalf("identity %q result = %#v, want exact work.requested input", identity, result)
		}
	}

	missing := census.ResolveDeclaredInputEndpoint("worker", "work.missing")
	if missing.Status != EndpointAssociationNotFound {
		t.Fatalf("missing status = %q, want not_found", missing.Status)
	}
	var associationErr *EndpointAssociationError
	if !errors.As(missing.Err(), &associationErr) || associationErr.Status != EndpointAssociationNotFound {
		t.Fatalf("missing error = %#v, want typed not-found", missing.Err())
	}
}

func TestResolveDeclaredInputEndpointDoesNotUseProducerSchemaKeyAsReceiverIdentity(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	fixtureRoot := canonicalrouting.CopyTemplateCreateThenSelectSameEvent(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	census := BuildAuthoredEventEndpointCensus(Wrap(bundle))

	setup := census.ResolveDeclaredInputEndpoint("account", "account.create")
	setupEndpoint, ok := setup.Endpoint()
	if !ok || setupEndpoint.PinName != "account.create" {
		t.Fatalf("account.create association = %#v, want exact account.create only", setup)
	}
	ready := census.ResolveDeclaredInputEndpoint("account", "account.ready")
	readyEndpoint, ok := ready.Endpoint()
	if !ok || readyEndpoint.PinName != "account.ready" {
		t.Fatalf("account.ready association = %#v, want exact account.ready only", ready)
	}
	if setupEndpoint.Event.CatalogKey != readyEndpoint.Event.CatalogKey {
		t.Fatalf("fixture did not prove shared producer schema key: setup=%#v ready=%#v", setupEndpoint.Event, readyEndpoint.Event)
	}
}

func TestResolveFanInInputForHandlerUsesExactEventIdentity(t *testing.T) {
	source := endpointCensusFixture(t, []runtimecontracts.FlowInputEventPin{{
		Event:      "work.requested",
		Resolution: runtimecontracts.FlowInputPinResolution{Mode: runtimecontracts.FlowInputResolutionModeFanIn},
	}})
	census := BuildAuthoredEventEndpointCensus(source)
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration("worker", "worker-node")
	if err != nil {
		t.Fatal(err)
	}
	for _, identity := range []string{"work.requested"} {
		result := census.ResolveFanInInputForHandler(node, identity)
		endpoint, ok := result.Endpoint()
		if !ok || endpoint.PinName != "work.requested" {
			t.Fatalf("handler identity %q result = %#v, want exact work.requested input", identity, result)
		}
	}

}

func TestAuthoredEventEndpointCensusMatchesScopedWildcardConsumers(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	tests := []struct {
		name      string
		fixture   string
		eventType string
		pattern   string
		nodeID    string
	}{
		{name: "root wildcard", fixture: filepath.Join("tests", "tier5-flow-lifecycle", "test-wildcard-subscription"), eventType: "task.completed", pattern: "*.completed", nodeID: "test-node"},
		{name: "deep imported scope", fixture: filepath.Join("tests", "tier11-flow-composition", "test-wildcard-deep-subscription"), eventType: "child/grandchild/task.done", pattern: "**/task.done", nodeID: "collector"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, filepath.Join(repoRoot, tc.fixture), runtimecontracts.DefaultPlatformSpecFile(repoRoot))
			if err != nil {
				t.Fatalf("load fixture: %v", err)
			}
			matched := BuildAuthoredEventEndpointCensus(Wrap(bundle)).MatchingConsumers("", tc.eventType)
			found := false
			for _, endpoint := range matched {
				if endpoint.NodeID == tc.nodeID && endpoint.Pattern && endpoint.Event.Authored == tc.pattern {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("scoped event %q consumers = %#v, want authored %s endpoint", tc.eventType, matched, tc.pattern)
			}
		})
	}
}

func TestAuthoredEventEndpointCensusResolvesNestedWildcardThroughFlowTree(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-wildcard-deep-subscription"),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load wildcard fixture: %v", err)
	}

	source := Wrap(bundle)
	census := BuildAuthoredEventEndpointCensus(source)
	for _, producer := range census.Producers() {
		if producer.Event.Canonical != "child/grandchild/task.done" {
			continue
		}
		matches, issues := census.ResolveTypedPubSubConsumerMatches(producer)
		for _, match := range matches {
			if match.Consumer.NodeID == "collector" {
				if len(issues) != 0 || match.Kind != TypedPubSubMatchPattern || match.Boundary != TypedPubSubBoundaryFlowTree || match.Authorization != nil {
					t.Fatalf("typed match = %#v issues = %#v, want flow-tree pattern without package authorization", match, issues)
				}
				return
			}
		}
		t.Fatalf("typed matches = %#v issues = %#v, want collector flow-tree pattern", matches, issues)
	}
	t.Fatal("task.done producer not found")
}

func TestAuthoredEventEndpointCensusConsumesScopedLocalWildcardAdmission(t *testing.T) {
	for _, authored := range []string{"task.*", "*"} {
		t.Run(authored, func(t *testing.T) {
			source := localWildcardEndpointCensusSource(authored)
			census := BuildAuthoredEventEndpointCensus(source)
			producer := AuthoredEventEndpoint{
				ID:        "child-producer",
				Direction: EventEndpointProducer,
				FlowID:    "child",
				Event:     ResolveFlowEventProof(source, "child", "task.done"),
			}
			matches, issues := census.ResolveTypedPubSubConsumerMatches(producer)
			if len(issues) != 0 || endpointCountFromMatches(matches, "listener") != 1 {
				t.Fatalf("typed relation = matches %#v issues %#v, want one local listener", matches, issues)
			}
			localConsumers := census.MatchingConsumers("child", "child/task.done")
			if got := endpointCountForNode(localConsumers, "listener"); got != 1 {
				t.Fatalf("local matching consumers = %#v, want scoped wildcard listener", localConsumers)
			}
			if localConsumers[0].Kind != EventEndpointNodeHandler || localConsumers[0].HandlerEvent != authored {
				t.Fatalf("local consumer = %#v, want admitted authored handler %q", localConsumers[0], authored)
			}
			var listener AuthoredEventEndpoint
			for _, endpoint := range census.Consumers() {
				if endpoint.NodeID == "listener" {
					listener = endpoint
					break
				}
			}
			siblingProof := ResolveFlowEventProof(source, "sibling", "task.done")
			if endpointMatchesProof(source, listener, siblingProof) {
				t.Fatalf("sibling event matched child-local wildcard: endpoint %#v proof %#v", listener, siblingProof)
			}
			siblingProducer := AuthoredEventEndpoint{ID: "sibling-producer", Direction: EventEndpointProducer, FlowID: "sibling", Event: siblingProof}
			siblingMatches, siblingIssues := census.ResolveTypedPubSubConsumerMatches(siblingProducer)
			if endpointCountFromMatches(siblingMatches, "listener") != 0 || len(siblingIssues) != 0 {
				t.Fatalf("sibling typed relation = matches %#v issues %#v, want no child listener", siblingMatches, siblingIssues)
			}
		})
	}
}

func TestAuthoredEventEndpointCensusTypedRelationClassifiesSameFlowExactlyOnce(t *testing.T) {
	source := endpointCensusFixture(t, nil)
	producer := AuthoredEventEndpoint{
		ID:        "producer",
		Direction: EventEndpointProducer,
		FlowID:    "worker",
		Event:     ResolveFlowEventProof(source, "worker", "work.completed"),
	}
	consumer := AuthoredEventEndpoint{ID: "exact", Direction: EventEndpointConsumer, FlowID: "worker", Event: ResolveFlowEventProof(source, "worker", "work.completed")}
	census := AuthoredEventEndpointCensus{source: source, consumers: []AuthoredEventEndpoint{consumer}}
	matches, issues := census.ResolveTypedPubSubConsumerMatches(producer)
	if len(issues) != 0 || len(matches) != 1 {
		t.Fatalf("matches = %#v issues = %#v, want one match", matches, issues)
	}
	if matches[0].Kind != TypedPubSubMatchExact || matches[0].Boundary != TypedPubSubBoundarySameFlow || matches[0].Authorization != nil {
		t.Fatalf("match = %#v, want exact/same_flow without import proof", matches[0])
	}
}

func TestAuthoredEventEndpointCensusClassifiesImportedPackageOwnPatternAsSameFlow(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-wildcard-deep-subscription"),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load wildcard fixture: %v", err)
	}
	worker := bundle.FlowTree.ByID["child/grandchild"]
	if worker == nil {
		t.Fatal("grandchild flow missing")
	}
	node := worker.Nodes["worker"]
	node.SubscribesTo = append(node.SubscribesTo, "task.*")
	node.EventHandlers["task.*"] = runtimecontracts.SystemNodeEventHandler{}
	worker.Nodes["worker"] = node
	bundle.Nodes["worker"] = node
	bundle.Semantics.NodeHandlers["worker"] = node.EventHandlers
	effective := bundle.Semantics.EffectiveNodes["worker"]
	effective.RuntimeSubscriptions = append(effective.RuntimeSubscriptions, "task.*")
	bundle.Semantics.EffectiveNodes["worker"] = effective

	census := BuildAuthoredEventEndpointCensus(Wrap(bundle))
	for _, producer := range census.Producers() {
		if producer.FlowID != "child/grandchild" || producer.Event.Canonical != "child/grandchild/task.done" {
			continue
		}
		matches, issues := census.ResolveTypedPubSubConsumerMatches(producer)
		for _, match := range matches {
			if match.Consumer.FlowID == "child/grandchild" && match.Consumer.Event.Authored == "task.*" {
				if len(issues) != 0 || match.Kind != TypedPubSubMatchPattern || match.Boundary != TypedPubSubBoundarySameFlow || match.Authorization != nil {
					t.Fatalf("match = %#v issues = %#v, want pattern/same_flow", match, issues)
				}
				return
			}
		}
		t.Fatalf("matches = %#v issues = %#v, want imported package's own pattern", matches, issues)
	}
	t.Fatal("grandchild task.done producer missing")
}

func TestAuthoredEventEndpointCensusTypedRelationAdmitsExactFlowTreeEquality(t *testing.T) {
	source := endpointCensusFixture(t, nil)
	producer := AuthoredEventEndpoint{ID: "producer", Direction: EventEndpointProducer, FlowID: "worker", Event: ResolveFlowEventProof(source, "worker", "work.completed")}
	consumer := AuthoredEventEndpoint{ID: "root-consumer", Direction: EventEndpointConsumer, FlowID: "", Event: producer.Event}
	census := AuthoredEventEndpointCensus{source: source, consumers: []AuthoredEventEndpoint{consumer}}

	matches, issues := census.ResolveTypedPubSubConsumerMatches(producer)
	if len(matches) != 1 || len(issues) != 0 || matches[0].Boundary != TypedPubSubBoundaryFlowTree || matches[0].Authorization != nil {
		t.Fatalf("cross-flow exact relation = matches %#v issues %#v, want one flow-tree edge without package authorization", matches, issues)
	}
}

func TestInvalidAuthoredSubscriptionsRejectConsumerRelativeDescendantIdentity(t *testing.T) {
	grandchild := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{FlowPath: "grandchild"},
		Path:   "child/grandchild",
		Events: map[string]runtimecontracts.EventCatalogEntry{"task.done": {}},
	}
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "child"},
		Path:  "child",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"listener": {ID: "listener", SubscribesTo: []string{"grandchild/task.done"}, EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"grandchild/task.done": {}}},
		},
		Children: []runtimecontracts.FlowContractView{grandchild},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}}
	bundle := &runtimecontracts.WorkflowContractBundle{FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &root,
		ByID: map[string]*runtimecontracts.FlowContractView{
			"child":      &root.Children[0],
			"grandchild": &root.Children[0].Children[0],
		},
	}}

	invalid := BuildAuthoredEventEndpointCensus(Wrap(bundle)).InvalidAuthoredSubscriptions()
	if len(invalid) != 1 {
		t.Fatalf("invalid subscriptions = %#v, want one child-relative qualified consumer", invalid)
	}
	got := invalid[0]
	if got.Consumer.NodeID != "listener" || got.Consumer.Event.Authored != "grandchild/task.done" || got.Admission.Failure() != AuthoredSubscriptionFailureQualifiedExact {
		t.Fatalf("invalid subscription = %#v, want child-relative listener rejected before resolution", got)
	}
}

func TestInvalidAuthoredSubscriptionsRejectAbsoluteSiblingIdentity(t *testing.T) {
	producer := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{FlowPath: "producer"},
		Path:   "producer",
		Events: map[string]runtimecontracts.EventCatalogEntry{"task.done": {}},
	}
	consumer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "consumer"},
		Path:  "consumer",
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"listener": {ID: "listener", SubscribesTo: []string{"producer/task.done"}, EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"producer/task.done": {}}},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, consumer}}
	bundle := &runtimecontracts.WorkflowContractBundle{FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &root,
		ByID: map[string]*runtimecontracts.FlowContractView{
			"producer": &root.Children[0],
			"consumer": &root.Children[1],
		},
	}}

	invalid := BuildAuthoredEventEndpointCensus(Wrap(bundle)).InvalidAuthoredSubscriptions()
	if len(invalid) != 1 {
		t.Fatalf("invalid subscriptions = %#v, want one absolute sibling consumer", invalid)
	}
	got := invalid[0]
	if got.Consumer.NodeID != "listener" || got.Consumer.Event.Authored != "producer/task.done" || got.Admission.Failure() != AuthoredSubscriptionFailureQualifiedExact {
		t.Fatalf("invalid subscription = %#v, want sibling listener rejected before resolution", got)
	}
}

func TestInvalidAuthoredSubscriptionsRejectFullURIWithoutFlowPathResolution(t *testing.T) {
	root := runtimecontracts.FlowContractView{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"listener": {
				ID:            "listener",
				SubscribesTo:  []string{"myapp://producer/task.done"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"myapp://producer/task.done": {}},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{Root: &root},
		Nodes:    root.Nodes,
	}

	invalid := BuildAuthoredEventEndpointCensus(Wrap(bundle)).InvalidAuthoredSubscriptions()
	if len(invalid) != 1 {
		t.Fatalf("invalid subscriptions = %#v, want one full-URI exact consumer", invalid)
	}
	got := invalid[0]
	if got.Consumer.NodeID != "listener" || got.Consumer.Event.Authored != "myapp://producer/task.done" || got.Admission.Failure() != AuthoredSubscriptionFailureQualifiedExact {
		t.Fatalf("invalid subscription = %#v, want unresolved full-URI listener fact", got)
	}
}

func TestEndpointCensusReusesBundleYAMLAndPreservesNodeAndAgentSourceLines(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixture := filepath.Join(repoRoot, "tests", "tier7-composition", "test-agent-emits-to-node")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		fixture,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	afterBundle := yamlsource.DefaultStats().ParseCount

	census := BuildAuthoredEventEndpointCensus(Wrap(bundle))
	if afterCensus := yamlsource.DefaultStats().ParseCount; afterCensus != afterBundle {
		t.Fatalf("census reparsed authoritative YAML: parse count %d -> %d", afterBundle, afterCensus)
	}
	assertEndpointSourceLine(t, census.Consumers(), EventEndpointNodeHandler, "complete-node", "", "task.completed", "nodes.yaml", 14)
	assertEndpointSourceLine(t, census.Consumers(), EventEndpointAgent, "", "test-agent", "task.assigned", "agents.yaml", 6)
	assertEndpointSourceLine(t, census.Producers(), EventEndpointAgent, "", "test-agent", "task.completed", "agents.yaml", 8)
}

func assertEndpointSourceLine(t *testing.T, endpoints []AuthoredEventEndpoint, kind EventEndpointKind, nodeID, agentID, eventType, sourceFile string, sourceLine int) {
	t.Helper()
	for _, endpoint := range endpoints {
		if endpoint.Kind == kind && endpoint.NodeID == nodeID && endpoint.AgentID == agentID && endpoint.Event.Authored == eventType {
			if endpoint.SourceFile != sourceFile || endpoint.SourceLine != sourceLine {
				t.Fatalf("endpoint source = %s:%d, want %s:%d", endpoint.SourceFile, endpoint.SourceLine, sourceFile, sourceLine)
			}
			return
		}
	}
	t.Fatalf("endpoint kind=%s node=%q agent=%q event=%q not found: %#v", kind, nodeID, agentID, eventType, endpoints)
}

func TestInvalidAuthoredSubscriptionsExcludeConnectedInputDelivery(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "examples", "routing", "parent-connect"),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load connected fixture: %v", err)
	}

	if invalid := BuildAuthoredEventEndpointCensus(Wrap(bundle)).InvalidAuthoredSubscriptions(); len(invalid) != 0 {
		t.Fatalf("connected input delivery misclassified as invalid authored subscription: %#v", invalid)
	}
}

func endpointCensusFixture(t testing.TB, inputPins []runtimecontracts.FlowInputEventPin) Source {
	t.Helper()
	return withCompiledTestPins(t, Wrap(endpointCensusBundle(inputPins)), map[string][]runtimecontracts.FlowInputEventPin{"worker": inputPins}, map[string][]runtimecontracts.FlowOutputEventPin{"worker": {{Event: "work.completed"}}})
}

func endpointCensusBundle(inputPins []runtimecontracts.FlowInputEventPin) *runtimecontracts.WorkflowContractBundle {
	node := runtimecontracts.SystemNodeContract{
		ID:               "worker-node",
		ProducesDeclared: true,
		Produces:         []string{},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"work.requested": {
				Emit: runtimecontracts.EmitSpec{Event: "work.completed"},
			},
		},
	}
	worker := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "worker"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs:  runtimecontracts.FlowInputPins{EventPins: inputPins},
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "work.completed"}}},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{"worker-node": node},
		Path:  "worker",
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{worker}}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{"worker": &root.Children[0]},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"worker": worker.Schema},
	}
}

func localWildcardEndpointCensusSource(authored string) Source {
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "child"},
		Path:  "child",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.done": {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"listener": {ID: "listener", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{authored: {}}},
		},
	}
	sibling := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{FlowPath: "sibling"},
		Path:   "sibling",
		Events: map[string]runtimecontracts.EventCatalogEntry{"task.done": {}},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child, sibling}}
	return Wrap(&runtimecontracts.WorkflowContractBundle{FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &root,
		ByID: map[string]*runtimecontracts.FlowContractView{
			"child":   &root.Children[0],
			"sibling": &root.Children[1],
		},
	}})
}

func endpointCountFromMatches(matches []TypedPubSubConsumerMatch, nodeID string) int {
	count := 0
	for _, match := range matches {
		if match.Consumer.NodeID == nodeID {
			count++
		}
	}
	return count
}

func endpointCountForNode(endpoints []AuthoredEventEndpoint, nodeID string) int {
	count := 0
	for _, endpoint := range endpoints {
		if endpoint.NodeID == nodeID {
			count++
		}
	}
	return count
}

func endpointCount(endpoints []AuthoredEventEndpoint, kind EventEndpointKind, flowID, eventType string) int {
	count := 0
	for _, endpoint := range endpoints {
		if endpoint.Kind == kind && endpoint.FlowID == flowID && (endpoint.Event.EventKey() == eventType || endpoint.Event.Local == eventType || endpoint.Event.Authored == eventType) {
			count++
		}
	}
	return count
}

func endpointKindSet(endpoints []AuthoredEventEndpoint) map[EventEndpointKind]bool {
	out := map[EventEndpointKind]bool{}
	for _, endpoint := range endpoints {
		out[endpoint.Kind] = true
	}
	return out
}
