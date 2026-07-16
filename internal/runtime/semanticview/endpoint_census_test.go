package semanticview

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestAuthoredEventEndpointCensusEnumeratesExecutableFactsAndAssertions(t *testing.T) {
	source := endpointCensusFixture([]runtimecontracts.FlowInputEventPin{{
		Name:  "work",
		Event: "work.requested",
		Resolution: runtimecontracts.FlowInputPinResolution{
			Mode:        "fan-in",
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

func TestAuthoredEventEndpointCensusIncludesCompiledHandlersOutsideEffectiveSubscriptions(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {ID: "worker"},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"worker": {"work.requested": {}},
			},
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				"worker": {ID: "worker"},
			},
		},
	}

	census := BuildAuthoredEventEndpointCensus(Wrap(bundle))
	if got := endpointCount(census.Consumers(), EventEndpointNodeHandler, "", "work.requested"); got != 1 {
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
				Inputs:  runtimecontracts.FlowInputPins{Events: []string{"flow.started"}},
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"flow.completed"}},
			},
			RequiredAgents: []runtimecontracts.FlowRequiredAgent{{Role: "reviewer", SubscribesTo: []string{"review.requested"}, Emits: []string{"review.completed"}}},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {ID: "worker", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.requested": {Emit: runtimecontracts.EmitSpec{Event: "work.completed"}}}},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"analyst": {ID: "analyst", Role: "analyst", Subscriptions: []string{"analysis.requested"}, EmitEvents: []string{"analysis.completed"}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"external.received": {Swarm: runtimecontracts.EventSwarmMetadata{Source: "external", Consumer: []string{"dashboard"}}},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Timers: []runtimecontracts.WorkflowTimerContract{{ID: "reminder", Event: "timer.fired", StartOn: "event:timer.started"}},
		},
	}
	census := BuildAuthoredEventEndpointCensus(Wrap(bundle))
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
	source := endpointCensusFixture([]runtimecontracts.FlowInputEventPin{{
		Name:  "work",
		Event: "work.requested",
	}})
	census := BuildAuthoredEventEndpointCensus(source)

	for _, identity := range []string{"work", "work.requested"} {
		result := census.ResolveDeclaredInputEndpoint("worker", identity)
		endpoint, ok := result.Endpoint()
		if !ok || endpoint.PinName != "work" {
			t.Fatalf("identity %q result = %#v, want work input", identity, result)
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

func TestResolveDeclaredInputEndpointRejectsAmbiguousIdentity(t *testing.T) {
	source := endpointCensusFixture([]runtimecontracts.FlowInputEventPin{
		{Name: "work-primary", Event: "work.requested"},
		{Name: "work-secondary", Event: "work.requested"},
	})

	result := BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint("worker", "work.requested")
	if result.Status != EndpointAssociationAmbiguous || len(result.Candidates) != 2 {
		t.Fatalf("result = %#v, want two-candidate ambiguity", result)
	}
}

func TestResolveFanInInputForHandlerSupportsEventAndPinNameAndRejectsAmbiguity(t *testing.T) {
	source := endpointCensusFixture([]runtimecontracts.FlowInputEventPin{{
		Name:       "work",
		Event:      "work.requested",
		Resolution: runtimecontracts.FlowInputPinResolution{Mode: "fan-in"},
	}})
	census := BuildAuthoredEventEndpointCensus(source)
	for _, identity := range []string{"work.requested", "work"} {
		result := census.ResolveFanInInputForHandler("worker", "worker-node", identity)
		endpoint, ok := result.Endpoint()
		if !ok || endpoint.PinName != "work" {
			t.Fatalf("handler identity %q result = %#v, want work input", identity, result)
		}
	}

	ambiguousSource := endpointCensusFixture([]runtimecontracts.FlowInputEventPin{
		{Name: "work-a", Event: "work.requested", Resolution: runtimecontracts.FlowInputPinResolution{Mode: "fan-in"}},
		{Name: "work-b", Event: "work.requested", Resolution: runtimecontracts.FlowInputPinResolution{Mode: "fan-in"}},
	})
	ambiguous := BuildAuthoredEventEndpointCensus(ambiguousSource).ResolveFanInInputForHandler("worker", "worker-node", "work.requested")
	if ambiguous.Status != EndpointAssociationAmbiguous || len(ambiguous.Candidates) != 2 {
		t.Fatalf("ambiguous result = %#v, want two candidates", ambiguous)
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

func TestAuthoredEventEndpointCensusResolvesImportedWildcardAsTypedRelation(t *testing.T) {
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
				if match.Kind != TypedPubSubMatchPattern || match.Boundary != TypedPubSubBoundaryImportBoundary || match.Authorization == nil {
					t.Fatalf("typed match = %#v, want authorized import-boundary pattern", match)
				}
				return
			}
		}
		var collector AuthoredEventEndpoint
		for _, consumer := range census.Consumers() {
			if consumer.NodeID == "collector" {
				collector = consumer
				break
			}
		}
		resolution := ResolveImportBoundaryWildcardSubscriptionForRelation(source, collector.PackageKey, collector.FlowID, "", nil, collector.Event.Authored)
		t.Fatalf("typed matches = %#v issues = %#v collector = %#v resolution = %#v", matches, issues, collector, resolution)
	}
	t.Fatal("task.done producer not found")
}

func TestAuthoredEventEndpointCensusTypedRelationClassifiesSameFlowExactlyOnce(t *testing.T) {
	source := endpointCensusFixture(nil)
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
	worker := bundle.FlowTree.ByID["grandchild"]
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
		if producer.FlowID != "grandchild" || producer.Event.Canonical != "child/grandchild/task.done" {
			continue
		}
		matches, issues := census.ResolveTypedPubSubConsumerMatches(producer)
		for _, match := range matches {
			if match.Consumer.FlowID == "grandchild" && match.Consumer.Event.Authored == "task.*" {
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

func TestAuthoredEventEndpointCensusTypedRelationRejectsCrossFlowExactEquality(t *testing.T) {
	source := endpointCensusFixture(nil)
	producer := AuthoredEventEndpoint{ID: "producer", Direction: EventEndpointProducer, FlowID: "worker", Event: ResolveFlowEventProof(source, "worker", "work.completed")}
	consumer := AuthoredEventEndpoint{ID: "root-consumer", Direction: EventEndpointConsumer, FlowID: "", Event: producer.Event}
	census := AuthoredEventEndpointCensus{source: source, consumers: []AuthoredEventEndpoint{consumer}}

	matches, issues := census.ResolveTypedPubSubConsumerMatches(producer)
	if len(matches) != 0 || len(issues) != 0 {
		t.Fatalf("cross-flow exact relation = matches %#v issues %#v, want no authorization and no edge", matches, issues)
	}
}

func TestTypedPubSubCrossFlowRelationDeduplicatesProofAndFailsClosedOnAmbiguity(t *testing.T) {
	producer := AuthoredEventEndpoint{ID: "producer", FlowID: "producer"}
	consumer := AuthoredEventEndpoint{ID: "consumer", FlowID: "consumer", Pattern: true}
	proof := FlowEventProof{FlowID: "producer", Canonical: "producer/task.done"}
	first := TypedPubSubAuthorizationProof{ParentPackageKey: ".", ChildPackageKey: "flows/consumer", ImportLabel: "consumer", Source: "producer", EventPattern: "producer/task.done", MatchPattern: "**/task.done", LocalizedEvent: "task.done", RouteSource: "import_boundary_wildcard_grant"}
	duplicate := first
	second := first
	second.ImportLabel = "consumer-shadow"

	deduplicated := matchingTypedPubSubAuthorizations([]ImportBoundaryWildcardPattern{
		{ParentPackageKey: first.ParentPackageKey, ChildPackageKey: first.ChildPackageKey, ImportLabel: first.ImportLabel, Source: first.Source, EventPattern: first.EventPattern, MatchPattern: first.MatchPattern, LocalizedEvent: first.LocalizedEvent, RouteSource: first.RouteSource},
		{ParentPackageKey: duplicate.ParentPackageKey, ChildPackageKey: duplicate.ChildPackageKey, ImportLabel: duplicate.ImportLabel, Source: duplicate.Source, EventPattern: duplicate.EventPattern, MatchPattern: duplicate.MatchPattern, LocalizedEvent: duplicate.LocalizedEvent, RouteSource: duplicate.RouteSource},
	}, proof)
	match, issue := resolveTypedPubSubCrossFlowRelation(producer, consumer, proof, deduplicated)
	if match == nil || issue != nil || match.AuthorizationProof != first.Identity() {
		t.Fatalf("duplicate proof relation = match %#v issue %#v, want one deterministic edge", match, issue)
	}

	match, issue = resolveTypedPubSubCrossFlowRelation(producer, consumer, proof, []TypedPubSubAuthorizationProof{first, second})
	if match != nil || issue == nil || issue.Failure != TypedPubSubFailureAuthorizationAmbiguous || len(issue.Authorizations) != 2 {
		t.Fatalf("distinct proof relation = match %#v issue %#v, want ambiguity and no edge", match, issue)
	}
}

func TestLegacyQualifiedSubscriptionsResolveConsumerRelativeDescendantIdentity(t *testing.T) {
	grandchild := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild", PackageKey: "flows/child/flows/grandchild"},
		Path:   "child/grandchild",
		Events: map[string]runtimecontracts.EventCatalogEntry{"task.done": {}},
	}
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child", PackageKey: "flows/child"},
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

	legacy := BuildAuthoredEventEndpointCensus(Wrap(bundle)).LegacyQualifiedSubscriptions()
	if len(legacy) != 1 {
		t.Fatalf("retired subscriptions = %#v, want one child-relative qualified consumer", legacy)
	}
	got := legacy[0]
	if got.Consumer.NodeID != "listener" || got.Consumer.Event.Authored != "grandchild/task.done" || got.TargetFlowID != "grandchild" || got.Event.Canonical != "child/grandchild/task.done" {
		t.Fatalf("retired subscription = %#v, want child-relative listener targeting grandchild", got)
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
	assertEndpointSourceLine(t, census.Consumers(), EventEndpointNodeHandler, "complete-node", "", "task.completed", filepath.Join(fixture, "nodes.yaml"), 14)
	assertEndpointSourceLine(t, census.Consumers(), EventEndpointAgent, "", "test-agent", "task.assigned", filepath.Join(fixture, "agents.yaml"), 5)
	assertEndpointSourceLine(t, census.Producers(), EventEndpointAgent, "", "test-agent", "task.completed", filepath.Join(fixture, "agents.yaml"), 7)
}

type endpointSourceFiles struct {
	Source
	nodes map[string]runtimecontracts.ContractItemSource
}

func (s endpointSourceFiles) NodeContractSource(nodeID string) (runtimecontracts.ContractItemSource, bool) {
	source, ok := s.nodes[nodeID]
	return source, ok
}

func TestEndpointCensusAcquiresUnprimedSourceThroughCanonicalOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes.yaml")
	body := fmt.Sprintf("# unique source: %s\nworker-node:\n  event_handlers:\n    work.requested: {}\n", path)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	source := endpointSourceFiles{
		Source: endpointCensusFixture(nil),
		nodes: map[string]runtimecontracts.ContractItemSource{
			"worker-node": {File: path},
		},
	}
	before := yamlsource.DefaultStats()
	census := BuildAuthoredEventEndpointCensus(source)
	after := yamlsource.DefaultStats()
	if delta := after.ParseCount - before.ParseCount; delta != 1 {
		t.Fatalf("endpoint census canonical-owner parse delta = %d, want 1", delta)
	}
	assertEndpointSourceLine(t, census.Consumers(), EventEndpointNodeHandler, "worker-node", "", "work.requested", path, 4)
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

func TestLegacyQualifiedSubscriptionsExcludeConnectedInputDelivery(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "examples", "routing", "parent-connect"),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load connected fixture: %v", err)
	}

	if legacy := BuildAuthoredEventEndpointCensus(Wrap(bundle)).LegacyQualifiedSubscriptions(); len(legacy) != 0 {
		t.Fatalf("connected input delivery misclassified as legacy direct delivery: %#v", legacy)
	}
}

func endpointCensusFixture(inputPins []runtimecontracts.FlowInputEventPin) Source {
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
	inputEvents := make([]string, 0, len(inputPins))
	for _, pin := range inputPins {
		inputEvents = append(inputEvents, pin.EventType())
	}
	worker := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "worker", Flow: "worker", PackageKey: "flows/worker"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs:  runtimecontracts.FlowInputPins{Events: inputEvents, EventPins: inputPins},
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"work.completed"}},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{"worker-node": node},
		Path:  "worker",
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{worker}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{"worker": &root.Children[0]},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"worker": worker.Schema},
	}
	return Wrap(bundle)
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
