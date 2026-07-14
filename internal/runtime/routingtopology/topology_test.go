package routingtopology

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatefanin"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatereply"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateselectexisting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateselectorcreate"
)

func TestBuildProjectsFanInConnectWithCompleteResolution(t *testing.T) {
	topology := Build(templatefanin.LoadSource(t, templatefanin.Options{}))
	if topology.SchemaVersion != SchemaVersion || topology.SourceAuthority != SourceAuthority || !topology.ProjectionOnly {
		t.Fatalf("identity = %q/%q, want canonical artifact identity", topology.SchemaVersion, topology.SourceAuthority)
	}
	var edge *Edge
	for i := range topology.Edges {
		candidate := &topology.Edges[i]
		if candidate.Scope == DeliveryScopeInterFlowConnect && candidate.Boundary != nil && candidate.Boundary.OutputPin == templatefanin.ProducerOutputPin {
			edge = candidate
			break
		}
	}
	if edge == nil {
		t.Fatalf("edges = %#v, want inter-flow fan-in edge", topology.Edges)
	}
	if edge.Boundary.From != "operating.operating_reported" || edge.Boundary.To != "portfolio.operating_reported" {
		t.Fatalf("boundary = %#v, want authored endpoints", edge.Boundary)
	}
	if !strings.Contains(edge.Boundary.AuthoredLocation, "package.yaml:") {
		t.Fatalf("boundary source = %q, want exact package.yaml:line", edge.Boundary.AuthoredLocation)
	}
	if edge.Resolution == nil || edge.Resolution.Mode != "fan-in" || edge.Resolution.FanIn == nil {
		t.Fatalf("resolution = %#v, want fan-in", edge.Resolution)
	}
	if edge.Resolution.FanIn.Window != "payload.period_id" || !reflect.DeepEqual(edge.Resolution.FanIn.DedupBy, []string{"payload.operating_id"}) || edge.Resolution.FanIn.Singleton != "portfolio" {
		t.Fatalf("fan-in = %#v, want window/dedup", edge.Resolution.FanIn)
	}
	if edge.RequiresRuntimeResolution {
		t.Fatal("singleton fan-in edge unexpectedly claims runtime recipient resolution")
	}
}

func TestBuildKeepsTypedPubSubAndConnectProofShapesMutuallyExclusive(t *testing.T) {
	topology := Build(templatefanin.LoadSource(t, templatefanin.Options{}))
	seen := map[DeliveryScope]bool{}
	for _, edge := range topology.Edges {
		seen[edge.Scope] = true
		switch edge.Scope {
		case DeliveryScopeTypedPubSub:
			if edge.TypedPubSub == nil || edge.Boundary != nil {
				t.Fatalf("typed pub/sub edge has mixed proof shape: %#v", edge)
			}
		case DeliveryScopeInterFlowConnect:
			if edge.TypedPubSub != nil || edge.Boundary == nil {
				t.Fatalf("connect edge has mixed proof shape: %#v", edge)
			}
		default:
			t.Fatalf("unknown delivery scope %q", edge.Scope)
		}
	}
	if !seen[DeliveryScopeTypedPubSub] || !seen[DeliveryScopeInterFlowConnect] {
		t.Fatalf("scopes = %#v, want both canonical scopes", seen)
	}
}

func TestBuildProjectsSelectAndSelectOrCreateModes(t *testing.T) {
	tests := []struct {
		name string
		mode string
		load func() Topology
	}{
		{name: "select", mode: runtimecontracts.FlowInputResolutionModeSelect, load: func() Topology { return Build(templateselectexisting.LoadSource(t)) }},
		{name: "select-or-create", mode: runtimecontracts.FlowInputResolutionModeSelectOrCreate, load: func() Topology { return Build(templateselectorcreate.LoadSource(t)) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var edge Edge
			found := false
			for _, candidate := range tc.load().Edges {
				if candidate.Scope == DeliveryScopeInterFlowConnect && candidate.Resolution != nil && candidate.Resolution.Mode == tc.mode {
					edge = candidate
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("topology has no inter-flow %s edge", tc.mode)
			}
			if edge.Resolution == nil || edge.Resolution.Mode != tc.mode || edge.Resolution.InstanceKey == nil {
				t.Fatalf("resolution = %#v, want %s instance key", edge.Resolution, tc.mode)
			}
			if !reflect.DeepEqual(edge.Resolution.InstanceKey.Fields, []string{"account_id"}) || len(edge.Resolution.InstanceKey.Mappings) != 1 {
				t.Fatalf("instance key = %#v, want carried account_id", edge.Resolution.InstanceKey)
			}
		})
	}
}

func TestBuildProjectsRunnableStaticConnect(t *testing.T) {
	connect := runtimecontracts.FlowPackageConnect{SourceFile: "package.yaml", SourceLine: 1, From: "producer.ready", To: "consumer.ready"}
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: "static", Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{
			Events: []string{"work.ready"}, EventPins: []runtimecontracts.FlowOutputEventPin{{Name: "ready", Event: "work.ready"}},
		}}},
		Path: "producer",
	}
	consumer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "consumer", Flow: "consumer"},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: "static", Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{
			Events: []string{"work.ready"}, EventPins: []runtimecontracts.FlowInputEventPin{{Name: "ready", Event: "work.ready"}},
		}}},
		Path: "consumer",
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, consumer}}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Package: runtimecontracts.ProjectPackageDocument{Connect: []runtimecontracts.FlowPackageConnect{connect}},
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowInputs:          map[string][]string{"consumer": {"work.ready"}},
			FlowOutputs:         map[string][]string{"producer": {"work.ready"}},
			FlowInputEventPins:  map[string][]runtimecontracts.FlowInputEventPin{"consumer": consumer.Schema.Pins.Inputs.EventPins},
			FlowOutputEventPins: map[string][]runtimecontracts.FlowOutputEventPin{"producer": producer.Schema.Pins.Outputs.EventPins},
			CompositionConnects: []runtimecontracts.FlowPackageConnect{connect},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"producer": producer.Schema, "consumer": consumer.Schema},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{"producer": &root.Children[0], "consumer": &root.Children[1]},
		},
	})
	edge := firstInterFlowEdge(t, Build(source))
	if edge.Resolution == nil || edge.Resolution.Mode != string(pinrouting.ConnectResolutionStatic) || edge.RequiresRuntimeResolution {
		t.Fatalf("static route = %#v, want complete runnable static resolution", edge)
	}
}

func TestBuildProjectsReplyAsPairedEdges(t *testing.T) {
	topology := Build(templatereply.LoadSource(t, templatereply.Options{}))
	roles := map[string]bool{}
	for _, edge := range topology.Edges {
		if edge.Scope == DeliveryScopeInterFlowConnect && edge.Resolution != nil && edge.Resolution.Reply != nil {
			roles[edge.Resolution.Reply.Role] = true
		}
	}
	if !roles[pinrouting.ConnectReplyRoleRequest] || !roles[pinrouting.ConnectReplyRoleResponse] {
		t.Fatalf("reply roles = %#v, want paired request/response edges", roles)
	}
}

func TestResolutionViewPreservesCreateStaticAddressAndBroadcastDeclarations(t *testing.T) {
	tests := []struct {
		name string
		plan pinrouting.ConnectRoutePlan
		want string
	}{
		{name: "create", want: runtimecontracts.FlowInputResolutionModeCreate, plan: pinrouting.ConnectRoutePlan{
			ResolutionKind: pinrouting.ConnectResolutionInstanceKey,
			InstanceKey:    &pinrouting.ConnectRoutePlanInstanceKey{Mode: runtimecontracts.FlowInputResolutionModeCreate, Mint: runtimecontracts.FlowInputResolutionMintUUID, As: "account_id", OnMissing: "create", OnConflict: "reject"},
		}},
		{name: "static", want: string(pinrouting.ConnectResolutionStatic), plan: pinrouting.ConnectRoutePlan{ResolutionKind: pinrouting.ConnectResolutionStatic}},
		{name: "address", want: string(pinrouting.ConnectResolutionAddress), plan: pinrouting.ConnectRoutePlan{ResolutionKind: pinrouting.ConnectResolutionAddress, Address: &pinrouting.ConnectRoutePlanAddress{By: "key", Source: "payload.id", Target: "entity.id", Cardinality: "one", Mode: "select"}}},
		{name: "broadcast", want: string(pinrouting.ConnectResolutionBroadcast), plan: pinrouting.ConnectRoutePlan{ResolutionKind: pinrouting.ConnectResolutionBroadcast, Delivery: pinrouting.ConnectDeliveryBroadcast, TargetKind: pinrouting.ConnectTargetKindTargetSet}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resolution := resolutionView(tc.plan)
			if resolution == nil || resolution.Mode != tc.want {
				t.Fatalf("resolution = %#v, want mode %s", resolution, tc.want)
			}
			if tc.name == "create" && (resolution.InstanceKey == nil || resolution.InstanceKey.Mint != "uuid" || resolution.InstanceKey.As != "account_id") {
				t.Fatalf("create resolution = %#v, want mint/as", resolution)
			}
		})
	}
}

func TestBuildKeepsInvalidConnectAsIssueOnly(t *testing.T) {
	topology := Build(templatefanin.LoadSource(t, templatefanin.Options{MissingWindow: true}))
	if len(topology.Issues) != 1 || topology.Issues[0].Failure != "route_plan_instance_resolution_invalid" {
		t.Fatalf("issues = %#v, want invalid fan-in route-plan issue", topology.Issues)
	}
	if len(topology.Issues[0].ID) != len("issue-")+16 {
		t.Fatalf("issue id = %q, want stable public issue digest", topology.Issues[0].ID)
	}
	if !strings.Contains(topology.Issues[0].AuthoredLocation, "package.yaml:") {
		t.Fatalf("issue source = %q, want exact package.yaml:line", topology.Issues[0].AuthoredLocation)
	}
	for _, edge := range topology.Edges {
		if edge.Scope == DeliveryScopeInterFlowConnect && edge.Boundary != nil && edge.Boundary.From == "operating.operating_reported" {
			t.Fatalf("invalid connect survived as executable edge: %#v", edge)
		}
	}
}

func TestBuildDoesNotReconstructMissingConnectSourceFromBundlePaths(t *testing.T) {
	source := templatefanin.LoadSource(t, templatefanin.Options{})
	bundle, ok := semanticview.Bundle(source)
	if !ok || len(bundle.Semantics.CompositionConnects) == 0 {
		t.Fatalf("source bundle/connects unavailable")
	}
	found := false
	for idx := range bundle.Semantics.CompositionConnects {
		if bundle.Semantics.CompositionConnects[idx].From != "operating.operating_reported" {
			continue
		}
		bundle.Semantics.CompositionConnects[idx].SourceFile = ""
		bundle.Semantics.CompositionConnects[idx].SourceLine = 0
		found = true
	}
	if !found {
		t.Fatal("canonical fan-in connect unavailable")
	}

	topology := Build(source)
	if len(topology.Issues) != 1 || topology.Issues[0].Failure != string(pinrouting.ConnectFailureSourceLocationMissing) || topology.Issues[0].AuthoredLocation != "" {
		t.Fatalf("issues = %#v, want source-location issue without renderer fallback", topology.Issues)
	}
	for _, edge := range topology.Edges {
		if edge.Scope == DeliveryScopeInterFlowConnect && edge.Boundary != nil && edge.Boundary.From == "operating.operating_reported" {
			t.Fatalf("connect without source proof survived as edge: %#v", edge)
		}
	}
}

func TestBuildDoesNotInventDeliveryEdgesForMetadataSources(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker": {ID: "worker", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"work.requested": {Emit: runtimecontracts.EmitSpec{Event: "external.received"}},
			}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"external.received": {Swarm: runtimecontracts.EventSwarmMetadata{Source: "external", Consumer: []string{"dashboard"}}},
		},
	})
	topology := Build(source)
	if len(topology.Producers) < 2 || len(topology.Consumers) < 2 {
		t.Fatalf("metadata endpoints missing from census: producers=%#v consumers=%#v", topology.Producers, topology.Consumers)
	}
	for _, edge := range topology.Edges {
		if edge.Producer.Kind == "external" || edge.Producer.Kind == "platform" || edge.Consumer.Kind == "external" || edge.Consumer.Kind == "platform" {
			t.Fatalf("metadata endpoint became executable route: %#v", edge)
		}
	}
}

func TestBuildProjectsLegacyQualifiedSubscriptionAsNonCanonicalDebt(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-child-flow-absolute-path"),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load legacy fixture: %v", err)
	}
	topology := Build(semanticview.Wrap(bundle))
	if len(topology.LegacyQualifiedSubscriptions) != 1 {
		t.Fatalf("legacy subscriptions = %#v, want one", topology.LegacyQualifiedSubscriptions)
	}
	legacy := topology.LegacyQualifiedSubscriptions[0]
	if legacy.Disposition != LegacyQualifiedSubscriptionDisposition || !legacy.RuntimeDelivery || legacy.CanonicalEdge || legacy.AuthoredLocation == "" {
		t.Fatalf("legacy disposition = %#v, want visible runtime debt outside canonical edges", legacy)
	}
	for _, edge := range topology.Edges {
		if edge.Consumer.ID == legacy.Consumer.ID {
			t.Fatalf("legacy qualified consumer leaked into canonical edges: %#v", edge)
		}
	}
}

func TestLegacyQualifiedSubscriptionInventoryMatchesE1aRetirementTracker(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	tests := []struct {
		fixture  string
		event    string
		consumer string
		location string
	}{
		{fixture: "test-child-flow-absolute-path", event: "child/task.done", consumer: "listener", location: "nodes.yaml:13"},
		{fixture: "test-child-flow-pin-wiring", event: "child/work.completed", consumer: "parent-listener", location: "nodes.yaml:13"},
		{fixture: "test-data-pin-wiring", event: "processor/process.done", consumer: "result-listener", location: "nodes.yaml:20"},
		{fixture: "test-gates-in-child-flow", event: "child/validation.done", consumer: "router", location: "nodes.yaml:5"},
		{fixture: "test-nested-three-levels", event: "child/grandchild/micro.done", consumer: "root-collector", location: "nodes.yaml:13"},
		{fixture: "test-tool-override", event: "child/child.done", consumer: "root-node", location: "nodes.yaml:4"},
	}
	for _, tc := range tests {
		t.Run(tc.fixture, func(t *testing.T) {
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, filepath.Join(repoRoot, "tests", "tier11-flow-composition", tc.fixture), runtimecontracts.DefaultPlatformSpecFile(repoRoot))
			if err != nil {
				t.Fatalf("load fixture: %v", err)
			}
			legacy := Build(semanticview.Wrap(bundle)).LegacyQualifiedSubscriptions
			if len(legacy) != 1 || legacy[0].Event.Canonical != tc.event || legacy[0].Consumer.NodeID != tc.consumer || !strings.HasSuffix(legacy[0].AuthoredLocation, tc.location) {
				t.Fatalf("legacy inventory = %#v, want %s/%s/%s", legacy, tc.event, tc.consumer, tc.location)
			}
		})
	}

	t.Run("tier8 boot missing pin", func(t *testing.T) {
		bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, filepath.Join(repoRoot, "tests", "tier8-boot-verification", "test-boot-missing-pin"), runtimecontracts.DefaultPlatformSpecFile(repoRoot))
		if err != nil {
			t.Fatalf("load fixture: %v", err)
		}
		legacy := Build(semanticview.Wrap(bundle)).LegacyQualifiedSubscriptions
		if len(legacy) != 1 || legacy[0].Event.Canonical != "child/task.result" || legacy[0].Consumer.NodeID != "dispatcher" || !strings.HasSuffix(legacy[0].AuthoredLocation, "nodes.yaml:4") {
			t.Fatalf("legacy inventory = %#v, want child/task.result dispatcher nodes.yaml:4", legacy)
		}
	})
}

func TestBuildProjectsImportedWildcardConsumerAsTypedPubSub(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-wildcard-deep-subscription"),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load wildcard fixture: %v", err)
	}

	topology := Build(semanticview.Wrap(bundle))
	for _, edge := range topology.Edges {
		if edge.Event.Canonical == "child/grandchild/task.done" && edge.Consumer.NodeID == "collector" && edge.Consumer.Pattern {
			if edge.Scope != DeliveryScopeTypedPubSub {
				t.Fatalf("wildcard edge scope = %q, want typed pub/sub", edge.Scope)
			}
			if edge.TypedPubSub == nil || edge.TypedPubSub.Match != "pattern" || edge.TypedPubSub.Boundary != "import_boundary" || edge.TypedPubSub.Authorization == nil {
				t.Fatalf("wildcard edge proof = %#v, want pattern/import_boundary authorization", edge.TypedPubSub)
			}
			authorization := edge.TypedPubSub.Authorization
			if authorization.ParentPackageKey != "." || authorization.ChildPackageKey != "flows/child" || authorization.EventPattern == "" || authorization.MatchPattern != "**/task.done" || authorization.RouteSource == "" {
				t.Fatalf("wildcard authorization = %#v, want stable parent/child/pattern/source proof", authorization)
			}
			return
		}
	}
	t.Fatalf("topology edges = %#v, want imported wildcard consumer", topology.Edges)
}

func TestIssueViewsProjectsTypedPubSubAmbiguityWithoutEdgeAuthority(t *testing.T) {
	issues := issueViews(nil, []semanticview.TypedPubSubConsumerIssue{{
		Failure:  semanticview.TypedPubSubFailureAuthorizationAmbiguous,
		Producer: semanticview.AuthoredEventEndpoint{ID: "producer"},
		Consumer: semanticview.AuthoredEventEndpoint{ID: "consumer"},
		Authorizations: []semanticview.TypedPubSubAuthorizationProof{
			{ChildPackageKey: "flows/consumer", ImportLabel: "first", EventPattern: "producer/task.done", MatchPattern: "**/task.done", LocalizedEvent: "task.done", RouteSource: "import_boundary_wildcard_grant"},
			{ChildPackageKey: "flows/consumer", ImportLabel: "second", EventPattern: "producer/task.done", MatchPattern: "**/task.done", LocalizedEvent: "task.done", RouteSource: "import_boundary_wildcard_grant"},
		},
	}})
	if len(issues) != 1 || issues[0].Failure != semanticview.TypedPubSubFailureAuthorizationAmbiguous || issues[0].From != "producer" || issues[0].To != "consumer" {
		t.Fatalf("issues = %#v, want one typed ambiguity projection", issues)
	}
}

func TestEdgeIDIncludesTypedPubSubAuthorizationProof(t *testing.T) {
	base := Edge{
		Scope:    DeliveryScopeTypedPubSub,
		Event:    EventIdentity{Canonical: "producer/task.done"},
		Producer: Endpoint{ID: "producer"},
		Consumer: Endpoint{ID: "consumer"},
		TypedPubSub: &TypedPubSub{
			Match:    "pattern",
			Boundary: "import_boundary",
			Authorization: &TypedPubSubAuthorizationProof{
				ChildPackageKey: "flows/consumer", ImportLabel: "first", EventPattern: "producer/task.done", MatchPattern: "**/task.done", LocalizedEvent: "task.done", RouteSource: "import_boundary_wildcard_grant",
			},
		},
	}
	changed := base
	changed.TypedPubSub = &TypedPubSub{Match: base.TypedPubSub.Match, Boundary: base.TypedPubSub.Boundary, Authorization: &TypedPubSubAuthorizationProof{
		ChildPackageKey: "flows/consumer", ImportLabel: "second", EventPattern: "producer/task.done", MatchPattern: "**/task.done", LocalizedEvent: "task.done", RouteSource: "import_boundary_wildcard_grant",
	}}
	if edgeID(base) == edgeID(changed) {
		t.Fatal("edge identity hid a distinct import authorization proof")
	}
}

func TestWithIssuesLinksLegacySubscriptionOnlyToStrongestDedicatedFinding(t *testing.T) {
	topology := Topology{LegacyQualifiedSubscriptions: []LegacyQualifiedSubscription{{
		ID: "legacy", AuthoredLocation: "nodes.yaml:13", Event: EventIdentity{Canonical: "child/task.done"},
	}}}
	warning := NewDiagnosticIssue("legacy_qualified_subscription", "semantic_drift_warning", "nodes.yaml:13", "warning", "migrate", "nodes.yaml:13")
	hard := NewDiagnosticIssue("legacy_qualified_subscription", "hard_invalidity", "nodes.yaml:13", "hard", "migrate", "nodes.yaml:13")
	unrelated := NewDiagnosticIssue("event_consumer_exists", "hard_invalidity", "child/task.done", "unrelated", "migrate", "nodes.yaml:13")

	got := WithIssues(topology, warning, unrelated, hard)
	if got.LegacyQualifiedSubscriptions[0].FindingID != hard.ID {
		t.Fatalf("finding id = %q, want strongest dedicated finding %q", got.LegacyQualifiedSubscriptions[0].FindingID, hard.ID)
	}
}

func TestBuildIsDeterministic(t *testing.T) {
	source := templatefanin.LoadSource(t, templatefanin.Options{})
	first, err := json.Marshal(Build(source))
	if err != nil {
		t.Fatalf("marshal first topology: %v", err)
	}
	second, err := json.Marshal(Build(source))
	if err != nil {
		t.Fatalf("marshal second topology: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("topology is nondeterministic:\n%s\n%s", first, second)
	}
	for _, edge := range Build(source).Edges {
		if len(edge.ID) != len("route-")+16 || edge.ID[:len("route-")] != "route-" {
			t.Fatalf("public route id = %q, want stable readable route digest", edge.ID)
		}
	}
}

func TestBuildEmptyTopologyUsesStableEmptyCollections(t *testing.T) {
	topology := Build(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	encoded, err := json.Marshal(topology)
	if err != nil {
		t.Fatalf("marshal empty topology: %v", err)
	}
	for _, field := range []string{"producers", "consumers", "input_pins", "output_pins", "root_input_sources", "boundary_exposures", "edges", "legacy_qualified_subscriptions", "issues"} {
		if !strings.Contains(string(encoded), `"`+field+`":[]`) {
			t.Fatalf("empty topology field %s is not a stable array: %s", field, encoded)
		}
	}
}

func firstInterFlowEdge(t *testing.T, topology Topology) Edge {
	t.Helper()
	for _, edge := range topology.Edges {
		if edge.Scope == DeliveryScopeInterFlowConnect {
			return edge
		}
	}
	t.Fatalf("topology has no inter-flow edge: %#v", topology)
	return Edge{}
}
