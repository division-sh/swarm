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
		if candidate.Scope == DeliveryScopeInterFlow && candidate.Boundary != nil && candidate.Boundary.OutputPin == templatefanin.ProducerOutputPin {
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
	if edge.Resolution == nil || edge.Resolution.Mode != "fan-in" || edge.Resolution.FanIn == nil {
		t.Fatalf("resolution = %#v, want fan-in", edge.Resolution)
	}
	if edge.Resolution.FanIn.Window != "payload.period_id" || !reflect.DeepEqual(edge.Resolution.FanIn.DedupBy, []string{"payload.report_id"}) {
		t.Fatalf("fan-in = %#v, want window/dedup", edge.Resolution.FanIn)
	}
	if edge.RequiresRuntimeResolution {
		t.Fatal("singleton fan-in edge unexpectedly claims runtime recipient resolution")
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
			edge := firstInterFlowEdge(t, tc.load())
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
		Package: runtimecontracts.ProjectPackageDocument{Connect: []runtimecontracts.FlowPackageConnect{{From: "producer.ready", To: "consumer.ready"}}},
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowInputs:          map[string][]string{"consumer": {"work.ready"}},
			FlowOutputs:         map[string][]string{"producer": {"work.ready"}},
			FlowInputEventPins:  map[string][]runtimecontracts.FlowInputEventPin{"consumer": consumer.Schema.Pins.Inputs.EventPins},
			FlowOutputEventPins: map[string][]runtimecontracts.FlowOutputEventPin{"producer": producer.Schema.Pins.Outputs.EventPins},
			CompositionConnects: []runtimecontracts.FlowPackageConnect{{From: "producer.ready", To: "consumer.ready"}},
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
		if edge.Scope == DeliveryScopeInterFlow && edge.Resolution != nil && edge.Resolution.Reply != nil {
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
	for _, edge := range topology.Edges {
		if edge.Scope == DeliveryScopeInterFlow {
			t.Fatalf("invalid connect survived as executable edge: %#v", edge)
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
			if edge.Scope != DeliveryScopeIntraFlow {
				t.Fatalf("wildcard edge scope = %q, want typed pub/sub", edge.Scope)
			}
			return
		}
	}
	t.Fatalf("topology edges = %#v, want imported wildcard consumer", topology.Edges)
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
	for _, field := range []string{"producers", "consumers", "input_pins", "output_pins", "boundary_exposures", "edges", "legacy_qualified_subscriptions", "issues"} {
		if !strings.Contains(string(encoded), `"`+field+`":[]`) {
			t.Fatalf("empty topology field %s is not a stable array: %s", field, encoded)
		}
	}
}

func firstInterFlowEdge(t *testing.T, topology Topology) Edge {
	t.Helper()
	for _, edge := range topology.Edges {
		if edge.Scope == DeliveryScopeInterFlow {
			return edge
		}
	}
	t.Fatalf("topology has no inter-flow edge: %#v", topology)
	return Edge{}
}
