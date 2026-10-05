package routingtopology

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestBuildProjectsCompiledParentLocalConsumers(t *testing.T) {
	for _, requester := range []bool{false, true} {
		for _, reply := range []bool{false, true} {
			for _, nested := range []bool{false, true} {
				for _, agent := range []bool{false, true} {
					t.Run(fmt.Sprintf("requester=%t/reply=%t/nested=%t/agent=%t", requester, reply, nested, agent), func(t *testing.T) {
						root, owner := canonicalrouting.CopyRootReplyConsumerBoundary(t, requester, reply, nested, agent)
						source := loadTopologySource(t, root)
						plan := parentLocalConsumerPlan(t, source, requester, false)
						topology := Build(source)
						edges := localConnectionEdges(t, topology, plan, 1)
						kind := semanticview.EventEndpointNodeHandler
						if agent {
							kind = semanticview.EventEndpointAgent
						}
						if edges[0].Consumer.Kind != kind {
							t.Fatalf("local consumer = %+v, want %s", edges[0].Consumer, kind)
						}
						assertStableTopology(t, source, topology)
						restore := canonicalrouting.RemoveRootReplyConsumer(t, owner, requester, agent)
						invalid := loadTopologySource(t, root)
						if len(pinrouting.CompileConnectGraph(invalid).Issues()) == 0 {
							t.Fatal("removing the actual local consumer preserved compilation")
						}
						for _, edge := range Build(invalid).Edges {
							if edge.Boundary != nil && edge.Boundary.From == connectEndpointRef(plan.SourceEndpoint()) && edge.Boundary.To == connectEndpointRef(plan.ReceiverEndpoint()) {
								t.Fatalf("refused connection acquired a projected edge: %+v", edge)
							}
						}
						restore()
						localConnectionEdges(t, Build(loadTopologySource(t, root)), plan, 1)
					})
				}
			}
		}
	}
}

func TestBuildProjectsRenamedParentLocalReplies(t *testing.T) {
	for _, requester := range []bool{false, true} {
		for _, nested := range []bool{false, true} {
			t.Run(fmt.Sprintf("requester=%t/nested=%t", requester, nested), func(t *testing.T) {
				root, owner := canonicalrouting.CopyRootReplyConsumerBoundary(t, requester, true, nested, false)
				canonicalrouting.RenameRootReplyEndpoints(t, owner, requester)
				source := loadTopologySource(t, root)
				plan := parentLocalConsumerPlan(t, source, requester, true)
				edges := localConnectionEdges(t, Build(source), plan, 1)
				if edges[0].Event.Local == edges[0].Consumer.Event.Local {
					t.Fatal("renamed receiver was projected using the source event")
				}
			})
		}
	}
}

func TestBuildPreservesParentLocalConsumerMultiplicity(t *testing.T) {
	for _, requester := range []bool{false, true} {
		t.Run(fmt.Sprint(requester), func(t *testing.T) {
			root, owner := canonicalrouting.CopyRootReplyConsumerBoundary(t, requester, true, false, false)
			canonicalrouting.AddRootReplyConsumerAgent(t, owner, requester)
			source := loadTopologySource(t, root)
			topology := Build(source)
			edges := localConnectionEdges(t, topology, parentLocalConsumerPlan(t, source, requester, false), 2)
			kinds := map[semanticview.EventEndpointKind]int{}
			for _, edge := range edges {
				kinds[edge.Consumer.Kind]++
			}
			if kinds[semanticview.EventEndpointNodeHandler] != 1 || kinds[semanticview.EventEndpointAgent] != 1 || edges[0].ID == edges[1].ID {
				t.Fatalf("lost independent local consumers: %+v", edges)
			}
			assertStableTopology(t, source, topology)
		})
	}
}

func TestBuildProjectsConnectedRootExportWithoutDelivery(t *testing.T) {
	root := canonicalrouting.CopyRootReplyBoundary(t, false, false)
	canonicalrouting.ApplyRootReplyBoundaryNegativeMutation(t, root, false, canonicalrouting.RootReplyExportObserverAbsent)
	source := loadTopologySource(t, root)
	topology := Build(source)
	if len(topology.Issues) != 0 {
		t.Fatalf("root export refused: %+v", topology.Issues)
	}
	for _, edge := range topology.Edges {
		if edge.Consumer.Event.Local == "request.finished" {
			t.Fatalf("output export became executable delivery: %+v", edge)
		}
	}
	var exports []BoundaryExposure
	for _, exposure := range topology.BoundaryExposures {
		if exposure.Output.FlowID == "." && exposure.Output.PinName == "request.finished" {
			exports = append(exports, exposure)
		}
	}
	if len(exports) != 1 || exports[0].Output.Direction != semanticview.EventEndpointOutputPin || exports[0].Boundary == nil || !exports[0].Boundary.ReceiverLocal || exports[0].Resolution == nil {
		t.Fatalf("connected export evidence = %+v", exports)
	}
	if _, public := source.FlowInputEventPin(".", "request.finished"); public {
		t.Fatal("root output acquired public input authority")
	}
	assertStableTopology(t, source, topology)
	canonicalrouting.ApplyRootReplyBoundaryNegativeMutation(t, root, false, canonicalrouting.RootReplyExportPinAbsent)
	if graph := pinrouting.CompileConnectGraph(loadTopologySource(t, root)); len(graph.Issues()) == 0 {
		t.Fatal("receiver with neither consumer nor export retained a connection")
	}
}

func TestBuildParentLocalSourceDoesNotRequireOutputPin(t *testing.T) {
	for _, requester := range []bool{false, true} {
		t.Run(fmt.Sprint(requester), func(t *testing.T) {
			source := loadTopologySource(t, canonicalrouting.CopyRootReplyBoundary(t, requester, true))
			topology := Build(source)
			found := false
			for _, plan := range pinrouting.CompileConnectGraph(source).Plans() {
				if !plan.SourceEndpoint().Readback().LocalEndpoint {
					continue
				}
				readback := plan.Readback()
				for _, edge := range topology.Edges {
					if edge.Boundary == nil || edge.Boundary.From != connectEndpointRef(plan.SourceEndpoint()) || edge.Boundary.To != connectEndpointRef(plan.ReceiverEndpoint()) {
						continue
					}
					found = true
					if !edge.Boundary.SourceLocal || edge.Boundary.SourceEventSchemaDigest != readback.Source.EventSchemaDigest || edge.Producer.Kind == semanticview.EventEndpointFlowOutputPin || edge.Consumer.Direction != semanticview.EventEndpointInputPin {
						t.Fatalf("local source or pinned child receiver changed role: %+v", edge)
					}
				}
			}
			if !found {
				t.Fatal("parent-local source disappeared without a fake output pin")
			}
		})
	}
}

func parentLocalConsumerPlan(t testing.TB, source semanticview.Source, requester, renamed bool) pinrouting.ConnectRoutePlan {
	t.Helper()
	want := "provider.requested"
	if requester {
		want = "provider.replied"
	}
	if renamed {
		want = "provider.received"
		if requester {
			want = "requester.received"
		}
	}
	graph := pinrouting.CompileConnectGraph(source)
	if len(graph.Issues()) != 0 {
		t.Fatalf("valid compiled graph rejected: %+v", graph.Issues())
	}
	for _, plan := range graph.Plans() {
		if receiver := plan.ReceiverEndpoint().Readback(); receiver.LocalEndpoint && receiver.LocalEvent == want {
			return plan
		}
	}
	t.Fatalf("missing compiled local receiver for %s", want)
	return pinrouting.ConnectRoutePlan{}
}

func localConnectionEdges(t testing.TB, topology Topology, plan pinrouting.ConnectRoutePlan, want int) []Edge {
	t.Helper()
	r := plan.Readback()
	var edges []Edge
	for _, edge := range topology.Edges {
		if edge.Scope != DeliveryScopeInterFlowConnect || edge.Boundary == nil || edge.Boundary.From != connectEndpointRef(plan.SourceEndpoint()) || edge.Boundary.To != connectEndpointRef(plan.ReceiverEndpoint()) {
			continue
		}
		if !edge.Boundary.ReceiverLocal || edge.Boundary.ReceiverEventSchemaDigest != r.Receiver.EventSchemaDigest || edge.Boundary.OwnerFlowPath != r.FlowPath || edge.Boundary.AuthoredLocation != r.AuthoredLocation || !strings.Contains(edge.Boundary.AuthoredLocation, "schema.yaml:") {
			t.Fatalf("compiled boundary evidence lost: %+v", edge)
		}
		if edge.Consumer.Direction != semanticview.EventEndpointConsumer || edge.Consumer.Event.Local != r.Receiver.LocalEvent || edge.Consumer.Event.Canonical != r.Receiver.ResolvedEvent || edge.Consumer.FlowID != r.Receiver.FlowID {
			t.Fatalf("local consumer became a pin or used source identity: %+v", edge)
		}
		if edge.Event.Local != r.Source.LocalEvent || edge.Event.Canonical != r.Source.ResolvedEvent || edge.Resolution == nil {
			t.Fatalf("source or resolution evidence lost: %+v", edge)
		}
		if reply := plan.ReplyResolution(); reply != nil {
			if edge.Resolution.Reply == nil || edge.Resolution.Reply.Role != reply.Readback().Role || edge.Resolution.Reply.CorrelationKey != reply.Readback().CorrelationKey {
				t.Fatalf("reply evidence lost: %+v", edge)
			}
		}
		edges = append(edges, edge)
	}
	if len(edges) != want {
		t.Fatalf("local edges = %+v, want %d", edges, want)
	}
	for _, input := range topology.InputPins {
		if input.FlowID == r.Receiver.FlowID && input.PinName == r.Receiver.LocalEvent {
			t.Fatalf("private return gained a public boundary: %+v", input)
		}
	}
	return edges
}

func assertStableTopology(t testing.TB, source semanticview.Source, topology Topology) {
	t.Helper()
	first, err := json.Marshal(topology)
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(Build(source))
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("repeat topology differs: %v", err)
	}
}
