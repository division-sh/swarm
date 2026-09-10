package routingtopology_test

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompiledLifecycleEmitterTopology(t *testing.T) {
	for _, tc := range []struct {
		name      string
		root      func(*testing.T) string
		want      []string
		exposures int
	}{
		{"gate_local", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleGateLocal)
		}, []string{".|gate_outcome|approve|typed_pubsub|.|collector|work.completed|exact"}, 0},
		{"loop_escape", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitterStatic(t, canonicalrouting.LifecycleStaticLoopLocal)
		}, []string{".|loop_escape||typed_pubsub|.|collector|loop.escaped|exact"}, 0},
		{"connected_output", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopConnected)
		}, []string{".|loop_escape||inter_flow_connect|sink||loop.escaped|"}, 1},
		{"disconnected_output", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleGateOutputDisconnected)
		}, nil, 1},
		{"scope_matrix", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitterStatic(t, canonicalrouting.LifecycleStaticScopeMatrix)
		}, []string{
			".|gate_outcome|approve|typed_pubsub|.|collector|work.completed|exact",
			"left|gate_outcome|approve|typed_pubsub|left|collector|left/work.completed|exact",
			"left/deep|gate_outcome|approve|typed_pubsub|left/deep|collector|left/deep/work.completed|exact",
			"right|gate_outcome|approve|typed_pubsub|right|collector|right/work.completed|exact",
		}, 0},
		{"wildcard_scope", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitterStatic(t, canonicalrouting.LifecycleStaticWildcardScope)
		}, []string{
			".|gate_outcome|approve|typed_pubsub|.|collector|work.completed|pattern",
			"left|gate_outcome|approve|typed_pubsub|left|collector|left/work.completed|pattern",
			"left/deep|gate_outcome|approve|typed_pubsub|left/deep|collector|left/deep/work.completed|pattern",
			"right|gate_outcome|approve|typed_pubsub|right|collector|right/work.completed|pattern",
		}, 0},
		{"foreign_only", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitterStatic(t, canonicalrouting.LifecycleStaticForeignOnly)
		}, []string{
			"left|gate_outcome|approve|typed_pubsub|left|collector|left/work.completed|exact",
			"left/deep|gate_outcome|approve|typed_pubsub|left/deep|collector|left/deep/work.completed|exact",
			"right|gate_outcome|approve|typed_pubsub|right|collector|right/work.completed|exact",
		}, 0},
		{"mixed_producer_output", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitterStatic(t, canonicalrouting.LifecycleStaticMixedOutput)
		}, []string{".|gate_outcome|approve|inter_flow_connect|sink||work.completed|", ".|node_handler||inter_flow_connect|sink||work.completed|"}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, tc.root(t), runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			topology := routingtopology.Build(source)
			if len(topology.Issues) != 0 {
				t.Fatalf("topology issues: %#v", topology.Issues)
			}
			var got []string
			ids := map[string]bool{}
			for _, edge := range topology.Edges {
				if edge.Producer.Event.Local != "work.completed" && edge.Producer.Event.Local != "loop.escaped" {
					continue
				}
				if edge.Producer.Direction != semanticview.EventEndpointProducer {
					continue
				}
				match := ""
				if edge.Scope == routingtopology.DeliveryScopeTypedPubSub {
					if edge.TypedPubSub == nil || edge.TypedPubSub.Boundary != "same_flow" || edge.Producer.FlowID != edge.Consumer.FlowID || edge.Boundary != nil {
						t.Fatalf("invalid typed proof: %#v", edge)
					}
					match = edge.TypedPubSub.Match
				} else if edge.Scope == routingtopology.DeliveryScopeInterFlowConnect {
					if edge.Boundary == nil || edge.Boundary.From != "."+edge.Producer.Event.Local || edge.Boundary.To != "sink."+edge.Producer.Event.Local || edge.Boundary.OutputPin != edge.Producer.Event.Local || edge.Boundary.InputPin != edge.Producer.Event.Local || edge.TypedPubSub != nil || edge.Consumer.Kind != semanticview.EventEndpointFlowInputPin {
						t.Fatalf("invalid connect proof: %#v", edge)
					}
				} else {
					t.Fatalf("unexpected delivery scope: %#v", edge)
				}
				if ids[edge.ID] {
					t.Fatalf("duplicate edge identity %s", edge.ID)
				}
				ids[edge.ID] = true
				consumer := edge.Consumer.NodeID
				if edge.Consumer.Kind == semanticview.EventEndpointAgent {
					consumer = edge.Consumer.AgentID
				}
				got = append(got, strings.Join([]string{edge.Producer.FlowID, string(edge.Producer.Kind), edge.Producer.Verdict, string(edge.Scope), edge.Consumer.FlowID, consumer, edge.Event.Canonical, match}, "|"))
			}
			sort.Strings(got)
			sort.Strings(tc.want)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("edges = %q, want %q", got, tc.want)
			}
			if len(topology.BoundaryExposures) != tc.exposures {
				t.Fatalf("exposures = %#v, want %d", topology.BoundaryExposures, tc.exposures)
			}
			for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).Producers() {
				if endpoint.Kind != semanticview.EventEndpointGateOutcome && endpoint.Kind != semanticview.EventEndpointLoopEscape {
					continue
				}
				var projected routingtopology.Endpoint
				for _, candidate := range topology.Producers {
					if candidate.ID == endpoint.ID {
						projected = candidate
					}
				}
				if projected.ID == "" || projected.StageID != endpoint.StageID || projected.DecisionID != endpoint.DecisionID || projected.Verdict != endpoint.Verdict || projected.LoopID != endpoint.LoopID || projected.Site != endpoint.Site || projected.SourceFile != endpoint.SourceFile || projected.SourceLine != endpoint.SourceLine || projected.SourceLocation != endpoint.SourceLocation {
					t.Fatalf("projection lost provenance: %#v -> %#v", endpoint, projected)
				}
			}
			wire, err := json.Marshal(authoringview.BuildRoutingTopology(source))
			if err != nil {
				t.Fatal(err)
			}
			var decoded routingtopology.Topology
			if err := json.Unmarshal(wire, &decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(topology, decoded) {
				t.Fatalf("authoring JSON disagrees with routing topology: %s", wire)
			}
			decoded.Producers[0].SourceFile = "hostile"
			if len(decoded.Edges) > 0 && decoded.Edges[0].TypedPubSub != nil {
				decoded.Edges[0].TypedPubSub.Boundary = "hostile"
			}
			again, err := json.Marshal(authoringview.BuildRoutingTopology(source))
			if err != nil {
				t.Fatal(err)
			}
			if string(again) != string(wire) {
				t.Fatal("projection rebuild changed after returned mutation")
			}
		})
	}
}
