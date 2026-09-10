package routingtopology_test

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompiledLifecycleEmitterLoopScopeTopology(t *testing.T) {
	for _, mode := range []string{"local", "wildcard", "foreign_only", "connected"} {
		t.Run(mode, func(t *testing.T) {
			var root string
			switch mode {
			case "connected":
				root = canonicalrouting.CopyLifecycleEmitterStatic(t, canonicalrouting.LifecycleStaticLoopScopeMatrix)
			case "local":
				root = canonicalrouting.CopyLifecycleEmitterLoopScopeTopology(t, canonicalrouting.LifecycleLoopScopeLocal)
			case "wildcard":
				root = canonicalrouting.CopyLifecycleEmitterLoopScopeTopology(t, canonicalrouting.LifecycleLoopScopeWildcard)
			case "foreign_only":
				root = canonicalrouting.CopyLifecycleEmitterLoopScopeTopology(t, canonicalrouting.LifecycleLoopScopeForeignOnly)
			}
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			topology := routingtopology.Build(semanticview.Wrap(bundle))
			var got, want []string
			for _, flow := range []string{".", "left", "left/deep", "right"} {
				prefix := ""
				if flow != "." {
					prefix = flow + "/"
				}
				event := prefix + "loop.escaped"
				if mode == "connected" {
					want = append(want, flow+"|loop_escape|inter_flow_connect|"+prefix+"sink|flow_input_pin||"+event)
					want = append(want, prefix+"sink|flow_input_pin|typed_pubsub|"+prefix+"sink|node_handler|collector|"+prefix+"sink/loop.escaped")
				} else if mode != "foreign_only" || flow != "." {
					want = append(want, flow+"|loop_escape|typed_pubsub|"+flow+"|agent|collector|"+event)
				}
			}
			for _, edge := range topology.Edges {
				if edge.Event.Local != "loop.escaped" {
					continue
				}
				consumer := edge.Consumer.NodeID
				if edge.Consumer.Kind == semanticview.EventEndpointAgent {
					consumer = edge.Consumer.AgentID
				}
				got = append(got, strings.Join([]string{edge.Producer.FlowID, string(edge.Producer.Kind), string(edge.Scope), edge.Consumer.FlowID, string(edge.Consumer.Kind), consumer, edge.Event.Canonical}, "|"))
				if edge.Producer.Kind == semanticview.EventEndpointLoopEscape {
					if edge.Producer.LoopID != "revision" || edge.Producer.Site != "loops.revision.escape.emit" || edge.Producer.SourceLine == 0 {
						t.Fatalf("loop provenance lost: %#v", edge.Producer)
					}
				}
				if edge.Scope == routingtopology.DeliveryScopeTypedPubSub {
					match := "exact"
					if mode == "wildcard" {
						match = "pattern"
					}
					if edge.Producer.FlowID != edge.Consumer.FlowID || edge.TypedPubSub == nil || edge.TypedPubSub.Boundary != "same_flow" || edge.TypedPubSub.Match != match {
						t.Fatalf("invalid same-flow proof: %#v", edge)
					}
				} else if edge.Boundary == nil || edge.Boundary.OwnerFlowPath != edge.Producer.FlowID || edge.Boundary.InputPin != "loop.escaped" || edge.Boundary.OutputPin != "loop.escaped" {
					t.Fatalf("invalid connect owner: %#v", edge)
				}
			}
			sort.Strings(got)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("loop scope edges = %q, want %q", got, want)
			}
		})
	}
}
