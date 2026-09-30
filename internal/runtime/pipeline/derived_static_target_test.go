package pipeline

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type derivedStaticRouteOwner struct{ run string }

func (o derivedStaticRouteOwner) HasFlowInstanceRoute(id runtimeflowidentity.RunScopedFlowInstance) bool {
	return id.RunID == o.run && id.Route.ScopeKey == "receiver" && id.Route.InstancePath == "parent/key/receiver"
}

func (derivedStaticRouteOwner) RetireCommittedFlowInstanceRoute(WorkflowEngineRouteRetirement) error {
	return nil
}

func TestDerivedStaticTargetRequiresOwnedRoute(t *testing.T) {
	source := testWorkflowNodeConnectedInputSource(t, "static")
	bundle, _ := semanticview.Bundle(source)
	run := eventtest.UUID("derived-static-target-run")
	pc := &PipelineCoordinator{module: &previewWorkflowModule{bundle: bundle}, flowRoutes: derivedStaticRouteOwner{run: run}}
	node := pipelineSourceNode(t, source, "receiver", "receiver-node")
	for _, tc := range []struct {
		name, run, path string
		want            bool
	}{
		{"exact", run, "receiver", true},
		{"nested materialized", run, "parent/key/receiver", true},
		{"unmaterialized descendant", run, "receiver/item", false},
		{"prefix lookalike", run, "receiverish/item", false},
		{"foreign", run, "foreign/item", false},
		{"wrong run", eventtest.UUID("other-static-target-run"), "parent/key/receiver", false},
	} {
		for _, qualified := range []bool{false, true} {
			t.Run(tc.name+"/qualified="+map[bool]string{false: "no", true: "yes"}[qualified], func(t *testing.T) {
				target := events.RouteIdentity{FlowInstance: tc.path}
				if qualified {
					target.FlowID = "receiver"
				}
				if got := pc.workflowNodeMatchesDeliveryTarget(node, tc.run, target); got != tc.want {
					t.Fatalf("target admission=%t want=%t, %#v", got, tc.want, target)
				}
			})
		}
	}
}
