package pinrouting

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestResolveTargetsCompleteParentRouteForPinDeclaredOutput(t *testing.T) {
	source := testPinRoutingSource()
	parent := events.RouteIdentity{
		FlowID:       "root",
		FlowInstance: "root/inst-1",
		EntityID:     "parent-ent",
	}

	result := Resolve(ResolutionInput{
		Source:      source,
		FlowID:      "child",
		EventType:   "child.done",
		ParentRoute: parent,
	}, events.Event{Type: "child.done"})

	if result.Failure != "" {
		t.Fatalf("Failure = %q, want empty", result.Failure)
	}
	if result.Target != parent {
		t.Fatalf("Target = %#v, want %#v", result.Target, parent)
	}
	if got := result.Event.TargetRoute(); got != parent {
		t.Fatalf("Event target = %#v, want %#v", got, parent)
	}
}

func TestResolveFailsClosedOnIncompleteParentRouteForPinDeclaredOutput(t *testing.T) {
	result := Resolve(ResolutionInput{
		Source:    testPinRoutingSource(),
		FlowID:    "child",
		EventType: "child.done",
		ParentRoute: events.RouteIdentity{
			FlowID:   "root",
			EntityID: "parent-ent",
		},
	}, events.Event{Type: "child.done"})

	if result.Failure != FailureParentRouteIncomplete {
		t.Fatalf("Failure = %q, want %q", result.Failure, FailureParentRouteIncomplete)
	}
	if got := result.Event.TargetRoute(); !got.Empty() {
		t.Fatalf("Event target = %#v, want empty on failed parent route", got)
	}
}

func TestResolveAllowsEntityOnlyParentRouteOnlyWhenExplicitlyAllowed(t *testing.T) {
	parent := events.RouteIdentity{EntityID: "parent-ent"}
	blocked := Resolve(ResolutionInput{
		Source:      testPinRoutingSource(),
		FlowID:      "child",
		EventType:   "child.done",
		ParentRoute: parent,
	}, events.Event{Type: "child.done"})
	if blocked.Failure != FailureParentRouteIncomplete {
		t.Fatalf("blocked Failure = %q, want %q", blocked.Failure, FailureParentRouteIncomplete)
	}

	allowed := Resolve(ResolutionInput{
		Source:                     testPinRoutingSource(),
		FlowID:                     "child",
		EventType:                  "child.done",
		ParentRoute:                parent,
		AllowEntityOnlyParentRoute: true,
	}, events.Event{Type: "child.done"})
	if allowed.Failure != "" {
		t.Fatalf("allowed Failure = %q, want empty", allowed.Failure)
	}
	if got := allowed.Event.TargetRoute(); got != parent {
		t.Fatalf("allowed target = %#v, want %#v", got, parent)
	}
}

func testPinRoutingSource() semanticview.Source {
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "child",
			Flow: "child",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"child.done"},
				},
			},
		},
		Path: "child",
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{child},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child": &child,
			},
		},
	})
}
