package pinrouting

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"time"
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
	}, events.NewProjectionEvent("", "child.done", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))

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
	}, events.NewProjectionEvent("", "child.done", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))

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
	}, events.NewProjectionEvent("", "child.done", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if blocked.Failure != FailureParentRouteIncomplete {
		t.Fatalf("blocked Failure = %q, want %q", blocked.Failure, FailureParentRouteIncomplete)
	}

	allowed := Resolve(ResolutionInput{
		Source:                     testPinRoutingSource(),
		FlowID:                     "child",
		EventType:                  "child.done",
		ParentRoute:                parent,
		AllowEntityOnlyParentRoute: true,
	}, events.NewProjectionEvent("", "child.done", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if allowed.Failure != "" {
		t.Fatalf("allowed Failure = %q, want empty", allowed.Failure)
	}
	if got := allowed.Event.TargetRoute(); got != parent {
		t.Fatalf("allowed target = %#v, want %#v", got, parent)
	}
}

func TestPinDeclaredOutputRecognizesRootSchemaOutputWithoutLeafFallback(t *testing.T) {
	source := testRootPinRoutingSource()

	if !PinDeclaredOutput(source, "", "root.ready") {
		t.Fatal("root output pin was not recognized")
	}
	if PinDeclaredOutput(source, "", "worker/root.ready") {
		t.Fatal("namespaced event matched root output pin by leaf name")
	}
}

func TestResolveFailsClosedForRootPinOutputWithoutTargetMechanism(t *testing.T) {
	result := Resolve(ResolutionInput{
		Source:    testRootPinRoutingSource(),
		EventType: "root.ready",
	}, events.NewProjectionEvent("", "root.ready", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))

	if result.Failure != FailureTargetRequiredMissing {
		t.Fatalf("Failure = %q, want %q", result.Failure, FailureTargetRequiredMissing)
	}
	if got := result.Event.TargetRoute(); !got.Empty() {
		t.Fatalf("Event target = %#v, want empty on failed root target resolution", got)
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

func testRootPinRoutingSource() semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"root.ready"},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"root.ready": {},
		},
	})
}
