package pinrouting

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
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

func TestResolveFlowMatchTargetsActiveDynamicInstanceByInstanceID(t *testing.T) {
	source := testFlowMatchPinRoutingSource()
	const flowInstance = "component-scaffold/aaaaaaaa-1111-4111-8111-aaaaaaaa1111"

	result := Resolve(ResolutionInput{
		Source:    source,
		FlowID:    "service-owner",
		EventType: "component.service.completed",
		Emit: runtimecontracts.EmitSpec{
			Target: runtimecontracts.EmitTargetSpec{
				Flow:  "component-scaffold",
				Match: map[string]runtimecontracts.ExpressionValue{"instance_id": runtimecontracts.RefExpression("payload.component_id")},
			},
		},
		MatchValues: map[string]string{"instance_id": "aaaaaaaa-1111-4111-8111-aaaaaaaa1111"},
		Descriptors: []Descriptor{{
			FlowInstance: flowInstance,
		}},
	}, events.NewProjectionEvent("", "component.service.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))

	if result.Failure != "" {
		t.Fatalf("Failure = %q, want empty", result.Failure)
	}
	if result.Target.FlowInstance != flowInstance {
		t.Fatalf("Target.FlowInstance = %q, want %q", result.Target.FlowInstance, flowInstance)
	}
	if result.Target.EntityID != runtimeflowidentity.EntityID(flowInstance) {
		t.Fatalf("Target.EntityID = %q, want derived flow instance entity id", result.Target.EntityID)
	}
}

func TestResolveFlowMatchTargetsActiveDynamicInstanceByFlowInstanceAndEntityID(t *testing.T) {
	source := testFlowMatchPinRoutingSource()
	const flowInstance = "component-scaffold/bbbbbbbb-2222-4222-8222-bbbbbbbb2222"
	entityID := runtimeflowidentity.EntityID(flowInstance)

	for _, tt := range []struct {
		name       string
		matchKey   string
		matchValue string
		descriptor Descriptor
	}{
		{
			name:       "flow_instance",
			matchKey:   "flow_instance",
			matchValue: flowInstance,
			descriptor: Descriptor{FlowInstance: flowInstance},
		},
		{
			name:       "entity_id",
			matchKey:   "entity_id",
			matchValue: entityID,
			descriptor: Descriptor{FlowInstance: flowInstance},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := Resolve(ResolutionInput{
				Source:    source,
				FlowID:    "service-owner",
				EventType: "component.service.completed",
				Emit: runtimecontracts.EmitSpec{
					Target: runtimecontracts.EmitTargetSpec{
						Flow:  "component-scaffold",
						Match: map[string]runtimecontracts.ExpressionValue{tt.matchKey: runtimecontracts.RefExpression("payload." + tt.matchKey)},
					},
				},
				MatchValues: map[string]string{tt.matchKey: tt.matchValue},
				Descriptors: []Descriptor{tt.descriptor},
			}, events.NewProjectionEvent("", "component.service.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))

			if result.Failure != "" {
				t.Fatalf("Failure = %q, want empty", result.Failure)
			}
			if result.Target.FlowInstance != flowInstance {
				t.Fatalf("Target.FlowInstance = %q, want %q", result.Target.FlowInstance, flowInstance)
			}
			if result.Target.EntityID != entityID {
				t.Fatalf("Target.EntityID = %q, want %q", result.Target.EntityID, entityID)
			}
		})
	}
}

func TestResolveFlowMatchFailsClosedOnAmbiguousActiveDynamicInstances(t *testing.T) {
	source := testFlowMatchPinRoutingSource()

	result := Resolve(ResolutionInput{
		Source:    source,
		FlowID:    "service-owner",
		EventType: "component.service.completed",
		Emit: runtimecontracts.EmitSpec{
			Target: runtimecontracts.EmitTargetSpec{
				Flow:  "component-scaffold",
				Match: map[string]runtimecontracts.ExpressionValue{"entity_id": runtimecontracts.RefExpression("payload.entity_id")},
			},
		},
		MatchValues: map[string]string{"entity_id": "shared-entity"},
		Descriptors: []Descriptor{
			{EntityID: "shared-entity", FlowInstance: "component-scaffold/a"},
			{EntityID: "shared-entity", FlowInstance: "component-scaffold/b"},
		},
	}, events.NewProjectionEvent("", "component.service.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))

	if result.Failure != FailureTargetAmbiguous {
		t.Fatalf("Failure = %q, want %q", result.Failure, FailureTargetAmbiguous)
	}
	if got := result.Event.TargetRoute(); !got.Empty() {
		t.Fatalf("Event target = %#v, want empty on ambiguous target", got)
	}
}

func TestResolveFlowMatchFailsClosedWhenDynamicInstanceDescriptorMissing(t *testing.T) {
	source := testFlowMatchPinRoutingSource()

	result := Resolve(ResolutionInput{
		Source:    source,
		FlowID:    "service-owner",
		EventType: "component.service.completed",
		Emit: runtimecontracts.EmitSpec{
			Target: runtimecontracts.EmitTargetSpec{
				Flow:  "component-scaffold",
				Match: map[string]runtimecontracts.ExpressionValue{"instance_id": runtimecontracts.RefExpression("payload.component_id")},
			},
		},
		MatchValues: map[string]string{"instance_id": "missing"},
		Descriptors: []Descriptor{{
			FlowInstance: "component-scaffold/live",
		}},
	}, events.NewProjectionEvent("", "component.service.completed", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))

	if result.Failure != FailureTargetUnreachableNoSub {
		t.Fatalf("Failure = %q, want %q", result.Failure, FailureTargetUnreachableNoSub)
	}
	if got := result.Event.TargetRoute(); !got.Empty() {
		t.Fatalf("Event target = %#v, want empty on missing target", got)
	}
	if len(result.Event.TargetRoutes()) != 0 {
		t.Fatalf("Event target_set = %#v, want empty on missing target", result.Event.TargetRoutes())
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

func testFlowMatchPinRoutingSource() semanticview.Source {
	serviceOwner := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "service-owner",
			Flow: "service-owner",
		},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"component.service.completed"},
				},
			},
		},
		Path: "service-owner",
	}
	componentScaffold := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "component-scaffold",
			Flow: "component-scaffold",
		},
		Path: "component-scaffold",
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{serviceOwner, componentScaffold},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"service-owner":      &serviceOwner,
				"component-scaffold": &componentScaffold,
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
