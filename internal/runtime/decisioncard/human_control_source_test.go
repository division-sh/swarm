package decisioncard

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

func TestHumanControlSourceRoundTripPreservesRequesterOwnership(t *testing.T) {
	operation, err := NewHumanTaskOperationID("run", "event\x00tool_call")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		route       events.RouteIdentity
		constructor func(events.RouteIdentity) (events.RoutingSource, error)
	}{
		{"static", events.RouteIdentity{FlowID: "child", FlowInstance: "outer/child"}, events.NewStaticFlowRoutingSource},
		{"singleton", events.RouteIdentity{FlowID: "singleton", FlowInstance: "singleton"}, events.NewStaticFlowRoutingSource},
		{"template", events.RouteIdentity{FlowID: "child", FlowInstance: "outer/child/one"}, events.NewConcreteTemplateInstanceRoutingSource},
	} {
		for _, entity := range []string{"", "entity"} {
			t.Run(tc.name+"/entity="+entity, func(t *testing.T) {
				route := tc.route
				route.EntityID = entity
				source, err := tc.constructor(route)
				if err != nil {
					t.Fatal(err)
				}
				anchor, err := NewHumanTaskAnchor(HumanTaskAnchor{RequesterAgentID: "requester", OperationID: operation, Category: "review", Scope: Scope{Kind: ScopeFlow, FlowInstance: route.FlowInstance}, Source: source})
				if err != nil {
					t.Fatal(err)
				}
				raw, err := canonicaljson.Bytes(anchor.SemanticValue().Interface())
				if err != nil {
					t.Fatal(err)
				}
				restored, err := DecodeAnchor("human_task", raw)
				if err != nil {
					t.Fatal(err)
				}
				for _, candidate := range []Anchor{anchor, restored} {
					control, err := candidate.ControlRoutingSource()
					if err != nil || control.Kind() != events.RoutingSourceFlowOwnedControl || control.Route() != route {
						t.Fatalf("control = %#v, %v; want %#v", control, err, route)
					}
					task, err := candidate.HumanTask()
					if err != nil {
						t.Fatal(err)
					}
					if err := validateHumanTaskSourceOwner(task, HumanTaskContinuation{RequesterRoute: route}); err != nil {
						t.Fatal(err)
					}
					for _, foreign := range []events.RouteIdentity{
						{FlowID: "foreign", FlowInstance: route.FlowInstance, EntityID: entity},
						{FlowID: route.FlowID, FlowInstance: "foreign", EntityID: entity},
						{FlowID: route.FlowID, FlowInstance: route.FlowInstance, EntityID: "foreign"},
					} {
						if err := validateHumanTaskSourceOwner(task, HumanTaskContinuation{RequesterRoute: foreign}); err == nil {
							t.Fatalf("accepted foreign requester %#v", foreign)
						}
					}
				}
			})
		}
	}
	root, err := events.NewRootRoutingSource("entity")
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := NewHumanTaskAnchor(HumanTaskAnchor{RequesterAgentID: "root", OperationID: operation, Category: "review", Scope: Scope{Kind: ScopeGlobal}, Source: root})
	if err != nil {
		t.Fatal(err)
	}
	control, err := anchor.ControlRoutingSource()
	if err != nil || control.Kind() != events.RoutingSourcePlatformControl {
		t.Fatalf("root control changed: %#v, %v", control, err)
	}
}

func TestStageGateStillRequiresEntityForEntitylessFlowSource(t *testing.T) {
	valid, err := testStageAnchor().StageGate()
	if err != nil {
		t.Fatal(err)
	}
	valid.FlowID = "child"
	valid.Source, err = events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: "child", FlowInstance: "child", EntityID: valid.EntityID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewStageGateAnchor(valid); err != nil {
		t.Fatal(err)
	}
	valid.EntityID = ""
	valid.Source, err = events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: "child", FlowInstance: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewStageGateAnchor(valid); err == nil {
		t.Fatal("accepted entityless stage gate")
	}
}
