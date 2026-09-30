package contracts

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestGuardTerminationPossibilityIsNotTransitionAuthority(t *testing.T) {
	node := identitytest.RootNode(t, "router")
	guard := &GuardSpec{Checks: []GuardCheck{{ID: "first", Check: "true"}, {ID: "second", Check: "false"}}, OnFail: "kill"}
	graph := BuildWorkflowStageTopology(".", "ready", []string{"ready", "done", "killed"}, []string{"done", "killed"},
		[]HandlerTransitionSemantic{{Node: node, EventType: "work", Guard: guard, AdvancesTo: "done"}}, nil, nil)
	got := graph.PossibleGuardTerminations()
	want := []WorkflowGuardTerminationPossibility{
		{From: "ready", To: "killed", Node: node, HandlerEvent: "work", GuardID: "first"},
		{From: "ready", To: "killed", Node: node, HandlerEvent: "work", GuardID: "second"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("possibilities = %#v, want %#v", got, want)
	}
	if _, ok := graph.LifecycleReachableStages("ready")["killed"]; !ok {
		t.Fatal("guard outcome not reachable")
	}
	if _, err := graph.AdmitTransition(WorkflowTransitionSite{Node: node, HandlerEvent: "work", AdvanceCarrier: HandlerAdvanceCarrierHandler}, "ready", "killed"); err == nil {
		t.Fatal("possibility lent ordinary transition authority")
	}
	if graph.StageCanReenter("killed") || len(graph.StronglyConnectedComponent("ready")) != 1 {
		t.Fatal("guard possibility changed authored-edge SCC semantics")
	}
	got[0].To = "done"
	if !reflect.DeepEqual(graph.PossibleGuardTerminations(), want) {
		t.Fatal("public projection changed canonical possibilities")
	}
	projection := (&WorkflowContractBundle{Semantics: WorkflowSemanticView{StageTopologies: map[string]WorkflowStageTopology{".": graph}}})
	copy, _ := projection.WorkflowStageTopology(".")
	copy.FlowID = "foreign"
	if len(copy.PossibleGuardTerminations()) != 0 || len(copy.LifecycleReachableStages("ready")) != 0 {
		t.Fatal("foreign graph projection borrowed source authority")
	}
}

func TestGuardTerminationJoinCombinationsRemainForbidden(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		handler := SystemNodeEventHandler{
			Guard: &GuardSpec{ID: "check", Check: "false", OnFail: "kill"},
			Join:  &JoinSpec{ID: "barrier", Stage: "ready", Members: JoinMembersSpec{FromFanOut: delivery}},
		}
		if delivery {
			handler.FanOut = &FanOutSpec{}
		}
		if err := ValidateJoinHandlerIsolation(handler); err == nil || !strings.Contains(err.Error(), "guard") {
			t.Fatalf("guard/join source widened: delivery=%v err=%v", delivery, err)
		}
	}
}

func TestGuardTerminationSourceScopePrecedesJoin(t *testing.T) {
	node := identitytest.RootNode(t, "router")
	kill := &GuardSpec{ID: "check", Check: "false", OnFail: "kill"}
	for _, tc := range []struct {
		name          string
		handler       HandlerTransitionSemantic
		wantSources   []string
		wantReachable bool
	}{
		{"ordinary", HandlerTransitionSemantic{Guard: kill}, []string{"ready", "unreachable"}, true},
		{"create", HandlerTransitionSemantic{Guard: kill, CreateEntity: true}, []string{"ready"}, true},
		{"unreachable loop", HandlerTransitionSemantic{Guard: kill, Loop: &LoopOperationSpec{Repeat: "revision", From: "unreachable"}}, []string{"unreachable"}, false},
		{"arrival join", HandlerTransitionSemantic{Guard: kill, Join: &JoinSpec{Stage: "unreachable"}}, []string{"ready", "unreachable"}, true},
		{"loop before join", HandlerTransitionSemantic{Guard: kill, Loop: &LoopOperationSpec{Repeat: "revision", From: "unreachable"}, Join: &JoinSpec{Stage: "ready"}}, []string{"unreachable"}, false},
		{"terminal loop", HandlerTransitionSemantic{Guard: kill, Loop: &LoopOperationSpec{Repeat: "revision", From: "done"}}, nil, false},
		{"no guard", HandlerTransitionSemantic{}, nil, false},
		{"reject", HandlerTransitionSemantic{Guard: &GuardSpec{ID: "check", Check: "false", OnFail: "reject"}}, nil, false},
		{"discard", HandlerTransitionSemantic{Guard: &GuardSpec{ID: "check", Check: "false", OnFail: "discard"}}, nil, false},
		{"escalate", HandlerTransitionSemantic{Guard: &GuardSpec{ID: "check", Check: "false", OnFailSpec: GuardFailureSpec{Action: GuardFailureActionEscalate}}}, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := tc.handler
			handler.Node, handler.EventType = node, "work"
			graph := BuildWorkflowStageTopology(".", "ready", []string{"ready", "unreachable", "done", "killed"}, []string{"done", "killed"}, []HandlerTransitionSemantic{handler}, nil, nil)
			var sources []string
			for _, possibility := range graph.PossibleGuardTerminations() {
				sources = append(sources, possibility.From)
			}
			_, reachable := graph.LifecycleReachableStages("ready")["killed"]
			if !reflect.DeepEqual(sources, tc.wantSources) || reachable != tc.wantReachable {
				t.Fatalf("sources=%v reachable=%v, want %v %v", sources, reachable, tc.wantSources, tc.wantReachable)
			}
		})
	}
}

func TestGuardTerminationFlowSourceAndCaseIsolation(t *testing.T) {
	kill := &GuardSpec{ID: "check", Check: "false", OnFail: "kill"}
	for _, flow := range []string{".", "child", "child/nested", "sibling"} {
		t.Run(flow, func(t *testing.T) {
			node := identitytest.ExecutableNode(t, flow, "router")
			foreign := identitytest.ExecutableNode(t, "foreign", "router")
			for _, target := range []string{"killed", "Killed", "done"} {
				graph := BuildWorkflowStageTopology(flow, "ready", []string{"ready", target}, []string{target}, []HandlerTransitionSemantic{{Node: node, EventType: "work", Guard: kill}}, nil, nil)
				_, reachable := graph.LifecycleReachableStages("ready")[target]
				if reachable != (target == "killed") {
					t.Fatalf("target %s reachable=%v", target, reachable)
				}
				other := BuildWorkflowStageTopology(flow, "ready", []string{"ready", target}, []string{target}, []HandlerTransitionSemantic{{Node: foreign, EventType: "work", Guard: kill}}, nil, nil)
				if len(other.PossibleGuardTerminations()) != 0 {
					t.Fatal("foreign flow guard lent possibility")
				}
			}
		})
	}
}
