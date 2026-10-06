package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestFinalStageExplicitLoopAndJoinSourcesCannotWidenEligibility(t *testing.T) {
	node, err := identity.AdmitExecutableNodeDeclaration(".", "controller")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"start", "admit", "repeat", "close", "join_complete", "join_deadline"} {
		for _, final := range []bool{false, true} {
			t.Run(kind+"/final="+boolString(final), func(t *testing.T) {
				transition := HandlerTransitionSemantic{Node: node, EventType: "work.received", AdvancesTo: "other"}
				var loops []WorkflowLoopPlan
				switch kind {
				case "start", "admit", "repeat", "close":
					loop := &LoopOperationSpec{From: "waiting"}
					switch kind {
					case "start":
						loop.Start = "revision"
					case "admit":
						loop.Admit = "revision"
					case "repeat":
						loop.Repeat = "revision"
					case "close":
						loop.Close = "revision"
					}
					transition.Loop = loop
					if kind == "repeat" {
						loops = []WorkflowLoopPlan{{ID: "revision", FlowID: ".", Escape: LoopEscapeSpec{AdvancesTo: "other"}, Operations: []WorkflowLoopOperationPlan{{Kind: LoopOperationRepeat, From: "waiting", Node: node, HandlerEvent: "work.received"}}}}
					}
				default:
					transition.AdvancesTo = ""
					join := &JoinSpec{ID: "members", Stage: "waiting"}
					if kind == "join_complete" {
						join.OnCompleteFound = true
						join.OnComplete = HandlerRuleEntry{AdvancesTo: "other", authored: true}
					} else {
						join.OnDeadlineFound = true
						join.Deadline = &JoinDeadlineSpec{After: "1h", From: JoinDeadlineFromStageEntry}
						join.OnDeadline = HandlerRuleEntry{AdvancesTo: "other", authored: true}
					}
					transition.Join = join
					handler, err := QualifySystemNodeHandlerRuleRefsForEvent(node, transition.EventType, SystemNodeEventHandler{Join: join})
					if err != nil {
						t.Fatal(err)
					}
					transition.Join = handler.Join
				}
				var finals []string
				if final {
					finals = []string{"waiting"}
				}
				graph := BuildWorkflowStageTopology(".", "waiting", []string{"waiting", "other"}, finals, []HandlerTransitionSemantic{transition}, nil, loops)
				if final {
					if len(graph.HandlerSourceErrors()) == 0 || !strings.Contains(graph.HandlerSourceErrors()[0], "final stage waiting") {
						t.Fatalf("invalid explicit source silently admitted: %+v", graph)
					}
					for _, edge := range graph.Edges {
						if edge.From == "waiting" {
							t.Fatalf("final exit entered executable graph: %+v", edge)
						}
					}
					for _, scope := range graph.Handlers {
						for _, stage := range scope.Stages {
							if stage == "waiting" {
								t.Fatal("final stage remained handler-eligible")
							}
						}
					}
					return
				}
				if len(graph.HandlerSourceErrors()) != 0 || len(graph.Edges) == 0 {
					t.Fatalf("non-final control lost its carrier: %+v", graph)
				}
				for _, edge := range graph.Edges {
					if _, err := graph.AdmitTransition(edge.Site(), edge.From, edge.To); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func TestFinalJoinDeclarationApplicabilityPreservesLoopAndFanOutKinds(t *testing.T) {
	node, err := identity.AdmitExecutableNodeDeclaration(".", "controller")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"arrival", "loop_arrival", "fan_out_delivery"} {
		for _, final := range []bool{false, true} {
			t.Run(kind+"/final="+boolString(final), func(t *testing.T) {
				join := &JoinSpec{ID: "results", Stage: "waiting", OnCompleteFound: true, OnComplete: HandlerRuleEntry{Emit: EmitSpec{Event: "work.completed"}}}
				transition := HandlerTransitionSemantic{Node: node, EventType: "work.received", Join: join}
				if kind == "loop_arrival" {
					transition.Loop = &LoopOperationSpec{Admit: "revision", From: "waiting"}
				}
				if kind == "fan_out_delivery" {
					join.Stage = ""
					join.Members.FromFanOut = true
				}
				var finals []string
				if final {
					finals = []string{"waiting"}
				}
				graph := BuildWorkflowStageTopology(".", "waiting", []string{"waiting", "other"}, finals, []HandlerTransitionSemantic{transition}, nil, nil)
				if len(graph.Edges) != 0 {
					t.Fatal("emit-only outcome fabricated an advance edge")
				}
				if final && kind != "fan_out_delivery" {
					if len(graph.HandlerSourceErrors()) == 0 {
						t.Fatal("non-advancing join source bypassed final admission")
					}
					return
				}
				if len(graph.HandlerSourceErrors()) != 0 {
					t.Fatalf("lawful source/kind changed: %v", graph.HandlerSourceErrors())
				}
			})
		}
	}
}
