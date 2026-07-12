package contracts

import "testing"

func TestWorkflowStageTopologyPreservesHandlerOriginAndEffectiveEvent(t *testing.T) {
	joinA := JoinSpec{
		ID: "join-a", Stage: "awaiting-a",
		OnComplete: HandlerRuleEntry{AdvancesTo: "complete-a"},
		Timeout:    JoinTimeoutSpec{After: "1h", Outcome: HandlerRuleEntry{AdvancesTo: "timeout-a"}},
	}
	joinB := JoinSpec{
		ID: "join-b", Stage: "awaiting-b",
		OnComplete: HandlerRuleEntry{AdvancesTo: "complete-b"},
		Timeout:    JoinTimeoutSpec{After: "2h", Outcome: HandlerRuleEntry{AdvancesTo: "timeout-b"}},
	}
	topology := BuildWorkflowStageTopology(
		"", "waiting",
		[]string{"waiting", "awaiting-a", "complete-a", "timeout-a", "awaiting-b", "complete-b", "timeout-b"},
		[]string{"complete-a", "timeout-a", "complete-b", "timeout-b"},
		[]HandlerTransitionSemantic{
			{NodeID: "join-node", EventType: "join.a.requested", Join: &joinA},
			{NodeID: "join-node", EventType: "join.b.requested", Join: &joinB},
		},
		nil,
		nil,
	)

	assertTargets := func(handler string, want ...string) {
		t.Helper()
		got := topology.HandlerTargets("join-node", handler)
		if len(got) != len(want) {
			t.Fatalf("HandlerTargets(%q) = %v, want %v", handler, got, want)
		}
		for idx := range want {
			if got[idx] != want[idx] {
				t.Fatalf("HandlerTargets(%q) = %v, want %v", handler, got, want)
			}
		}
	}
	assertTargets("join.a.requested", "complete-a", "timeout-a")
	assertTargets("join.b.requested", "complete-b", "timeout-b")

	for _, edge := range topology.Edges {
		if edge.Source != string(HandlerAdvanceCarrierJoinTimeout) {
			continue
		}
		if edge.EventType != "platform.join_timeout" {
			t.Fatalf("join timeout effective event = %q", edge.EventType)
		}
		if edge.HandlerEvent != "join.a.requested" && edge.HandlerEvent != "join.b.requested" {
			t.Fatalf("join timeout handler origin = %q", edge.HandlerEvent)
		}
	}
}

func TestWorkflowStageTopologyStampsLoopAndTimerOrigins(t *testing.T) {
	topology := BuildWorkflowStageTopology(
		"", "drafting", []string{"drafting", "review", "exhausted", "expired"}, []string{"exhausted", "expired"},
		nil,
		[]WorkflowTimerContract{{ID: "review.expire", Stage: "review", StageOwned: true, Event: WorkflowStageTimerInternalEvent, AdvancesTo: "expired"}},
		[]WorkflowLoopPlan{{
			ID: "revision", Escape: LoopEscapeSpec{AdvancesTo: "exhausted"},
			Operations: []WorkflowLoopOperationPlan{{NodeID: "review-node", HandlerEvent: "review.revision_requested", Kind: LoopOperationRepeat, From: "review"}},
		}},
	)
	for _, edge := range topology.Edges {
		switch edge.Source {
		case "loop.escape":
			if edge.HandlerEvent != "review.revision_requested" {
				t.Fatalf("loop escape origin = %q", edge.HandlerEvent)
			}
		case "timer":
			if edge.HandlerEvent != "" {
				t.Fatalf("stage timer handler origin = %q, want empty", edge.HandlerEvent)
			}
		}
	}
}

func TestTopologyEdgeIdentityIncludesHandlerOrigin(t *testing.T) {
	stages := map[string]struct{}{"waiting": {}, "done": {}}
	edges := appendTopologyEdge(nil, stages, WorkflowStageTopologyEdge{From: "waiting", To: "done", Source: "handler.join.timeout", NodeID: "node", HandlerEvent: "a", EventType: "platform.join_timeout"})
	edges = appendTopologyEdge(edges, stages, WorkflowStageTopologyEdge{From: "waiting", To: "done", Source: "handler.join.timeout", NodeID: "node", HandlerEvent: "b", EventType: "platform.join_timeout"})
	if len(edges) != 2 {
		t.Fatalf("edges = %#v, want distinct handler origins", edges)
	}
}
