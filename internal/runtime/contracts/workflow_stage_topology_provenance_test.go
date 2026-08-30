package contracts

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

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
	joinNode := identitytest.RootNode(t, "join-node")
	topology := BuildWorkflowStageTopology(
		"", "waiting",
		[]string{"waiting", "awaiting-a", "complete-a", "timeout-a", "awaiting-b", "complete-b", "timeout-b"},
		[]string{"complete-a", "timeout-a", "complete-b", "timeout-b"},
		[]HandlerTransitionSemantic{
			{Node: joinNode, EventType: "join.a.requested", Join: &joinA},
			{Node: joinNode, EventType: "join.b.requested", Join: &joinB},
		},
		nil,
		nil,
	)

	assertTargets := func(handler string, want ...string) {
		t.Helper()
		got := topology.HandlerTargets(joinNode, handler)
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
	reviewNode := identitytest.RootNode(t, "review-node")
	topology := BuildWorkflowStageTopology(
		"", "drafting", []string{"drafting", "review", "exhausted", "expired"}, []string{"exhausted", "expired"},
		nil,
		[]WorkflowTimerContract{{ID: "review.expire", Stage: "review", StageOwned: true, Event: WorkflowStageTimerInternalEvent, AdvancesTo: "expired"}},
		[]WorkflowLoopPlan{{
			ID: "revision", Escape: LoopEscapeSpec{AdvancesTo: "exhausted"},
			Operations: []WorkflowLoopOperationPlan{{Node: reviewNode, HandlerEvent: "review.revision_requested", Kind: LoopOperationRepeat, From: "review"}},
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
	node := identitytest.RootNode(t, "node")
	edges := appendTopologyEdge(nil, stages, WorkflowStageTopologyEdge{From: "waiting", To: "done", Source: "handler.join.timeout", Node: node, HandlerEvent: "a", EventType: "platform.join_timeout"})
	edges = appendTopologyEdge(edges, stages, WorkflowStageTopologyEdge{From: "waiting", To: "done", Source: "handler.join.timeout", Node: node, HandlerEvent: "b", EventType: "platform.join_timeout"})
	if len(edges) != 2 {
		t.Fatalf("edges = %#v, want distinct handler origins", edges)
	}
}

func TestWorkflowStageTopologyScopesDeliveryJoinCompletionToHandlerLifecycle(t *testing.T) {
	delivery := JoinSpec{
		ID: "all-delivered", Members: JoinMembersSpec{FromFanOut: true, fromFanOutFound: true},
		OnComplete: HandlerRuleEntry{AdvancesTo: "done"}, OnCompleteFound: true,
	}
	for _, tc := range []struct {
		name   string
		flowID string
		nodeID string
	}{
		{name: "root", nodeID: "dispatcher"},
		{name: "nested flow", flowID: "child", nodeID: "dispatcher"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := identitytest.RootNode(t, tc.nodeID)
			if tc.flowID != "" {
				node = identitytest.FlowNode(t, tc.flowID, tc.nodeID)
			}
			topology := BuildWorkflowStageTopology(
				tc.flowID, "active", []string{"active", "waiting", "done"}, []string{"done"},
				[]HandlerTransitionSemantic{{Node: node, EventType: "batch.requested", Join: &delivery}}, nil, nil,
			)
			var got []WorkflowStageTopologyEdge
			for _, edge := range topology.Edges {
				if edge.Source == string(HandlerAdvanceCarrierJoinOnComplete) {
					got = append(got, edge)
				}
			}
			if len(got) != 2 || got[0].From != "active" || got[1].From != "waiting" || got[0].To != "done" || got[1].To != "done" {
				t.Fatalf("delivery join completion edges = %#v, want every ordinary nonterminal handler stage -> done", got)
			}
		})
	}
}

func TestWorkflowStageTopologyKeepsDeliveryJoinCompletionInsideLoopRegion(t *testing.T) {
	delivery := JoinSpec{
		ID: "all-delivered", Members: JoinMembersSpec{FromFanOut: true, fromFanOutFound: true},
		OnComplete: HandlerRuleEntry{AdvancesTo: "drafting"}, OnCompleteFound: true,
	}
	node := identitytest.FlowNode(t, "child", "dispatcher")
	topology := BuildWorkflowStageTopology(
		"child", "drafting", []string{"drafting", "review", "done"}, []string{"done"},
		[]HandlerTransitionSemantic{
			{Node: node, EventType: "draft.ready", AdvancesTo: "review", Loop: &LoopOperationSpec{Admit: "revision", From: "drafting"}},
			{Node: node, EventType: "batch.requested", Join: &delivery, Loop: &LoopOperationSpec{Repeat: "revision", From: "review"}},
		}, nil,
		[]WorkflowLoopPlan{{
			FlowID: "child", ID: "revision", Escape: LoopEscapeSpec{AdvancesTo: "done"},
			Operations: []WorkflowLoopOperationPlan{{Node: node, HandlerEvent: "batch.requested", Kind: LoopOperationRepeat, From: "review"}},
		}},
	)
	component := topology.StronglyConnectedComponent("review")
	if len(component) != 2 || component[0] != "drafting" || component[1] != "review" {
		t.Fatalf("delivery join loop component = %#v, want [drafting review]", component)
	}
	var completion WorkflowStageTopologyEdge
	for _, edge := range topology.Edges {
		if edge.Source == "loop.repeat" && edge.HandlerEvent == "batch.requested" && edge.To == "drafting" {
			completion = edge
		}
	}
	if completion.From != "review" || completion.LoopID != "revision" || completion.LoopOperation != LoopOperationRepeat {
		t.Fatalf("delivery join loop completion edge = %#v", completion)
	}
}
