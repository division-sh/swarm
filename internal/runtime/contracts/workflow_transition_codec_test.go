package contracts

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestCompiledTransitionCodecRejectsContradictoryCoordinates(t *testing.T) {
	node, err := identity.AdmitExecutableNodeDeclaration(".", "arbitrary_receiver")
	if err != nil {
		t.Fatal(err)
	}
	graph := BuildWorkflowStageTopology(".", "ready", []string{"ready", "done"}, []string{"done"},
		[]HandlerTransitionSemantic{{Node: node, EventType: "advance", AdvancesTo: "done"}}, nil, nil)
	value, err := graph.AdmitTransition(WorkflowTransitionSite{Node: node, HandlerEvent: "advance", AdvanceCarrier: HandlerAdvanceCarrierHandler}, "ready", "done")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var original map[string]any
	if err := json.Unmarshal(raw, &original); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, key string
		value     any
	}{
		{"foreign_flow", "Flow", "child"},
		{"protocol_event", "EventType", "platform.join_timeout"},
		{"handler_whitespace", "HandlerEvent", " advance"},
		{"timer_on_handler", "TimerID", "foreign"},
		{"delay_on_handler", "After", "1h"},
		{"timed_handler", "Timed", true},
		{"operation_without_loop", "LoopOperation", "repeat"},
		{"loop_without_operation", "LoopID", "revision"},
		{"foreign_decision", "DecisionID", "review"},
		{"foreign_verdict", "Verdict", "approve"},
		{"runtime_owner", "InternalOwner", "runtime"},
		{"unknown_member", "FutureCompatibility", true},
		{"foreign_source", "Source", "timer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := make(map[string]any, len(original)+1)
			for key, v := range original {
				fields[key] = v
			}
			fields[tc.key] = tc.value
			bad, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			target := value
			if err := json.Unmarshal(bad, &target); err == nil {
				t.Fatal("contradictory carrier hydrated")
			}
			if target != value {
				t.Fatal("failed hydration mutated prior admitted value")
			}
		})
	}
	var roundtrip CompiledTransition
	if err := json.Unmarshal(raw, &roundtrip); err != nil || roundtrip != value {
		t.Fatalf("roundtrip: %v", err)
	}
	if err := roundtrip.ValidateAgainst(graph); err != nil {
		t.Fatal(err)
	}
	for _, malformedFirst := range []bool{false, true} {
		hostile := graph
		hostile.Edges = append([]WorkflowStageTopologyEdge(nil), graph.Edges...)
		hostile.Edges = append(hostile.Edges, graph.Edges[0])
		if malformedFirst {
			hostile.Edges[0].EventType = "wrong.protocol"
		}
		if _, err := hostile.AdmitTransition(value.Edge().Site(), "ready", "done"); err == nil {
			t.Fatalf("ambiguous or malformed matching carrier admitted, malformed first=%v", malformedFirst)
		}
	}
}
