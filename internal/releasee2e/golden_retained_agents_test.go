package releasee2e

import "testing"

func TestGoldenRetainedIdleAgentProofRejectsMissingDuplicateAndForeignActors(t *testing.T) {
	scout := goldenAgentSummary{AgentID: "scout-worker", Role: "golden_scout", FlowInstance: "scout", ExecutionMode: "mock", Status: "idle"}
	candidate := goldenAgentSummary{AgentID: "candidate-worker", Role: "golden_candidate", FlowInstance: "candidate/one", ExecutionMode: "mock", Status: "idle"}
	expected := []goldenRetainedAgentExpectation{
		{runID: "first", summary: scout}, {runID: "first", summary: candidate},
		{runID: "second", summary: scout}, {runID: "second", summary: candidate},
	}
	valid := []goldenAgentSummary{candidate, scout, candidate, scout}
	if !goldenRetainedIdleAgentSetMatches(valid, expected) {
		t.Fatal("equal public paths in two runs lost their exact multiplicity")
	}
	for _, kind := range []string{"missing", "duplicate", "foreign-path", "wrong-role", "wrong-mode", "running", "stopped"} {
		t.Run(kind, func(t *testing.T) {
			changed := append([]goldenAgentSummary(nil), valid...)
			switch kind {
			case "missing":
				changed = changed[:len(changed)-1]
			case "duplicate":
				changed[0] = scout
			case "foreign-path":
				changed[0].FlowInstance = "candidate/foreign"
			case "wrong-role":
				changed[0].Role = "golden_scout"
			case "wrong-mode":
				changed[0].ExecutionMode = "live"
			case "running", "stopped":
				changed[0].Status = kind
			}
			if goldenRetainedIdleAgentSetMatches(changed, expected) {
				t.Fatalf("%s actor set was accepted: %#v", kind, changed)
			}
		})
	}
}
