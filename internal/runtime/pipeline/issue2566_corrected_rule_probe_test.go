package pipeline

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestIssue2566CorrectedMarkedEndTimerStartMatcher(t *testing.T) {
	graph := contracts.BuildWorkflowStageTopology(".", "waiting", []string{"waiting", "done"}, []string{"done"}, nil, nil, nil)
	end, err := graph.ResolveStage("done")
	if err != nil || !end.IsTerminal() {
		t.Fatalf("probe must use the actual marked end: %v", err)
	}
	timer := contracts.WorkflowTimerContract{ID: "done.notice", Stage: "done", StageOwned: true, Event: "notice", Delay: "1h"}
	if !workflowTimerShouldStartOnTransition(timer, "waiting", "done", "work.done") {
		t.Fatal("baseline witness no longer reproduces; refresh the no-arm consumer census")
	}
	t.Log("current start matcher selects the final-stage timer on entry; this focused matcher witness is not a served/both-store execution claim")
}
