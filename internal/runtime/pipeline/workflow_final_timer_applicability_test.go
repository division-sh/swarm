package pipeline

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestWorkflowFinalTimerApplicabilityUsesExactCatalog(t *testing.T) {
	root := contracts.BuildWorkflowStageTopology(".", "waiting", []string{"waiting", "Done"}, []string{"Done"}, nil, nil, nil)
	child := contracts.BuildWorkflowStageTopology("child", "waiting", []string{"waiting", "Done"}, nil, nil, nil, nil)
	stateless := contracts.BuildWorkflowStageTopology("stateless", "", nil, nil, nil, nil, nil)
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{
		StageTopologies: map[string]contracts.WorkflowStageTopology{".": root, "child": child, "stateless": stateless},
	}})
	for _, owned := range []bool{false, true} {
		for _, tc := range []struct {
			flow, stage string
			mayArm      bool
			valid       bool
		}{
			{".", "waiting", true, true},
			{".", "Done", false, true},
			{".", "done", false, false},
			{".", "pending", false, false},
			{"child", "Done", true, true},
			{"stateless", "pending", true, true},
			{"stateless", "Pending", false, false},
			{"unknown", "Done", false, false},
		} {
			timer := contracts.WorkflowTimerContract{ID: "check", FlowID: tc.flow, Stage: tc.stage, StageOwned: owned}
			mayArm, err := workflowTimerMayArmAtStage(source, timer, tc.stage)
			if (err == nil) != tc.valid || mayArm != tc.mayArm {
				t.Fatalf("flow=%q stage=%q stageOwned=%t: mayArm=%t err=%v", tc.flow, tc.stage, owned, mayArm, err)
			}
		}
	}
	if _, err := workflowTimerMayArmAtStage(nil, contracts.WorkflowTimerContract{ID: "check", FlowID: "."}, "waiting"); err == nil {
		t.Fatal("missing source catalog treated as eligibility")
	}
}

func TestWorkflowFinalTimerNoArmDoesNotChangeAcceptedTopologyValidation(t *testing.T) {
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{
		StageTopologies: map[string]contracts.WorkflowStageTopology{".": contracts.BuildWorkflowStageTopology(".", "waiting", []string{"waiting", "done"}, []string{"done"}, nil, nil, nil)},
	}})
	timer := contracts.WorkflowTimerContract{ID: "already-accepted", FlowID: ".", Stage: "waiting", StageOwned: true}
	if mayArm, err := workflowTimerMayArmAtStage(source, timer, "done"); err != nil || mayArm {
		t.Fatalf("final stage can newly arm: %t %v", mayArm, err)
	}
	if err := validateWorkflowTimerTopology(source, timer); err != nil {
		t.Fatalf("new-arm applicability leaked into accepted topology validation: %v", err)
	}
}
