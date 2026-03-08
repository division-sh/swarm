package pipeline

import "testing"

func TestEmpirePipelineWorkflow_LoadsFromContracts(t *testing.T) {
	workflow := DefaultPipelineWorkflow()
	if workflow == nil {
		t.Fatal("expected workflow definition")
	}
	if workflow.Name != "empire_vertical_pipeline" {
		t.Fatalf("unexpected workflow name: %s", workflow.Name)
	}
	for _, stage := range []PipelineStage{
		StageDiscovered,
		StageScoring,
		StageShortlisted,
		StageResearching,
		StageMVPSpeccing,
		StageCTOSpecReview,
		StageBranding,
		StageReadyForReview,
		StageApproved,
		StageKilled,
	} {
		if _, ok := workflow.Stage(stage); !ok {
			t.Fatalf("missing expected stage %s", stage)
		}
	}
}

func TestEmpirePipelineWorkflow_AllowsWildcardFactoryKill(t *testing.T) {
	workflow := DefaultPipelineWorkflow()
	if _, ok := workflow.Transition(WorkflowState{Stage: StageResearching}, StageKilled); !ok {
		t.Fatal("expected researching -> killed via wildcard transition")
	}
}

func TestEmpirePipelineWorkflow_SupportsSeedTransitions(t *testing.T) {
	workflow := DefaultPipelineWorkflow()
	for _, to := range []PipelineStage{StageDiscovered, StageScoring, StageResearching} {
		transition, ok := workflow.Transition(WorkflowState{}, to)
		if !ok {
			t.Fatalf("expected seed transition to %s", to)
		}
		if transition.Name == "" {
			t.Fatalf("expected named synthetic transition to %s", to)
		}
	}
}
