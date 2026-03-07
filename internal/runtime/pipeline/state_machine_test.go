package pipeline

import "testing"

func TestCanTransitionPipelineStage(t *testing.T) {
	cases := []struct {
		from PipelineStage
		to   PipelineStage
		ok   bool
	}{
		{"", StageDiscovered, true},
		{StageDiscovered, StageScoring, true},
		{StageShortlisted, StageResearching, true},
		{StageReadyForReview, StageApproved, true},
		{StageApproved, StageOperating, true},
		{StageDiscovered, StageOperating, false},
		{StageKilled, StageApproved, false},
	}
	for _, tc := range cases {
		if got := CanTransitionPipelineStage(tc.from, tc.to); got != tc.ok {
			t.Fatalf("transition %s -> %s = %v, want %v", tc.from, tc.to, got, tc.ok)
		}
	}
}

func TestEmpirePipelineWorkflow_ExposesNamedTransitions(t *testing.T) {
	workflow := EmpirePipelineWorkflow()
	if workflow == nil {
		t.Fatal("expected workflow definition")
	}
	state := WorkflowState{Stage: StageShortlisted}
	transition, ok := workflow.Transition(state, StageResearching)
	if !ok {
		t.Fatal("expected shortlisted -> researching transition")
	}
	if transition.Name != "shortlisted_to_researching" {
		t.Fatalf("unexpected transition name: %s", transition.Name)
	}
	if len(transition.Actions) == 0 || transition.Actions[0].Name != "emit_validation_started" {
		t.Fatalf("unexpected transition actions: %+v", transition.Actions)
	}
}

func TestEmpirePipelineWorkflow_ExposesTerminalStages(t *testing.T) {
	workflow := EmpirePipelineWorkflow()
	stage, ok := workflow.Stage(StageKilled)
	if !ok {
		t.Fatal("expected killed stage")
	}
	if !stage.Terminal {
		t.Fatal("expected killed stage to be terminal")
	}
}
