package pipeline

import "strings"

type PipelineStage string

var (
	// Deprecated compatibility aliases. Generic MAS state should come from
	// workflow_instances.current_state and the active flow contract bundle.
	StageDiscovered     = PipelineStage("discovered")
	StageScoring        = PipelineStage("scoring")
	StageShortlisted    = PipelineStage("shortlisted")
	StageResearching    = PipelineStage("researching")
	StageMVPSpeccing    = PipelineStage("mvp_speccing")
	StageSpecReview     = PipelineStage("spec_review")
	StageCTOSpecReview  = PipelineStage("cto_spec_review")
	StageBranding       = PipelineStage("branding")
	StageReadyForReview = PipelineStage("ready_for_review")
	StageApproved       = PipelineStage("approved")
	StageOperating      = PipelineStage("operating")
	StageKilled         = PipelineStage("killed")
)

func NormalizePipelineStage(raw string) PipelineStage {
	return PipelineStage(strings.TrimSpace(raw))
}

func CanTransitionPipelineStage(from, to PipelineStage) bool {
	workflow := DefaultPipelineWorkflow()
	if workflow == nil {
		return from == to
	}
	return workflow.CanTransition(WorkflowState{Stage: NormalizePipelineStage(string(from))}, NormalizePipelineStage(string(to)))
}

func PipelineWorkflowTransition(from, to PipelineStage) (WorkflowTransition, bool) {
	workflow := DefaultPipelineWorkflow()
	if workflow == nil {
		return WorkflowTransition{}, false
	}
	return workflow.Transition(WorkflowState{Stage: NormalizePipelineStage(string(from))}, NormalizePipelineStage(string(to)))
}
