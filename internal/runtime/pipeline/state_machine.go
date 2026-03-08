package pipeline

import "strings"

type PipelineStage string

const (
	StageDiscovered     PipelineStage = "discovered"
	StageScoring        PipelineStage = "scoring"
	StageShortlisted    PipelineStage = "shortlisted"
	StageResearching    PipelineStage = "researching"
	StageMVPSpeccing    PipelineStage = "mvp_speccing"
	StageSpecReview     PipelineStage = "spec_review"
	StageCTOSpecReview  PipelineStage = "cto_spec_review"
	StageBranding       PipelineStage = "branding"
	StageReadyForReview PipelineStage = "ready_for_review"
	StageApproved       PipelineStage = "approved"
	StageOperating      PipelineStage = "operating"
	StageKilled         PipelineStage = "killed"
)

func NormalizePipelineStage(raw string) PipelineStage {
	switch strings.TrimSpace(raw) {
	case string(StageDiscovered):
		return StageDiscovered
	case string(StageScoring):
		return StageScoring
	case string(StageShortlisted):
		return StageShortlisted
	case string(StageResearching):
		return StageResearching
	case string(StageMVPSpeccing):
		return StageMVPSpeccing
	case string(StageSpecReview):
		return StageSpecReview
	case string(StageCTOSpecReview):
		return StageCTOSpecReview
	case string(StageBranding):
		return StageBranding
	case string(StageReadyForReview):
		return StageReadyForReview
	case string(StageApproved):
		return StageApproved
	case string(StageOperating):
		return StageOperating
	case string(StageKilled):
		return StageKilled
	default:
		return PipelineStage(strings.TrimSpace(raw))
	}
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
