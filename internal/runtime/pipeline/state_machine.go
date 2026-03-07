package pipeline

import "strings"

type PipelineStage string

type PipelineTransition struct {
	From   PipelineStage `json:"from"`
	To     PipelineStage `json:"to"`
	Reason string        `json:"reason,omitempty"`
}

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

var allowedStageTransitions = map[PipelineStage]map[PipelineStage]struct{}{
	"": {
		StageDiscovered:  {},
		StageScoring:     {},
		StageResearching: {},
	},
	StageDiscovered:     {StageScoring: {}, StageResearching: {}, StageKilled: {}},
	StageScoring:        {StageShortlisted: {}, StageKilled: {}},
	StageShortlisted:    {StageResearching: {}, StageReadyForReview: {}, StageKilled: {}},
	StageResearching:    {StageMVPSpeccing: {}, StageReadyForReview: {}, StageKilled: {}},
	StageMVPSpeccing:    {StageSpecReview: {}, StageCTOSpecReview: {}, StageBranding: {}, StageKilled: {}},
	StageSpecReview:     {StageMVPSpeccing: {}, StageCTOSpecReview: {}, StageKilled: {}},
	StageCTOSpecReview:  {StageMVPSpeccing: {}, StageBranding: {}, StageKilled: {}},
	StageBranding:       {StageReadyForReview: {}, StageResearching: {}, StageKilled: {}},
	StageReadyForReview: {StageApproved: {}, StageResearching: {}, StageKilled: {}},
	StageApproved:       {StageOperating: {}, StageKilled: {}},
	StageOperating:      {StageKilled: {}},
	StageKilled:         {},
}

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
	if from == to {
		return true
	}
	allowed, ok := allowedStageTransitions[from]
	if !ok {
		allowed = allowedStageTransitions[""]
	}
	_, ok = allowed[to]
	return ok
}
