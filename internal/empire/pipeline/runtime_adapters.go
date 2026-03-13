package empire

import runtimepipeline "empireai/internal/runtime/pipeline"

func runtimeValidationSnapshot(s runtimepipeline.ValidationContextSnapshot) ValidationContextSnapshot {
	return ValidationContextSnapshot{
		Research:    s.Research,
		Spec:        s.Spec,
		CTONotes:    s.CTONotes,
		Brand:       s.Brand,
		Scoring:     s.Scoring,
		SpecVersion: s.SpecVersion,
	}
}

func runtimeScoreDimensionResultMap(in map[string]runtimepipeline.ScoreDimensionResult) map[string]ScoreDimensionResult {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]ScoreDimensionResult, len(in))
	for key, value := range in {
		out[key] = ScoreDimensionResult{
			Score:      value.Score,
			Evidence:   value.Evidence,
			Confidence: value.Confidence,
		}
	}
	return out
}

func runtimeContestedMap(in map[string]runtimepipeline.ContestedDimension) map[string]ContestedDimension {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]ContestedDimension, len(in))
	for key, value := range in {
		out[key] = runtimeContestedDimension(value)
	}
	return out
}

func runtimeContestedDimension(in runtimepipeline.ContestedDimension) ContestedDimension {
	options := make([]ScoreDimensionResult, 0, len(in.Options))
	for _, option := range in.Options {
		options = append(options, ScoreDimensionResult{
			Score:      option.Score,
			Evidence:   option.Evidence,
			Confidence: option.Confidence,
		})
	}
	return ContestedDimension{
		Dimension: in.Dimension,
		Scores:    append([]int(nil), in.Scores...),
		Evidence:  append([]string(nil), in.Evidence...),
		Spread:    in.Spread,
		Options:   options,
	}
}

func runtimeScoringComposite(in runtimepipeline.ScoringComposite) ScoringComposite {
	return ScoringComposite{
		Result:         in.Result,
		Reason:         in.Reason,
		CompositeScore: in.CompositeScore,
		ViabilityScore: in.ViabilityScore,
		MarketScore:    in.MarketScore,
		Dimensions:     runtimeScoreDimensionResultMap(in.Dimensions),
		Rubric:         in.Rubric,
		Partial:        in.Partial,
	}
}

func runtimeScoringAccumulatorInput(in runtimepipeline.ScoringAccumulatorInput) ScoringAccumulatorInput {
	return ScoringAccumulatorInput{
		Rubric:    in.Rubric,
		Expected:  append([]string(nil), in.Expected...),
		Received:  runtimeScoreDimensionResultMap(in.Received),
		Partial:   in.Partial,
	}
}

func runtimepipelineScoreDimensionResultMap(in map[string]ScoreDimensionResult) map[string]runtimepipeline.ScoreDimensionResult {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]runtimepipeline.ScoreDimensionResult, len(in))
	for key, value := range in {
		out[key] = runtimepipeline.ScoreDimensionResult{
			Score:      value.Score,
			Evidence:   value.Evidence,
			Confidence: value.Confidence,
		}
	}
	return out
}

func runtimeScanCompletedBuildInput(in runtimepipeline.ScanCompletedBuildInput) ScanCompletedBuildInput {
	return ScanCompletedBuildInput{
		ScanID:          in.ScanID,
		CampaignID:      in.CampaignID,
		Mode:            in.Mode,
		Geography:       in.Geography,
		ReportsReceived: in.ReportsReceived,
		Expected:        in.Expected,
		Complete:        in.Complete,
		Discovered:      in.Discovered,
		Skipped:         in.Skipped,
		PendingDedup:    in.PendingDedup,
		TimedOut:        in.TimedOut,
		ShardsTotal:     in.ShardsTotal,
		ShardsCompleted: in.ShardsCompleted,
		ShardsFailed:    in.ShardsFailed,
	}
}
