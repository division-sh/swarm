package empire

import (
	"sort"
	"strings"
)

func BuildScoringRequestedPayload(verticalID, verticalName, geography, mode, rubric string, dimensions []string, discoveryContext map[string]any) ScoringRequestedPayload {
	if dimensions == nil {
		dimensions = []string{}
	}
	if discoveryContext == nil {
		discoveryContext = map[string]any{}
	}
	return ScoringRequestedPayload{
		VerticalID:          strings.TrimSpace(verticalID),
		VerticalName:        strings.TrimSpace(verticalName),
		Geography:           strings.TrimSpace(geography),
		Mode:                strings.TrimSpace(mode),
		Rubric:              strings.TrimSpace(rubric),
		DimensionsRequested: append([]string{}, dimensions...),
		DiscoveryContext:    cloneMap(discoveryContext),
	}
}

func ResolveScoringAnalysisRecipient(recipients []string, excludedAgent string) string {
	if len(recipients) == 0 {
		return ""
	}
	sort.Strings(recipients)
	excludedAgent = strings.TrimSpace(excludedAgent)
	for _, recipient := range recipients {
		candidate := strings.TrimSpace(recipient)
		if candidate == "" || !strings.Contains(strings.ToLower(candidate), "analysis-agent") {
			continue
		}
		if excludedAgent != "" && strings.EqualFold(candidate, excludedAgent) {
			continue
		}
		return candidate
	}
	return ""
}

func BuildScoringContestedPayload(verticalID, dimension string, contest ContestedDimension, rubric, mode string) ScoringContestedPayload {
	return ScoringContestedPayload{
		VerticalID: strings.TrimSpace(verticalID),
		Dimension:  strings.TrimSpace(dimension),
		Scores:     append([]int{}, contest.Scores...),
		Evidence:   append([]string{}, contest.Evidence...),
		Spread:     contest.Spread,
		Rubric:     strings.TrimSpace(rubric),
		Mode:       strings.TrimSpace(mode),
	}
}

func BuildVerticalScoredPayload(verticalID string, result ScoringComposite, verticalName, geography, mode string) VerticalScoredPayload {
	out := VerticalScoredPayload{
		VerticalID:     strings.TrimSpace(verticalID),
		Result:         strings.TrimSpace(result.Result),
		Reason:         strings.TrimSpace(result.Reason),
		CompositeScore: result.CompositeScore,
		ViabilityScore: result.ViabilityScore,
		MarketScore:    result.MarketScore,
		Dimensions:     result.Dimensions,
		Rubric:         strings.TrimSpace(result.Rubric),
		Partial:        result.Partial,
		Mode:           strings.TrimSpace(mode),
		VerticalName:   strings.TrimSpace(verticalName),
		Geography:      strings.TrimSpace(geography),
	}
	if out.Dimensions == nil {
		out.Dimensions = map[string]ScoreDimensionResult{}
	}
	return out
}

func BuildVerticalShortlistedPayload(verticalID string, composite, viability float64, scoringPayload map[string]any) VerticalShortlistedPayload {
	if scoringPayload == nil {
		scoringPayload = map[string]any{}
	}
	return VerticalShortlistedPayload{
		VerticalID:     strings.TrimSpace(verticalID),
		CompositeScore: composite,
		ViabilityScore: viability,
		ScoringPayload: scoringPayload,
	}
}

func BuildVerticalMarginalPayload(verticalID string, result ScoringComposite) VerticalMarginalPayload {
	dim := result.Dimensions
	if dim == nil {
		dim = map[string]ScoreDimensionResult{}
	}
	return VerticalMarginalPayload{
		VerticalID:        strings.TrimSpace(verticalID),
		CompositeScore:    result.CompositeScore,
		ViabilityScore:    result.ViabilityScore,
		Dimensions:        dim,
		PromotionEligible: true,
	}
}

func BuildVerticalRejectedPayload(verticalID string, result ScoringComposite) VerticalRejectedPayload {
	return VerticalRejectedPayload{
		VerticalID: strings.TrimSpace(verticalID),
		Reason:     strings.TrimSpace(result.Reason),
	}
}
