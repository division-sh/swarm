package empire

import (
	"fmt"
	"strings"
)

var rubricDimensions = map[string][]string{
	"universal": {"build_complexity", "automation_completeness", "icp_crispness", "distribution_leverage", "time_to_value", "operational_drag", "pain_severity", "competition_gap", "monetization_clarity", "retention_architecture", "expansion_potential"},
}

var rubricWeights = map[string]map[string]float64{
	"universal": {
		"icp_crispness":          0.15,
		"distribution_leverage":  0.15,
		"time_to_value":          0.15,
		"operational_drag":       0.15,
		"pain_severity":          0.10,
		"competition_gap":        0.10,
		"monetization_clarity":   0.10,
		"retention_architecture": 0.05,
		"expansion_potential":    0.05,
	},
}

var tier1Dimensions = map[string][]string{"universal": {"icp_crispness", "distribution_leverage", "time_to_value", "operational_drag"}}
var tier1DimensionFloor = map[string]int{"universal": 50}
var tier1SubscoreFloor = map[string]float64{"universal": 60}

type scoringMarginalDrainRule struct {
	MinHighDims   int
	HighThreshold int
}

var marginalDrainRules = map[string]scoringMarginalDrainRule{"universal": {MinHighDims: 2, HighThreshold: 70}}

type scoringHardGate struct {
	Dimension string
	MinScore  int
	Reason    string
}

var rubricGates = map[string][]scoringHardGate{
	"universal": {
		{Dimension: "build_complexity", MinScore: 50, Reason: "gate_build_complexity"},
		{Dimension: "automation_completeness", MinScore: 50, Reason: "gate_automation_completeness"},
	},
}

func ExpectedScoringDimensions(rubric string) []string {
	dims := rubricDimensions[strings.TrimSpace(rubric)]
	if len(dims) == 0 {
		dims = rubricDimensions["universal"]
	}
	return append([]string{}, dims...)
}

func SelectScoringRubric(mode string) string {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case "automation_micro", "local_services", "saas_gap", "saas_trend", "corpus", "derived":
		return "universal"
	default:
		return "universal"
	}
}

func ComputeComposite(in ScoringAccumulatorInput) ScoringComposite {
	rubric := strings.TrimSpace(in.Rubric)
	if rubric == "" {
		rubric = "universal"
	}
	weights := rubricWeights[rubric]
	if len(weights) == 0 {
		weights = rubricWeights["universal"]
	}
	tier1Set := tier1Dimensions[rubric]
	if len(tier1Set) == 0 {
		tier1Set = tier1Dimensions["universal"]
	}
	floor := tier1DimensionFloor[rubric]
	if floor <= 0 {
		floor = tier1DimensionFloor["universal"]
	}
	subscoreFloor := tier1SubscoreFloor[rubric]
	if subscoreFloor <= 0 {
		subscoreFloor = tier1SubscoreFloor["universal"]
	}
	marginalRule, ok := marginalDrainRules[rubric]
	if !ok {
		marginalRule = marginalDrainRules["universal"]
	}
	dimensions := make(map[string]ScoreDimensionResult, len(in.Expected))
	composite, compositeWeight := 0.0, 0.0
	tier1Sum, tier1Weight := 0.0, 0.0
	marketSum, marketWeight := 0.0, 0.0
	for _, dim := range in.Expected {
		res, ok := in.Received[dim]
		if !ok {
			res = ScoreDimensionResult{Score: 0, Evidence: "missing_dimension_timeout"}
		}
		dimensions[dim] = res
		w := weights[dim]
		if w > 0 {
			composite += float64(res.Score) * w
			compositeWeight += w
		}
		if !dimensionInSet(tier1Set, dim) {
			if w > 0 {
				marketSum += float64(res.Score) * w
				marketWeight += w
			}
			continue
		}
		if w > 0 {
			tier1Sum += float64(res.Score) * w
			tier1Weight += w
		}
	}
	viability, market := 0.0, 0.0
	if tier1Weight > 0 {
		viability = tier1Sum / tier1Weight
	}
	if marketWeight > 0 {
		market = marketSum / marketWeight
	}
	if compositeWeight > 0 {
		composite = composite / compositeWeight
	}
	if gates, ok := rubricGates[rubric]; ok {
		for _, gate := range gates {
			if res, exists := dimensions[gate.Dimension]; exists && res.Score < gate.MinScore {
				return ScoringComposite{Result: "rejected", Reason: gate.Reason, CompositeScore: composite, ViabilityScore: viability, MarketScore: market, Dimensions: dimensions, Rubric: rubric, Partial: in.Partial}
			}
		}
	}
	for _, dim := range tier1Set {
		if res, exists := dimensions[dim]; exists && res.Score < floor {
			return ScoringComposite{Result: "rejected", Reason: fmt.Sprintf("tier1_dimension_floor_%s", strings.TrimSpace(dim)), CompositeScore: composite, ViabilityScore: viability, MarketScore: market, Dimensions: dimensions, Rubric: rubric, Partial: in.Partial}
		}
	}
	if viability < subscoreFloor {
		return ScoringComposite{Result: "rejected", Reason: "viability_floor_execution_fit", CompositeScore: composite, ViabilityScore: viability, MarketScore: market, Dimensions: dimensions, Rubric: rubric, Partial: in.Partial}
	}
	if composite < 55 {
		return ScoringComposite{Result: "rejected", Reason: "composite_below_threshold", CompositeScore: composite, ViabilityScore: viability, MarketScore: market, Dimensions: dimensions, Rubric: rubric, Partial: in.Partial}
	}
	out := ScoringComposite{Result: "marginal", CompositeScore: composite, ViabilityScore: viability, MarketScore: market, Dimensions: dimensions, Rubric: rubric, Partial: in.Partial}
	if composite >= 75 {
		out.Result = "shortlisted"
		return out
	}
	highCount := 0
	for _, dim := range tier1Set {
		if res, exists := dimensions[dim]; exists && res.Score >= marginalRule.HighThreshold {
			highCount++
		}
	}
	if highCount < marginalRule.MinHighDims {
		out.Result = "rejected"
		out.Reason = "marginal_drain"
	}
	return out
}

func dimensionInSet(set []string, dim string) bool {
	dim = strings.TrimSpace(dim)
	if dim == "" {
		return false
	}
	for _, item := range set {
		if strings.TrimSpace(item) == dim {
			return true
		}
	}
	return false
}
