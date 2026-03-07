package empire

import "testing"

func TestComputeComposite(t *testing.T) {
	input := ScoringAccumulatorInput{
		Rubric:   "universal",
		Expected: ExpectedScoringDimensions("universal"),
		Received: map[string]ScoreDimensionResult{},
	}
	for _, dim := range input.Expected {
		input.Received[dim] = ScoreDimensionResult{Score: 78, Evidence: "ok"}
	}
	res := ComputeComposite(input)
	if res.Result != "shortlisted" {
		t.Fatalf("expected shortlisted, got %+v", res)
	}
	if res.CompositeScore < 77.9 || res.CompositeScore > 78.1 {
		t.Fatalf("expected composite ~78, got %.3f", res.CompositeScore)
	}
}

func TestComputeComposite_RejectionCascade(t *testing.T) {
	expected := ExpectedScoringDimensions("universal")
	gate := ScoringAccumulatorInput{Rubric: "universal", Expected: expected, Received: map[string]ScoreDimensionResult{}}
	for _, dim := range expected {
		gate.Received[dim] = ScoreDimensionResult{Score: 80, Evidence: "ok"}
	}
	gate.Received["build_complexity"] = ScoreDimensionResult{Score: 40, Evidence: "gate"}
	if got := ComputeComposite(gate).Reason; got != "gate_build_complexity" {
		t.Fatalf("expected first cascade reject gate_build_complexity, got %q", got)
	}

	tier1 := ScoringAccumulatorInput{Rubric: "universal", Expected: expected, Received: map[string]ScoreDimensionResult{}}
	for _, dim := range expected {
		tier1.Received[dim] = ScoreDimensionResult{Score: 80, Evidence: "ok"}
	}
	tier1.Received["icp_crispness"] = ScoreDimensionResult{Score: 40, Evidence: "tier1"}
	if got := ComputeComposite(tier1).Reason; got != "tier1_dimension_floor_icp_crispness" {
		t.Fatalf("expected tier1 floor reject, got %q", got)
	}
}
