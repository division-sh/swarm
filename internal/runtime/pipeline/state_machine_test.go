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
