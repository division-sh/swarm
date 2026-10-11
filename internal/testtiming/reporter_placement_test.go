package testtiming

import (
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func reviewedReporterRoots() []RootTime {
	return []RootTime{
		{TimingCell: TimingCell{Package: "github.com/division-sh/swarm/internal/serveapp", Root: "TestIssue2394ServedOriginalReporterMeasuredIssuanceBothStores", Environment: "ci-postgres-gateway-empty-v1", Count: "count-1"}, Seconds: 37.63},
		{TimingCell: TimingCell{Package: "github.com/division-sh/swarm/internal/runtime/conformance", Root: "TestIssue2394ReporterFiveHundredServingWindowBothStores", Environment: "ci-postgres-gateway-empty-v1", Count: "count-1"}, Seconds: 36.50},
	}
}

func TestTestTimeReviewedReporterPlacementIsFiniteAndFullyCharged(t *testing.T) {
	base := []RootTime{timingRoot("TestOld", 2000)}
	reference := timingReferenceRoots(t, base, rootTimingPolicy())
	for _, root := range reviewedReporterRoots() {
		for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
			for _, mode := range []TestTimeMode{TestTimePR, TestTimeStrict} {
				t.Run(root.Root+"/"+tier+"/"+string(mode), func(t *testing.T) {
					candidate := append(slices.Clone(base), root)
					row, problems := compareTimingTier(reference, candidate, rootTimingPolicy(), tier, mode)
					approved := tier != testplanning.ProfileCore
					if row.AddedSeconds != root.Seconds || math.Abs(row.Growth-root.Seconds) > 1e-9 || row.Allowance != 100 || (len(problems) == 0) != approved {
						t.Fatalf("reporter placement lost scope or charged cost: %+v / %v", row, problems)
					}
					if approved {
						if len(row.Warnings) != 1 {
							t.Fatalf("missing single placement warning: %+v", row)
						}
						for _, provenance := range []string{testTimeReporterPlacementApproval, testTimeReporterPlacementSource, testTimeReporterPlacementPlan, "measured receipt", "sha256="} {
							if !strings.Contains(row.Warnings[0], provenance) {
								t.Fatalf("placement lost %q provenance: %+v", provenance, row)
							}
						}
						if strings.Contains(row.Warnings[0], "(run ") {
							t.Fatal("local measured receipt acquired a hosted run identity")
						}
					}
					candidate[1].Seconds = 100.001
					row, problems = compareTimingTier(reference, candidate, rootTimingPolicy(), tier, mode)
					if (len(problems) != 0) != (mode == TestTimePR || !approved) || row.AddedSeconds != 100.001 || math.Abs(row.Growth-100.001) > 1e-9 || row.Allowance != 100 || row.StrictStatus != BudgetFail {
						t.Fatalf("reporter placement or counterfactual lost aggregate cost: %+v / %v", row, problems)
					}
					if approved && mode == TestTimeStrict && (len(row.Warnings) != 2 || !strings.Contains(row.Warnings[0], testTimeReporterPlacementApproval) || !strings.Contains(row.Warnings[1], "strict aggregate growth is advisory")) {
						t.Fatalf("reporter advisory cost lost placement provenance: %+v", row)
					}
				})
			}
		}
	}
}

func TestTestTimeReviewedReporterPlacementRejectsForeignAndPromotedCells(t *testing.T) {
	for _, root := range reviewedReporterRoots() {
		for name, change := range map[string]func(*TimingCell){
			"foreign package":     func(c *TimingCell) { c.Package += "/foreign" },
			"foreign root":        func(c *TimingCell) { c.Root += "Future" },
			"old stress root":     func(c *TimingCell) { c.Root = "TestIssue2394ReporterFiveHundredDelayedCommitsBothStores" },
			"foreign environment": func(c *TimingCell) { c.Environment = "local-postgres-gateway-empty-v1" },
			"cached count":        func(c *TimingCell) { c.Count = "cache-default" },
			"backend alias":       func(c *TimingCell) { c.Backend = "postgres" },
		} {
			t.Run(root.Root+"/"+name, func(t *testing.T) {
				cell := root.TimingCell
				change(&cell)
				for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
					if reviewedTimingPlacement(cell, tier) != "" {
						t.Fatal("foreign cell inherited reporter placement")
					}
				}
			})
		}
		for _, tier := range []string{testplanning.ProfileCore, "", "unknown", "FULL"} {
			if reviewedTimingPlacement(root.TimingCell, tier) != "" {
				t.Fatalf("reporter placement accepted tier %q", tier)
			}
		}
		reference := timingReferenceRoots(t, []RootTime{timingRoot("TestOld", 2000)}, rootTimingPolicy())
		reference = append(reference, ReferenceRootTime{RootTime: root, Tiers: []string{testplanning.ProfileLifecycle, testplanning.ProfileFull}})
		row, problems := compareTimingTier(reference, []RootTime{timingRoot("TestOld", 2000), root}, rootTimingPolicy(), testplanning.ProfileCore, TestTimePR)
		if len(problems) != 1 || row.AddedSeconds != root.Seconds || !strings.Contains(problems[0], ">30s") {
			t.Fatalf("core promotion inherited reporter placement: %+v / %v", row, problems)
		}
	}
}

func TestTestTimeReporterParallelParentIsChargedOnce(t *testing.T) {
	for _, root := range reviewedReporterRoots() {
		unit := testplanning.ProofUnit{Run: "^" + root.Root + "$", SelectedRoots: []testplanning.TestRoot{{Package: root.Package, Name: root.Root}}}
		item := CommandEvidence{EnvironmentID: root.Environment, CountMode: root.Count, Report: Report{Tests: []TestTiming{
			{Package: root.Package, Test: root.Root, Elapsed: 0},
			{Package: root.Package, Test: root.Root + "/sqlite", Elapsed: root.Seconds},
			{Package: root.Package, Test: root.Root + "/postgres", Elapsed: root.Seconds - 1},
		}}}
		observed, err := unitRootTimes(unit, item)
		if err != nil || len(observed) != 1 || observed[0] != root {
			t.Fatalf("parallel parent lost conservative non-additive charge: %+v / %v", observed, err)
		}
	}
}
