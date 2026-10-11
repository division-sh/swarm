package testtiming

import (
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func reviewedNativeRoot(seconds float64) RootTime {
	return RootTime{TimingCell: TimingCell{
		Package:     "github.com/division-sh/swarm/cmd/swarm-unused",
		Root:        "TestAttemptQualifiedNativeDownloadsUseRealUnionAndRefuseOldPartners",
		Environment: "ci-postgres-gateway-empty-v1", Count: "count-1",
	}, Seconds: seconds}
}

func TestTestTimeReviewedNativePlacementIsFiniteAndFullyCharged(t *testing.T) {
	base := []RootTime{timingRoot("TestOld", 2000)}
	reference := timingReferenceRoots(t, base, rootTimingPolicy())
	for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
		for _, mode := range []TestTimeMode{TestTimePR, TestTimeStrict} {
			t.Run(tier+"/"+string(mode), func(t *testing.T) {
				candidate := append(slices.Clone(base), reviewedNativeRoot(56.170))
				row, problems := compareTimingTier(reference, candidate, rootTimingPolicy(), tier, mode, true)
				approved := tier == testplanning.ProfileFull
				if row.AddedSeconds != 56.170 || math.Abs(row.Growth-56.170) > 1e-9 || row.Allowance != 100 || len(problems) != 0 || (row.StrictStatus == BudgetPass) != approved {
					t.Fatalf("native placement lost scope or full cost: %+v / %v", row, problems)
				}
				if approved && (len(row.Warnings) != 1 || !strings.Contains(row.Warnings[0], testTimeNativePlacementApproval) || !strings.Contains(row.Warnings[0], testTimeNativePlacementSource) || !strings.Contains(row.Warnings[0], "measurement run 38155342859 attempt 1") || !strings.Contains(row.Warnings[0], "original timing verdict RED")) {
					t.Fatalf("finite approval lost exact/red measurement provenance: %+v", row)
				}
				if !approved && (len(row.Warnings) != 1 || !strings.Contains(row.Warnings[0], "unreviewed placement is advisory")) {
					t.Fatalf("unapproved placement lost advisory attribution: %+v", row)
				}
				candidate[1].Seconds = 100.001
				row, problems = compareTimingTier(reference, candidate, rootTimingPolicy(), tier, mode, true)
				if len(problems) != 0 || row.AddedSeconds != 100.001 || row.StrictStatus != BudgetFail {
					t.Fatalf("native placement waived added-cost bound: %+v / %v", row, problems)
				}
				if approved && mode == TestTimeStrict && (len(row.Warnings) != 2 || !strings.Contains(row.Warnings[1], "strict aggregate growth is advisory")) {
					t.Fatalf("native full charge lost advisory counterfactual: %+v", row)
				}
			})
		}
	}
}

func TestTestTimeReviewedNativePlacementRejectsForeignCellsAndPromotion(t *testing.T) {
	root := reviewedNativeRoot(56.170)
	for name, change := range map[string]func(*TimingCell){
		"foreign package":     func(c *TimingCell) { c.Package += "/foreign" },
		"foreign root":        func(c *TimingCell) { c.Root += "Future" },
		"foreign environment": func(c *TimingCell) { c.Environment = "local-postgres-gateway-empty-v1" },
		"wrong count":         func(c *TimingCell) { c.Count = "cache-default" },
		"backend alias":       func(c *TimingCell) { c.Backend = "linux" },
	} {
		t.Run(name, func(t *testing.T) {
			cell := root.TimingCell
			change(&cell)
			for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
				if reviewedTimingPlacement(cell, tier) != "" {
					t.Fatal("foreign native cell inherited placement")
				}
			}
		})
	}
	for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, "", "unknown", "FULL"} {
		if reviewedTimingPlacement(root.TimingCell, tier) != "" {
			t.Fatalf("native placement accepted tier %q", tier)
		}
	}
	base := []RootTime{timingRoot("TestOld", 2000)}
	reference := append(timingReferenceRoots(t, base, rootTimingPolicy()), ReferenceRootTime{RootTime: root, Tiers: []string{testplanning.ProfileFull}})
	for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle} {
		row, problems := compareTimingTier(reference, append(slices.Clone(base), root), rootTimingPolicy(), tier, TestTimePR, true)
		if len(problems) != 0 || row.AddedSeconds != root.Seconds || row.StrictStatus != BudgetFail || len(row.Warnings) != 1 || !strings.Contains(row.Warnings[0], ">30s") || !strings.Contains(row.Warnings[0], "unreviewed placement is advisory") {
			t.Fatalf("native promotion inherited full approval: %+v / %v", row, problems)
		}
	}
}
