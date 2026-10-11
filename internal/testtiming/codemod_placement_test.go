package testtiming

import (
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func reviewedCodemodRoot(name string, seconds float64) RootTime {
	root := timingRoot(name, seconds)
	root.Package = "github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations"
	return root
}

func TestTestTimeReviewedCodemodPlacementIsFiniteAndFullyCharged(t *testing.T) {
	roots := []string{
		"TestReviewedSnapshotCandidateOverlayTypeChecks",
		"TestNativeTimerSuccessorObserverRejectsRepublicationBothStores",
		"TestNativeHandlerExecutionWindowRejectsEarlyProductionDispatchBothStores",
		"TestNativeBusSourceBoundaryRejectsLostProductionClaimAndSettlementFacts",
		"TestNativeBusOriginAssertionFailureJoinsBlockedSQLAndRuntimeOwners",
	}
	base := []RootTime{timingRoot("TestOld", 2000)}
	reference := timingReferenceRoots(t, base, rootTimingPolicy())
	for _, name := range roots {
		for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
			for _, mode := range []TestTimeMode{TestTimePR, TestTimeStrict} {
				t.Run(name+"/"+tier+"/"+string(mode), func(t *testing.T) {
					approved := tier == testplanning.ProfileFull || tier == testplanning.ProfileLifecycle && name != roots[0]
					candidate := append(slices.Clone(base), reviewedCodemodRoot(name, 40))
					row, problems := compareTimingTier(reference, candidate, rootTimingPolicy(), tier, mode)
					if row.AddedSeconds != 40 || row.Growth != 40 || row.Allowance != 100 || (len(problems) == 0) != approved {
						t.Fatalf("placement lost scope or cost: %+v / %v", row, problems)
					}
					if approved && (len(row.Warnings) != 1 || !strings.Contains(row.Warnings[0], testTimeCodemodPlacementApproval) || !strings.Contains(row.Warnings[0], testTimeCodemodPlacementSource)) {
						t.Fatalf("approved disposition lost exact provenance: %+v", row)
					}
					candidate[1].Seconds = 100.001
					row, problems = compareTimingTier(reference, candidate, rootTimingPolicy(), tier, mode)
					if len(problems) == 0 || row.AddedSeconds != 100.001 || row.StrictStatus != BudgetFail {
						t.Fatalf("placement waived aggregate bounds: %+v / %v", row, problems)
					}
				})
			}
		}
	}
}

func TestTestTimeReviewedCodemodPlacementRejectsForeignAndPromotedCells(t *testing.T) {
	root := reviewedCodemodRoot("TestNativeTimerSuccessorObserverRejectsRepublicationBothStores", 40)
	for name, change := range map[string]func(*TimingCell){
		"foreign package":     func(c *TimingCell) { c.Package += "/foreign" },
		"foreign name":        func(c *TimingCell) { c.Root += "Future" },
		"foreign environment": func(c *TimingCell) { c.Environment = "local-postgres-gateway-empty-v1" },
		"cached count":        func(c *TimingCell) { c.Count = "cache-default" },
		"backend alias":       func(c *TimingCell) { c.Backend = "postgres" },
	} {
		t.Run(name, func(t *testing.T) {
			cell := root.TimingCell
			change(&cell)
			for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
				if hasReviewedCodemodPlacement(cell, tier) {
					t.Fatal("foreign cell inherited a finite placement approval")
				}
			}
		})
	}
	for _, tier := range []string{testplanning.ProfileCore, "", "unknown", "FULL"} {
		if hasReviewedCodemodPlacement(root.TimingCell, tier) {
			t.Fatalf("unapproved tier accepted: %q", tier)
		}
	}
	// A future move back into core remains newly promoted work, not a receipt
	// retained merely because it once ran in full.
	reference := timingReferenceRoots(t, []RootTime{timingRoot("TestOld", 2000)}, rootTimingPolicy())
	reference = append(reference, ReferenceRootTime{RootTime: root, Tiers: []string{testplanning.ProfileLifecycle, testplanning.ProfileFull}})
	row, problems := compareTimingTier(reference, []RootTime{timingRoot("TestOld", 2000), root}, rootTimingPolicy(), testplanning.ProfileCore, TestTimePR)
	if len(problems) != 1 || row.AddedSeconds != 40 || !strings.Contains(problems[0], ">30s") {
		t.Fatalf("core promotion inherited full-tier approval: %+v / %v", row, problems)
	}
}
