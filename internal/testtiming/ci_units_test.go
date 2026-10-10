package testtiming

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestCIUnitsTimingAddsFrozenFullWithoutReprojectingLowerTiers(t *testing.T) {
	plan, evidence := rootEvidenceFixture(t)
	plan.Profile = testplanning.ProfileLifecycle
	plan.ExtraUnits = []string{plan.Units[0].ID}
	plan.Units[0].WorkloadProfile = plan.Profile
	plan.Digest = ""
	raw, _ := json.Marshal(plan)
	digest := sha256.Sum256(raw)
	plan.Digest = hex.EncodeToString(digest[:])
	evidence.Profile, evidence.WorkloadProfile, evidence.PlanDigest = plan.Profile, plan.Profile, plan.Digest
	pkg := plan.Units[0].Packages[0]
	cell := TimingCell{Package: pkg, Environment: evidence.EnvironmentID, Count: evidence.CountMode}
	policy := rootTimingPolicy()
	policy.SpecialPackages = []string{pkg}
	policy.Units = map[string]testplanning.UnitPolicy{plan.Units[0].ID: {Packages: []string{pkg}, Run: "^TestParent$"}}
	policy.Profiles[testplanning.ProfileFull] = testplanning.ProfilePolicy{Units: plan.ExtraUnits}
	ordinary, removed := cell, cell
	ordinary.Root, removed.Root = "TestOrdinary", "TestRemoved"
	reference := TestTimeReference{RunID: TestTimeReferenceRunID, Source: plan.HeadSHA, BuildContext: plan.BuildContext, Roots: []ReferenceRootTime{
		{RootTime: RootTime{TimingCell: ordinary, Seconds: 100}, Tiers: []string{"core", "lifecycle", "full"}},
		{RootTime: RootTime{TimingCell: removed, Seconds: 1000}, Tiers: []string{"full"}},
	}}
	for _, tc := range []struct {
		name string
		base bool
		cost float64
		want BudgetStatus
	}{
		{"retained", true, 10, BudgetPass},
		{"growth", true, 170, BudgetFail},
		{"new-small", false, 20, BudgetPass},
		{"new-over-placement", false, 31, BudgetFail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref := reference
			ref.Roots = slices.Clone(reference.Roots)
			if tc.base {
				parent := cell
				parent.Root = "TestParent"
				ref.Roots = append(ref.Roots, ReferenceRootTime{RootTime: RootTime{TimingCell: parent, Seconds: 100}, Tiers: []string{"full"}})
			}
			receipt := evidence
			receipt.Report.Tests = slices.Clone(evidence.Report.Tests)
			receipt.Report.Tests[0].Elapsed = tc.cost
			got := EvaluateTestTime(ref, policy, plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt})
			if got.Status != tc.want || len(got.Tiers) != 3 {
				t.Fatalf("supplement cost bypassed full row: %+v", got)
			}
			for _, row := range got.Tiers[:2] {
				if row.Baseline != 100 || row.Candidate != 100 || row.Growth != 0 || len(row.Added) != 0 {
					t.Fatalf("frozen lower-tier projection changed: %+v", row)
				}
			}
			if len(got.Tiers[2].Removed) != 2 {
				t.Fatal("absent full work earned removal credit")
			}
		})
	}
	for _, change := range []string{"missing", "skipped", "foreign-unit", "foreign-attempt", "foreign-source", "wrong-count"} {
		t.Run(change, func(t *testing.T) {
			receipt := evidence
			receipt.Report.Tests = slices.Clone(evidence.Report.Tests)
			items := []CommandEvidence{receipt}
			switch change {
			case "missing":
				items = nil
			case "skipped":
				receipt.Report.Tests[0].Result = "skip"
			case "foreign-unit":
				receipt.UnitID = "foreign"
			case "foreign-attempt":
				receipt.WorkflowAttempt++
			case "foreign-source":
				receipt.HeadSHA = "foreign"
			case "wrong-count":
				receipt.CountMode = "cache-default"
			}
			if change != "missing" {
				items[0] = receipt
			}
			if got := EvaluateTestTime(reference, policy, plan, TestTimeReferenceRunID, 1, items); got.Status != BudgetIncomplete {
				t.Fatalf("invalid supplement granted timing credit: %+v", got)
			}
		})
	}
}
