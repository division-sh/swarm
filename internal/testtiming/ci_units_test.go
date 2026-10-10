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
	plan.Digest = ""
	raw, _ := json.Marshal(plan)
	digest := sha256.Sum256(raw)
	plan.Digest = hex.EncodeToString(digest[:])
	evidence.Profile, evidence.PlanDigest = plan.Profile, plan.Digest
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
		{"retained-growth-advisory", true, 170, BudgetWarn},
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
			got := EvaluateTestTime(ref, policy, plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt}, TestTimePR)
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
	for _, change := range []string{"missing", "skipped", "foreign-unit", "foreign-attempt", "foreign-source", "wrong-count", "downgraded-workload"} {
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
			case "downgraded-workload":
				receipt.WorkloadProfile = testplanning.ProfileLifecycle
			}
			if change != "missing" {
				items[0] = receipt
			}
			if got := EvaluateTestTime(reference, policy, plan, TestTimeReferenceRunID, 1, items, TestTimePR); got.Status != BudgetIncomplete {
				t.Fatalf("invalid supplement granted timing credit: %+v", got)
			}
		})
	}
}

func TestCIUnitsTimingCanonicalSupplementCannotEscapeCost(t *testing.T) {
	const release = "github.com/division-sh/swarm/internal/releasee2e"
	releaseRoots := []string{
		"TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends", "TestCompiledProcessFullLifecycleJourneysSQLitePostgres",
		"TestGoldenAgentWorkloadSQLiteSmoke", "TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration1", "TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration2",
	}
	policy := testplanning.Policy{
		Version: testplanning.PolicyVersion, Module: "example",
		Planning:        testplanning.PlanningPolicy{TargetSeconds: 120, MaxShards: 1, UnknownPackageSeconds: 1},
		SpecialPackages: []string{"example/p", release, testplanning.SoakPackage},
		Profiles: map[string]testplanning.ProfilePolicy{
			"core":      {CountMode: "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", Units: []string{"ordinary"}},
			"lifecycle": {CountMode: "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", Units: []string{"ordinary", "release-lifecycle"}},
			"full":      {CountMode: "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", Units: []string{"ordinary", "extra", "release-full", "soak-sqlite", "soak-postgres"}},
		},
		Units: map[string]testplanning.UnitPolicy{
			"ordinary":          {Packages: []string{"example/p"}, Run: "^TestOrdinary$", CountMode: "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", BudgetClass: "full"},
			"extra":             {Packages: []string{"example/p"}, Run: "^TestExtra$", CountMode: "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", BudgetClass: "full"},
			"release-lifecycle": {Packages: []string{release}, Run: "^(TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends|TestCompiledProcessFullLifecycleJourneysSQLitePostgres|TestGoldenAgentWorkloadSQLiteSmoke)$", CountMode: "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", BudgetClass: "full"},
			"release-full":      {Packages: []string{release}, Run: "^(TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends|TestCompiledProcessFullLifecycleJourneysSQLitePostgres|TestGoldenAgentWorkloadSQLiteSmoke|TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration[12])$", CountMode: "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", BudgetClass: "full"},
		},
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		policy.Units["soak-"+backend] = testplanning.UnitPolicy{Packages: []string{testplanning.SoakPackage}, Run: testplanning.SoakRun + "/^" + backend + "$", GoTimeout: testplanning.SoakGoTimeout, CountMode: "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", BudgetClass: "soak"}
	}
	for _, profile := range []string{"core", "lifecycle"} {
		t.Run(profile, func(t *testing.T) {
			plan, err := testplanning.BuildPlan(policy, testplanning.WeightModel{Version: testplanning.WeightModelVersion, SourceRunID: "fixture", Packages: map[string]float64{"example/p": 1}}, []string{"example/p", release, testplanning.SoakPackage}, profile, "supplement fixture", "head", testplanning.BuildOptions{ExtraUnits: []string{"extra"}})
			if err != nil {
				t.Fatal(err)
			}
			inventory := testplanning.RootInventory{BuildContext: testplanning.BuildContext{GOOS: "linux", GOARCH: "amd64", CGOEnabled: "1"}, Packages: map[string]testplanning.PackageRoots{
				"example/p":              {Package: "example/p", HasTestFiles: true, Roots: []string{"TestOrdinary", "TestExtra"}},
				release:                  {Package: release, HasTestFiles: true, Roots: releaseRoots},
				testplanning.SoakPackage: {Package: testplanning.SoakPackage, HasTestFiles: true, Roots: []string{testplanning.SoakTest}},
			}}
			if err := testplanning.BindExecution(&plan, inventory, nil, policy); err != nil {
				t.Fatal(err)
			}
			for _, cost := range []float64{5, 5.001, 31} {
				var receipts []CommandEvidence
				var baseline []RootTime
				for _, unit := range plan.Units {
					e := timingTestEvidence(plan, unit.ID, AttemptPrimary, 10)
					e.BuildContext, e.WorkloadProfile, e.ExecutionTier = plan.BuildContext, unit.WorkloadProfile, unit.ExecutionTier
					for _, root := range unit.SelectedRoots {
						seconds := 0.0
						if unit.ID == "ordinary" {
							seconds = 100
						}
						if unit.ID == "extra" {
							seconds = cost
						}
						e.Report.Tests = append(e.Report.Tests, TestTiming{Package: root.Package, Test: root.Name, Result: "pass", Elapsed: seconds})
						if unit.ID != "extra" {
							baseline = append(baseline, RootTime{TimingCell: TimingCell{Package: root.Package, Root: root.Name, Environment: e.EnvironmentID, Count: e.CountMode}, Seconds: seconds})
						}
					}
					e.Report.Summary.Tests = len(e.Report.Tests)
					receipts = append(receipts, e)
				}
				reference := TestTimeReference{BuildContext: plan.BuildContext, Roots: timingReferenceRoots(t, baseline, policy)}
				got := EvaluateTestTime(reference, policy, plan, 1, 1, receipts, TestTimePR)
				want := BudgetPass
				if cost > 5 {
					want = BudgetFail
				}
				if got.Status != want || got.Tiers[len(got.Tiers)-1].Tier != "full" || got.Tiers[len(got.Tiers)-1].AddedSeconds != cost {
					t.Fatalf("supplement bypassed full comparison: %+v", got)
				}
			}
		})
	}
}
