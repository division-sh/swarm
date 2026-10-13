package testplanning

import (
	"context"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
)

const catalogTierPackage = "github.com/division-sh/swarm/internal/runtime/cataloge2e"

type catalogTierFixture struct {
	policy    Policy
	model     WeightModel
	inventory RootInventory
	proofs    []ParityProof
	packages  []string
}

func loadCatalogTierFixture(t *testing.T) catalogTierFixture {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join(root, ".github/test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	policy, err := LoadPolicy(file)
	if err != nil {
		t.Fatal(err)
	}
	weights, err := os.Open(filepath.Join(root, GeneratedWeightModelPath))
	if err != nil {
		t.Fatal(err)
	}
	defer weights.Close()
	model, err := LoadWeightModel(weights)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := DiscoverRootInventory(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := LoadParityProofs(filepath.Join(root, "internal/apiv1/testdata/public_surface_backend_matrix.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var packages []string
	for pkg := range inventory.Packages {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)
	return catalogTierFixture{policy, model, inventory, proofs, packages}
}

func (f catalogTierFixture) bind(t *testing.T, tier string, extras ...string) RunPlan {
	t.Helper()
	plan, err := BuildPlan(f.policy, f.model, f.packages, tier, "catalog tier proof", "test-head", BuildOptions{ExtraUnits: extras})
	if err == nil {
		err = BindExecution(&plan, f.inventory, f.proofs, f.policy)
	}
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestPromotedCatalogUnitRetainsFullOnlyChildren(t *testing.T) {
	f := loadCatalogTierFixture(t)
	// Reproduce the membership-only promotion independently of the checked-in tier.
	f.policy.Profiles = maps.Clone(f.policy.Profiles)
	lifecycle := f.policy.Profiles[ProfileLifecycle]
	lifecycle.Units = slices.DeleteFunc(slices.Clone(lifecycle.Units), func(id string) bool {
		return id == "catalog-oracle-guards" || id == "catalog-required-smoke" || id == "catalog-runtime" || id == "catalog-replay-1"
	})
	lifecycle.Units = append(lifecycle.Units, "catalog-runtime", "catalog-replay-1")
	f.policy.Profiles[ProfileLifecycle] = lifecycle
	f.proofs = append(f.proofs, ParityProof{
		ID: "catalog-promoted-child-regression", Kind: "go_test", Profile: ProfileFull,
		Name:     "TestTerminalMiddleMemberFailureRetainsSuffixBothStores",
		Path:     "internal/runtime/cataloge2e/terminal_retirement_failure_test.go",
		Children: []string{"sqlite", "postgres", "reviewer-required-child"},
	})
	full := f.bind(t, ProfileFull)
	for _, tier := range []string{ProfileCore, ProfileLifecycle} {
		t.Run(tier, func(t *testing.T) {
			extras := []string{"catalog-runtime", "catalog-replay-1"}
			if tier == ProfileLifecycle {
				extras = nil
			}
			plan := f.bind(t, tier, extras...)
			for _, id := range []string{"catalog-runtime", "catalog-replay-1"} {
				got, err := plan.Unit(id)
				want, wantErr := full.Unit(id)
				if err != nil || wantErr != nil {
					t.Fatalf("catalog unit: %v / %v", err, wantErr)
				}
				if !reflect.DeepEqual(got.RequiredTests, want.RequiredTests) {
					t.Fatalf("%s promoted/supplemental unit %s lost full required roots/children", tier, id)
				}
				if got.WorkloadProfile != ProfileFull || !reflect.DeepEqual(got.RequiredChildren, want.RequiredChildren) {
					t.Fatalf("%s unit %s lost full workload/declared children", tier, id)
				}
			}
		})
	}
}

func TestCatalogTierSelectionsPreserveCanonicalOwners(t *testing.T) {
	f := loadCatalogTierFixture(t)
	full := f.bind(t, ProfileFull)
	for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		base := f.bind(t, tier)
		if tier != ProfileCore {
			for _, id := range []string{"catalog-runtime", "catalog-replay-1"} {
				if _, err := base.Unit(id); err != nil {
					t.Fatalf("%s omits promoted whole catalog unit %s", tier, id)
				}
			}
		}
		var available []string
		for _, id := range []string{"catalog-runtime", "catalog-replay-1", "catalog-replay-2-3"} {
			if !slices.Contains(f.policy.Profiles[tier].Units, id) {
				available = append(available, id)
			}
		}
		for mask := 0; mask < 1<<len(available); mask++ {
			var extras []string
			for i, id := range available {
				if mask&(1<<i) != 0 {
					extras = append(extras, id)
				}
			}
			t.Run(tier+"/"+strings.Join(extras, "+"), func(t *testing.T) {
				plan := f.bind(t, tier, extras...)
				assertCatalogTierOwners(t, f, plan, full)
				assertNoncatalogUnitsEqual(t, base, plan)
				if plan.Digest == "" || plan.validateBatches() != nil {
					t.Fatal("catalog selection did not bind its digest and batches")
				}
			})
		}
		for _, name := range f.inventory.Packages[catalogTierPackage].Roots {
			selected := false
			for _, unit := range base.Units {
				selected = selected || slices.Contains(unit.SelectedRoots, TestRoot{Package: catalogTierPackage, Name: name})
			}
			got, err := f.policy.SelectTimingCell(tier, TestRoot{Package: catalogTierPackage, Name: name}, "")
			if err != nil || got != selected {
				t.Fatalf("%s timing/execution selection differs for %s: %t / %t: %v", tier, name, got, selected, err)
			}
		}
	}
}

func assertCatalogTierOwners(t *testing.T, f catalogTierFixture, plan, full RunPlan) {
	t.Helper()
	owners := map[TestRoot]int{}
	for _, unit := range plan.Units {
		for _, root := range unit.SelectedRoots {
			owners[root]++
		}
		if !wholeCatalogUnit(unit.ID) {
			continue
		}
		original, err := full.Unit(unit.ID)
		if err != nil || unit.WorkloadProfile != ProfileFull || MeasureUnit(unit, 0) != MeasureUnit(original, 0) ||
			!reflect.DeepEqual(unit.SelectedRoots, original.SelectedRoots) || !reflect.DeepEqual(unit.RequiredTests, original.RequiredTests) ||
			!reflect.DeepEqual(unit.RequiredChildren, original.RequiredChildren) || !reflect.DeepEqual(unit.DeferredTests, original.DeferredTests) {
			t.Fatalf("whole catalog unit changed its canonical execution/obligation: %s: %v", unit.ID, err)
		}
	}
	for _, name := range []string{"TestStaticDataInvocationGoldenConsumesAdmittedIdentity", "TestCatalogReplayCleanCensus", "TestCatalogReplayProofReferences", "TestCatalogRequiredSmoke"} {
		if owners[TestRoot{Package: catalogTierPackage, Name: name}] != 1 {
			t.Fatalf("catalog canary %s does not have one owner", name)
		}
	}
	for _, root := range plan.DeferredRoots {
		if root.Package == catalogTierPackage && (owners[root.TestRoot] != 0 || root.MinimumTier == ProfileCore) {
			t.Fatalf("catalog deferral hides selected/core obligation: %+v", root)
		}
	}
	_, runtimeErr := plan.Unit("catalog-runtime")
	_, replayErr := plan.Unit("catalog-replay-1")
	want := map[string]bool{
		"catalog-oracle-guards":        runtimeErr != nil && replayErr != nil,
		"catalog-replay-oracle-guards": runtimeErr == nil && replayErr != nil,
		"catalog-runtime-oracle-guard": runtimeErr != nil && replayErr == nil,
		"catalog-required-smoke":       replayErr != nil,
	}
	for id, present := range want {
		_, err := plan.Unit(id)
		if (err == nil) != present {
			t.Fatalf("complementary canary %s presence=%t, want %t", id, err == nil, present)
		}
	}
	for _, id := range []string{"catalog-runtime-scatter-safety", "catalog-runtime-fork-readiness"} {
		if slices.Contains(f.policy.Profiles[plan.Profile].Units, id) {
			got, err := plan.Unit(id)
			original, _ := full.Unit(id)
			if err != nil || !reflect.DeepEqual(got.SelectedRoots, original.SelectedRoots) {
				t.Fatalf("disjoint catalog sibling %s changed", id)
			}
		}
	}
}

func assertNoncatalogUnitsEqual(t *testing.T, before, after RunPlan) {
	t.Helper()
	for _, unit := range before.Units {
		if slices.Contains(unit.Packages, catalogTierPackage) {
			continue
		}
		got, err := after.Unit(unit.ID)
		if err != nil || !reflect.DeepEqual(got, unit) {
			t.Fatalf("noncatalog recipe/obligations changed for %s: %v", unit.ID, err)
		}
	}
}

func TestCatalogTierCompositionRefusesInvalidOwners(t *testing.T) {
	f := loadCatalogTierFixture(t)
	for _, tc := range []struct {
		name, tier string
		extras     []string
		edit       func(*Policy)
	}{
		{name: "promoted-runtime-noop", tier: ProfileLifecycle, extras: []string{"catalog-runtime"}},
		{name: "promoted-replay-noop", tier: ProfileLifecycle, extras: []string{"catalog-replay-1"}},
		{name: "full-noop", tier: ProfileFull, extras: []string{"catalog-replay-2-3"}},
		{name: "duplicate-extra", tier: ProfileCore, extras: []string{"catalog-runtime", "catalog-runtime"}},
		{name: "foreign-extra", tier: ProfileCore, extras: []string{"catalog-unknown"}},
		{name: "private-complement-extra", tier: ProfileCore, extras: []string{"catalog-replay-oracle-guards"}},
		{name: "missing-complement-unit", tier: ProfileCore, extras: []string{"catalog-runtime"}, edit: func(p *Policy) { delete(p.Units, "catalog-replay-oracle-guards") }},
		{name: "missing-complement-root", tier: ProfileCore, extras: []string{"catalog-runtime"}, edit: func(p *Policy) {
			u := p.Units["catalog-replay-oracle-guards"]
			u.Run = "^TestCatalogReplayCleanCensus$"
			p.Units["catalog-replay-oracle-guards"] = u
		}},
		{name: "foreign-complement-root", tier: ProfileCore, extras: []string{"catalog-runtime"}, edit: func(p *Policy) {
			u := p.Units["catalog-replay-oracle-guards"]
			u.Run = "^TestTerminalMiddleMemberFailureRetainsSuffixBothStores$"
			p.Units["catalog-replay-oracle-guards"] = u
		}},
		{name: "arbitrary-overlap", tier: ProfileLifecycle, edit: func(p *Policy) {
			u := p.Units["catalog-runtime-scatter-safety"]
			u.Run = "^(TestScatterGatherSafetyBothStores|TestTerminalMiddleMemberFailureRetainsSuffixBothStores)$"
			p.Units["catalog-runtime-scatter-safety"] = u
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := f.policy
			policy.Profiles, policy.Units = maps.Clone(policy.Profiles), maps.Clone(policy.Units)
			if tc.edit != nil {
				tc.edit(&policy)
			}
			plan, err := BuildPlan(policy, f.model, f.packages, tc.tier, "catalog refusal", "test-head", BuildOptions{ExtraUnits: tc.extras})
			if err == nil {
				err = BindExecution(&plan, f.inventory, f.proofs, policy)
			}
			if err == nil {
				t.Fatal("invalid catalog owner accepted")
			}
			t.Log(err)
		})
	}
	for _, edit := range []func(*ProofUnit){
		func(u *ProofUnit) { u.Run = "^TestTerminalMiddleMemberFailureRetainsSuffixBothStores$" },
		func(u *ProofUnit) { u.CountMode = "cache-default" },
		func(u *ProofUnit) { u.EnvironmentID = "ci-postgres-explicit-v1" },
		func(u *ProofUnit) { u.GoTimeout = "20m" },
	} {
		plan := f.bind(t, ProfileLifecycle)
		for i := range plan.Units {
			if plan.Units[i].ID == "catalog-runtime" {
				edit(&plan.Units[i])
			}
		}
		if err := BindExecution(&plan, f.inventory, f.proofs, f.policy); err == nil {
			t.Fatal("changed whole-catalog recipe accepted")
		}
	}
}

func TestCatalogPromotionPreservesNoncatalogAndFullTruth(t *testing.T) {
	f := loadCatalogTierFixture(t)
	legacy := f
	legacy.policy.Profiles = maps.Clone(f.policy.Profiles)
	p := legacy.policy.Profiles[ProfileLifecycle]
	p.Units = slices.DeleteFunc(slices.Clone(p.Units), wholeCatalogUnit)
	p.Units = append(p.Units, "catalog-oracle-guards", "catalog-required-smoke")
	legacy.policy.Profiles[ProfileLifecycle] = p
	for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		old, current := legacy.bind(t, tier), f.bind(t, tier)
		assertNoncatalogUnitsEqual(t, old, current)
		if tier == ProfileFull || tier == ProfileCore {
			if !reflect.DeepEqual(old.Units, current.Units) || !reflect.DeepEqual(old.Batches, current.Batches) {
				t.Fatalf("%s base execution changed with lifecycle promotion", tier)
			}
		}
	}
}

func TestCatalogCompositionUsesOnlyCompatibleFullWeights(t *testing.T) {
	f := loadCatalogTierFixture(t)
	full := f.bind(t, ProfileFull)
	f.model.Units = maps.Clone(f.model.Units)
	f.model.Units[ProfileLifecycle] = maps.Clone(f.model.Units[ProfileLifecycle])
	f.model.Units[ProfileCore] = maps.Clone(f.model.Units[ProfileCore])
	for _, id := range []string{"catalog-runtime", "catalog-replay-1", "catalog-replay-2-3"} {
		u, _ := full.Unit(id)
		f.model.Units[ProfileLifecycle][id] = MeasureUnit(u, 999)
		f.model.Units[ProfileCore][id] = MeasureUnit(u, 999)
	}
	for _, id := range []string{"catalog-runtime-oracle-guard", "catalog-replay-oracle-guards"} {
		u := f.policy.Units["catalog-oracle-guards"]
		old := ProofUnit{ID: id, Packages: u.Packages, Run: u.Run, CountMode: u.CountMode, EnvironmentID: u.EnvironmentIDs[VenueCI], BudgetClass: u.BudgetClass}
		f.model.Units[ProfileCore][id] = MeasureUnit(old, 999)
	}
	for _, selection := range []struct {
		tier   string
		extras []string
	}{
		{ProfileLifecycle, []string{"catalog-replay-2-3"}},
		{ProfileCore, []string{"catalog-runtime"}},
		{ProfileCore, []string{"catalog-replay-1"}},
	} {
		plan := f.bind(t, selection.tier, selection.extras...)
		for _, unit := range plan.Units {
			if wholeCatalogUnit(unit.ID) {
				original, _ := full.Unit(unit.ID)
				if unit.WeightSeconds != original.WeightSeconds {
					t.Fatalf("whole unit %s reused a lower-tier weight", unit.ID)
				}
			} else if unit.ID == "catalog-runtime-oracle-guard" || unit.ID == "catalog-replay-oracle-guards" {
				if unit.WeightSeconds != f.policy.Planning.UnknownPackageSeconds {
					t.Fatalf("complement %s reused a different selector's weight", unit.ID)
				}
			}
		}
	}
}
