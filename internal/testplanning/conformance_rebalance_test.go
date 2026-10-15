package testplanning

import (
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestConformanceReporterRebalancePreservesActualPlans(t *testing.T) {
	policy, inventory, parity, packages := loadVerifyRunPlanInputs(t)
	file, err := os.Open(filepath.Join("..", "..", GeneratedWeightModelPath))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	model, err := LoadWeightModel(file)
	if err != nil {
		t.Fatal(err)
	}
	// Restore only the two pre-rebalance selectors on the current native census.
	original := policy
	original.Units = maps.Clone(policy.Units)
	first := original.Units["conformance-1"]
	first.Run = "^Test[FH].*$"
	original.Units["conformance-1"] = first
	second := original.Units["conformance-2"]
	second.Skip = ""
	original.Units["conformance-2"] = second
	moved := TestRoot{Package: policy.Module + "/internal/runtime/conformance", Name: conformanceRebalancedReporterRoot}
	if !slices.Contains(inventory.Packages[moved.Package].Roots, moved.Name) {
		t.Fatal("rebalance root is absent from the active source inventory")
	}
	for _, venue := range []string{VenueCI, VenueLocal} {
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			t.Run(venue+"/"+tier, func(t *testing.T) {
				plans := make([]RunPlan, 2)
				for i, selected := range []Policy{original, policy} {
					plan, err := BuildPlan(selected, model, packages, tier, "conformance rebalance", "same-source", BuildOptions{Venue: venue})
					if err == nil {
						err = BindExecution(&plan, inventory, parity, selected)
					}
					if err != nil {
						t.Fatal(err)
					}
					plans[i] = plan
				}
				before, after := plans[0], plans[1]
				oldOwners, newOwners := conformancePlanOwners(before), conformancePlanOwners(after)
				if tier == ProfileCore {
					if !reflect.DeepEqual(before.Units, after.Units) || len(newOwners[moved]) != 0 {
						t.Fatal("core selection or execution recipe changed")
					}
				} else {
					if !slices.Equal(oldOwners[moved], []string{"conformance-2"}) || !slices.Equal(newOwners[moved], []string{"conformance-1"}) {
						t.Fatalf("reporter must move exactly once: before=%v after=%v", oldOwners[moved], newOwners[moved])
					}
					oldReporter, _ := before.Unit("conformance-2")
					newReporter, _ := after.Unit("conformance-1")
					if !reflect.DeepEqual(conformanceReporterObligation(oldReporter), conformanceReporterObligation(newReporter)) || conformanceReporterObligation(newReporter).Name == "" {
						t.Fatal("reporter required-summary/receipt obligation changed or disappeared")
					}
				}
				delete(oldOwners, moved)
				delete(newOwners, moved)
				if !reflect.DeepEqual(oldOwners, newOwners) || !slices.Equal(before.Packages, after.Packages) || len(before.Units) != len(after.Units) {
					t.Fatal("root/Example/Fuzz union or another root's owner changed")
				}
				for _, old := range before.Units {
					current, err := after.Unit(old.ID)
					if err != nil {
						t.Fatal(err)
					}
					if old.ID == "conformance-1" || old.ID == "conformance-2" {
						old = conformanceWithoutReporter(old)
						current = conformanceWithoutReporter(current)
					}
					if !reflect.DeepEqual(old, current) {
						t.Fatalf("other roots/children, recipe or execution flags changed in %s", old.ID)
					}
				}
				t.Logf("%s/%s: %d owned roots conserved; reporter %v -> %v; complete plan digests %s -> %s", venue, tier, len(newOwners), conformancePlanOwners(before)[moved], conformancePlanOwners(after)[moved], before.Digest, after.Digest)
			})
		}
	}
}

func TestConformanceReporterExclusionAdmissionIsExact(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	rest := policy.Units["conformance-2"]
	admit := func(unit UnitPolicy) error {
		return validateSoakSelection(unit.Packages, unit.Run, unit.Skip, unit.GoTimeout, unit.CountMode, unit.BudgetClass)
	}
	if err := admit(rest); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*UnitPolicy)
	}{
		{"foreign package", func(u *UnitPolicy) { u.Packages = []string{ServedReporterPackage} }},
		{"additional package", func(u *UnitPolicy) { u.Packages = append(slices.Clone(u.Packages), ServedReporterPackage) }},
		{"foreign root", func(u *UnitPolicy) { u.Skip = "^TestOther$" }},
		{"prefix skip", func(u *UnitPolicy) { u.Skip = "^TestNumeric.*$" }},
		{"unanchored skip", func(u *UnitPolicy) { u.Skip = strings.TrimSuffix(u.Skip, "$") }},
		{"backend skip", func(u *UnitPolicy) { u.Skip += "/postgres" }},
		{"backend run", func(u *UnitPolicy) { u.Run += "/sqlite" }},
		{"broader run", func(u *UnitPolicy) { u.Run = "^(Test|Example|Fuzz)" }},
		{"timeout", func(u *UnitPolicy) { u.GoTimeout = "5m" }},
		{"count", func(u *UnitPolicy) { u.CountMode = "cache-default" }},
		{"budget", func(u *UnitPolicy) { u.BudgetClass = "full" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unit := rest
			tc.edit(&unit)
			if err := admit(unit); err == nil {
				t.Fatal("noncanonical reporter exclusion admitted")
			}
		})
	}
}

func conformancePlanOwners(plan RunPlan) map[TestRoot][]string {
	owners := map[TestRoot][]string{}
	for _, unit := range plan.Units {
		for _, root := range unit.SelectedRoots {
			owners[root] = append(owners[root], unit.ID)
		}
	}
	for _, ids := range owners {
		slices.Sort(ids)
	}
	return owners
}

func conformanceReporterObligation(unit ProofUnit) RequiredTest {
	for _, required := range unit.RequiredTests {
		if required.Name == conformanceRebalancedReporterRoot {
			return required
		}
	}
	return RequiredTest{}
}

func conformanceWithoutReporter(unit ProofUnit) ProofUnit {
	unit.SelectedRoots = slices.DeleteFunc(slices.Clone(unit.SelectedRoots), func(root TestRoot) bool { return root.Name == conformanceRebalancedReporterRoot })
	unit.RequiredTests = slices.DeleteFunc(slices.Clone(unit.RequiredTests), func(root RequiredTest) bool { return root.Name == conformanceRebalancedReporterRoot })
	unit.Run, unit.Skip, unit.WeightSeconds = "", "", 0
	return unit
}

func TestConformanceReporterRebalanceRejectsMissingOrDuplicateOwner(t *testing.T) {
	policy, inventory, parity, packages := loadVerifyRunPlanInputs(t)
	model := WeightModel{Version: WeightModelVersion, SourceRunID: "conformance-negative-controls"}
	for _, tier := range []string{ProfileLifecycle, ProfileFull} {
		for _, defect := range []string{"missing", "duplicate"} {
			t.Run(tier+"/"+defect, func(t *testing.T) {
				changed := policy
				changed.Units = maps.Clone(policy.Units)
				id := "conformance-1"
				unit := changed.Units[id]
				if defect == "missing" {
					unit.Run = "^Test[FH].*$"
				} else {
					id = "conformance-2"
					unit = changed.Units[id]
					unit.Skip = ""
				}
				changed.Units[id] = unit
				plan, err := BuildPlan(changed, model, packages, tier, "invalid reporter ownership", "same-source")
				if err == nil {
					err = BindExecution(&plan, inventory, parity, changed)
				}
				if err == nil || !strings.Contains(err.Error(), conformanceRebalancedReporterRoot) {
					t.Fatalf("%s reporter owner not refused at its real plan boundary: %v", defect, err)
				}
			})
		}
	}
}
