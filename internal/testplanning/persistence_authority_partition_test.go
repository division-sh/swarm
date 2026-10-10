package testplanning

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"testing"
)

const persistenceDebtCensusUnit = "persistence-authority-debt-census"
const persistenceDebtRoot = "TestPersistenceAuthorityDebtRatchet"
const persistenceNativeFamilyRoot = "TestNativeFixtureFamiliesDoNotReceiveRawAuthority"
const persistenceDebtCensusRun = `^(TestNativeFixtureFamiliesDoNotReceiveRawAuthority|TestPersistenceAuthorityDebtRatchet)$`
const persistenceDebtAdmissionRun = `^($|[^T].*|T($|[^e].*|e($|[^s].*|s($|[^t].*|t($|[^NP].*|N($|[^a].*|a($|[^t].*|t($|[^i].*|i($|[^v].*|v($|[^e].*|e($|[^F].*|F($|[^i].*|i($|[^x].*|x($|[^t].*|t($|[^u].*|u($|[^r].*|r($|[^e].*|e($|[^F].*|F($|[^a].*|a($|[^m].*|m($|[^i].*|i($|[^l].*|l($|[^i].*|i($|[^e].*|e($|[^s].*|s($|[^D].*|D($|[^o].*|o($|[^N].*|N($|[^o].*|o($|[^t].*|t($|[^R].*|R($|[^e].*|e($|[^c].*|c($|[^e].*|e($|[^i].*|i($|[^v].*|v($|[^e].*|e($|[^R].*|R($|[^a].*|a($|[^w].*|w($|[^A].*|A($|[^u].*|u($|[^t].*|t($|[^h].*|h($|[^o].*|o($|[^r].*|r($|[^i].*|i($|[^t].*|t($|[^y].*|y.+))))))))))))))))))))))))))))))))))))))))))))|P($|[^e].*|e($|[^r].*|r($|[^s].*|s($|[^i].*|i($|[^s].*|s($|[^t].*|t($|[^e].*|e($|[^n].*|n($|[^c].*|c($|[^e].*|e($|[^A].*|A($|[^u].*|u($|[^t].*|t($|[^h].*|h($|[^o].*|o($|[^r].*|r($|[^i].*|i($|[^t].*|t($|[^y].*|y($|[^D].*|D($|[^e].*|e($|[^b].*|b($|[^t].*|t($|[^R].*|R($|[^a].*|a($|[^t].*|t($|[^c].*|c($|[^h].*|h($|[^e].*|e($|[^t].*|t.+)))))))))))))))))))))))))))))))))))$`

var persistenceNativeFamilyChildren = []string{
	"TestInboundSetupSeedDoesNotReceiveRawAuthority",
	"TestNativeActivitySetupDoesNotReceiveRawAuthority",
	"TestNativeChannelTerminalFixturesDoNotReceiveRawAuthority",
	"TestNativeJournalFixturesDoNotReceiveRawAuthority",
	"TestNativeLoopClaimFixturesDoNotReceiveRawAuthority",
	"TestNativeMockFixturesDoNotReceiveRawAuthority",
	"TestNativeAPIReadSetupDoesNotReceiveRawAuthority",
}

func loadPersistenceDebtPolicy(t *testing.T) Policy {
	t.Helper()
	file, err := os.Open(filepath.Join("..", "..", ".github/test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	policy, err := LoadPolicy(file)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func validatePersistenceDebtEnvelopes(policy Policy) error {
	for _, id := range []string{"store-admission-full", persistenceDebtCensusUnit} {
		unit, ok := policy.Units[id]
		packages := []string{policy.Module + "/internal/store"}
		run, skip, timeout := persistenceDebtCensusRun, "", "20m"
		children := map[string][]string{persistenceNativeFamilyRoot: persistenceNativeFamilyChildren}
		if id == "store-admission-full" {
			packages = append(packages, policy.Module+"/internal/testpostgres")
			run, skip = persistenceDebtAdmissionRun, ""
			timeout = ""
			children = nil
		}
		if !ok || !slices.Equal(unit.Packages, packages) || unit.Run != run || unit.Skip != skip || unit.CountMode != "count-1" || unit.EnvironmentID != "" || !reflect.DeepEqual(unit.EnvironmentIDs, managedEnvironmentIDs()) || unit.BudgetClass != "broad" || unit.GoTimeout != timeout || !reflect.DeepEqual(unit.RequiredChildren, children) {
			return fmt.Errorf("%s changed persistence proof envelope: %+v", id, unit)
		}
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			count := 0
			for _, member := range policy.Profiles[tier].Units {
				if member == id {
					count++
				}
			}
			if count != 1 {
				return fmt.Errorf("%s has %d owners in %s, want one", id, count, tier)
			}
		}
	}
	return nil
}

func TestPersistenceAuthorityCensusPartitionPreservesEveryRoot(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	if err := validatePersistenceDebtEnvelopes(policy); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join("..", "..")
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
	model := WeightModel{Version: WeightModelVersion, SourceRunID: "persistence-partition"}
	for _, venue := range []string{VenueCI, VenueLocal} {
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			t.Run(venue+"/"+tier, func(t *testing.T) {
				plan, err := BuildPlan(policy, model, packages, tier, "exact census partition", "test-head", BuildOptions{Venue: venue})
				if err != nil {
					t.Fatal(err)
				}
				if err := BindExecution(&plan, inventory, proofs, policy); err != nil {
					t.Fatal(err)
				}
				owners := map[TestRoot][]string{}
				for _, unit := range plan.Units {
					for _, selected := range unit.SelectedRoots {
						if selected.Package == policy.Module+"/internal/store" || selected.Package == policy.Module+"/internal/testpostgres" {
							owners[selected] = append(owners[selected], unit.ID)
						}
					}
				}
				wantTotal := 0
				for _, pkg := range []string{policy.Module + "/internal/store", policy.Module + "/internal/testpostgres"} {
					for _, name := range inventory.Packages[pkg].Roots {
						want := "store-admission-full"
						if pkg == policy.Module+"/internal/store" && (name == persistenceDebtRoot || name == persistenceNativeFamilyRoot) {
							want = persistenceDebtCensusUnit
						}
						if !slices.Equal(owners[TestRoot{Package: pkg, Name: name}], []string{want}) {
							t.Fatalf("%s.%s owners=%v, want only %s", pkg, name, owners[TestRoot{Package: pkg, Name: name}], want)
						}
						wantTotal++
					}
				}
				if len(owners) != wantTotal {
					t.Fatalf("changed root union: got %d want %d", len(owners), wantTotal)
				}
				census, err := plan.Unit(persistenceDebtCensusUnit)
				if err != nil || len(census.RequiredTests) != 2 || census.RequiredTests[0].Name != persistenceNativeFamilyRoot || census.RequiredTests[1].Name != persistenceDebtRoot || len(census.DeferredTests) != 0 || !slices.Equal(census.RequiredChildren[persistenceNativeFamilyRoot], persistenceNativeFamilyChildren) {
					t.Fatalf("census completion obligation missing: %+v, %v", census, err)
				}
				t.Logf("complete union=%d; census=2 roots plus all7 required family children; admission=%d; exact multiplicity one", wantTotal, wantTotal-2)
			})
		}
	}
}

func TestPersistenceAuthorityCensusPartitionRejectsEnvelopeDrift(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*UnitPolicy)
	}{
		{"omitted-census", func(u *UnitPolicy) { u.Run = "^$" }},
		{"omitted-native-family", func(u *UnitPolicy) { u.Run = "^TestPersistenceAuthorityDebtRatchet$" }},
		{"omitted-required-child", func(u *UnitPolicy) {
			u.RequiredChildren[persistenceNativeFamilyRoot] = u.RequiredChildren[persistenceNativeFamilyRoot][:6]
		}},
		{"duplicate-required-child", func(u *UnitPolicy) {
			u.RequiredChildren[persistenceNativeFamilyRoot] = append(u.RequiredChildren[persistenceNativeFamilyRoot], persistenceNativeFamilyChildren[0])
		}},
		{"removed-required-children", func(u *UnitPolicy) { u.RequiredChildren = nil }},
		{"extra-root", func(u *UnitPolicy) { u.Run = "^TestPersistenceAuthorityDebt" }},
		{"changed-count", func(u *UnitPolicy) { u.CountMode = "cache-default" }},
		{"unjustified-skip", func(u *UnitPolicy) { u.Skip = "^Test" }},
		{"timeout-inflation", func(u *UnitPolicy) { u.GoTimeout = "30m" }},
		{"environment", func(u *UnitPolicy) { u.EnvironmentID = "other" }},
		{"package", func(u *UnitPolicy) { u.Packages = []string{"other"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := loadPersistenceDebtPolicy(t)
			unit := policy.Units[persistenceDebtCensusUnit]
			test.edit(&unit)
			policy.Units[persistenceDebtCensusUnit] = unit
			if validatePersistenceDebtEnvelopes(policy) == nil {
				t.Fatal("changed census envelope accepted")
			}
		})
	}
	for _, test := range []struct {
		name string
		edit func(*UnitPolicy)
	}{
		{"duplicate-census", func(u *UnitPolicy) { u.Run = "." }},
		{"narrowed-store-roots", func(u *UnitPolicy) { u.Run = "^TestPersistenceAuthorityDebt" }},
		{"changed-admission-count", func(u *UnitPolicy) { u.CountMode = "cache-default" }},
		{"lost-testpostgres", func(u *UnitPolicy) { u.Packages = u.Packages[:1] }},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := loadPersistenceDebtPolicy(t)
			unit := policy.Units["store-admission-full"]
			test.edit(&unit)
			policy.Units["store-admission-full"] = unit
			if validatePersistenceDebtEnvelopes(policy) == nil {
				t.Fatal("changed admission envelope accepted")
			}
		})
	}
	for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		t.Run("missing-tier/"+tier, func(t *testing.T) {
			policy := loadPersistenceDebtPolicy(t)
			profile := policy.Profiles[tier]
			profile.Units = slices.DeleteFunc(slices.Clone(profile.Units), func(id string) bool { return id == persistenceDebtCensusUnit })
			policy.Profiles[tier] = profile
			if validatePersistenceDebtEnvelopes(policy) == nil {
				t.Fatal("census retiered out of required profile")
			}
		})
	}
	pattern := regexp.MustCompile(persistenceDebtAdmissionRun)
	for _, root := range []string{persistenceDebtRoot, persistenceNativeFamilyRoot} {
		if pattern.MatchString(root) {
			t.Fatal("positive admission complement duplicates a census root")
		}
		for i := 0; i < len(root); i++ {
			for _, name := range []string{root[:i], root[:i] + "OtherProof"} {
				if !pattern.MatchString(name) {
					t.Fatalf("positive complement narrowed prefix sibling %q", name)
				}
			}
		}
		for _, suffix := range []string{"Extra", "Guard", "2"} {
			if !pattern.MatchString(root + suffix) {
				t.Fatal("positive complement dropped a census-name sibling")
			}
		}
	}
}
