package testplanning

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
)

var storeAdmissionUnits = []string{
	"store-admission-full",
	"store-admission-debt-01", "store-admission-debt-02", "store-admission-debt-03",
	"store-admission-debt-04", "store-admission-debt-05", "store-admission-debt-06",
}

// B's frozen allocation (6221052526). Durations are mixed-head planning inputs,
// never measured weights, timing allowances or executable selection authority.
func storeAdmissionManifest() (map[string]string, error) {
	raw, err := os.ReadFile(filepath.Join("../..", ".github/audit-artifacts/issue-2698-store-admission-partition.tsv"))
	if err != nil {
		return nil, err
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != "955b093165bfe9a5c367df868ad8719efb9aef6b329cc132fd6c4eaa98b7c78c" {
		return nil, fmt.Errorf("B's frozen partition manifest changed: %s", got)
	}
	r := csv.NewReader(strings.NewReader(string(raw)))
	r.Comma = '\t'
	rows, err := r.ReadAll()
	if err != nil || len(rows) != 230 || !slices.Equal(rows[0], []string{"unit", "root", "seconds", "observation"}) {
		return nil, fmt.Errorf("invalid 229-root partition manifest: %v", err)
	}
	owners := map[string]string{}
	for _, row := range rows[1:] {
		id := "store-admission-full"
		if row[0] != "base" {
			id = "store-admission-debt-" + strings.TrimPrefix(row[0], "d")
		}
		if !slices.Contains(storeAdmissionUnits, id) || owners[row[1]] != "" || !strings.HasPrefix(row[1], "Test") || strings.Contains(row[1], "/") {
			return nil, fmt.Errorf("invalid/duplicate manifest root: %v", row)
		}
		owners[row[1]] = id
	}
	return owners, nil
}

func validateStoreAdmissionEnvelopes(policy Policy) error {
	for _, pkg := range []string{policy.Module + "/internal/store", policy.Module + "/internal/testpostgres"} {
		if !slices.Contains(policy.SpecialPackages, pkg) {
			return fmt.Errorf("%s lost selected ownership", pkg)
		}
	}
	for id := range policy.Units {
		if strings.HasPrefix(id, "store-admission-") && !slices.Contains(storeAdmissionUnits, id) {
			return fmt.Errorf("unreviewed admission unit %s", id)
		}
	}
	for _, id := range storeAdmissionUnits {
		u, ok := policy.Units[id]
		want := UnitPolicy{Packages: []string{policy.Module + "/internal/store"}, Run: u.Run,
			CountMode: "count-1", EnvironmentIDs: managedEnvironmentIDs(), BudgetClass: "broad"}
		if id == "store-admission-full" {
			want.Packages = append(want.Packages, policy.Module+"/internal/testpostgres")
		}
		if !ok || !reflect.DeepEqual(u, want) || !strings.HasPrefix(u.Run, "^") || !strings.HasSuffix(u.Run, "$") || strings.Contains(u.Run, "/") {
			return fmt.Errorf("%s changed admission execution envelope: %+v", id, u)
		}
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			count := 0
			for _, member := range policy.Profiles[tier].Units {
				if member == id {
					count++
				}
			}
			if count != 1 {
				return fmt.Errorf("%s has %d %s owners, want one", id, count, tier)
			}
		}
	}
	owners, err := storeAdmissionManifest()
	if err != nil {
		return err
	}
	patterns := map[string]*regexp.Regexp{}
	for _, id := range storeAdmissionUnits {
		pattern, err := regexp.Compile(policy.Units[id].Run)
		if err != nil {
			return err
		}
		patterns[id] = pattern
		for _, census := range []string{persistenceDebtRoot, persistenceNativeFamilyRoot} {
			if pattern.MatchString(census) {
				return fmt.Errorf("%s duplicates census root %s", id, census)
			}
		}
	}
	for root, want := range owners {
		var actual []string
		for _, id := range storeAdmissionUnits {
			if patterns[id].MatchString(root) {
				actual = append(actual, id)
			}
		}
		if !slices.Equal(actual, []string{want}) {
			return fmt.Errorf("%s owners=%v, want only %s", root, actual, want)
		}
	}
	return policy.Validate()
}

func TestStoreAdmissionPartitionConservesBoundRootsAndExecutionEnvelopes(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	if err := validateStoreAdmissionEnvelopes(policy); err != nil {
		t.Fatal(err)
	}
	f := loadCatalogTierFixture(t)
	original := loadPersistenceDebtPolicy(t)
	old := original.Units["store-admission-full"]
	old.Run = persistenceDebtAdmissionRun
	original.Units["store-admission-full"] = old
	for _, id := range storeAdmissionUnits[1:] {
		delete(original.Units, id)
		for tier, p := range original.Profiles {
			p.Units = slices.DeleteFunc(p.Units, func(member string) bool { return member == id })
			original.Profiles[tier] = p
		}
	}
	type obligation struct {
		RequiredTest
		Count, Environment, Workload, Timeout, Budget string
	}
	obligations := func(plan RunPlan) map[TestRoot][]obligation {
		result := map[TestRoot][]obligation{}
		for _, unit := range plan.Units {
			for _, required := range unit.RequiredTests {
				result[required.TestRoot] = append(result[required.TestRoot], obligation{
					required, unit.CountMode, unit.EnvironmentID, unit.WorkloadProfile, unit.GoTimeout, unit.BudgetClass,
				})
			}
		}
		return result
	}
	for _, venue := range []string{VenueCI, VenueLocal} {
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			t.Run(venue+"/"+tier, func(t *testing.T) {
				var plans [2]RunPlan
				for i, selected := range []Policy{original, policy} {
					plan, err := BuildPlan(selected, f.model, f.packages, tier, "admission partition", "same-source", BuildOptions{Venue: venue})
					if err == nil {
						err = BindExecution(&plan, f.inventory, f.proofs, selected)
					}
					if err != nil {
						t.Fatal(err)
					}
					plans[i] = plan
				}
				if !reflect.DeepEqual(obligations(plans[0]), obligations(plans[1])) ||
					!slices.Equal(plans[0].Packages, plans[1].Packages) || !reflect.DeepEqual(plans[0].DeferredRoots, plans[1].DeferredRoots) {
					t.Fatal("partition changed roots, children/backends, profile, count, environment or unrelated deferrals")
				}
				oldCensus, _ := plans[0].Unit(persistenceDebtCensusUnit)
				newCensus, _ := plans[1].Unit(persistenceDebtCensusUnit)
				if !reflect.DeepEqual(oldCensus, newCensus) {
					t.Fatal("dedicated two-root census/exception changed")
				}
				counts := map[string]int{}
				for _, unit := range plans[1].Units {
					if slices.Contains(storeAdmissionUnits, unit.ID) {
						if _, stale := f.model.UnitSeconds(tier, unit); stale || unit.WeightSeconds != policy.Planning.UnknownPackageSeconds {
							t.Fatalf("%s reused historical selection weight or mixed manifest timing", unit.ID)
						}
					}
					for _, root := range unit.SelectedRoots {
						if root.Package == policy.Module+"/internal/testpostgres" && unit.ID != "store-admission-full" {
							t.Fatalf("testpostgres escaped original owner: %+v in %s", root, unit.ID)
						}
						if root.Package == policy.Module+"/internal/store" && slices.Contains(storeAdmissionUnits, unit.ID) {
							counts[unit.ID]++
						}
					}
				}
				for i, want := range []int{96, 18, 18, 19, 18, 40, 20} {
					if counts[storeAdmissionUnits[i]] != want {
						t.Fatalf("store allocation %v differs from B's 229-root manifest", counts)
					}
				}
				t.Logf("%s/%s: exact 229-root allocation %v; unchanged testpostgres and two-root census", venue, tier, counts)
			})
		}
	}
}

func TestStoreAdmissionPartitionNativeSelectorsKeepWholeChildren(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	owners, err := storeAdmissionManifest()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateStoreAdmissionEnvelopes(policy); err != nil {
		t.Fatal(err)
	}
	units := make([]ProofUnit, 0, len(storeAdmissionUnits)+1)
	for _, id := range append(slices.Clone(storeAdmissionUnits), persistenceDebtCensusUnit) {
		u := policy.Units[id]
		units = append(units, ProofUnit{ID: id, Packages: u.Packages, Run: u.Run, Skip: u.Skip, GoTimeout: u.GoTimeout, CountMode: u.CountMode, BudgetClass: u.BudgetClass})
	}
	if err := ValidateGoProofUnitPartition(filepath.Join("..", "store"), units); err != nil {
		t.Fatal(err)
	}
	if err := ValidateGoProofPartition(filepath.Join("..", "testpostgres"), []string{policy.Units["store-admission-full"].Run}); err != nil {
		t.Fatal(err)
	}
	// Go's actual -run entrance must agree with finite whole-root ownership, not
	// just the guard's RE2 matcher. Every root has both backend/case descendants.
	dir := t.TempDir()
	var source strings.Builder
	source.WriteString("package proof\nimport \"testing\"\n")
	for _, name := range slices.Sorted(maps.Keys(owners)) {
		fmt.Fprintf(&source, "func %s(t *testing.T) { for _, b := range []string{\"sqlite\",\"postgres\"} { t.Run(b, func(t *testing.T) { t.Run(\"retained_case\",func(t *testing.T){}) }) } }\n", name)
	}
	for name, content := range map[string]string{"go.mod": "module proof\ngo 1.26.0\n", "proof_test.go": source.String()} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range storeAdmissionUnits {
		t.Run(id, func(t *testing.T) {
			command := exec.CommandContext(t.Context(), "go", "test", "-json", "-count=1", "-run", policy.Units[id].Run, ".")
			command.Dir = dir
			raw, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("native selector: %v\n%s", err, raw)
			}
			seen := map[string]int{}
			decoder := json.NewDecoder(strings.NewReader(string(raw)))
			for {
				var event struct{ Action, Test string }
				err := decoder.Decode(&event)
				if err == io.EOF {
					break
				}
				if err != nil || event.Action == "fail" || event.Action == "skip" {
					t.Fatalf("incomplete native proof: %+v, %v", event, err)
				}
				if event.Action == "pass" && event.Test != "" {
					seen[event.Test]++
				}
			}
			want := map[string]int{}
			for root, owner := range owners {
				if owner == id {
					for _, suffix := range []string{"", "/sqlite", "/postgres", "/sqlite/retained_case", "/postgres/retained_case"} {
						want[root+suffix] = 1
					}
				}
			}
			if !reflect.DeepEqual(seen, want) {
				t.Fatalf("native selector lost/duplicated roots or backend/case children: got %d want %d", len(seen), len(want))
			}
		})
	}
}

func TestStoreAdmissionPartitionRefusesUnassignedFutureDebt(t *testing.T) {
	f := loadCatalogTierFixture(t)
	const root = "TestPersistenceAuthorityDebtProspectiveUnassigned"
	pkg := f.policy.Module + "/internal/store"
	entry := f.inventory.Packages[pkg]
	entry.Roots = append(slices.Clone(entry.Roots), root)
	f.inventory.Packages[pkg] = entry
	plan, err := BuildPlan(f.policy, f.model, f.packages, ProfileFull, "new root negative", "head")
	if err != nil {
		t.Fatal(err)
	}
	if err := BindExecution(&plan, f.inventory, f.proofs, f.policy); err == nil || !strings.Contains(err.Error(), root) {
		t.Fatalf("new unassigned debt root must refuse, not disappear: %v", err)
	}
}

func TestStoreAdmissionPartitionRejectsSelectionAndEnvelopeDrift(t *testing.T) {
	for _, change := range []string{"omit", "duplicate-root", "reassign", "foreign-root", "partial-backend", "partial-case", "skip", "timeout", "cached", "budget", "environment", "package", "testpostgres", "packable", "children", "unknown-unit", "core", "lifecycle", "full", "duplicate-tier"} {
		t.Run(change, func(t *testing.T) {
			policy := loadPersistenceDebtPolicy(t)
			id := storeAdmissionUnits[1]
			u := policy.Units[id]
			switch change {
			case "omit":
				u.Run = "^TestMissing$"
			case "duplicate-root":
				u.Run = policy.Units[storeAdmissionUnits[2]].Run
			case "reassign":
				other := policy.Units[storeAdmissionUnits[2]]
				u.Run, other.Run = other.Run, u.Run
				policy.Units[storeAdmissionUnits[2]] = other
			case "foreign-root":
				u.Run = "^Test.*$"
			case "partial-backend":
				u.Run += "/postgres"
			case "partial-case":
				u.Run += "/sqlite/case"
			case "skip":
				u.Skip = "^Test"
			case "timeout":
				u.GoTimeout = "20m"
			case "cached":
				u.CountMode = "cache-default"
			case "budget":
				u.BudgetClass = "full"
			case "environment":
				u.EnvironmentIDs = maps.Clone(u.EnvironmentIDs)
				u.EnvironmentIDs[VenueCI] = "local-postgres-gateway-empty-v1"
			case "package":
				u.Packages = append(u.Packages, policy.Module+"/internal/testpostgres")
			case "testpostgres":
				base := policy.Units[storeAdmissionUnits[0]]
				base.Packages = base.Packages[:1]
				policy.Units[storeAdmissionUnits[0]] = base
			case "packable":
				u.Packable = true
			case "children":
				u.RequiredChildren = map[string][]string{"TestPersistenceAuthorityDebtAnalysisCacheIdentity": {"sqlite"}}
			case "unknown-unit":
				policy.Units["store-admission-debt-07"] = u
			case "core", "lifecycle", "full", "duplicate-tier":
				tier := change
				if tier == "duplicate-tier" {
					tier = ProfileFull
				}
				p := policy.Profiles[tier]
				p.Units = slices.Clone(p.Units)
				if change == "duplicate-tier" {
					p.Units = append(p.Units, id)
				} else {
					p.Units = slices.DeleteFunc(p.Units, func(member string) bool { return member == id })
				}
				policy.Profiles[tier] = p
			}
			policy.Units[id] = u
			if err := validateStoreAdmissionEnvelopes(policy); err == nil {
				t.Fatal("changed partition envelope/selection accepted")
			}
		})
	}
}
