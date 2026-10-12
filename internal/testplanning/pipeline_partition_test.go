package testplanning

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

const pipelineProofPackage = "github.com/division-sh/swarm/internal/runtime/pipeline"

func pipelinePartitionUnits() []ProofUnit {
	return []ProofUnit{
		{ID: "pipeline-activation", Run: `^Test(A2|CompiledTransition|Guard|PipelineCompiled|WorkflowCurrentTransition|JoinCapturedLoop).*$`},
		{ID: "pipeline-timer-join", Run: `^Test(WorkflowTimer|WorkflowJoin|AcceptedWorkflowTimer|MutationFreeAcceptedEventTimer|Issue2564M09).*$`},
		{ID: "pipeline-rest", Run: `^(Test|Fuzz|Example)`, Skip: `^Test(A2|CompiledTransition|Guard|PipelineCompiled|WorkflowCurrentTransition|JoinCapturedLoop|WorkflowTimer|WorkflowJoin|AcceptedWorkflowTimer|MutationFreeAcceptedEventTimer|Issue2564M09).*$`},
	}
}

func declaredPipelineUnits(policy Policy) []ProofUnit {
	units := pipelinePartitionUnits()
	for i := range units {
		declared := policy.Units[units[i].ID]
		units[i] = ProofUnit{ID: units[i].ID, Packages: declared.Packages, Run: declared.Run, Skip: declared.Skip,
			CountMode: declared.CountMode, EnvironmentID: declared.EnvironmentIDs[VenueCI],
			BudgetClass: declared.BudgetClass, GoTimeout: declared.GoTimeout}
	}
	return units
}

func validatePipelineEnvelopes(policy Policy) error {
	if count := slices.Index(policy.SpecialPackages, pipelineProofPackage); count < 0 {
		return fmt.Errorf("pipeline is not a special package")
	}
	for _, expected := range pipelinePartitionUnits() {
		want := UnitPolicy{Packages: []string{pipelineProofPackage}, Run: expected.Run, Skip: expected.Skip,
			CountMode: "count-1", EnvironmentIDs: managedEnvironmentIDs(), BudgetClass: "broad"}
		if expected.ID == "pipeline-rest" {
			want.RequiredChildren = map[string][]string{"TestCommittedAgentReadinessPartialAttachmentBothStores": {
				"sqlite/missing_source/shutdown_false", "sqlite/missing_sibling/shutdown_false",
				"sqlite/missing_source/shutdown_true", "sqlite/missing_sibling/shutdown_true",
				"postgres/missing_source/shutdown_false", "postgres/missing_sibling/shutdown_false",
				"postgres/missing_source/shutdown_true", "postgres/missing_sibling/shutdown_true",
			}}
		}
		if !reflect.DeepEqual(policy.Units[expected.ID], want) {
			return fmt.Errorf("%s changed whole-root execution envelope", expected.ID)
		}
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			count := 0
			for _, id := range policy.Profiles[tier].Units {
				if id == expected.ID {
					count++
				}
			}
			if count != 1 {
				return fmt.Errorf("%s has %d %s owners", tier, count, expected.ID)
			}
		}
	}
	return policy.Validate()
}

func TestPipelineRestExclusionAdmissionIsExact(t *testing.T) {
	rest := pipelinePartitionUnits()[2]
	rest.Packages, rest.CountMode, rest.BudgetClass = []string{pipelineProofPackage}, "count-1", "broad"
	admit := func(unit ProofUnit) error {
		return validateSoakSelection(unit.Packages, unit.Run, unit.Skip, unit.GoTimeout, unit.CountMode, unit.BudgetClass)
	}
	if err := admit(rest); err != nil {
		t.Fatalf("exact whole-root rest exclusion refused: %v", err)
	}
	for _, tc := range []struct {
		name string
		edit func(*ProofUnit)
	}{
		{"foreign package", func(u *ProofUnit) { u.Packages = []string{SoakPackage} }},
		{"additional package", func(u *ProofUnit) { u.Packages = append(u.Packages, SoakPackage) }},
		{"foreign skip", func(u *ProofUnit) { u.Skip = "^TestOther$" }},
		{"widened skip", func(u *ProofUnit) { u.Skip = "^Test.*$" }},
		{"noncanonical skip", func(u *ProofUnit) { u.Skip = strings.TrimSuffix(u.Skip, "$") }},
		{"empty run", func(u *ProofUnit) { u.Run = "" }},
		{"missing fuzz and example", func(u *ProofUnit) { u.Run = "^Test" }},
		{"widened run", func(u *ProofUnit) { u.Run = ".*" }},
		{"backend run", func(u *ProofUnit) { u.Run += "/postgres" }},
		{"case skip", func(u *ProofUnit) { u.Skip += "/case" }},
		{"timeout", func(u *ProofUnit) { u.GoTimeout = "10m" }},
		{"count", func(u *ProofUnit) { u.CountMode = "cache-default" }},
		{"budget", func(u *ProofUnit) { u.BudgetClass = "full" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unit := rest
			unit.Packages = slices.Clone(rest.Packages)
			tc.edit(&unit)
			if err := admit(unit); err == nil {
				t.Fatal("noncanonical non-soak exclusion admitted")
			}
		})
	}
}

func TestPipelinePartitionPreservesNativePlansAndAllOtherRoots(t *testing.T) {
	policy, inventory, parity, packages := loadVerifyRunPlanInputs(t)
	if err := validatePipelineEnvelopes(policy); err != nil {
		t.Fatal(err)
	}
	if err := ValidateGoProofUnitPartition(filepath.Join("..", "runtime", "pipeline"), declaredPipelineUnits(policy)); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join("..", "..", GeneratedWeightModelPath))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	model, err := LoadWeightModel(file)
	if err != nil {
		t.Fatal(err)
	}
	// Restore the original indivisible package on the same native source census.
	original := loadPersistenceDebtPolicy(t)
	original.SpecialPackages = slices.DeleteFunc(original.SpecialPackages, func(pkg string) bool { return pkg == pipelineProofPackage })
	for _, unit := range pipelinePartitionUnits() {
		delete(original.Units, unit.ID)
		for tier, profile := range original.Profiles {
			profile.Units = slices.DeleteFunc(profile.Units, func(id string) bool { return id == unit.ID })
			original.Profiles[tier] = profile
		}
	}
	type obligation struct {
		RequiredTest
		Count, Environment, Workload, Timeout, Budget string
	}
	obligations := func(t *testing.T, plan RunPlan) map[TestRoot][]obligation {
		t.Helper()
		result := map[TestRoot][]obligation{}
		for _, unit := range plan.Units {
			for _, required := range unit.RequiredTests {
				if _, duplicate := result[required.TestRoot]; duplicate && required.Package == pipelineProofPackage {
					t.Fatalf("duplicate proof owner: %+v", required.TestRoot)
				}
				result[required.TestRoot] = append(result[required.TestRoot], obligation{required, unit.CountMode, unit.EnvironmentID, unit.WorkloadProfile, unit.GoTimeout, unit.BudgetClass})
			}
		}
		return result
	}
	for _, venue := range []string{VenueCI, VenueLocal} {
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			t.Run(venue+"/"+tier, func(t *testing.T) {
				plans := make([]RunPlan, 2)
				for i, selected := range []Policy{original, policy} {
					plan, err := BuildPlan(selected, model, packages, tier, "pipeline partition comparison", "same-source", BuildOptions{Venue: venue})
					if err != nil {
						t.Fatal(err)
					}
					if err := BindExecution(&plan, inventory, parity, selected); err != nil {
						t.Fatal(err)
					}
					plans[i] = plan
				}
				if !reflect.DeepEqual(obligations(t, plans[0]), obligations(t, plans[1])) || !slices.Equal(plans[0].Packages, plans[1].Packages) || !reflect.DeepEqual(plans[0].DeferredRoots, plans[1].DeferredRoots) {
					t.Fatal("partition changed native roots/children, recipes, workloads, budgets or unrelated tier deferrals")
				}
				counts := map[string]int{}
				for _, unit := range plans[1].Units {
					for _, root := range unit.SelectedRoots {
						if root.Package == pipelineProofPackage {
							if !strings.HasPrefix(unit.ID, "pipeline-") {
								t.Fatalf("pipeline root survives in %s", unit.ID)
							}
							counts[unit.ID]++
						}
					}
					if old, err := plans[0].Unit(unit.ID); err == nil && strings.HasPrefix(unit.ID, "broad-") && !slices.Equal(old.Packages, unit.Packages) {
						t.Logf("rebinned %s: %d -> %d packages (complete bound obligations compared)", unit.ID, len(old.Packages), len(unit.Packages))
					}
				}
				total := counts["pipeline-activation"] + counts["pipeline-timer-join"] + counts["pipeline-rest"]
				if total != len(inventory.Packages[pipelineProofPackage].Roots) || len(counts) != 3 {
					t.Fatalf("incomplete active native pipeline union: %v, inventory=%d", counts, len(inventory.Packages[pipelineProofPackage].Roots))
				}
				t.Logf("native pipeline owners=%v total=%d old=%s new=%s", counts, total, plans[0].Digest, plans[1].Digest)
			})
		}
	}
}

func TestPipelinePartitionRejectsEnvelopeAndMembershipDrift(t *testing.T) {
	for _, change := range []string{"package", "run", "skip", "timeout", "count", "environment", "budget", "packable", "children", "special", "core", "lifecycle", "full", "duplicate"} {
		t.Run(change, func(t *testing.T) {
			policy := loadPersistenceDebtPolicy(t)
			unit := policy.Units["pipeline-rest"]
			switch change {
			case "package":
				unit.Packages = []string{SoakPackage}
			case "run":
				unit.Run = "^Test"
			case "skip":
				unit.Skip = ""
			case "timeout":
				unit.GoTimeout = "15m"
			case "count":
				unit.CountMode = "cache-default"
			case "environment":
				unit.EnvironmentIDs = maps.Clone(unit.EnvironmentIDs)
				unit.EnvironmentIDs[VenueCI] = "local-postgres-gateway-empty-v1"
			case "budget":
				unit.BudgetClass = "full"
			case "packable":
				unit.Packable = true
			case "children":
				unit.RequiredChildren = map[string][]string{"TestFuture": {"sqlite"}}
			case "special":
				policy.SpecialPackages = slices.DeleteFunc(policy.SpecialPackages, func(pkg string) bool { return pkg == pipelineProofPackage })
			case "core", "lifecycle", "full", "duplicate":
				tier := change
				if change == "duplicate" {
					tier = ProfileFull
				}
				profile := policy.Profiles[tier]
				if change == "duplicate" {
					profile.Units = append(profile.Units, "pipeline-rest")
				} else {
					profile.Units = slices.DeleteFunc(profile.Units, func(id string) bool { return id == "pipeline-rest" })
				}
				policy.Profiles[tier] = profile
			}
			policy.Units["pipeline-rest"] = unit
			if err := validatePipelineEnvelopes(policy); err == nil {
				t.Fatal("changed partition envelope accepted")
			}
		})
	}
}

const prospectivePipelineSource = `package proof
import ("fmt"; "testing")
type Future struct{}
func TestA2Future(t *testing.T) { backends(t) }
func TestWorkflowTimerFuture(t *testing.T) { backends(t) }
func TestFuture(t *testing.T) { backends(t) }
func FuzzFuture(f *testing.F) { f.Add("seed"); f.Fuzz(func(t *testing.T, s string) {}) }
func ExampleFuture() { fmt.Println("future"); /* Output: future */ }
func backends(t *testing.T) { for _, b := range []string{"sqlite", "postgres"} { t.Run(b, func(t *testing.T) { t.Run("case", func(t *testing.T) {}) }) } }
`

func pipelineInvocationFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for name, contents := range map[string]string{"go.mod": "module proof\ngo 1.25.0\n", "proof_test.go": prospectivePipelineSource} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestPipelinePartitionFutureWholeRootsAndNativeInvocation(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	units := declaredPipelineUnits(policy)
	dir := pipelineInvocationFixture(t)
	if err := ValidateGoProofUnitPartition(dir, units); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"omit activation", "omit timer", "omit rest", "duplicate", "overlap", "future gap", "backend", "case", "empty rest"} {
		t.Run(change, func(t *testing.T) {
			changed := slices.Clone(units)
			switch change {
			case "omit activation":
				changed = changed[1:]
			case "omit timer":
				changed = append(changed[:1:1], changed[2])
			case "omit rest":
				changed = changed[:2]
			case "duplicate":
				changed = append(changed, changed[0])
			case "overlap":
				changed[0].Run = "^Test.*$"
			case "future gap":
				changed[0].Run = "^TestA2Existing$"
			case "backend":
				changed[0].Run += "/sqlite"
			case "case":
				changed[1].Run += "/postgres/case"
			case "empty rest":
				changed[2].Run = ""
			}
			if err := ValidateGoProofUnitPartition(dir, changed); err == nil {
				t.Fatal("whole-root omission/overlap/filter accepted")
			}
		})
	}
	passes := map[string]int{}
	for _, unit := range units {
		args := []string{"test", "-json", "-count=1", "-run=" + unit.Run}
		if unit.Skip != "" {
			args = append(args, "-skip="+unit.Skip)
		}
		command := exec.CommandContext(context.Background(), "go", args...)
		command.Dir = dir
		command.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=")
		raw, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("native whole-root invocation %s: %v\n%s", unit.ID, err, raw)
		}
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		for {
			var event struct{ Action, Test string }
			if err := decoder.Decode(&event); err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			if event.Action == "pass" && event.Test != "" {
				passes[event.Test]++
			}
		}
	}
	want := map[string]int{"FuzzFuture": 1, "FuzzFuture/seed#0": 1, "ExampleFuture": 1}
	for _, root := range []string{"TestA2Future", "TestWorkflowTimerFuture", "TestFuture"} {
		want[root] = 1
		for _, backend := range []string{"sqlite", "postgres"} {
			want[root+"/"+backend], want[root+"/"+backend+"/case"] = 1, 1
		}
	}
	if !reflect.DeepEqual(passes, want) {
		t.Fatalf("native root/backend/case executions=%v, want=%v", passes, want)
	}
}
