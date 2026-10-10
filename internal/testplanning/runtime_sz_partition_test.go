package testplanning

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

const originalRuntimeSZRun = `^Test[S-Z].*$`
const originalRuntimeSZChildren = "948ca28d566a020c6689516404b061fa4bde16aa1250fa34ea630c622a4682e2"

var runtimeSZUnits = []string{"store-runtime-selected", "store-runtime-s-z-rest"}

func TestRuntimeSZPartitionPreservesCompleteRootsAndChildren(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	if err := validateRuntimeSZEnvelopes(policy); err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeSZChildren(policy); err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeSZPartition(policy, filepath.Join("..", "..", "internal/store/internal/runtimepersistence")); err != nil {
		t.Fatal(err)
	}
}

func validateRuntimeSZEnvelopes(policy Policy) error {
	if _, exists := policy.Units["store-runtime-full-06"]; exists {
		return fmt.Errorf("retired full-06 unit survives")
	}
	for _, id := range runtimeSZUnits {
		unit, exists := policy.Units[id]
		if !exists || !reflect.DeepEqual(unit.Packages, []string{policy.Module + "/internal/store/internal/runtimepersistence"}) ||
			unit.CountMode != "count-1" || unit.EnvironmentID != "" || !reflect.DeepEqual(unit.EnvironmentIDs, managedEnvironmentIDs()) ||
			unit.BudgetClass != "broad" || unit.Skip != "" || unit.GoTimeout != "" || strings.Contains(unit.Run, "/") ||
			!strings.HasPrefix(unit.Run, "^Test") || !strings.HasSuffix(unit.Run, "$") {
			return fmt.Errorf("%s changed execution envelope: %+v", id, unit)
		}
		for _, profile := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			count := 0
			for _, member := range policy.Profiles[profile].Units {
				if member == id {
					count++
				}
			}
			want := 1
			if profile == ProfileCore {
				want = 0
			}
			if count != want {
				return fmt.Errorf("%s has %d %s owners, want %d", id, count, profile, want)
			}
		}
	}
	return nil
}

func validateRuntimeSZChildren(policy Policy) error {
	children := map[string][]string{}
	for _, id := range runtimeSZUnits {
		selected, err := regexp.Compile(policy.Units[id].Run)
		if err != nil {
			return err
		}
		for root, cells := range policy.Units[id].RequiredChildren {
			if _, duplicate := children[root]; duplicate || !selected.MatchString(root) {
				return fmt.Errorf("%s has duplicate or unselected child owner %s", id, root)
			}
			children[root] = cells
		}
	}
	// Freeze the original backend/fault obligations, including their order,
	// without maintaining a second executable child inventory.
	data, err := json.Marshal(children)
	if err != nil {
		return err
	}
	if actual := fmt.Sprintf("%x", sha256.Sum256(data)); actual != originalRuntimeSZChildren {
		return fmt.Errorf("original full-06 required children changed: %s", actual)
	}
	return nil
}

func validateRuntimeSZPartition(policy Policy, dir string) error {
	old := regexp.MustCompile(originalRuntimeSZRun)
	matchers := []func(string) bool{func(name string) bool { return !old.MatchString(name) }}
	for _, id := range runtimeSZUnits {
		pattern, err := regexp.Compile(policy.Units[id].Run)
		if err != nil {
			return err
		}
		matchers = append(matchers, pattern.MatchString)
	}
	return validateGoProofMatchers(dir, matchers)
}

func TestRuntimeSZPartitionOwnsProspectivePrefixRoots(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	selected := regexp.MustCompile(policy.Units[runtimeSZUnits[0]].Run)
	rest := regexp.MustCompile(policy.Units[runtimeSZUnits[1]].Run)
	names := []string{"TestS", "TestSe", "TestSel", "TestSele", "TestSelec", "TestSelect", "TestSelecte", "TestSelected", "TestSelectedFuture", "TestSelection", "TestSelenium", "TestSQL", "TestStanding", "TestT", "TestZ", "TestOther"}
	for i := 1; i < len("Selected"); i++ {
		for _, ch := range "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_" {
			names = append(names, "Test"+"Selected"[:i]+string(ch)+"Future")
		}
	}
	old := regexp.MustCompile(originalRuntimeSZRun)
	for _, name := range names {
		wantSelected := strings.HasPrefix(name, "TestSelected")
		wantRest := old.MatchString(name) && !wantSelected
		if selected.MatchString(name) != wantSelected || rest.MatchString(name) != wantRest {
			t.Errorf("%s selected=%v rest=%v, want %v/%v", name, selected.MatchString(name), rest.MatchString(name), wantSelected, wantRest)
		}
	}
}

func TestRuntimeSZPartitionRejectsScopeAndBackendDrift(t *testing.T) {
	for _, change := range []string{"omit", "overlap", "foreign", "partial", "skip", "cached", "budget", "timeout", "environment", "optional", "duplicate", "core", "backend_loss", "extra_child", "duplicate_child", "retired"} {
		t.Run(change, func(t *testing.T) {
			policy := loadPersistenceDebtPolicy(t)
			unit := policy.Units[runtimeSZUnits[0]]
			root := "TestSelectedTerminatedOriginStartupBothStores"
			switch change {
			case "omit":
				unit.Run = "^TestSelectedMissing$"
			case "overlap":
				unit.Run = originalRuntimeSZRun
			case "foreign":
				unit.Run = "^Test.*$"
			case "partial":
				unit.Run += "/sqlite"
			case "skip":
				unit.Skip = "postgres"
			case "cached":
				unit.CountMode = "cache-default"
			case "budget":
				unit.BudgetClass = "full"
			case "timeout":
				unit.GoTimeout = "20m"
			case "environment":
				unit.EnvironmentID = "foreign"
			case "optional":
				profile := policy.Profiles[ProfileLifecycle]
				for i, id := range profile.Units {
					if id == runtimeSZUnits[0] {
						profile.Units = append(profile.Units[:i:i], profile.Units[i+1:]...)
						break
					}
				}
				policy.Profiles[ProfileLifecycle] = profile
			case "duplicate", "core":
				profileName := ProfileFull
				if change == "core" {
					profileName = ProfileCore
				}
				profile := policy.Profiles[profileName]
				profile.Units = append(profile.Units, runtimeSZUnits[0])
				policy.Profiles[profileName] = profile
			case "backend_loss":
				unit.RequiredChildren[root] = unit.RequiredChildren[root][:21]
			case "extra_child":
				unit.RequiredChildren[root] = append(unit.RequiredChildren[root], "postgres/foreign")
			case "duplicate_child":
				other := policy.Units[runtimeSZUnits[1]]
				other.RequiredChildren[root] = unit.RequiredChildren[root]
				policy.Units[runtimeSZUnits[1]] = other
			case "retired":
				policy.Units["store-runtime-full-06"] = unit
			}
			policy.Units[runtimeSZUnits[0]] = unit
			if validateRuntimeSZEnvelopes(policy) != nil || validateRuntimeSZChildren(policy) != nil ||
				validateRuntimeSZPartition(policy, filepath.Join("..", "..", "internal/store/internal/runtimepersistence")) != nil {
				return
			}
			t.Fatal("changed proof obligations accepted")
		})
	}
}

func TestRuntimeSZPartitionCensusRejectsFutureGap(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "future_test.go"), []byte("package future\nimport \"testing\"\nfunc TestSelectedFuture(t *testing.T) {}\nfunc TestSeleniumFuture(t *testing.T) {}\nfunc TestOther(t *testing.T) {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeSZPartition(policy, dir); err != nil {
		t.Fatal(err)
	}
	unit := policy.Units[runtimeSZUnits[1]]
	unit.Run = "^TestStanding.*$"
	policy.Units[runtimeSZUnits[1]] = unit
	if err := validateRuntimeSZPartition(policy, dir); err == nil {
		t.Fatal("new S-Z root escaped its partition census")
	}
}
