package testplanning

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var conformance2394Units = []string{
	"conformance-2", "conformance-2394-core", "conformance-2394-pressure", "conformance-2394-reporter",
	"conformance-2394-reporter-window",
}

const conformance2394Soak = "TestIssue2394TwentyTwoIntentFifteenMinuteSoakBothStores"

func conformance2394Fixture(t *testing.T) (Policy, []string, string) {
	t.Helper()
	root := filepath.Join("..", "..")
	f, err := os.Open(filepath.Join(root, ".github", "test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	policy, err := LoadPolicy(f)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "internal/runtime/conformance")
	paths, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, path := range paths {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			// TestMain owns native worker entry but is not an executable test root.
			if ok && fn.Recv == nil && fn.Name.Name != "TestMain" && (strings.HasPrefix(fn.Name.Name, "Test") || strings.HasPrefix(fn.Name.Name, "Example") || strings.HasPrefix(fn.Name.Name, "Fuzz")) {
				names = append(names, fn.Name.Name)
			}
		}
	}
	sort.Strings(names)
	return policy, names, dir
}

func validateConformance2394Partition(policy Policy, names []string) ([][]string, error) {
	pkg := []string{"github.com/division-sh/swarm/internal/runtime/conformance"}
	selectors := make([]unitSelector, len(conformance2394Units))
	groups := make([][]string, len(selectors))
	for i, id := range conformance2394Units {
		u := policy.Units[id]
		wantSkip := ""
		if id == "conformance-2" {
			wantSkip = "^" + conformanceRebalancedReporterRoot + "$"
		}
		if !reflect.DeepEqual(u.Packages, pkg) || u.CountMode != "count-1" || u.EnvironmentID != "" || !reflect.DeepEqual(u.EnvironmentIDs, managedEnvironmentIDs()) || u.BudgetClass != "broad" || u.GoTimeout != "" || u.Skip != wantSkip || strings.Contains(u.Run, "/") {
			return nil, fmt.Errorf("%s changed package/count/environment/budget or filtered a root: %+v", id, u)
		}
		if _, err := regexp.Compile(u.Run); err != nil {
			return nil, err
		}
		selectors[i] = compileUnitSelector(ProofUnit{Run: u.Run, Skip: u.Skip})
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		id := "conformance-soak-" + backend
		want := UnitPolicy{Packages: pkg, Run: "^" + conformance2394Soak + "$/^" + backend + "$", GoTimeout: "22m", CountMode: "count-1", EnvironmentIDs: managedEnvironmentIDs(), BudgetClass: "soak"}
		if !reflect.DeepEqual(policy.Units[id], want) {
			return nil, fmt.Errorf("%s changed mandatory original soak: %+v", id, policy.Units[id])
		}
	}
	ids := append(append([]string{}, conformance2394Units...), "conformance-soak-sqlite", "conformance-soak-postgres")
	for _, profile := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		for _, id := range ids {
			count := 0
			for _, member := range policy.Profiles[profile].Units {
				if member == id {
					count++
				}
			}
			want := 1
			if profile == ProfileCore && id != "conformance-2394-pressure" || profile != ProfileFull && (strings.HasPrefix(id, "conformance-soak-") || id == "conformance-2394-reporter") {
				want = 0
			}
			if count != want {
				return nil, fmt.Errorf("%s has %d scheduled %s units, want %d", profile, count, id, want)
			}
		}
	}
	old := regexp.MustCompile(`^(Test($|[^FHV].*)|Example.*|Fuzz.*)$`)
	for _, name := range names {
		owners := 0
		for i, selector := range selectors {
			selected, err := selector.matches(name)
			if err != nil {
				return nil, err
			}
			if selected {
				owners++
				groups[i] = append(groups[i], name)
			}
		}
		want := 0
		if old.MatchString(name) && name != conformance2394Soak && name != conformanceRebalancedReporterRoot {
			want = 1
		}
		if owners != want {
			return nil, fmt.Errorf("%s has %d owners, want %d from original conformance-2", name, owners, want)
		}
	}
	for i, group := range groups {
		if len(group) == 0 {
			return nil, fmt.Errorf("%s is empty", conformance2394Units[i])
		}
	}
	return groups, nil
}

func TestConformanceVolumeFanOutProofPartition(t *testing.T) {
	policy, names, _ := conformance2394Fixture(t)
	const unitID = "conformance-heavy-fanout"
	const selection = `^TestVolumeFanOut(ExactJobflow1362ImportRouteAndSettleBothStores|ServingCardinalityMixedOutputPartitionEquivalenceBothStores)$`
	unit := policy.Units[unitID]
	want := UnitPolicy{
		Packages: []string{"github.com/division-sh/swarm/internal/runtime/conformance"},
		Run:      selection, CountMode: "count-1", EnvironmentIDs: managedEnvironmentIDs(), BudgetClass: "full",
	}
	if !reflect.DeepEqual(unit, want) {
		t.Fatalf("heavy fan-out proof envelope changed: %+v", unit)
	}
	selected := regexp.MustCompile(selection)
	general := regexp.MustCompile(policy.Units["conformance-1"].Run)
	complement := regexp.MustCompile(policy.Units["conformance-2"].Run)
	var selectedNames []string
	for _, name := range names {
		if !strings.HasPrefix(name, "TestVolume") {
			continue
		}
		if !selected.MatchString(name) || general.MatchString(name) || complement.MatchString(name) {
			t.Fatalf("volume proof %s is missing or overlaps another conformance unit", name)
		}
		selectedNames = append(selectedNames, name)
	}
	if len(selectedNames) != 2 {
		t.Fatalf("heavy fan-out partition has %d roots, want two: %v", len(selectedNames), selectedNames)
	}
	for _, profile := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		count := 0
		for _, id := range policy.Profiles[profile].Units {
			if id == unitID {
				count++
			}
		}
		want := 0
		if profile == ProfileFull {
			want = 1
		}
		if count != want {
			t.Fatalf("%s schedules %s %d times, want %d", profile, unitID, count, want)
		}
	}
}

func TestConformance2394PartitionRejectsScopeDrift(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*UnitPolicy)
	}{
		{"omitted", func(u *UnitPolicy) { u.Run = "^$" }},
		{"overlap", func(u *UnitPolicy) { u.Run = "^TestIssue2394.*$" }},
		{"foreign_root", func(u *UnitPolicy) { u.Run = "^Test.*$" }},
		{"backend_skip", func(u *UnitPolicy) { u.Skip = "postgres" }},
		{"backend_selection", func(u *UnitPolicy) { u.Run += "/sqlite" }},
		{"count", func(u *UnitPolicy) { u.CountMode = "cache-default" }},
		{"budget", func(u *UnitPolicy) { u.BudgetClass = "full" }},
		{"go_timeout", func(u *UnitPolicy) { u.GoTimeout = "5m" }},
		{"environment", func(u *UnitPolicy) { u.EnvironmentID = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy, names, _ := conformance2394Fixture(t)
			u := policy.Units["conformance-2394-core"]
			tc.edit(&u)
			policy.Units["conformance-2394-core"] = u
			if _, err := validateConformance2394Partition(policy, names); err == nil {
				t.Fatal("changed root scope/envelope accepted")
			}
		})
	}
	t.Run("unclassified_new_root", func(t *testing.T) {
		policy, names, _ := conformance2394Fixture(t)
		names = append(names, "TestIssue2394UnclassifiedNewProof")
		if _, err := validateConformance2394Partition(policy, names); err == nil {
			t.Fatal("new unselected ordinary root accepted")
		}
	})
	t.Run("optional_profile", func(t *testing.T) {
		policy, names, _ := conformance2394Fixture(t)
		p := policy.Profiles[ProfileCore]
		var keep []string
		for _, id := range p.Units {
			if id != "conformance-2394-pressure" {
				keep = append(keep, id)
			}
		}
		p.Units = keep
		policy.Profiles[ProfileCore] = p
		if _, err := validateConformance2394Partition(policy, names); err == nil {
			t.Fatal("optional ordinary unit accepted")
		}
	})
	t.Run("shortened_soak", func(t *testing.T) {
		policy, names, _ := conformance2394Fixture(t)
		u := policy.Units["conformance-soak-postgres"]
		u.GoTimeout = "3m"
		policy.Units["conformance-soak-postgres"] = u
		if _, err := validateConformance2394Partition(policy, names); err == nil {
			t.Fatal("changed original soak accepted")
		}
	})
}
