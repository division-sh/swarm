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
			if ok && fn.Recv == nil && (strings.HasPrefix(fn.Name.Name, "Test") || strings.HasPrefix(fn.Name.Name, "Example") || strings.HasPrefix(fn.Name.Name, "Fuzz")) {
				names = append(names, fn.Name.Name)
			}
		}
	}
	sort.Strings(names)
	return policy, names, dir
}

func validateConformance2394Partition(policy Policy, names []string) ([][]string, error) {
	pkg := []string{"github.com/division-sh/swarm/internal/runtime/conformance"}
	patterns := make([]*regexp.Regexp, len(conformance2394Units))
	groups := make([][]string, len(patterns))
	for i, id := range conformance2394Units {
		u := policy.Units[id]
		if !reflect.DeepEqual(u.Packages, pkg) || u.CountMode != "count-1" || u.EnvironmentID != "ci-postgres-gateway-empty-v1" || u.BudgetClass != "broad" || u.GoTimeout != "" || u.Skip != "" || strings.Contains(u.Run, "/") {
			return nil, fmt.Errorf("%s changed package/count/environment/budget or filtered a root: %+v", id, u)
		}
		var err error
		patterns[i], err = regexp.Compile(u.Run)
		if err != nil {
			return nil, err
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		id := "conformance-soak-" + backend
		want := UnitPolicy{Packages: pkg, Run: "^" + conformance2394Soak + "$/^" + backend + "$", GoTimeout: "22m", CountMode: "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", BudgetClass: "soak"}
		if !reflect.DeepEqual(policy.Units[id], want) {
			return nil, fmt.Errorf("%s changed mandatory original soak: %+v", id, policy.Units[id])
		}
	}
	ids := append(append([]string{}, conformance2394Units...), "conformance-soak-sqlite", "conformance-soak-postgres")
	for _, profile := range []string{ProfilePRCommon, ProfilePREscalated, ProfileFull, ProfileNightly} {
		for _, id := range ids {
			count := 0
			for _, member := range policy.Profiles[profile].Units {
				if member == id {
					count++
				}
			}
			want := 1
			if strings.HasPrefix(id, "conformance-soak-") && profile != ProfileNightly {
				want = 0
			}
			if count != want {
				return nil, fmt.Errorf("%s has %d scheduled %s units, want %d", profile, count, id, want)
			}
		}
	}
	old := regexp.MustCompile(`^(Test($|[^FH].*)|Example.*|Fuzz.*)$`)
	for _, name := range names {
		owners := 0
		for i, pattern := range patterns {
			if pattern.MatchString(name) {
				owners++
				groups[i] = append(groups[i], name)
			}
		}
		want := 0
		if old.MatchString(name) && name != conformance2394Soak {
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

func TestConformance2394PartitionPreservesCompleteRoots(t *testing.T) {
	policy, names, dir := conformance2394Fixture(t)
	groups, err := validateConformance2394Partition(policy, names)
	if err != nil {
		t.Fatal(err)
	}
	// #2307 adds these eight general roots to the reviewed 113-root census.
	// The generated-results guard adds one more. No prior root, backend, or
	// command envelope moves between partitions.
	actionRetirementRoots := []string{
		"TestActionRetirementCorpusLedgerIsComplete",
		"TestActionRetirementCorpusHasNoLiveAuthoredActions",
		"TestActionRetirementCorpusScannerPreservesHomonymsAndRejectsAliases",
		"TestActionRetirementCorpusScannerRejectsTruncatedFragments",
		"TestRetiredActionHistoricalFixtureFailsClosed",
		"TestNoRetiredHandlerActionInterpreters",
		"TestNoRetiredHandlerActionInterpretersRejectsHostileRestoration",
		"TestCanonicalFormsRegistryPinsHandlerActionRetirement",
	}
	want := []int{122, 14, 5, 1}
	for i, group := range groups {
		if len(group) != want[i] {
			t.Fatalf("%s census=%d, want reviewed %d; account new roots explicitly", conformance2394Units[i], len(group), want[i])
		}
		for _, name := range group {
			t.Logf("%s\t%s", conformance2394Units[i], name)
		}
	}
	const preparedFaultProof = "TestSemanticProofPreparedFaultMatchesRawBothStores"
	if i := sort.SearchStrings(groups[0], preparedFaultProof); i == len(groups[0]) || groups[0][i] != preparedFaultProof {
		t.Fatalf("general conformance partition omitted %s", preparedFaultProof)
	}
	for _, name := range actionRetirementRoots {
		if i := sort.SearchStrings(groups[0], name); i == len(groups[0]) || groups[0][i] != name {
			t.Fatalf("general conformance partition omitted reviewed #2307 root %s", name)
		}
	}
	const generatedResultsProof = "TestActionRetirementCorpusExcludesGeneratedTestResults"
	if i := sort.SearchStrings(groups[0], generatedResultsProof); i == len(groups[0]) || groups[0][i] != generatedResultsProof {
		t.Fatalf("general conformance partition omitted generated-results guard %s", generatedResultsProof)
	}
	t.Log("complete disjoint census:141 =121 general +14 core +5 pressure +1 reporter")
	for _, profile := range []string{ProfilePRCommon, ProfilePREscalated, ProfileFull, ProfileNightly} {
		var units []ProofUnit
		for _, id := range policy.Profiles[profile].Units {
			u := policy.Units[id]
			for _, pkg := range u.Packages {
				if pkg == policy.Module+"/internal/runtime/conformance" {
					units = append(units, ProofUnit{ID: id, Packages: u.Packages, Run: u.Run, Skip: u.Skip, GoTimeout: u.GoTimeout, CountMode: u.CountMode, BudgetClass: u.BudgetClass})
				}
			}
		}
		if err := ValidateConformanceProofPartition(dir, units); err != nil {
			t.Fatalf("%s: %v", profile, err)
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
		p := policy.Profiles[ProfilePRCommon]
		var keep []string
		for _, id := range p.Units {
			if id != "conformance-2394-pressure" {
				keep = append(keep, id)
			}
		}
		p.Units = keep
		policy.Profiles[ProfilePRCommon] = p
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
