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

func TestRuntimeFanOutPartitionPreservesCompleteRoots(t *testing.T) {
	root := filepath.Join("..", "..")
	file, err := os.Open(filepath.Join(root, ".github", "test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	policy, err := LoadPolicy(file)
	if err != nil {
		t.Fatal(err)
	}
	ids := runtimeFanOutUnits
	if err := validateRuntimeFanOutEnvelopes(policy); err != nil {
		t.Fatal(err)
	}
	old := regexp.MustCompile(`^Test([D-E].*|F($|[^o].*|o($|[^r].*|r($|[^k].*|k($|[^G].*)))))$`)
	patterns := make([]*regexp.Regexp, len(ids))
	for i, id := range ids {
		patterns[i] = regexp.MustCompile(policy.Units[id].Run)
	}
	dir := filepath.Join(root, "internal/store/internal/runtimepersistence")
	paths, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	var groups [3][]string
	for _, path := range paths {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			name := fn.Name.Name
			if strings.HasPrefix(name, "TestFanOut") {
				process := strings.HasPrefix(name, "TestFanOutProcess")
				if patterns[1].MatchString(name) == process || patterns[2].MatchString(name) != process {
					t.Errorf("%s must retain its complete process/non-process owner", name)
				}
			}
			owners := 0
			for i, pattern := range patterns {
				if pattern.MatchString(name) {
					owners++
					groups[i] = append(groups[i], name)
				}
			}
			want := 0
			if old.MatchString(name) {
				want = 1
			}
			if owners != want {
				t.Errorf("%s has %d owners, want %d from original full02", name, owners, want)
			}
		}
	}
	for i, group := range groups {
		sort.Strings(group)
		if len(group) == 0 {
			t.Fatalf("%s empty", ids[i])
		}
		for _, name := range group {
			t.Logf("%s\t%s", ids[i], name)
		}
	}
	t.Logf("complete disjoint census: %d = %d D-F + %d fanout + %d process", len(groups[0])+len(groups[1])+len(groups[2]), len(groups[0]), len(groups[1]), len(groups[2]))
	// Keep the whole-package census: new roots cannot escape via either partition.
	for _, profile := range []string{ProfilePRCommon, ProfilePREscalated, ProfileFull, ProfileNightly} {
		var runs []string
		for _, id := range policy.Profiles[profile].Units {
			unit := policy.Units[id]
			for _, pkg := range unit.Packages {
				if pkg == policy.Module+"/internal/store/internal/runtimepersistence" {
					runs = append(runs, unit.Run)
				}
			}
		}
		if err := ValidateGoProofPartition(dir, runs); err != nil {
			t.Fatalf("%s: %v", profile, err)
		}
	}
}

var runtimeFanOutUnits = []string{"store-runtime-full-02", "store-runtime-fanout", "store-runtime-fanout-process"}

func validateRuntimeFanOutEnvelopes(policy Policy) error {
	for _, id := range runtimeFanOutUnits {
		u := policy.Units[id]
		if !reflect.DeepEqual(u.Packages, []string{"github.com/division-sh/swarm/internal/store/internal/runtimepersistence"}) ||
			u.CountMode != "count-1" || u.EnvironmentID != "ci-postgres-gateway-empty-v1" ||
			u.BudgetClass != "broad" || u.GoTimeout != "" || u.Skip != "" || strings.Contains(u.Run, "/") {
			return fmt.Errorf("%s changed proof envelope: %+v", id, u)
		}
		for _, profile := range []string{ProfilePRCommon, ProfilePREscalated, ProfileFull, ProfileNightly} {
			count := 0
			for _, member := range policy.Profiles[profile].Units {
				if member == id {
					count++
				}
			}
			if count != 1 {
				return fmt.Errorf("%s has %d mandatory %s units", profile, count, id)
			}
		}
	}
	return nil
}

func TestRuntimeFanOutPartitionRejectsScopeDrift(t *testing.T) {
	load := func(t *testing.T) Policy {
		t.Helper()
		f, err := os.Open(filepath.Join("..", "..", ".github", "test-proof-plan.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		p, err := LoadPolicy(f)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	for _, tc := range []struct {
		name string
		edit func(*UnitPolicy)
	}{
		{"omitted", func(u *UnitPolicy) { u.Run = "^$" }},
		{"overlap", func(u *UnitPolicy) { u.Run = "^TestFanOut.*$" }},
		{"foreign_root", func(u *UnitPolicy) { u.Run = "^Test.*$" }},
		{"backend_skip", func(u *UnitPolicy) { u.Skip = "postgres" }},
		{"partial_root", func(u *UnitPolicy) { u.Run += "/sqlite" }},
		{"cached", func(u *UnitPolicy) { u.CountMode = "cache-default" }},
		{"budget", func(u *UnitPolicy) { u.BudgetClass = "full" }},
		{"timeout", func(u *UnitPolicy) { u.GoTimeout = "5m" }},
		{"environment", func(u *UnitPolicy) { u.EnvironmentID = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := load(t)
			u := p.Units["store-runtime-fanout-process"]
			tc.edit(&u)
			p.Units["store-runtime-fanout-process"] = u
			if validateRuntimeFanOutEnvelopes(p) != nil {
				return
			}
			var runs []string
			for _, id := range p.Profiles[ProfilePRCommon].Units {
				unit := p.Units[id]
				if reflect.DeepEqual(unit.Packages, u.Packages) {
					runs = append(runs, unit.Run)
				}
			}
			if ValidateGoProofPartition(filepath.Join("..", "..", "internal/store/internal/runtimepersistence"), runs) == nil {
				t.Fatal("changed proof coverage accepted")
			}
		})
	}
	for _, profile := range []string{ProfilePRCommon, ProfilePREscalated, ProfileFull, ProfileNightly} {
		t.Run("missing_"+profile, func(t *testing.T) {
			p := load(t)
			row := p.Profiles[profile]
			var keep []string
			for _, id := range row.Units {
				if id != "store-runtime-fanout-process" {
					keep = append(keep, id)
				}
			}
			row.Units = keep
			p.Profiles[profile] = row
			if validateRuntimeFanOutEnvelopes(p) == nil {
				t.Fatal("optional process proof accepted")
			}
		})
	}
	// Exercise the positive complement at every partial prefix, not just today's roots.
	p := load(t)
	ordinary := regexp.MustCompile(p.Units["store-runtime-fanout"].Run)
	process := regexp.MustCompile(p.Units["store-runtime-fanout-process"].Run)
	for i := 0; i <= len("Process"); i++ {
		for _, suffix := range []string{"", "NewProof", "process", "x"} {
			name := "TestFanOut" + "Process"[:i] + suffix
			wantProcess := strings.HasPrefix(name, "TestFanOutProcess")
			if process.MatchString(name) != wantProcess || ordinary.MatchString(name) == wantProcess {
				t.Fatalf("new root %s omitted, duplicated or misclassified", name)
			}
		}
	}
}
