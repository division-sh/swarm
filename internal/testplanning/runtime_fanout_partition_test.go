package testplanning

import (
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
	ids := []string{"store-runtime-full-02", "store-runtime-fanout"}
	old := regexp.MustCompile(`^Test([D-E].*|F($|[^o].*|o($|[^r].*|r($|[^k].*|k($|[^G].*)))))$`)
	patterns := make([]*regexp.Regexp, len(ids))
	for i, id := range ids {
		unit := policy.Units[id]
		if !reflect.DeepEqual(unit.Packages, []string{"github.com/division-sh/swarm/internal/store/internal/runtimepersistence"}) ||
			unit.CountMode != "count-1" || unit.EnvironmentID != "ci-postgres-gateway-empty-v1" ||
			unit.BudgetClass != "broad" || unit.GoTimeout != "" || unit.Skip != "" || strings.Contains(unit.Run, "/") {
			t.Fatalf("%s changed workload, repetition, environment or budget: %+v", id, unit)
		}
		patterns[i] = regexp.MustCompile(unit.Run)
		for _, profile := range []string{ProfilePRCommon, ProfilePREscalated, ProfileFull, ProfileNightly} {
			count := 0
			for _, member := range policy.Profiles[profile].Units {
				if member == id {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("%s has %d mandatory %s units, want 1", profile, count, id)
			}
		}
	}
	dir := filepath.Join(root, "internal/store/internal/runtimepersistence")
	paths, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	var groups [2][]string
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
	t.Logf("complete disjoint census: %d = %d remainder + %d fanout", len(groups[0])+len(groups[1]), len(groups[0]), len(groups[1]))
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
