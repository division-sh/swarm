package testchanged

import (
	"reflect"
	"testing"
)

func TestPlanChangedSelectsChangedGoPackageAndReverseDependents(t *testing.T) {
	pkgs := []Package{
		{ImportPath: "github.com/division-sh/swarm/internal/a", RelDir: "internal/a"},
		{ImportPath: "github.com/division-sh/swarm/internal/b", RelDir: "internal/b", Imports: []string{"github.com/division-sh/swarm/internal/a"}},
		{ImportPath: "github.com/division-sh/swarm/internal/c", RelDir: "internal/c", TestImports: []string{"github.com/division-sh/swarm/internal/b"}},
	}
	plan, err := PlanChanged(".", pkgs, []ChangedFile{{Path: "internal/a/a.go", Status: "M"}})
	if err != nil {
		t.Fatalf("plan changed: %v", err)
	}
	want := []string{"./internal/a", "./internal/b", "./internal/c"}
	if got := packagePatterns(plan.Packages); !reflect.DeepEqual(got, want) {
		t.Fatalf("packages = %#v, want %#v", got, want)
	}
	if got := packagePatterns(plan.SeedPackages); !reflect.DeepEqual(got, []string{"./internal/a"}) {
		t.Fatalf("seed packages = %#v", got)
	}
	if got := packagePatterns(plan.DependentPackages); !reflect.DeepEqual(got, []string{"./internal/b", "./internal/c"}) {
		t.Fatalf("dependent packages = %#v", got)
	}
}

func TestPlanChangedUsesXTestImportsForReverseDependents(t *testing.T) {
	pkgs := []Package{
		{ImportPath: "github.com/division-sh/swarm/internal/a", RelDir: "internal/a"},
		{ImportPath: "github.com/division-sh/swarm/internal/exttest", RelDir: "internal/exttest", XTestImports: []string{"github.com/division-sh/swarm/internal/a"}},
	}
	plan, err := PlanChanged(".", pkgs, []ChangedFile{{Path: "internal/a/a_test.go", Status: "M"}})
	if err != nil {
		t.Fatalf("plan changed: %v", err)
	}
	want := []string{"./internal/a", "./internal/exttest"}
	if got := packagePatterns(plan.Packages); !reflect.DeepEqual(got, want) {
		t.Fatalf("packages = %#v, want %#v", got, want)
	}
}

func TestPlanChangedMapsNonGoFilesInsidePackageDirectories(t *testing.T) {
	pkgs := []Package{
		{ImportPath: "github.com/division-sh/swarm/internal/a", RelDir: "internal/a"},
	}
	plan, err := PlanChanged(".", pkgs, []ChangedFile{{Path: "internal/a/testdata/case.yaml", Status: "M"}})
	if err != nil {
		t.Fatalf("plan changed: %v", err)
	}
	if got, want := packagePatterns(plan.Packages), []string{"./internal/a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("packages = %#v, want %#v", got, want)
	}
}

func TestPlanChangedMapsTextFixturesInsidePackageDirectories(t *testing.T) {
	pkgs := []Package{
		{ImportPath: "github.com/division-sh/swarm/internal/a", RelDir: "internal/a"},
	}
	plan, err := PlanChanged(".", pkgs, []ChangedFile{{Path: "internal/a/testdata/golden.txt", Status: "M"}})
	if err != nil {
		t.Fatalf("plan changed: %v", err)
	}
	if got, want := packagePatterns(plan.Packages), []string{"./internal/a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("packages = %#v, want %#v", got, want)
	}
	if plan.DocsOnly {
		t.Fatalf("DocsOnly = true, want false")
	}
}

func TestPlanChangedDeletedGoFileWithoutCurrentPackageFallsBackToFullSuite(t *testing.T) {
	pkgs := []Package{
		{ImportPath: "github.com/division-sh/swarm/internal/a", RelDir: "internal/a"},
	}
	plan, err := PlanChanged(".", pkgs, []ChangedFile{{Path: "internal/deleted/old.go", Status: "D"}})
	if err != nil {
		t.Fatalf("plan changed: %v", err)
	}
	if !plan.FullSuite {
		t.Fatalf("FullSuite = false, want true")
	}
	if got, want := TestCommand(plan, nil), []string{"go", "test", "./..."}; !reflect.DeepEqual(got, want) {
		t.Fatalf("command = %#v, want %#v", got, want)
	}
}

func TestPlanChangedGlobalFilesFallBackToFullSuite(t *testing.T) {
	plan, err := PlanChanged(".", nil, []ChangedFile{{Path: "go.mod", Status: "M"}})
	if err != nil {
		t.Fatalf("plan changed: %v", err)
	}
	if !plan.FullSuite {
		t.Fatalf("FullSuite = false, want true")
	}
	if got, want := plan.FullSuiteReasons, []string{"go.mod changed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("full suite reasons = %#v, want %#v", got, want)
	}
}

func TestPlanChangedDocsOnlySelectsNoPackages(t *testing.T) {
	pkgs := []Package{
		{ImportPath: "github.com/division-sh/swarm", RelDir: "."},
	}
	plan, err := PlanChanged(".", pkgs, []ChangedFile{
		{Path: "README.md", Status: "M"},
		{Path: "docs/local-testing.md", Status: "M"},
	})
	if err != nil {
		t.Fatalf("plan changed: %v", err)
	}
	if !plan.DocsOnly {
		t.Fatalf("DocsOnly = false, want true")
	}
	if len(plan.Packages) != 0 {
		t.Fatalf("packages = %#v, want none", packagePatterns(plan.Packages))
	}
	if got := TestCommand(plan, nil); got != nil {
		t.Fatalf("command = %#v, want nil", got)
	}
}

func TestPlanChangedExecutableMarkdownFixtureFallsBackToFullSuite(t *testing.T) {
	path := "tests/tier7-composition/test-agent-emits-to-node/prompts/test-agent.md"
	pkgs := []Package{
		{ImportPath: "github.com/division-sh/swarm/internal/runtime/swarmflowtest", RelDir: "internal/runtime/swarmflowtest"},
		{ImportPath: "github.com/division-sh/swarm/internal/runtime/cataloge2e", RelDir: "internal/runtime/cataloge2e"},
		{ImportPath: "github.com/division-sh/swarm/internal/runtime/runforkexecution", RelDir: "internal/runtime/runforkexecution"},
	}
	plan, err := PlanChanged(".", pkgs, []ChangedFile{{Path: path, Status: "M"}})
	if err != nil {
		t.Fatalf("plan changed: %v", err)
	}
	if plan.DocsOnly {
		t.Fatalf("DocsOnly = true, want false")
	}
	if !plan.FullSuite {
		t.Fatalf("FullSuite = false, want true")
	}
	if got, want := plan.FullSuiteReasons, []string{path + " has no owning Go package"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("full suite reasons = %#v, want %#v", got, want)
	}
	if got, want := TestCommand(plan, nil), []string{"go", "test", "./..."}; !reflect.DeepEqual(got, want) {
		t.Fatalf("command = %#v, want %#v", got, want)
	}
}

func TestPlanChangedUnownedNonDocFallsBackToFullSuite(t *testing.T) {
	plan, err := PlanChanged(".", nil, []ChangedFile{{Path: ".github/workflows/ci.yml", Status: "M"}})
	if err != nil {
		t.Fatalf("plan changed: %v", err)
	}
	if !plan.FullSuite {
		t.Fatalf("FullSuite = false, want true")
	}
}

func packagePatterns(packages []Package) []string {
	out := make([]string, 0, len(packages))
	for _, pkg := range packages {
		out = append(out, pkg.Pattern())
	}
	return out
}
