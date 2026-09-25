package testplanning

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestCurrentProofPlansBindActiveRequiredRoots(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join(root, ".github/test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	policy, err := LoadPolicy(file)
	if err != nil {
		t.Fatal(err)
	}
	modelFile, err := os.Open(filepath.Join(root, GeneratedWeightModelPath))
	if err != nil {
		t.Fatal(err)
	}
	defer modelFile.Close()
	model, err := LoadWeightModel(modelFile)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := DiscoverRootInventory(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := LoadParityProofs(filepath.Join(root, "internal/apiv1/testdata/public_surface_backend_matrix.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	packages := make([]string, 0, len(inventory.Packages))
	for pkg := range inventory.Packages {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)
	for _, profile := range []string{ProfileLocal, ProfilePRCommon, ProfilePREscalated, ProfileFull, ProfileNightly} {
		t.Run(profile, func(t *testing.T) {
			plan, err := BuildPlan(policy, model, packages, profile, "census", "test-head")
			if err != nil {
				t.Fatal(err)
			}
			if err := BindExecution(&plan, inventory, proofs); err != nil {
				t.Fatal(err)
			}
			if plan.BuildContext.GOOS == "" || len(plan.Units) == 0 {
				t.Fatalf("unbound plan: %+v", plan)
			}
		})
	}
	for _, profile := range []string{ProfilePRCommon, ProfilePREscalated} {
		plan, err := BuildPlan(policy, model, packages, profile, "semantic PR", "test-head", BuildOptions{IncludeParityFull: true, IncludeSoak: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := BindExecution(&plan, inventory, proofs); err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"parity-served-source-artifact", "parity-connected-channel-onboarding-surface", "parity-destructive-reset-crash", "parity-golden-forced-restart", "conformance-soak-sqlite", "conformance-soak-postgres"} {
			unit, err := plan.Unit(id)
			if err != nil {
				t.Fatal(err)
			}
			if len(unit.RequiredTests) == 0 {
				t.Errorf("%s has no required proof", id)
			}
		}
	}
}

func TestRootDiscoveryRejectsEffectiveGOFLAGS(t *testing.T) {
	t.Setenv("GOFLAGS", "-run=^TestDefinitelyAbsent$")
	_, err := EffectiveBuildContext(context.Background(), ".")
	if err == nil || !strings.Contains(err.Error(), "GOFLAGS") {
		t.Fatalf("effective GOFLAGS = %v", err)
	}
}

func TestFilteredCatalogNewRootFailsBeforeExecution(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	policyFile, err := os.Open(filepath.Join(root, ".github/test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer policyFile.Close()
	policy, err := LoadPolicy(policyFile)
	if err != nil {
		t.Fatal(err)
	}
	modelFile, err := os.Open(filepath.Join(root, GeneratedWeightModelPath))
	if err != nil {
		t.Fatal(err)
	}
	defer modelFile.Close()
	model, err := LoadWeightModel(modelFile)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := DiscoverRootInventory(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	const catalog = "github.com/division-sh/swarm/internal/testcatalog"
	entry := inventory.Packages[catalog]
	entry.Roots = append(entry.Roots, "TestNinthUnmatchedCatalogProof")
	inventory.Packages[catalog] = entry
	packages := make([]string, 0, len(inventory.Packages))
	for pkg := range inventory.Packages {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)
	plan, err := BuildPlan(policy, model, packages, ProfileLocal, "adversarial catalog root", "test-head")
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := LoadParityProofs(filepath.Join(root, "internal/apiv1/testdata/public_surface_backend_matrix.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := BindExecution(&plan, inventory, proofs); err == nil || !strings.Contains(err.Error(), "TestNinthUnmatchedCatalogProof") {
		t.Fatalf("ninth filtered catalog root was accepted: %v", err)
	}
}

func TestRootDiscoveryRejectsPersistedGOFLAGS(t *testing.T) {
	t.Setenv("GOENV", filepath.Join(t.TempDir(), "goenv"))
	command := exec.Command("go", "env", "-w", "GOFLAGS=-run=^TestDefinitelyAbsent$")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("persist test selector: %v: %s", err, output)
	}
	_, err := EffectiveBuildContext(context.Background(), ".")
	if err == nil || !strings.Contains(err.Error(), "GOFLAGS") {
		t.Fatalf("persisted GOFLAGS = %v", err)
	}
}

func TestRootCensusExcludesLowercaseHelperNames(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "roots_test.go", `package roots
import "testing"
func TestRun(t *testing.T) {}
func Testhelper(t *testing.T) {}
func FuzzInput(f *testing.F) {}
func Fuzzhelper(f *testing.F) {}
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && executableTestRoot(fn) {
			got = append(got, fn.Name.Name)
		}
	}
	if strings.Join(got, ",") != "TestRun,FuzzInput" {
		t.Fatalf("active roots = %v", got)
	}
}
