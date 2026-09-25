package testplanning

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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
			if profile == ProfilePRCommon || profile == ProfileFull {
				golden, err := plan.Unit("hitl-releasee2e-golden")
				if err != nil {
					t.Fatal(err)
				}
				wantRequired := "TestGoldenAgentWorkloadSQLiteSmoke"
				wantDeferred := "TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends"
				if profile == ProfileFull {
					wantRequired, wantDeferred = wantDeferred, wantRequired
				}
				if !unitRequires(golden, wantRequired) || !unitDefers(golden, wantDeferred) {
					t.Fatalf("%s golden workload classification: required=%v deferred=%v", profile, golden.RequiredTests, golden.DeferredTests)
				}
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

func unitRequires(unit ProofUnit, name string) bool {
	for _, root := range unit.RequiredTests {
		if root.Name == name {
			return true
		}
	}
	return false
}

func unitDefers(unit ProofUnit, name string) bool {
	for _, root := range unit.DeferredTests {
		if root.Name == name {
			return true
		}
	}
	return false
}

func TestFiniteDeferralDeclarationsNameActiveRoots(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := DiscoverRootInventory(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	for pkg, names := range rootDeferralReasons {
		entry, ok := inventory.Packages["github.com/division-sh/swarm/"+pkg]
		if !ok {
			t.Errorf("deferred package %s is absent", pkg)
			continue
		}
		for name, reason := range names {
			if strings.TrimSpace(reason) == "" {
				t.Errorf("deferred root %s.%s has no reason", pkg, name)
			}
			if !slices.Contains(entry.Roots, name) {
				t.Errorf("deferred root %s.%s is not executable", pkg, name)
			}
		}
	}
}

func TestUnixProofOnlyDefersOnWindows(t *testing.T) {
	root := TestRoot{Package: "github.com/division-sh/swarm/cmd/swarm-test-changed", Name: "TestFullSuiteFallbackExecutesCanonicalWrapper"}
	if reason, _ := deferredRootReason(ProofUnit{}, root, BuildContext{GOOS: "linux"}); reason != "" {
		t.Fatalf("Linux proof was deferred: %s", reason)
	}
	if reason, _ := deferredRootReason(ProofUnit{}, root, BuildContext{GOOS: "windows"}); reason == "" {
		t.Fatal("Windows-only skip was not declared")
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

func TestRootDiscoveryMatchesNativeGoExecution(t *testing.T) {
	root := t.TempDir()
	for name, source := range map[string]string{
		"go.mod":   "module example.com/rootprobe\n\ngo 1.24\n",
		"probe.go": "package rootprobe\n",
		"roots_test.go": `package rootprobe
import check "testing"
func TestAliased(t *check.T) {}
func TestUnnamed(*check.T) {}
func testhelper(t *check.T) {}
func Example_documentation() {}
func Example_withOutput() {
 println("ok")
 // Output: ok
}
func Example_emptyOutput() {
 // Output:
}
`,
		"dot_test.go": `package rootprobe
import . "testing"
func TestDot(*T) {}
`,
		"ignored_test.go": `//go:build never
package rootprobe
import "testing"
func TestExcluded(t *testing.T) {}
`,
		"helperonly/helper.go":      "package helperonly\n",
		"helperonly/helper_test.go": "package helperonly\nfunc helper() {}\n",
	} {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("go", "test", "-list", ".", "-count=1")
	command.Dir = root
	out, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("native Go test list: %v: %s", err, out)
	}
	inventory, err := DiscoverRootInventory(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	entry := inventory.Packages["example.com/rootprobe"]
	want := []string{"Example_emptyOutput", "Example_withOutput", "TestAliased", "TestDot", "TestUnnamed"}
	if strings.Join(entry.Roots, ",") != strings.Join(want, ",") {
		t.Fatalf("active roots = %v, want %v", entry.Roots, want)
	}
	for _, name := range want {
		if !strings.Contains(string(out), "\n"+name+"\n") && !strings.HasPrefix(string(out), name+"\n") {
			t.Fatalf("native Go did not list %s: %s", name, out)
		}
	}
	for _, name := range []string{"Example_documentation", "testhelper", "TestExcluded"} {
		if strings.Contains(string(out), name) {
			t.Fatalf("native Go unexpectedly listed %s: %s", name, out)
		}
	}
	if helper := inventory.Packages["example.com/rootprobe/helperonly"]; helper.HasTestFiles || len(helper.Roots) != 0 {
		t.Fatalf("helper-only package requires execution: %+v", helper)
	}
	if err := ValidateGoProofPartition(root, []string{"^(Test|Example)"}); err != nil {
		t.Fatalf("structural partition disagrees with native roots: %v", err)
	}
}
