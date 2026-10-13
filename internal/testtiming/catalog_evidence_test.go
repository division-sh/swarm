package testtiming

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

type catalogEvidenceFixture struct {
	policy    testplanning.Policy
	inventory testplanning.RootInventory
	proofs    []testplanning.ParityProof
	packages  []string
}

func loadCatalogEvidenceFixture(t *testing.T) catalogEvidenceFixture {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join(root, ".github/test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	policy, err := testplanning.LoadPolicy(file)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := testplanning.DiscoverRootInventory(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := testplanning.LoadParityProofs(filepath.Join(root, "internal/apiv1/testdata/public_surface_backend_matrix.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var packages []string
	for pkg := range inventory.Packages {
		packages = append(packages, pkg)
	}
	return catalogEvidenceFixture{policy, inventory, proofs, packages}
}

func (f catalogEvidenceFixture) unit(t *testing.T, tier string, extras ...string) testplanning.ProofUnit {
	t.Helper()
	plan, err := testplanning.BuildPlan(f.policy, testplanning.WeightModel{Version: testplanning.WeightModelVersion, SourceRunID: "catalog-child-proof"}, f.packages,
		tier, "catalog child validation", "test-head", testplanning.BuildOptions{ExtraUnits: extras})
	if err == nil {
		err = testplanning.BindExecution(&plan, f.inventory, f.proofs, f.policy)
	}
	if err != nil {
		t.Fatal(err)
	}
	unit, err := plan.Unit("catalog-runtime")
	if err != nil {
		t.Fatal(err)
	}
	return unit
}

func TestPromotedCatalogChildEvidenceFailsClosed(t *testing.T) {
	f := loadCatalogEvidenceFixture(t)
	const parent = "TestTerminalMiddleMemberFailureRetainsSuffixBothStores"
	children := []string{"postgres", "reviewer-required-child", "sqlite"}
	f.proofs = append(f.proofs, testplanning.ParityProof{ID: "catalog-child-validation-regression", Kind: "go_test", Name: parent,
		Path: "internal/runtime/cataloge2e/terminal_retirement_failure_test.go", Profile: testplanning.ProfileFull, Children: children})
	assertCatalogChildEvidence(t, f.unit(t, testplanning.ProfileLifecycle), parent, children)
}

func TestCatalogTerminalMiddlePolicyChildrenFailClosed(t *testing.T) {
	f := loadCatalogEvidenceFixture(t)
	const parent = "TestTerminalMiddleMemberFailureRetainsSuffixBothStores"
	children := []string{"postgres", "sqlite"}
	for _, tier := range []string{testplanning.ProfileFull, testplanning.ProfileLifecycle, testplanning.ProfileCore} {
		t.Run(tier, func(t *testing.T) {
			var extras []string
			if tier == testplanning.ProfileCore {
				extras = []string{"catalog-runtime"}
			}
			assertCatalogChildEvidence(t, f.unit(t, tier, extras...), parent, children)
		})
	}
}

func assertCatalogChildEvidence(t *testing.T, unit testplanning.ProofUnit, parent string, children []string) {
	t.Helper()
	found := false
	var report Report
	for _, selected := range unit.SelectedRoots {
		report.Tests = append(report.Tests, TestTiming{Package: selected.Package, Test: selected.Name, Result: "pass"})
	}
	for _, required := range unit.RequiredTests {
		if required.Name == parent {
			found = slices.Equal(required.Children, children)
		}
		for _, child := range required.Children {
			report.Tests = append(report.Tests, TestTiming{Package: required.Package, Test: required.Name + "/" + child, Result: "pass"})
		}
	}
	if !found {
		t.Fatal("promoted unit lost its full-only child and backend obligations")
	}
	if problems := requiredExecutionProblems(unit, report); len(problems) != 0 {
		t.Fatalf("complete synthetic validator control refused: %v", problems)
	}
	for _, child := range children {
		for _, verdict := range []string{"missing", "skip", "fail"} {
			t.Run(child+"/"+verdict, func(t *testing.T) {
				changed := report
				changed.Tests = slices.Clone(report.Tests)
				name := parent + "/" + child
				for i, row := range changed.Tests {
					if row.Test != name {
						continue
					}
					if verdict == "missing" {
						changed.Tests = slices.Delete(changed.Tests, i, i+1)
					} else {
						changed.Tests[i].Result = verdict
					}
					break
				}
				if problems := requiredExecutionProblems(unit, changed); !strings.Contains(strings.Join(problems, ";"), name) {
					t.Fatalf("passing parent hid %s child %s: %v", verdict, child, problems)
				}
			})
		}
	}
}
