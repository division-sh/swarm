package testtiming

import (
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestCommandAdmissionRootManifestAndReceiptMatrix(t *testing.T) {
	for _, tc := range []struct {
		name       string
		structural bool
		mutate     func(*testplanning.RunPlan, *CommandEvidence)
	}{
		{"empty-plan", true, func(p *testplanning.RunPlan, _ *CommandEvidence) { p.Units, p.Batches = nil, nil }},
		{"empty-unit-packages", true, func(p *testplanning.RunPlan, _ *CommandEvidence) { p.Units[0].Packages = nil }},
		{"zero-root-obligations", true, func(p *testplanning.RunPlan, e *CommandEvidence) {
			p.Units[0].SelectedRoots, p.Units[0].RequiredTests, p.Units[0].TestBearingPackages, p.Units[0].RequiredChildren = nil, nil, nil, nil
			e.Report.Tests = nil
		}},
		{"missing-root-classification", true, func(p *testplanning.RunPlan, _ *CommandEvidence) { p.Units[0].RequiredTests = nil }},
		{"duplicate-selected-root", true, func(p *testplanning.RunPlan, _ *CommandEvidence) {
			p.Units[0].SelectedRoots = append(p.Units[0].SelectedRoots, p.Units[0].SelectedRoots[0])
		}},
		{"unselected-required-root", true, func(p *testplanning.RunPlan, _ *CommandEvidence) { p.Units[0].RequiredTests[0].Name = "TestOutside" }},
		{"noncanonical-root", true, func(p *testplanning.RunPlan, _ *CommandEvidence) {
			p.Units[0].SelectedRoots[0].Name, p.Units[0].RequiredTests[0].Name = " TestParent", " TestParent"
		}},
		{"child-as-root", true, func(p *testplanning.RunPlan, _ *CommandEvidence) {
			p.Units[0].SelectedRoots[0].Name, p.Units[0].RequiredTests[0].Name = "TestParent/first", "TestParent/first"
		}},
		{"missing-test-bearing-classification", true, func(p *testplanning.RunPlan, _ *CommandEvidence) { p.Units[0].TestBearingPackages = nil }},
		{"foreign-root-package", true, func(p *testplanning.RunPlan, e *CommandEvidence) {
			p.Units[0].SelectedRoots[0].Package, p.Units[0].RequiredTests[0].Package = "foreign/package", "foreign/package"
			p.Units[0].TestBearingPackages = []string{"foreign/package"}
			for i := range e.Report.Tests {
				e.Report.Tests[i].Package = "foreign/package"
			}
		}},
		{"duplicate-required-child", true, func(p *testplanning.RunPlan, _ *CommandEvidence) {
			p.Units[0].RequiredTests[0].Children = append(p.Units[0].RequiredTests[0].Children, "first")
		}},
		{"empty-required-child", true, func(p *testplanning.RunPlan, _ *CommandEvidence) {
			p.Units[0].RequiredTests[0].Children = append(p.Units[0].RequiredTests[0].Children, "")
		}},
		{"dropped-declared-child", true, func(p *testplanning.RunPlan, e *CommandEvidence) {
			p.Units[0].RequiredTests[0].Children = []string{"first"}
			e.Report.Tests = e.Report.Tests[:2]
		}},
		{"unselected-declared-child-root", true, func(p *testplanning.RunPlan, _ *CommandEvidence) {
			p.Units[0].RequiredChildren["TestOutside"] = []string{"child"}
		}},
		{"missing-parent-row", false, func(_ *testplanning.RunPlan, e *CommandEvidence) { e.Report.Tests = e.Report.Tests[1:] }},
		{"missing-child-row", false, func(_ *testplanning.RunPlan, e *CommandEvidence) { e.Report.Tests = e.Report.Tests[:2] }},
		{"failed-child-row", false, func(_ *testplanning.RunPlan, e *CommandEvidence) {
			e.Report.Tests[2].Result = "fail"
			e.Report.Summary.FailedTests = 1
		}},
		{"skipped-parent-row", false, func(_ *testplanning.RunPlan, e *CommandEvidence) {
			e.Report.Tests[0].Result = "skip"
			e.Report.Summary.SkippedTests = 1
		}},
		{"duplicate-root-row", false, func(_ *testplanning.RunPlan, e *CommandEvidence) {
			e.Report.Tests = append(e.Report.Tests, e.Report.Tests[0])
		}},
		{"undeclared-root-row", false, func(_ *testplanning.RunPlan, e *CommandEvidence) {
			e.Report.Tests = append(e.Report.Tests, TestTiming{Package: e.Packages[0], Test: "TestOutside", Result: "pass", Elapsed: 1})
		}},
		{"child-of-undeclared-root", false, func(_ *testplanning.RunPlan, e *CommandEvidence) {
			e.Report.Tests = append(e.Report.Tests, TestTiming{Package: e.Packages[0], Test: "TestOutside/child", Result: "pass", Elapsed: 1})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, receipt := rootEvidenceFixture(t)
			plan.Units[0].RequiredTests[0].Children = []string{"first", "second"}
			plan.Units[0].RequiredChildren = map[string][]string{"TestParent": {"first", "second"}}
			rebindTimingFixture(t, &plan, &receipt)
			roots, err := observedRootTimes(plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt})
			if err != nil {
				t.Fatal(err)
			}
			reference := TestTimeReference{BuildContext: plan.BuildContext, Roots: timingReferenceRoots(t, roots, rootTimingPolicy())}
			tc.mutate(&plan, &receipt)
			receipt.Report.Summary.Tests = len(receipt.Report.Tests)
			receipt.Report.Summary.Events = len(receipt.Report.Tests) + len(receipt.Report.Packages)
			rebindTimingFixture(t, &plan, &receipt)
			identity := ValidateCommandIdentity(receipt, plan)
			if (len(identity) != 0) != tc.structural {
				t.Fatalf("structural admission=%v, want refusal=%v", identity, tc.structural)
			}
			if problems := ValidateCommandEvidence(receipt, plan); len(problems) == 0 {
				t.Fatal("malformed root proof admitted")
			}
			for _, advisory := range []bool{false, true} {
				got := EvaluateBudget(timingTestPolicy(), EvaluationOptions{Plan: plan, WorkflowRunID: TestTimeReferenceRunID, WorkflowAttempt: 1, AdvisoryTiming: advisory}, []CommandEvidence{receipt})
				if got.Status != BudgetIncomplete || got.ExitCode() == 0 {
					t.Fatalf("budget granted malformed root credit: %+v", got)
				}
			}
			for _, mode := range []TestTimeMode{TestTimePR, TestTimeStrict} {
				got := EvaluateTestTime(reference, rootTimingPolicy(), plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt}, mode)
				if got.Status != BudgetIncomplete || len(got.Tiers) != 0 {
					t.Fatalf("timing granted partial root credit: %+v", got)
				}
			}
		})
	}
}

func TestCommandAdmissionRootCellsPreserveDistinctExecutionScopes(t *testing.T) {
	for _, scope := range []string{"duplicate", "environment", "count"} {
		t.Run(scope, func(t *testing.T) {
			plan, receipt := rootEvidenceFixture(t)
			plan.Units[0].Run = "^TestParent$"
			peer := plan.Units[0]
			peer.ID += "-peer"
			switch scope {
			case "environment":
				peer.EnvironmentID += "-peer"
			case "count":
				peer.CountMode = CountModeCacheDefault
			}
			plan.Units = append(plan.Units, peer)
			batch := plan.Batches[0]
			batch.ID, batch.Units = peer.ID, []string{peer.ID}
			plan.Batches = append(plan.Batches, batch)
			rebindTimingFixture(t, &plan, &receipt)
			other := receipt
			other.UnitID, other.Surface = peer.ID, peer.ID
			other.EnvironmentID, other.CountMode = peer.EnvironmentID, peer.CountMode
			receipt.Run, other.Run = plan.Units[0].Run, peer.Run
			if err := plan.Validate(); err != nil {
				t.Fatal(err)
			}
			roots, err := observedRootTimes(plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt, other})
			if scope == "duplicate" {
				if problems := ValidateCommandIdentity(receipt, plan); !strings.Contains(strings.Join(problems, " "), "duplicate executable root cell") || err == nil {
					t.Fatalf("duplicate cells admitted: %v / %v", problems, err)
				}
			} else if err != nil || len(roots) != 2 {
				t.Fatalf("distinct %s scope refused: %v / %v", scope, roots, err)
			}
		})
	}
	plan := soakCIPlan(t, soakCIPolicy(t), testplanning.ProfileFull)
	if problems := commandPlanProblems(plan); len(problems) != 0 {
		t.Fatalf("two disjoint soak backend cells refused: %v", problems)
	}
}

func TestCommandAdmissionRetainsGovernedDeferralAndUntestedPackages(t *testing.T) {
	plan, _ := rootEvidenceFixture(t)
	unit := &plan.Units[0]
	unit.Packages = []string{"github.com/division-sh/swarm/internal/packartifact", "github.com/division-sh/swarm/no-tests"}
	unit.TestBearingPackages = unit.Packages[:1]
	required := testplanning.TestRoot{Package: unit.Packages[0], Name: "TestRequired"}
	deferred := testplanning.TestRoot{Package: unit.Packages[0], Name: "TestProjectPackImportSubprocessHelper"}
	unit.SelectedRoots = []testplanning.TestRoot{required, deferred}
	unit.RequiredTests = []testplanning.RequiredTest{{TestRoot: required}}
	unit.DeferredTests = []testplanning.DeferredTest{{TestRoot: deferred, Reason: "subprocess entry point, not a standalone proof"}}
	plan.Packages = slices.Clone(unit.Packages)
	rebindTimingFixture(t, &plan, &CommandEvidence{})
	receipt := timingTestEvidence(plan, unit.ID, AttemptPrimary, 1)
	if problems := ValidateCommandEvidence(receipt, plan); len(problems) != 0 {
		t.Fatalf("governed deferral or untested package refused: %v", problems)
	}
	for _, mutation := range []string{"unclassified", "arbitrary-reason", "required-and-deferred"} {
		t.Run(mutation, func(t *testing.T) {
			changed := plan
			changed.Units = slices.Clone(plan.Units)
			changed.Units[0].DeferredTests = slices.Clone(unit.DeferredTests)
			switch mutation {
			case "unclassified":
				changed.Units[0].DeferredTests = nil
			case "arbitrary-reason":
				changed.Units[0].DeferredTests[0].Reason = "optional"
			case "required-and-deferred":
				changed.Units[0].RequiredTests = append(slices.Clone(unit.RequiredTests), testplanning.RequiredTest{TestRoot: deferred})
			}
			e := receipt
			rebindTimingFixture(t, &changed, &e)
			if problems := ValidateCommandEvidence(e, changed); len(problems) == 0 {
				t.Fatal("invalid root classification admitted")
			}
		})
	}
}

func TestCommandAdmissionRejectsOneRootlessUnitBesideValidPeer(t *testing.T) {
	plan, receipt := rootEvidenceFixture(t)
	peer := plan.Units[0]
	peer.ID += "-peer"
	peer.Packages = []string{peer.Packages[0] + "/peer"}
	peer.SelectedRoots = []testplanning.TestRoot{{Package: peer.Packages[0], Name: "TestPeer"}}
	peer.RequiredTests = []testplanning.RequiredTest{{TestRoot: peer.SelectedRoots[0], Children: []string{"first", "second"}}}
	peer.TestBearingPackages = slices.Clone(peer.Packages)
	peer.RequiredChildren = map[string][]string{"TestPeer": {"first", "second"}}
	plan.Units = append(plan.Units, peer)
	batch := plan.Batches[0]
	batch.ID, batch.Units = peer.ID, []string{peer.ID}
	plan.Batches = append(plan.Batches, batch)
	plan.Packages = append(plan.Packages, peer.Packages...)
	sort.Strings(plan.Packages)
	other := receipt
	other.UnitID, other.Surface, other.Packages = peer.ID, peer.ID, slices.Clone(peer.Packages)
	other.Report.Tests, other.Report.Packages = slices.Clone(receipt.Report.Tests), slices.Clone(receipt.Report.Packages)
	other.Report.Packages[0].Package = peer.Packages[0]
	for i := range other.Report.Tests {
		other.Report.Tests[i].Package = peer.Packages[0]
		other.Report.Tests[i].Test = strings.Replace(other.Report.Tests[i].Test, "TestParent", "TestPeer", 1)
	}
	rebindTimingFixture(t, &plan, &receipt)
	other.PlanDigest = plan.Digest
	roots, err := observedRootTimes(plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt, other})
	if err != nil || len(roots) != 2 {
		t.Fatalf("valid peer control: %v / %v", roots, err)
	}
	reference := TestTimeReference{BuildContext: plan.BuildContext, Roots: timingReferenceRoots(t, roots, rootTimingPolicy())}
	plan.Units[0].SelectedRoots, plan.Units[0].RequiredTests, plan.Units[0].TestBearingPackages = nil, nil, nil
	receipt.Report.Tests, receipt.Report.Summary.Tests = nil, 0
	rebindTimingFixture(t, &plan, &receipt)
	other.PlanDigest = plan.Digest
	if err := plan.Validate(); err != nil {
		t.Fatalf("counterexample is not well-formed: %v", err)
	}
	for _, e := range []CommandEvidence{receipt, other} {
		if problems := ValidateCommandIdentity(e, plan); len(problems) == 0 {
			t.Fatal("rootless peer evaded whole-plan admission")
		}
	}
	if got := EvaluateBudget(timingTestPolicy(), EvaluationOptions{Plan: plan, AdvisoryTiming: true}, []CommandEvidence{receipt, other}); got.Status != BudgetIncomplete {
		t.Fatalf("partial budget credit: %+v", got)
	}
	for _, mode := range []TestTimeMode{TestTimePR, TestTimeStrict} {
		if got := EvaluateTestTime(reference, rootTimingPolicy(), plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt, other}, mode); got.Status != BudgetIncomplete || len(got.Tiers) != 0 {
			t.Fatalf("partial timing cells admitted: %+v", got)
		}
	}
}
