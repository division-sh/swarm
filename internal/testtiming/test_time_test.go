package testtiming

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func timingRoot(name string, seconds float64) RootTime {
	return RootTime{TimingCell: TimingCell{Package: "example/p", Root: name, Environment: "ci-postgres-gateway-empty-v1", Count: "count-1"}, Seconds: seconds}
}

func rootTimingPolicy() testplanning.Policy {
	return testplanning.Policy{Profiles: map[string]testplanning.ProfilePolicy{"core": {}, "lifecycle": {}, "full": {}}}
}

func TestTestTimeGrowthRequiresBothBounds(t *testing.T) {
	for _, test := range []struct {
		name         string
		base, growth float64
		fail         bool
	}{
		{"5 percent boundary", 100, 5, false},
		{"5 percent exceeded despite under 600", 100, 5.001, true},
		{"600 boundary", 20000, 600, false},
		{"600 exceeded despite under 5 percent", 20000, 600.001, true},
		{"no growth", 100, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			row, problems := compareTimingTier([]RootTime{timingRoot("TestOld", test.base)}, []RootTime{timingRoot("TestOld", test.base+test.growth)}, rootTimingPolicy(), "core")
			if (len(problems) != 0) != test.fail || math.Abs(row.Growth-test.growth) > 1e-8 {
				t.Fatalf("row=%+v problems=%v", row, problems)
			}
		})
	}
}

func TestTestTimePopulationChangesCannotHideGrowth(t *testing.T) {
	base := []RootTime{timingRoot("TestOld", 100), timingRoot("TestRemoved", 3000)}
	candidate := []RootTime{timingRoot("TestOld", 101), timingRoot("TestNew", 30.001)}
	row, problems := compareTimingTier(base, candidate, rootTimingPolicy(), "core")
	if row.Retained != 1 || len(row.Added) != 1 || len(row.Removed) != 1 || math.Abs(row.Growth-31.001) > 1e-8 || len(problems) != 1 || !strings.Contains(problems[0], "independent placement approval") {
		t.Fatalf("row=%+v problems=%v", row, problems)
	}
	// A rename cannot be inferred as cheaper retained work or offset other cost.
	row, _ = compareTimingTier(base, []RootTime{timingRoot("TestRenamed", 1)}, rootTimingPolicy(), "core")
	if row.Retained != 0 || len(row.Added) != 1 || len(row.Removed) != 2 || row.Growth != 1 {
		t.Fatalf("renamed population=%+v", row)
	}
}

func TestTestTimePinnedReferenceRejectsReplacementAndUnauthorizedException(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", ".github", "test-time-reference.json"))
	if err != nil {
		t.Fatal(err)
	}
	reference, err := LoadTestTimeReference(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(reference.Roots) != 11424 || reference.PlanDigest != "e1d4f6cd8fadd4feab1f7aa975600fe6d07bc39fb3943928a407c62aaf6a95b2" {
		t.Fatal("incomplete reference")
	}
	var total float64
	var soakCells int
	for _, root := range reference.Roots {
		total += root.Seconds
		if root.Backend != "" {
			soakCells++
		}
	}
	if math.Abs(total-15739.18) > 1e-6 || soakCells != 2 {
		t.Fatalf("total=%f soak=%d", total, soakCells)
	}
	for _, edit := range []func([]byte) []byte{
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(TestTimeReferenceSource), []byte(strings.Repeat("a", 40)), 1)
		},
		func(raw []byte) []byte { return bytes.Replace(raw, []byte(`"seconds":0`), []byte(`"seconds":1`), 1) },
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"exception":"Test-Time: approved by author"`), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":2,"version":1`), 1)
		},
	} {
		if _, err := LoadTestTimeReference(bytes.NewReader(edit(raw))); err == nil {
			t.Fatal("replacement or author-declared exception accepted")
		}
	}
}

func rootEvidenceFixture(t *testing.T) (testplanning.RunPlan, CommandEvidence) {
	t.Helper()
	_, policy := committedTimingPolicies(t)
	plan := committedBroadBudgetPlan(t, policy)
	unit := plan.Units[0]
	unit.SelectedRoots = []testplanning.TestRoot{{Package: unit.Packages[0], Name: "TestParent"}}
	unit.RequiredTests = []testplanning.RequiredTest{{TestRoot: unit.SelectedRoots[0]}}
	unit.RequiredChildren = nil
	unit.DeferredTests = nil
	unit.TestBearingPackages = []string{unit.Packages[0]}
	unit.WorkloadProfile = "full"
	plan.Units = []testplanning.ProofUnit{unit}
	for _, batch := range plan.Batches {
		if batch.ID == unit.ID {
			plan.Batches = []testplanning.ProofBatch{batch}
			break
		}
	}
	plan.Packages = unit.Packages
	plan.Profile = "full"
	plan.DeferredRoots = nil
	plan.BuildContext = testplanning.BuildContext{GOOS: "linux", GOARCH: "amd64", CGOEnabled: "1"}
	plan.HeadSHA = TestTimeReferenceSource
	plan.Digest = ""
	raw, _ := json.Marshal(plan)
	digest := sha256.Sum256(raw)
	plan.Digest = hex.EncodeToString(digest[:])
	evidence := timingTestEvidence(plan, unit.ID, AttemptPrimary, 10)
	evidence.WorkflowRunID = TestTimeReferenceRunID
	evidence.BuildContext = plan.BuildContext
	evidence.WorkloadProfile = unit.WorkloadProfile
	evidence.ExecutionTier = unit.ExecutionTier
	evidence.Report.Tests = []TestTiming{
		{Package: unit.Packages[0], Test: "TestParent", Result: "pass", Elapsed: 10},
		{Package: unit.Packages[0], Test: "TestParent/first", Result: "pass", Elapsed: 7},
		{Package: unit.Packages[0], Test: "TestParent/second", Result: "pass", Elapsed: 8},
	}
	evidence.Report.Summary.Tests = 3
	return plan, evidence
}

func TestTestTimeAdmissionAndParentChildCount(t *testing.T) {
	plan, evidence := rootEvidenceFixture(t)
	roots, err := observedRootTimes(plan, TestTimeReferenceRunID, 1, []CommandEvidence{evidence})
	if err != nil || len(roots) != 1 || roots[0].Seconds != 10 {
		t.Fatalf("roots=%+v err=%v", roots, err)
	}
	evidence.Report.Tests[0].Elapsed = 1
	roots, err = observedRootTimes(plan, TestTimeReferenceRunID, 1, []CommandEvidence{evidence})
	if err != nil || roots[0].Seconds != 8 {
		t.Fatalf("direct-child fallback=%v %v", roots, err)
	}
	for _, test := range []struct {
		name string
		edit func(*CommandEvidence)
	}{
		{"missing root", func(e *CommandEvidence) { e.Report.Tests = nil; e.Report.Summary.Tests = 0 }},
		{"wrong head", func(e *CommandEvidence) { e.HeadSHA = "other" }},
		{"foreign attempt", func(e *CommandEvidence) { e.WorkflowAttempt++ }},
		{"failure", func(e *CommandEvidence) { e.ExitCode = 1 }},
		{"duplicate child observation", func(e *CommandEvidence) { e.Report.Summary.DuplicateTestEvents++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, item := rootEvidenceFixture(t)
			test.edit(&item)
			if _, err := observedRootTimes(plan, TestTimeReferenceRunID, 1, []CommandEvidence{item}); err == nil {
				t.Fatal("invalid proof accepted as timing savings")
			}
		})
	}
	if _, err := observedRootTimes(plan, TestTimeReferenceRunID, 1, nil); err == nil {
		t.Fatal("missing unit accepted")
	}
	if _, err := observedRootTimes(plan, TestTimeReferenceRunID, 1, []CommandEvidence{evidence, evidence}); err == nil {
		t.Fatal("duplicate unit accepted")
	}
}

func TestTestTimeScopeAndTierProjection(t *testing.T) {
	plan, evidence := rootEvidenceFixture(t)
	reference, err := CaptureTestTimeReference(plan, []CommandEvidence{evidence})
	if err != nil {
		t.Fatal(err)
	}
	result := EvaluateTestTime(reference, rootTimingPolicy(), plan, TestTimeReferenceRunID, 1, []CommandEvidence{evidence})
	if result.Status != BudgetPass || len(result.Tiers) != 3 {
		t.Fatalf("%+v", result)
	}
	for _, change := range []func(*TestTimeReference){
		func(ref *TestTimeReference) { ref.BuildContext.CGOEnabled = "0" },
		func(ref *TestTimeReference) { ref.Roots[0].Count = "cache-default" },
		func(ref *TestTimeReference) { ref.Roots[0].Environment = "other" },
	} {
		var fresh TestTimeReference
		raw, _ := json.Marshal(reference)
		_ = json.Unmarshal(raw, &fresh)
		change(&fresh)
		if got := EvaluateTestTime(fresh, rootTimingPolicy(), plan, TestTimeReferenceRunID, 1, []CommandEvidence{evidence}); got.Status != BudgetIncomplete {
			t.Fatalf("scope admitted %+v", got)
		}
	}
	_, policy := committedTimingPolicies(t)
	root := testplanning.TestRoot{Package: testplanning.SoakPackage, Name: testplanning.SoakTest}
	for _, tier := range []string{"core", "lifecycle", "full"} {
		for _, backend := range []string{"sqlite", "postgres"} {
			got, err := policy.SelectTimingCell(tier, root, backend)
			if err != nil || got != (tier == "full") {
				t.Fatalf("%s/%s selected=%v err=%v", tier, backend, got, err)
			}
		}
	}
}

func TestAdvisoryPRTimingPreservesProofFailures(t *testing.T) {
	plan, evidence := rootEvidenceFixture(t)
	evidence.ElapsedSeconds = 1000
	for _, advisory := range []bool{false, true} {
		opts := EvaluationOptions{Plan: plan, WorkflowRunID: TestTimeReferenceRunID, WorkflowAttempt: 1, AdvisoryTiming: advisory}
		result := EvaluateBudget(timingTestPolicy(), opts, []CommandEvidence{evidence})
		want := BudgetFail
		if advisory {
			want = BudgetWarn
		}
		if result.Status != want {
			t.Fatalf("advisory=%v %+v", advisory, result)
		}
		bad := evidence
		bad.ExitCode = 1
		if result := EvaluateBudget(timingTestPolicy(), opts, []CommandEvidence{bad}); result.Status != BudgetIncomplete {
			t.Fatalf("invalid proof downgraded %+v", result)
		}
		if result := EvaluateBudget(timingTestPolicy(), opts, nil); result.Status != BudgetIncomplete {
			t.Fatalf("missing proof downgraded %+v", result)
		}
	}
}
