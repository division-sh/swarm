package testtiming

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
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

func timingReferenceRoots(t *testing.T, roots []RootTime, policy testplanning.Policy) []ReferenceRootTime {
	t.Helper()
	frozen, err := referenceTimingRoots(roots, policy)
	if err != nil {
		t.Fatal(err)
	}
	return frozen
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
			base := timingReferenceRoots(t, []RootTime{timingRoot("TestOld", test.base)}, rootTimingPolicy())
			row, problems := compareTimingTier(base, []RootTime{timingRoot("TestOld", test.base+test.growth)}, rootTimingPolicy(), "core")
			if (len(problems) != 0) != test.fail || math.Abs(row.Growth-test.growth) > 1e-8 {
				t.Fatalf("row=%+v problems=%v", row, problems)
			}
		})
	}
}

func TestTestTimePopulationChangesCannotHideGrowth(t *testing.T) {
	base := timingReferenceRoots(t, []RootTime{timingRoot("TestOld", 100), timingRoot("TestRemoved", 3000)}, rootTimingPolicy())
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

func timingTierChangePolicy(slowInCore, special bool) testplanning.Policy {
	policy := rootTimingPolicy()
	if special {
		policy.SpecialPackages = []string{"example/p"}
	}
	run := "^TestFast$"
	if slowInCore {
		run = "^(TestFast|TestSlow)$"
	}
	policy.Units = map[string]testplanning.UnitPolicy{
		"thin": {Packages: []string{"example/p"}, Run: run},
		"full": {Packages: []string{"example/p"}, Run: "^(TestFast|TestSlow)$"},
	}
	policy.Profiles["core"] = testplanning.ProfilePolicy{Units: []string{"thin"}}
	policy.Profiles["lifecycle"] = testplanning.ProfilePolicy{Units: []string{"thin"}}
	policy.Profiles["full"] = testplanning.ProfilePolicy{Units: []string{"full"}}
	return policy
}

func TestTestTimeTierChangesCannotReprojectPinnedBaseline(t *testing.T) {
	for _, test := range []struct {
		name                            string
		referenceSlow, referenceSpecial bool
		currentSlow, currentSpecial     bool
		baseline, growth                float64
		added, removed                  int
	}{
		{"promotion", false, true, true, true, 100, 500, 1, 0},
		{"demotion", true, true, false, true, 600, 0, 0, 1},
		{"special package becomes broad", false, true, false, false, 100, 500, 1, 0},
		{"broad package becomes special", false, false, false, true, 600, 0, 0, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			roots := []RootTime{timingRoot("TestFast", 100), timingRoot("TestSlow", 500)}
			base := timingReferenceRoots(t, roots, timingTierChangePolicy(test.referenceSlow, test.referenceSpecial))
			current := timingTierChangePolicy(test.currentSlow, test.currentSpecial)
			for _, tier := range []string{"core", "lifecycle"} {
				row, problems := compareTimingTier(base, roots, current, tier)
				if row.Baseline != test.baseline || row.Growth != test.growth || len(row.Added) != test.added || len(row.Removed) != test.removed || row.Allowance != test.baseline*0.05 || (len(problems) != 0) != (test.growth > 0) {
					t.Fatalf("%s: row=%+v problems=%v", tier, row, problems)
				}
			}
			row, problems := compareTimingTier(base, roots, current, "full")
			if row.Baseline != 600 || row.Growth != 0 || row.Retained != 2 || len(problems) != 0 {
				t.Fatalf("unchanged full cost: row=%+v problems=%v", row, problems)
			}
		})
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
	if len(reference.Roots) != 11556 || reference.PlanDigest != "033294796270ddff56f716548fcb53820d6f932099131202fedf44b34060f8ab" {
		t.Fatal("incomplete reference")
	}
	var total float64
	var soakCells int
	var approvedA9 bool
	for _, root := range reference.Roots {
		total += root.Seconds
		if root.Backend != "" {
			soakCells++
		}
		if root.Package == "github.com/division-sh/swarm/internal/serveapp" && root.Root == "TestA9ReleaseKeyedIngressConstructionBothStores" {
			if root.Seconds != 41.82 || root.Count != "count-1" || root.Environment != "ci-postgres-gateway-empty-v1" || !slices.Equal(root.Tiers, []string{"lifecycle", "full"}) {
				t.Fatalf("approved A9 placement or observation changed: %+v", root)
			}
			approvedA9 = true
		}
	}
	if math.Abs(total-16414.28) > 1e-6 || soakCells != 2 || !approvedA9 {
		t.Fatalf("total=%f soak=%d approvedA9=%v", total, soakCells, approvedA9)
	}
	for _, want := range []struct {
		tier    string
		cells   int
		seconds float64
	}{
		{"core", 7420, 2569.89},
		{"lifecycle", 11455, 13009.42},
		{"full", 11556, 16414.28},
	} {
		roots, problems := projectReferenceTimingRoots(reference.Roots, want.tier)
		var seconds float64
		for _, root := range roots {
			seconds += root.Seconds
		}
		if len(problems) != 0 || len(roots) != want.cells || math.Abs(seconds-want.seconds) > 1e-6 {
			t.Fatalf("changed pinned %s population: cells=%d seconds=%f problems=%v", want.tier, len(roots), seconds, problems)
		}
	}
	for _, tiers := range [][]string{nil, {"core"}, {"unknown", "full"}, {"core", "core", "full"}, {"full", "core"}} {
		root := reference.Roots[0]
		root.Tiers = tiers
		if _, problems := projectReferenceTimingRoots([]ReferenceRootTime{root}, "core"); len(problems) == 0 {
			t.Fatalf("invalid reference membership accepted: %v", tiers)
		}
	}
	for _, edit := range []func([]byte) []byte{
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"run_id":37937174260`), []byte(`"run_id":37706007387`), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"attempt":1`), []byte(`"attempt":2`), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(TestTimeReferenceHead), []byte("cbfb3e267dd48d3e880e2f96bc03e9c41633973b"), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(TestTimeApproval), []byte("https://github.com/division-sh/swarm/issues/2535#issuecomment-6075837853"), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(reference.PlanDigest), []byte("e1d4f6cd8fadd4feab1f7aa975600fe6d07bc39fb3943928a407c62aaf6a95b2"), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(TestTimeReferenceSource), []byte("5661cf9f42e443d04bfe774f8b4e87e95bea44d6"), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(TestTimeReferenceSource), []byte(strings.Repeat("a", 40)), 1)
		},
		func(raw []byte) []byte { return bytes.Replace(raw, []byte(`"seconds":0`), []byte(`"seconds":1`), 1) },
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"version":2`), []byte(`"version":2,"exception":"Test-Time: approved by author"`), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"version":2`), []byte(`"version":3,"version":2`), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(TestTimeReferencePolicy), []byte(strings.Repeat("b", 64)), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"tiers":["core","lifecycle","full"]`), []byte(`"tiers":["full"]`), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"count":"count-1"`), []byte(`"count":"cache-default"`), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"environment":"ci-postgres-gateway-empty-v1"`), []byte(`"environment":"foreign"`), 1)
		},
		func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"cgo_enabled":"1"`), []byte(`"cgo_enabled":"0"`), 1)
		},
	} {
		changed := edit(raw)
		if bytes.Equal(changed, raw) {
			t.Fatal("reference tamper counterexample did not change the input")
		}
		if _, err := LoadTestTimeReference(bytes.NewReader(changed)); err == nil {
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
	roots, err := observedRootTimes(plan, TestTimeReferenceRunID, 1, []CommandEvidence{evidence})
	if err != nil {
		t.Fatal(err)
	}
	reference := TestTimeReference{Version: 2, RunID: TestTimeReferenceRunID, Source: plan.HeadSHA, BuildContext: plan.BuildContext, Roots: timingReferenceRoots(t, roots, rootTimingPolicy())}
	if _, err := CaptureTestTimeReference(plan, []CommandEvidence{evidence}, []byte("version: 2\n")); err == nil || !strings.Contains(err.Error(), "exact policy") {
		t.Fatalf("capture admitted another policy: %v", err)
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
