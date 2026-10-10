package testtiming

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestTestTimePRStrictVerdictMatrix(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		baseline, after, removed float64
		added                    []float64
		pr, strict               BudgetStatus
	}{
		{"historical-core-totals", 2569.890, 2648.340, 0, splitTimingCost(99.360, 171), BudgetWarn, BudgetFail},
		{"retained-only", 100, 110, 0, nil, BudgetWarn, BudgetFail},
		{"added-boundary", 100, 100, 0, []float64{5}, BudgetPass, BudgetPass},
		{"added-over-limit", 100, 100, 0, []float64{5.001}, BudgetFail, BudgetFail},
		{"speedup-cannot-subsidize", 100, 80, 0, []float64{8}, BudgetFail, BudgetPass},
		{"removed-neutral", 100, 100, 3000, splitTimingCost(160, 8), BudgetFail, BudgetFail},
		{"placement-within-total", 2000, 2000, 0, []float64{30.001}, BudgetFail, BudgetFail},
		{"placement-boundary", 2000, 2000, 0, []float64{30}, BudgetPass, BudgetPass},
		{"600-boundary", 20000, 20000, 0, splitTimingCost(600, 20), BudgetPass, BudgetPass},
		{"600-over-limit", 20000, 20000, 0, splitTimingCost(600.001, 21), BudgetFail, BudgetFail},
	} {
		for _, mode := range []TestTimeMode{TestTimePR, TestTimeStrict} {
			t.Run(tc.name+"/"+string(mode), func(t *testing.T) {
				base := []RootTime{timingRoot("TestOld", tc.baseline)}
				if tc.removed != 0 {
					base = append(base, timingRoot("TestRemoved", tc.removed))
				}
				candidate := []RootTime{timingRoot("TestOld", tc.after)}
				added := 0.0
				for i, cost := range tc.added {
					candidate = append(candidate, timingRoot(fmt.Sprintf("TestNew%03d", i), cost))
					added += cost
				}
				row, problems := compareTimingTier(timingReferenceRoots(t, base, rootTimingPolicy()), candidate, rootTimingPolicy(), "core", mode)
				status := BudgetPass
				if len(row.Warnings) != 0 {
					status = BudgetWarn
				}
				if len(problems) != 0 {
					status = BudgetFail
				}
				want := tc.strict
				if mode == TestTimePR {
					want = tc.pr
				}
				if status != want || row.StrictStatus != tc.strict || row.Retained != 1 || len(row.Added) != len(tc.added) {
					t.Fatalf("verdict=%s want=%s row=%+v problems=%v", status, want, row, problems)
				}
				if math.Abs(row.AddedSeconds-added) > 1e-8 || math.Abs(row.RetainedDelta-(tc.after-tc.baseline)) > 1e-8 || math.Abs(row.Growth-(tc.after-tc.baseline+added)) > 1e-8 {
					t.Fatalf("cost decomposition drifted: %+v", row)
				}
			})
		}
	}
}

func splitTimingCost(total float64, cells int) []float64 {
	parts := make([]float64, cells)
	for i := range parts {
		parts[i] = total / float64(cells)
	}
	return parts
}

func TestTestTimeModeAdmissionAndHostileProofs(t *testing.T) {
	mutations := map[string]func(*CommandEvidence){
		"foreign-source":      func(e *CommandEvidence) { e.HeadSHA = "foreign" },
		"foreign-plan":        func(e *CommandEvidence) { e.PlanDigest = "foreign" },
		"foreign-run":         func(e *CommandEvidence) { e.WorkflowRunID++ },
		"foreign-attempt":     func(e *CommandEvidence) { e.WorkflowAttempt++ },
		"foreign-environment": func(e *CommandEvidence) { e.EnvironmentID = "foreign" },
		"wrong-count":         func(e *CommandEvidence) { e.CountMode = "cache-default" },
		"wrong-build":         func(e *CommandEvidence) { e.BuildContext.CGOEnabled = "0" },
		"downgraded-workload": func(e *CommandEvidence) { e.WorkloadProfile = "core" },
		"failed-command":      func(e *CommandEvidence) { e.ExitCode = 1 },
		"failed-child":        func(e *CommandEvidence) { e.Report.Tests[1].Result = "fail"; e.Report.Summary.FailedTests = 1 },
		"skipped-child":       func(e *CommandEvidence) { e.Report.Tests[1].Result = "skip"; e.Report.Summary.SkippedTests = 1 },
		"missing-child":       func(e *CommandEvidence) { e.Report.Tests = e.Report.Tests[:2]; e.Report.Summary.Tests = 2 },
		"missing-root":        func(e *CommandEvidence) { e.Report.Tests = e.Report.Tests[1:]; e.Report.Summary.Tests = 2 },
		"duplicate-root": func(e *CommandEvidence) {
			e.Report.Tests = append(e.Report.Tests, e.Report.Tests[0])
			e.Report.Summary.Tests++
		},
		"malformed":     func(e *CommandEvidence) { e.Report.Summary.MalformedLines++ },
		"negative-time": func(e *CommandEvidence) { e.Report.Tests[0].Elapsed = -1 },
		"nan-time":      func(e *CommandEvidence) { e.Report.Tests[0].Elapsed = math.NaN() },
		"infinite-time": func(e *CommandEvidence) { e.ElapsedSeconds = math.Inf(1) },
		"missing-package": func(e *CommandEvidence) {
			e.Report.Packages = nil
			e.Report.Summary.Packages = 0
			e.Report.Summary.PackageElapsedSec = 0
		},
	}
	for _, mode := range []TestTimeMode{TestTimePR, TestTimeStrict} {
		plan, receipt := rootEvidenceFixture(t)
		plan.Units[0].RequiredTests[0].Children = []string{"first", "second"}
		rebindTimingFixture(t, &plan, &receipt)
		roots, err := observedRootTimes(plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt})
		if err != nil {
			t.Fatal(err)
		}
		reference := TestTimeReference{BuildContext: plan.BuildContext, Roots: timingReferenceRoots(t, roots, rootTimingPolicy())}
		if got := EvaluateTestTime(reference, rootTimingPolicy(), plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt}, mode); got.Status != BudgetPass {
			t.Fatalf("valid complete proof rejected: %+v", got)
		}
		for name, mutate := range mutations {
			t.Run(string(mode)+"/"+name, func(t *testing.T) {
				e := receipt
				e.Report.Tests = slices.Clone(receipt.Report.Tests)
				mutate(&e)
				if got := EvaluateTestTime(reference, rootTimingPolicy(), plan, TestTimeReferenceRunID, 1, []CommandEvidence{e}, mode); got.Status != BudgetIncomplete {
					t.Fatalf("invalid evidence softened: %+v", got)
				}
			})
		}
		for _, receipts := range [][]CommandEvidence{nil, {receipt, receipt}} {
			if got := EvaluateTestTime(reference, rootTimingPolicy(), plan, TestTimeReferenceRunID, 1, receipts, mode); got.Status != BudgetIncomplete {
				t.Fatal("missing/duplicate execution softened")
			}
		}
		if got := EvaluateTestTime(reference, rootTimingPolicy(), plan, TestTimeReferenceRunID, 1, []CommandEvidence{receipt}, "unknown"); got.Status != BudgetIncomplete {
			t.Fatal("unknown comparison mode admitted")
		}
	}
}

func rebindTimingFixture(t *testing.T, plan *testplanning.RunPlan, receipt *CommandEvidence) {
	t.Helper()
	plan.Digest = ""
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	plan.Digest = hex.EncodeToString(digest[:])
	receipt.PlanDigest = plan.Digest
}

func TestTestTimeEventModeIsExact(t *testing.T) {
	for _, event := range []string{"pull_request", "PULL_REQUEST", "pull_request ", "pull_request_target", "schedule", "workflow_dispatch", "push", "", "unknown"} {
		want := TestTimeStrict
		if event == "pull_request" {
			want = TestTimePR
		}
		if got := TestTimeModeForEvent(event); got != want {
			t.Fatalf("event %q gave %q, want %q", event, got, want)
		}
	}
}

func TestTestTimeStatusCompositionAndJobAttachment(t *testing.T) {
	statuses := []BudgetStatus{BudgetPass, BudgetWarn, BudgetFail, BudgetIncomplete}
	for i, before := range statuses {
		for j, incoming := range statuses {
			result := BudgetResult{Status: before, Problems: []string{"original cause"}}
			result.AttachTestTime(TestTimeResult{Status: incoming})
			if result.Status != statuses[max(i, j)] || result.Problems[0] != "original cause" || result.TestTime.Status != incoming {
				t.Fatalf("non-monotone status composition: %+v", result)
			}
		}
	}
	plan, receipt := rootEvidenceFixture(t)
	start := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	job := ActionJob{ID: 1, RunID: TestTimeReferenceRunID, RunAttempt: 1, HeadSHA: plan.HeadSHA,
		Name: "Go proof " + plan.Batches[0].ID, Status: "completed", Conclusion: "success",
		CreatedAt: start, StartedAt: start, CompletedAt: start.Add(time.Minute),
		Steps: []ActionStep{
			{Name: "Run exact planned proof unit", Status: "completed", Conclusion: "success", StartedAt: start, CompletedAt: start.Add(10 * time.Second)},
			{Name: "Upload proof evidence", Status: "completed", Conclusion: "success", StartedAt: start.Add(10 * time.Second), CompletedAt: start.Add(20 * time.Second)},
		}}
	for _, afterJobs := range []bool{false, true} {
		for _, outcome := range []string{"success", "failure", "cancelled", "missing", "foreign-attempt"} {
			jobs := []ActionJob{job}
			jobs[0].Conclusion = outcome
			if outcome == "missing" {
				jobs = nil
			}
			if outcome == "foreign-attempt" {
				jobs[0].RunAttempt++
			}
			result := EvaluateBudget(timingTestPolicy(), EvaluationOptions{Plan: plan, WorkflowRunID: TestTimeReferenceRunID, WorkflowAttempt: 1}, []CommandEvidence{receipt})
			if !afterJobs {
				result.AttachTestTime(TestTimeResult{Status: BudgetWarn})
			}
			AttachJobEvidence(&result, plan, TestTimeReferenceRunID, 1, plan.HeadSHA, jobs)
			if afterJobs {
				result.AttachTestTime(TestTimeResult{Status: BudgetWarn})
			}
			want := BudgetIncomplete
			if outcome == "success" {
				want = BudgetWarn
			}
			if result.Status != want || (result.ExitCode() == 0) != (want == BudgetWarn) {
				t.Fatalf("afterJobs=%v outcome=%s: %+v", afterJobs, outcome, result)
			}
		}
	}
}

func TestTestTimeJSONMarkdownRetainsExhaustiveIdentity(t *testing.T) {
	var baseline, candidate []RootTime
	for i := 30; i >= 0; i-- {
		name := fmt.Sprintf("TestRetained%02d", i)
		baseline = append(baseline, timingRoot(name, 100))
		candidate = append(candidate, timingRoot(name, 100+float64(i)))
	}
	row, problems := compareTimingTier(timingReferenceRoots(t, baseline, rootTimingPolicy()), candidate, rootTimingPolicy(), "core", TestTimePR)
	if len(problems) != 0 || len(row.RetainedCells) != 31 || len(row.Warnings) == 0 {
		t.Fatalf("retained attribution incomplete: %+v %v", row, problems)
	}
	result := TestTimeResult{Mode: TestTimePR, Status: BudgetWarn, Tiers: []TestTimeTier{row}, Warnings: row.Warnings}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var decoded TestTimeResult
	if err := json.Unmarshal(raw, &decoded); err != nil || !reflect.DeepEqual(result, decoded) {
		t.Fatalf("JSON lost attribution: %v", err)
	}
	for i, cell := range decoded.Tiers[0].RetainedCells {
		if cell.Root != fmt.Sprintf("TestRetained%02d", i) || cell.Baseline != 100 || cell.Candidate != 100+float64(i) || cell.Delta != float64(i) || cell.Environment == "" || cell.Count == "" {
			t.Fatalf("wrong exact cell evidence: %+v", cell)
		}
	}
	var markdown bytes.Buffer
	if err := WriteTestTimeMarkdown(&markdown, result); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"pr_added_cost", "Added cost", "Strict counterfactual", "20 of 31", "tiers[].retained_cells", "Advisory:", "TestRetained30", "+30.000s"} {
		if !strings.Contains(markdown.String(), text) {
			t.Fatalf("Markdown lost %q", text)
		}
	}
}
