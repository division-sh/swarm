package testtiming

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestWholeJobEvidenceIsExactAndIncludesAllCosts(t *testing.T) {
	plan := timingTestPlan(t)
	var commands []CommandEvidence
	var jobs []ActionJob
	start := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	for i, unit := range plan.Units {
		commands = append(commands, timingTestEvidence(plan, unit.ID, AttemptPrimary, 10))
		at := start.Add(time.Duration(i) * 30 * time.Second)
		jobs = append(jobs, ActionJob{
			ID: int64(i + 1), RunID: 1, RunAttempt: 1, HeadSHA: plan.HeadSHA,
			Name: "Go proof " + unit.ID, Status: "completed", Conclusion: "success",
			CreatedAt: at.Add(-5 * time.Second), StartedAt: at, CompletedAt: at.Add(time.Minute),
			Steps: []ActionStep{
				{Name: "Run exact planned proof unit", Status: "completed", Conclusion: "success", StartedAt: at.Add(10 * time.Second), CompletedAt: at.Add(30 * time.Second)},
				{Name: "Upload proof evidence", Status: "completed", Conclusion: "success", StartedAt: at.Add(30 * time.Second), CompletedAt: at.Add(40 * time.Second)},
			},
		})
	}
	// Exercise the actual paginated gh transport, not an unrelated timing DTO.
	raw, err := json.Marshal([]any{map[string]any{"jobs": jobs[:1]}, map[string]any{"jobs": jobs[1:]}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ReadActionJobs(bytes.NewReader(raw))
	if err != nil || len(decoded) != len(jobs) {
		t.Fatalf("jobs=%v err=%v", decoded, err)
	}
	opts := EvaluationOptions{Plan: plan, WorkflowRunID: 1, WorkflowAttempt: 1}
	result := EvaluateBudget(timingTestPolicy(), opts, commands)
	AttachJobEvidence(&result, plan, 1, 1, decoded)
	if result.Status != BudgetPass || result.Jobs.RunnerMinutes != float64(len(jobs)) || result.Jobs.QueueSeconds != float64(5*len(jobs)) || result.Jobs.PeakConcurrency != 2 {
		t.Fatalf("whole-job report: %+v jobs=%+v", result, result.Jobs)
	}
	var report bytes.Buffer
	if err := WriteBudgetMarkdown(&report, result); err != nil || !strings.Contains(report.String(), "60s | 5s | 50s") {
		t.Fatalf("whole-job markdown=%s err=%v", report.String(), err)
	}
	for _, kind := range []string{"missing", "duplicate", "wrong_head", "wrong_attempt", "wrong_run", "unfinished", "missing_upload", "invalid_time", "unknown_unit"} {
		t.Run(kind, func(t *testing.T) {
			values := append([]ActionJob(nil), decoded...)
			switch kind {
			case "missing":
				values = values[1:]
			case "duplicate":
				values = append(values, values[0])
			case "wrong_head":
				values[0].HeadSHA = "wrong"
			case "wrong_attempt":
				values[0].RunAttempt++
			case "wrong_run":
				values[0].RunID++
			case "unfinished":
				values[0].Status = "in_progress"
			case "missing_upload":
				values[0].Steps = values[0].Steps[:1]
			case "invalid_time":
				values[0].CompletedAt = values[0].CreatedAt
			case "unknown_unit":
				values[0].Name = "Go proof unplanned"
			}
			got := EvaluateBudget(timingTestPolicy(), opts, commands)
			AttachJobEvidence(&got, plan, 1, 1, values)
			if got.Status != BudgetIncomplete {
				t.Fatalf("invalid evidence accepted: %+v", got)
			}
		})
	}
	commands[0].WorkflowAttempt = 2
	if got := EvaluateBudget(timingTestPolicy(), opts, commands); got.Status != BudgetIncomplete {
		t.Fatalf("command from a different attempt accepted: %+v", got)
	}
	if _, err := ReadActionJobs(strings.NewReader(string(raw) + " {}")); err == nil {
		t.Fatal("trailing jobs evidence accepted")
	}
}
