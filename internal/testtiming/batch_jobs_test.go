package testtiming

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestBatchJobEvidenceCountsPhysicalCostOnceAndEveryCommand(t *testing.T) {
	policy := testplanning.Policy{Version: testplanning.PolicyVersion, Module: "module",
		Planning:        testplanning.PlanningPolicy{TargetSeconds: 100, MaxShards: 1, UnknownPackageSeconds: 30},
		SpecialPackages: []string{"module/selected"}, Profiles: map[string]testplanning.ProfilePolicy{},
		Units: map[string]testplanning.UnitPolicy{}, Projections: map[string]testplanning.ProjectionPolicy{}}
	for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
		policy.Profiles[tier] = testplanning.ProfilePolicy{CountMode: CountModeOne, EnvironmentID: "env", Units: []string{"one", "two"}}
	}
	for _, id := range []string{"one", "two"} {
		policy.Units[id] = testplanning.UnitPolicy{Packages: []string{"module/selected"}, Run: "^Test" + id + "$", CountMode: CountModeOne, EnvironmentID: "env", BudgetClass: "full", Packable: true}
	}
	model := testplanning.WeightModel{Version: testplanning.WeightModelVersion, SourceRunID: "fixture", Units: map[string]map[string]testplanning.UnitWeight{testplanning.ProfileFull: {}}}
	plan, err := testplanning.BuildPlan(policy, model, []string{"module/selected"}, testplanning.ProfileFull, "fixture", "head")
	if err != nil {
		t.Fatal(err)
	}
	for _, unit := range plan.Units {
		model.Units[testplanning.ProfileFull][unit.ID] = testplanning.MeasureUnit(unit, 40)
	}
	plan, err = testplanning.BuildPlan(policy, model, []string{"module/selected"}, testplanning.ProfileFull, "fixture", "head")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 10, 3, 1, 0, 0, 0, time.UTC)
	job := ActionJob{ID: 100, RunID: 1, RunAttempt: 1, HeadSHA: "head", Name: "Go proof " + plan.Batches[0].ID,
		Status: "completed", Conclusion: "success", CreatedAt: start, StartedAt: start, CompletedAt: start.Add(time.Minute),
		Steps: []ActionStep{{Name: "Run exact planned proof unit", Status: "completed", Conclusion: "success", StartedAt: start, CompletedAt: start.Add(50 * time.Second)},
			{Name: "Upload proof evidence", Status: "completed", Conclusion: "success", StartedAt: start.Add(50 * time.Second), CompletedAt: start.Add(time.Minute)}}}
	commands := []CommandEvidence{timingTestEvidence(plan, "one", AttemptPrimary, 10), timingTestEvidence(plan, "two", AttemptPrimary, 20)}
	opts := EvaluationOptions{Plan: plan, WorkflowRunID: 1, WorkflowAttempt: 1}
	result := EvaluateBudget(timingTestPolicy(), opts, commands)
	AttachJobEvidence(&result, plan, 1, 1, "head", []ActionJob{job})
	if result.Status != BudgetPass || result.Jobs.JobCount != 1 || result.Jobs.UnitCount != 2 || result.Jobs.RunnerMinutes != 1 || result.Surfaces[0].Job != result.Surfaces[1].Job {
		t.Fatalf("batch cost/evidence: %+v", result)
	}
	for _, mutation := range []string{"missing_command", "duplicate_command", "failed_command", "cancelled_job", "duplicate_job", "wrong_attempt", "over_budget"} {
		t.Run(mutation, func(t *testing.T) {
			cs, jobs := append([]CommandEvidence(nil), commands...), []ActionJob{job}
			switch mutation {
			case "missing_command":
				cs = cs[:1]
			case "duplicate_command":
				cs = append(cs, cs[0])
			case "failed_command":
				cs[0].ExitCode = 1
			case "cancelled_job":
				jobs[0].Conclusion = "cancelled"
			case "duplicate_job":
				jobs = append(jobs, jobs[0])
			case "wrong_attempt":
				jobs[0].RunAttempt++
			case "over_budget":
				cs[1].ElapsedSeconds = 1000
			}
			bad := EvaluateBudget(timingTestPolicy(), opts, cs)
			AttachJobEvidence(&bad, plan, 1, 1, "head", jobs)
			if bad.Status == BudgetPass || bad.Status == BudgetWarn {
				t.Fatalf("invalid batch admitted: %+v", bad)
			}
		})
	}
}
