package testtiming

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
)

// ActionJob is the GitHub Actions attempt-specific jobs API projection. Retain
// every step so cache, provisioning, build and upload costs cannot disappear
// behind the duration of the nested go test command.
type ActionJob struct {
	ID          int64        `json:"id"`
	RunID       int64        `json:"run_id"`
	RunAttempt  int          `json:"run_attempt"`
	HeadSHA     string       `json:"head_sha"`
	Name        string       `json:"name"`
	Status      string       `json:"status"`
	Conclusion  string       `json:"conclusion"`
	CreatedAt   time.Time    `json:"created_at"`
	StartedAt   time.Time    `json:"started_at"`
	CompletedAt time.Time    `json:"completed_at"`
	Steps       []ActionStep `json:"steps"`
}

type ActionStep struct {
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Conclusion  string    `json:"conclusion"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
}

type JobTiming struct {
	Job             ActionJob `json:"job"`
	ElapsedSeconds  float64   `json:"elapsed_seconds"`
	StartLagSeconds float64   `json:"start_lag_seconds"`
}

type JobSummary struct {
	Profile         string  `json:"profile"`
	RunID           int64   `json:"run_id"`
	RunAttempt      int     `json:"run_attempt"`
	WorkflowHeadSHA string  `json:"workflow_head_sha"`
	ExecutionSHA    string  `json:"execution_sha"`
	UnitCount       int     `json:"unit_count"`
	RunnerMinutes   float64 `json:"runner_minutes"`
	StartLagSeconds float64 `json:"start_lag_seconds"`
	MakespanSeconds float64 `json:"makespan_seconds"`
	PeakConcurrency int     `json:"peak_concurrency"`
}

// ReadActionJobs consumes gh api --paginate --slurp output, not a first-page
// approximation. Extra API fields are intentionally ignored by this projection.
func ReadActionJobs(r io.Reader) ([]ActionJob, error) {
	var pages []struct {
		Jobs []ActionJob `json:"jobs"`
	}
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&pages); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("trailing jobs evidence: %v", err)
	}
	var jobs []ActionJob
	for _, page := range pages {
		jobs = append(jobs, page.Jobs...)
	}
	return jobs, nil
}

// AttachJobEvidence joins whole jobs to already plan-bound command evidence.
// It does not infer a pass from a missing job or substitute another run attempt.
// workflowHeadSHA comes from the triggering event, not the checkout: PR jobs
// report the branch head even when the plan executes a synthetic merge commit.
func AttachJobEvidence(result *BudgetResult, plan testplanning.RunPlan, runID int64, attempt int, workflowHeadSHA string, jobs []ActionJob) {
	problem := func(message string) {
		result.Problems = append(result.Problems, message)
		result.Status = BudgetIncomplete
	}
	if runID <= 0 || attempt <= 0 {
		problem("positive workflow run ID and attempt are required")
		return
	}
	if strings.TrimSpace(workflowHeadSHA) == "" {
		problem("workflow head SHA from the triggering event is required")
		return
	}
	expected := map[string]int{}
	for i, surface := range result.Surfaces {
		expected["Go proof "+surface.Surface] = i
	}
	seen := map[string]bool{}
	ids := map[int64]bool{}
	type endpoint struct {
		at    time.Time
		delta int
	}
	var endpoints []endpoint
	var first, last time.Time
	summary := &JobSummary{Profile: plan.Profile, RunID: runID, RunAttempt: attempt, WorkflowHeadSHA: workflowHeadSHA, ExecutionSHA: plan.HeadSHA}
	for _, job := range jobs {
		if !strings.HasPrefix(job.Name, "Go proof ") {
			continue
		}
		index, ok := expected[job.Name]
		if !ok {
			problem("unexpected proof job " + job.Name)
			continue
		}
		if seen[job.Name] || ids[job.ID] || job.ID <= 0 {
			problem("duplicate/invalid proof job " + job.Name)
			continue
		}
		seen[job.Name], ids[job.ID] = true, true
		if job.RunID != runID || job.RunAttempt != attempt || job.HeadSHA != workflowHeadSHA {
			problem("wrong run/attempt/head for " + job.Name)
			continue
		}
		if job.Status != "completed" || job.CreatedAt.IsZero() || job.StartedAt.Before(job.CreatedAt) || job.CompletedAt.Before(job.StartedAt) {
			problem("incomplete or invalid job timestamps for " + job.Name)
			continue
		}
		if job.Conclusion != "success" {
			problem("proof job did not succeed: " + job.Name)
		}
		timing := &JobTiming{Job: job, ElapsedSeconds: job.CompletedAt.Sub(job.StartedAt).Seconds(), StartLagSeconds: job.StartedAt.Sub(job.CreatedAt).Seconds()}
		result.Surfaces[index].Job = timing
		if command := result.Surfaces[index].PrimarySeconds; command != nil && *command > timing.ElapsedSeconds+1 {
			problem("primary command exceeds its whole job: " + job.Name)
		}
		proofSteps, uploadSteps := 0, 0
		for _, step := range job.Steps {
			if step.Conclusion == "skipped" {
				continue
			}
			if step.Status != "completed" || step.StartedAt.Before(job.StartedAt) || step.CompletedAt.Before(step.StartedAt) || step.CompletedAt.After(job.CompletedAt) {
				problem("invalid step timing for " + job.Name + ": " + step.Name)
			}
			if step.Name == "Run exact planned proof unit" {
				proofSteps++
			}
			if step.Name == "Upload proof evidence" {
				uploadSteps++
			}
		}
		if proofSteps != 1 || uploadSteps != 1 {
			problem("missing/duplicate proof or upload step for " + job.Name)
		}
		summary.UnitCount++
		summary.RunnerMinutes += timing.ElapsedSeconds / 60
		summary.StartLagSeconds += timing.StartLagSeconds
		if first.IsZero() || job.StartedAt.Before(first) {
			first = job.StartedAt
		}
		if job.CompletedAt.After(last) {
			last = job.CompletedAt
		}
		endpoints = append(endpoints, endpoint{job.StartedAt, 1}, endpoint{job.CompletedAt, -1})
	}
	for name := range expected {
		if !seen[name] {
			problem("missing whole-job evidence for " + name)
		}
	}
	sort.Slice(endpoints, func(i, j int) bool {
		if endpoints[i].at.Equal(endpoints[j].at) {
			return endpoints[i].delta < endpoints[j].delta
		}
		return endpoints[i].at.Before(endpoints[j].at)
	})
	active := 0
	for _, point := range endpoints {
		active += point.delta
		summary.PeakConcurrency = max(summary.PeakConcurrency, active)
	}
	if !first.IsZero() {
		summary.MakespanSeconds = last.Sub(first).Seconds()
	}
	result.Jobs = summary
}
