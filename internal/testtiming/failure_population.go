package testtiming

import "fmt"

type FailureRun struct {
	ID         int64            `json:"id"`
	Event      string           `json:"event"`
	HeadSHA    string           `json:"head_sha"`
	Status     string           `json:"status"`
	Conclusion string           `json:"conclusion"`
	Attempts   []FailureAttempt `json:"attempts"`
}

type FailureAttempt struct {
	Number        int               `json:"number"`
	Conclusion    string            `json:"conclusion"`
	Failures      []ObservedFailure `json:"failures"`
	Problems      []string          `json:"problems"`
	CancelledJobs []int64           `json:"cancelled_jobs,omitempty"`
	UnstartedJobs []int64           `json:"unstarted_jobs,omitempty"`
}

type FailurePopulation struct {
	PRRuns             int `json:"pr_runs"`
	RedRuns            int `json:"runs_with_failed_attempts"`
	PRAttributable     int `json:"pr_owned_only_runs"`
	NonPROnly          int `json:"reviewed_non_pr_only_runs"`
	UnknownOnly        int `json:"unknown_only_runs"`
	Mixed              int `json:"mixed_runs"`
	InFlight           int `json:"in_flight_runs"`
	Cancelled          int `json:"cancelled_runs"`
	EarlierRedNowGreen int `json:"earlier_red_now_green_runs"`
	CancelledJobs      int `json:"cancelled_jobs"`
	UnstartedJobs      int `json:"unstarted_jobs"`
}

func (l FailureLedger) Summarize(runs []FailureRun) (FailurePopulation, error) {
	var summary FailurePopulation
	if err := l.Validate(); err != nil {
		return summary, err
	}
	seen := map[int64]bool{}
	for _, run := range runs {
		if run.ID <= 0 || seen[run.ID] || !hexIdentity(run.HeadSHA, 20) {
			return summary, fmt.Errorf("missing/duplicate/foreign failure population run %d", run.ID)
		}
		seen[run.ID] = true
		if run.Event != "pull_request" {
			continue
		}
		summary.PRRuns++
		for _, attempt := range run.Attempts {
			summary.CancelledJobs += len(attempt.CancelledJobs)
			summary.UnstartedJobs += len(attempt.UnstartedJobs)
		}
		if run.Status != "completed" {
			summary.InFlight++
		}
		if run.Conclusion == "cancelled" {
			summary.Cancelled++
		}
		classes, red, err := l.failureRunClasses(run)
		if err != nil {
			return summary, err
		}
		if !red {
			continue
		}
		summary.RedRuns++
		if run.Conclusion == "success" {
			summary.EarlierRedNowGreen++
		}
		if len(classes) > 1 {
			summary.Mixed++
		} else if classes["PR-owned"] {
			summary.PRAttributable++
		} else if classes["non-PR-proven"] {
			summary.NonPROnly++
		} else {
			summary.UnknownOnly++
		}
	}
	return summary, nil
}

func (l FailureLedger) failureRunClasses(run FailureRun) (map[string]bool, bool, error) {
	classes := map[string]bool{}
	seen := map[int]bool{}
	red := false
	for _, attempt := range run.Attempts {
		if attempt.Number <= 0 || seen[attempt.Number] {
			return nil, false, fmt.Errorf("duplicate/invalid attempt on run %d", run.ID)
		}
		seen[attempt.Number] = true
		if len(attempt.Problems) != 0 {
			classes["unknown"] = true
		}
		if attempt.Conclusion == "failure" || attempt.Conclusion == "timed_out" {
			red = true
			if len(attempt.Failures) == 0 {
				classes["unknown"] = true
			}
		}
		for _, failure := range attempt.Failures {
			if failure.RunID != run.ID || failure.Attempt != attempt.Number || failure.HeadSHA != run.HeadSHA {
				return nil, false, fmt.Errorf("foreign failure observation on run %d", run.ID)
			}
			if failure.Conclusion == "cancelled" {
				continue
			}
			match, err := l.Match(failure)
			if err != nil {
				return nil, false, err
			}
			classes[match.Attribution] = true
			red = true
		}
	}
	if run.Conclusion == "failure" || run.Conclusion == "timed_out" {
		red = true
		if len(classes) == 0 {
			classes["unknown"] = true
		}
	}
	return classes, red, nil
}
