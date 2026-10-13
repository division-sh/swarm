package testtiming

import "fmt"

type FailureRun struct {
	ID         int64            `json:"id"`
	RunAttempt int              `json:"run_attempt"`
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
	PRRuns             int                               `json:"pr_runs"`
	RedRuns            int                               `json:"runs_with_failed_attempts"`
	PRAttributable     int                               `json:"pr_owned_only_runs"`
	NonPROnly          int                               `json:"reviewed_non_pr_only_runs"`
	UnknownOnly        int                               `json:"unknown_only_runs"`
	Mixed              int                               `json:"mixed_runs"`
	InFlight           int                               `json:"in_flight_runs"`
	Cancelled          int                               `json:"cancelled_runs"`
	EarlierRedNowGreen int                               `json:"earlier_red_now_green_runs"`
	CancelledJobs      int                               `json:"cancelled_jobs"`
	UnstartedJobs      int                               `json:"unstarted_jobs"`
	Incomplete         int                               `json:"incompletely_observed_runs"`
	ByPhenomenon       map[string]FailureClassPopulation `json:"by_phenomenon"`
}

type FailureClassPopulation struct {
	RedRuns             int `json:"red_runs"`
	ReviewedNonPRRuns   int `json:"reviewed_non_pr_runs"`
	UnknownObservations int `json:"unknown_observations"`
}

func (l FailureLedger) Summarize(runs []FailureRun) (FailurePopulation, error) {
	summary := FailurePopulation{ByPhenomenon: make(map[string]FailureClassPopulation)}
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
		countFailureExposure(run, &summary)
		classes, red, err := l.failureRunClasses(run)
		if err != nil {
			return summary, err
		}
		if !red {
			continue
		}
		l.countFailurePhenomena(run, &summary)
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

func completeFailureHistory(run FailureRun) bool {
	// Exact cardinality plus unique numbers in this range requires every attempt
	// from 1 through the declared latest attempt, regardless of input order.
	if run.RunAttempt <= 0 || len(run.Attempts) != run.RunAttempt {
		return false
	}
	seen := make(map[int]bool)
	for _, attempt := range run.Attempts {
		if attempt.Number < 1 || attempt.Number > run.RunAttempt || seen[attempt.Number] {
			return false
		}
		seen[attempt.Number] = true
	}
	return true
}

func countFailureExposure(run FailureRun, summary *FailurePopulation) {
	incomplete := !completeFailureHistory(run)
	for _, attempt := range run.Attempts {
		summary.CancelledJobs += len(attempt.CancelledJobs)
		summary.UnstartedJobs += len(attempt.UnstartedJobs)
		incomplete = incomplete || len(attempt.Problems) != 0
		for _, failure := range attempt.Failures {
			incomplete = incomplete || failure.Problem != "" || !hexIdentity(failure.Source, 20)
		}
	}
	if incomplete {
		summary.Incomplete++
	}
	if run.Status != "completed" {
		summary.InFlight++
	}
	if run.Conclusion == "cancelled" {
		summary.Cancelled++
	}
}

// Class counts can overlap within a run. They are not additive probabilities or
// substitutes for the whole-run non-PR-only lower bound.
func (l FailureLedger) countFailurePhenomena(run FailureRun, summary *FailurePopulation) {
	seen, nonPR := map[string]bool{}, map[string]bool{}
	if !completeFailureHistory(run) {
		seen["unknown-variance"] = true
	}
	for _, attempt := range run.Attempts {
		if len(attempt.Problems) != 0 || ((attempt.Conclusion == "failure" || attempt.Conclusion == "timed_out") && len(attempt.Failures) == 0) {
			seen["unknown-variance"] = true
		}
		for _, failure := range attempt.Failures {
			if failure.Conclusion == "cancelled" {
				continue
			}
			match, _ := l.Match(failure) // Already validated by failureRunClasses.
			seen[match.Phenomenon] = true
			count := summary.ByPhenomenon[match.Phenomenon]
			if match.Attribution == "non-PR-proven" {
				nonPR[match.Phenomenon] = true
			} else if match.Attribution == "unknown" {
				count.UnknownObservations++
			}
			summary.ByPhenomenon[match.Phenomenon] = count
		}
	}
	if len(seen) == 0 {
		seen["unknown-variance"] = true
	}
	for kind := range seen {
		count := summary.ByPhenomenon[kind]
		count.RedRuns++
		if nonPR[kind] {
			count.ReviewedNonPRRuns++
		}
		summary.ByPhenomenon[kind] = count
	}
}

func (l FailureLedger) failureRunClasses(run FailureRun) (map[string]bool, bool, error) {
	classes := map[string]bool{}
	if !completeFailureHistory(run) {
		classes["unknown"] = true
	}
	seen := map[int]bool{}
	red := false
	for _, attempt := range run.Attempts {
		if attempt.Number <= 0 || seen[attempt.Number] || run.RunAttempt > 0 && attempt.Number > run.RunAttempt {
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
		observedRed, err := l.mergeFailureObservations(run, attempt, classes)
		if err != nil {
			return nil, false, err
		}
		red = red || observedRed
	}
	if run.Conclusion == "failure" || run.Conclusion == "timed_out" {
		red = true
		if len(classes) == 0 {
			classes["unknown"] = true
		}
	}
	return classes, red, nil
}

func (l FailureLedger) mergeFailureObservations(run FailureRun, attempt FailureAttempt, classes map[string]bool) (bool, error) {
	red := false
	for _, failure := range attempt.Failures {
		if failure.RunID != run.ID || failure.Attempt != attempt.Number || failure.HeadSHA != run.HeadSHA {
			return false, fmt.Errorf("foreign failure observation on run %d", run.ID)
		}
		if failure.Conclusion == "cancelled" {
			continue
		}
		match, err := l.Match(failure)
		if err != nil {
			return false, err
		}
		classes[match.Attribution] = true
		red = true
	}
	return red, nil
}
