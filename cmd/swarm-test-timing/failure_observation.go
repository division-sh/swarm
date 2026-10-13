package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/testtiming"
)

func observeFailureAttempt(ctx context.Context, run failureRunMetadata, number int) testtiming.FailureAttempt {
	attempt := testtiming.FailureAttempt{Number: number}
	path := fmt.Sprintf("repos/division-sh/swarm/actions/runs/%d/attempts/%d", run.ID, number)
	var metadata failureRunMetadata
	if err := failureAPI(ctx, path, &metadata); err != nil {
		attempt.Problems = append(attempt.Problems, err.Error())
		return attempt
	}
	attempt.Conclusion = metadata.Conclusion
	if metadata.ID != run.ID || metadata.RunAttempt != number || metadata.HeadSHA != run.HeadSHA {
		attempt.Problems = append(attempt.Problems, "foreign attempt metadata")
		return attempt
	}
	plan, err := mergedRunPlan(ctx, "division-sh/swarm", metadata.QualifiedRun)
	if err != nil {
		attempt.Problems = append(attempt.Problems, "exact execution plan unavailable: "+err.Error())
	}
	jobs, err := readFailureAttemptJobs(ctx, path)
	if err != nil {
		attempt.Problems = append(attempt.Problems, err.Error())
		return attempt
	}
	primary := false
	for _, job := range jobs {
		if (job.Conclusion == "failure" || job.Conclusion == "timed_out") && !failureAggregateJob(job.Name) {
			primary = true
		}
	}
	seen := map[int64]bool{}
	for _, job := range jobs {
		if job.RunID != run.ID || job.RunAttempt != number || job.HeadSHA != run.HeadSHA || seen[job.ID] {
			attempt.Problems = append(attempt.Problems, "foreign/duplicate job observation")
			continue
		}
		seen[job.ID] = true
		if job.StartedAt.IsZero() {
			attempt.UnstartedJobs = append(attempt.UnstartedJobs, job.ID)
		}
		if job.Conclusion == "cancelled" {
			attempt.CancelledJobs = append(attempt.CancelledJobs, job.ID)
		}
		if (job.Conclusion != "failure" && job.Conclusion != "timed_out" && job.Conclusion != "cancelled") || (primary && failureAggregateJob(job.Name)) {
			continue
		}
		failure := testtiming.ObservedFailure{RunID: run.ID, Attempt: number, JobID: job.ID, HeadSHA: run.HeadSHA, Source: plan.HeadSHA, GOOS: plan.BuildContext.GOOS, Job: job.Name, Conclusion: job.Conclusion}
		if plan.HeadSHA == "" {
			failure.Problem = "exact executed source unavailable"
		}
		if job.Conclusion == "cancelled" && len(job.Steps) == 0 {
			failure.Problem = "cancelled before execution; no test result"
			attempt.Failures = append(attempt.Failures, failure)
			continue
		}
		attempt.Failures = append(attempt.Failures, observeFailureJob(ctx, failure)...)
	}
	return attempt
}

func readFailureAttemptJobs(ctx context.Context, path string) ([]testtiming.ActionJob, error) {
	var jobs []testtiming.ActionJob
	for page := 1; ; page++ {
		var batch struct {
			Total int                    `json:"total_count"`
			Jobs  []testtiming.ActionJob `json:"jobs"`
		}
		if err := failureAPI(ctx, fmt.Sprintf("%s/jobs?per_page=100&page=%d", path, page), &batch); err != nil {
			return nil, err
		}
		jobs = append(jobs, batch.Jobs...)
		if len(jobs) == batch.Total {
			break
		}
		if len(batch.Jobs) == 0 || len(jobs) > batch.Total {
			return nil, fmt.Errorf("incomplete/contradictory jobs population")
		}
	}
	return jobs, nil
}

func observeFailureJob(ctx context.Context, failure testtiming.ObservedFailure) []testtiming.ObservedFailure {
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(readCtx, "gh", "api", "--allow-escape-sequences", fmt.Sprintf("repos/division-sh/swarm/actions/jobs/%d/logs", failure.JobID)).Output()
	if err != nil {
		failure.Problem = "log unavailable: " + err.Error()
		return []testtiming.ObservedFailure{failure}
	}
	failures, err := nativeFailureEvents(raw, failure)
	if err != nil {
		failure.Problem = err.Error()
		return []testtiming.ObservedFailure{failure}
	}
	return failures
}

func failureAggregateJob(name string) bool {
	return name == "Required test summary" || name == "CI timing budget" || strings.HasPrefix(name, "CI tier: ")
}

func nativeFailureEvents(raw []byte, job testtiming.ObservedFailure) ([]testtiming.ObservedFailure, error) {
	failed := map[string]testtiming.ObservedFailure{}
	contextLines := map[string][]string{}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 4096), 10<<20)
	for scanner.Scan() {
		line := scanner.Text()
		payload := line
		if !strings.HasPrefix(line, "{") {
			stamp, rest, ok := strings.Cut(line, " ")
			if !ok {
				continue
			}
			if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil {
				continue
			}
			payload = rest
		}
		var event struct{ Action, Package, Test, Output string }
		if err := json.Unmarshal([]byte(payload), &event); err != nil {
			if strings.Contains(payload, `"Action"`) {
				return nil, fmt.Errorf("malformed native failure event: %w", err)
			}
			continue
		}
		if event.Package == "" || event.Action == "" {
			continue
		}
		key := event.Package + "\x00" + event.Test
		if event.Action == "output" {
			contextLines[key] = append(contextLines[key], event.Output)
			if len(contextLines[key]) > 8 {
				contextLines[key] = contextLines[key][1:]
			}
		}
		if event.Action == "fail" && event.Test != "" {
			failure := job
			failure.Package, failure.Test, failure.Detail = event.Package, event.Test, strings.Join(contextLines[key], "")
			failed[key] = failure
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(failed) == 0 {
		job.Detail = string(raw)
		return []testtiming.ObservedFailure{job}, nil
	}
	var failures []testtiming.ObservedFailure
	for key, failure := range failed {
		parent := false
		for child := range failed {
			if strings.HasPrefix(child, key+"/") {
				parent = true
			}
		}
		if !parent {
			failures = append(failures, failure)
		}
	}
	sort.Slice(failures, func(i, j int) bool {
		return failures[i].Package+failures[i].Test < failures[j].Package+failures[j].Test
	})
	return failures, nil
}
