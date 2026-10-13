package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testtiming"
)

func checkFailureAllObservationIntegrity(t *testing.T) {
	for _, event := range []string{"pull_request", "workflow_dispatch", "push", "schedule"} {
		for _, mode := range []string{"missing-first", "missing-middle", "missing-final", "missing-count", "attempt-problem", "failure-problem", "missing-source", "invalid-source", "complete", "complete-red"} {
			t.Run("all-observation/"+event+"/"+mode, func(t *testing.T) {
				run := testtiming.FailureRun{ID: 123, RunAttempt: 3, Event: event, HeadSHA: strings.Repeat("a", 40), Status: "completed", Conclusion: "success", Attempts: []testtiming.FailureAttempt{{Number: 1, Conclusion: "success"}, {Number: 2, Conclusion: "success"}, {Number: 3, Conclusion: "success"}}}
				switch mode {
				case "missing-first":
					run.Attempts = run.Attempts[1:]
				case "missing-middle":
					run.Attempts = append(run.Attempts[:1], run.Attempts[2:]...)
				case "missing-final":
					run.Attempts = run.Attempts[:2]
				case "missing-count":
					run.RunAttempt = 0
				case "attempt-problem":
					run.Attempts[2].Problems = []string{"exact plan unavailable"}
				case "failure-problem", "missing-source", "invalid-source", "complete-red":
					run.Conclusion, run.Attempts[2].Conclusion = "failure", "failure"
					failure := testtiming.ObservedFailure{RunID: run.ID, Attempt: 3, JobID: 11, HeadSHA: run.HeadSHA, Source: strings.Repeat("b", 40), GOOS: "linux", Job: "Go proof owned", Test: "TestReal", Detail: "observed RED", Conclusion: "failure"}
					if mode == "failure-problem" {
						failure.Problem = "HTTP 410: job logs expired"
					}
					if mode == "missing-source" {
						failure.Source = ""
					}
					if mode == "invalid-source" {
						failure.Source = strings.Repeat("z", 40)
					}
					run.Attempts[2].Failures = []testtiming.ObservedFailure{failure}
				}
				for _, mixed := range []bool{false, true} {
					runs := []testtiming.FailureRun{run}
					if mixed {
						companion := "pull_request"
						if event == "pull_request" {
							companion = "workflow_dispatch"
						}
						runs = append(runs, testtiming.FailureRun{ID: 124, RunAttempt: 1, Event: companion, HeadSHA: run.HeadSHA, Status: "completed", Conclusion: "success", Attempts: []testtiming.FailureAttempt{{Number: 1, Conclusion: "success"}}})
					}
					report, _, err := runFailureIntegrityFixture(t, runs)
					complete := mode == "complete" || mode == "complete-red"
					if (err == nil) != complete {
						t.Fatalf("observation integrity mixed=%t: %+v / %v", mixed, report, err)
					}
					wantPR := 0
					if event == "pull_request" {
						wantPR++
					}
					if mixed && event != "pull_request" {
						wantPR++
					}
					if report.Summary.PRRuns != wantPR || event != "pull_request" && (report.Summary.Incomplete != 0 || report.Summary.RedRuns != 0) {
						t.Fatalf("non-PR inspection contaminated PR projection: %+v", report.Summary)
					}
				}
			})
		}
	}
	t.Run("all-observation/mixed-two-incomplete", func(t *testing.T) {
		runs := []testtiming.FailureRun{
			{ID: 123, RunAttempt: 2, Event: "pull_request", HeadSHA: strings.Repeat("a", 40), Status: "completed", Conclusion: "success", Attempts: []testtiming.FailureAttempt{{Number: 2, Conclusion: "success"}}},
			{ID: 124, RunAttempt: 1, Event: "workflow_dispatch", HeadSHA: strings.Repeat("a", 40), Status: "completed", Conclusion: "success", Attempts: []testtiming.FailureAttempt{{Number: 1, Conclusion: "success", Problems: []string{"exact plan unavailable"}}}},
		}
		report, _, err := runFailureIntegrityFixture(t, runs)
		if err == nil || !strings.Contains(err.Error(), "2 runs") || report.Summary.PRRuns != 1 || report.Summary.Incomplete != 1 {
			t.Fatalf("all-run integrity reused the PR-only count: %+v / %v", report, err)
		}
	})
}

func runFailureIntegrityFixture(t *testing.T, runs []testtiming.FailureRun) (failurePopulationReport, string, error) {
	t.Helper()
	root := t.TempDir()
	input, result := filepath.Join(root, "population.json"), filepath.Join(root, "report.json")
	if err := writeJSON(input, runs); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err := runFailures([]string{"-population", input, "-ledger", "../../.github/flake-ledger.json", "-json", result}, &output)
	var report failurePopulationReport
	if readErr := readJSON(result, &report); readErr != nil || !reflect.DeepEqual(report.Runs, runs) {
		t.Fatalf("original observations were not retained: %+v / %v", report, readErr)
	}
	return report, output.String(), err
}

func checkFailureNonPRLiveIntegrity(t *testing.T) {
	for _, event := range []string{"pull_request", "workflow_dispatch", "push", "schedule"} {
		for _, mode := range []string{"valid", "missing-log", "missing-plan", "missing-artifact"} {
			t.Run("live-observation/"+event+"/"+mode, func(t *testing.T) {
				run := failureObservationRun()
				run.Event = event
				installFailureObservationAPI(t, run, mode)
				first := run
				first.RunAttempt, first.Conclusion = 1, "success"
				raw, err := json.Marshal(first)
				if err != nil {
					t.Fatal(err)
				}
				writeFailureFixture(t, "run-first.json", raw)
				raw, err = os.ReadFile(filepath.Join(os.Getenv("FAILURE_API"), "plan.zip"))
				if err != nil {
					t.Fatal(err)
				}
				plan, _, err := mergedPlanArchive(raw)
				if err != nil {
					t.Fatal(err)
				}
				selection, err := plan.CISelectionReport(run.ID, 1)
				if err != nil {
					t.Fatal(err)
				}
				writeFailureFixture(t, "plan-first.zip", artifactPlanArchive(t, plan, selection, nil))
				if mode == "missing-artifact" {
					writeFailureFixture(t, "proof-list.json", []byte(`{"total_count":0,"artifacts":[]}`))
				}
				result := filepath.Join(t.TempDir(), "report.json")
				var output bytes.Buffer
				err = runFailures([]string{"-run", "123", "-ledger", "../../.github/flake-ledger.json", "-json", result}, &output)
				var report failurePopulationReport
				if readErr := readJSON(result, &report); readErr != nil || len(report.Runs) != 1 || report.Runs[0].Event != event || len(report.Runs[0].Attempts) != 2 || len(report.Runs[0].Attempts[1].Failures) != 1 {
					t.Fatalf("lost live observation: %+v / %v", report, readErr)
				}
				failure := report.Runs[0].Attempts[1].Failures[0]
				if (err == nil) != (mode == "valid") {
					t.Fatalf("live integrity: %+v / %v", report, err)
				}
				if event != "pull_request" && (report.Summary.PRRuns != 0 || report.Summary.Incomplete != 0 || report.Summary.RedRuns != 0) {
					t.Fatalf("non-PR inspection entered PR statistics: %+v", report.Summary)
				}
				if mode == "missing-log" && (!strings.Contains(failure.Problem, "HTTP 410") || !strings.Contains(output.String(), "HTTP 410") || failure.Test != "") {
					t.Fatalf("lost stderr or adopted failed transport stdout: %+v / %s", failure, output.String())
				}
				if (mode == "missing-plan" || mode == "missing-artifact") && (failure.Source != "" || failure.Problem == "") {
					t.Fatalf("missing authority became complete evidence: %+v", failure)
				}
			})
		}
	}
}
