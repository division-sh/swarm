package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testtiming"
)

func TestFailureObservationRetainsNativeBoundaries(t *testing.T) {
	job := testtiming.ObservedFailure{RunID: 1, Attempt: 1, JobID: 2, HeadSHA: strings.Repeat("a", 40), Source: strings.Repeat("b", 40), Job: "Go proof owned"}
	var transcript bytes.Buffer
	write := func(action, name, output string) {
		line, err := json.Marshal(struct{ Action, Package, Test, Output string }{action, "proof", name, output})
		if err != nil {
			t.Fatal(err)
		}
		transcript.WriteString("2026-10-13T00:00:00Z ")
		transcript.Write(line)
		transcript.WriteByte('\n')
	}
	write("output", "TestPassing", `{"Action":"fail","Package":"child","Test":"TestExpected"}`)
	write("pass", "TestPassing", "")
	write("output", "TestReal/sqlite", "precise assertion")
	write("fail", "TestReal/sqlite", "")
	write("fail", "TestReal", "")
	write("fail", "", "")
	failures, err := nativeFailureEvents(transcript.Bytes(), job)
	if err != nil || len(failures) != 1 || failures[0].Test != "TestReal/sqlite" || failures[0].Detail != "precise assertion" || failures[0].Source != job.Source {
		t.Fatalf("lost actual leaf or adopted nested expected RED: %+v / %v", failures, err)
	}
	if !failureAggregateJob("Required test summary") || !failureAggregateJob("CI timing budget") || failureAggregateJob("Go proof owned") {
		t.Fatal("primary failure attribution changed")
	}
	if err := runFailures([]string{"-run", "1", "-last", "2"}, &bytes.Buffer{}); err == nil {
		t.Fatal("ambiguous observation command accepted")
	}
	if err := runFailures([]string{"-run", "1", "-last", "-1"}, &bytes.Buffer{}); err == nil {
		t.Fatal("negative population selection accepted")
	}
	if _, err := nativeFailureEvents([]byte(`{"Action":"fail",`), job); err == nil {
		t.Fatal("malformed native event became a ledger observation")
	}
	checkFailureAttemptAdmission(t)
}

func checkFailureAttemptAdmission(t *testing.T) {
	for _, mode := range []string{"valid", "foreign-attempt", "foreign-job", "duplicate-job", "missing-plan", "missing-jobs", "missing-log", "unstarted-cancellation"} {
		t.Run(mode, func(t *testing.T) {
			run := failureRunMetadata{QualifiedRun: testplanning.QualifiedRun{ID: 123, RunAttempt: 2, HeadSHA: strings.Repeat("a", 40), Status: "completed", Conclusion: "failure"}, Event: "pull_request"}
			installFailureObservationAPI(t, run, mode)
			attempt := observeFailureAttempt(context.Background(), run, 2)
			switch mode {
			case "valid":
				if len(attempt.Problems) != 0 || len(attempt.Failures) != 1 || attempt.Failures[0].Source != strings.Repeat("b", 40) || attempt.Failures[0].Test != "TestReal/sqlite" {
					t.Fatalf("native admission: %+v", attempt)
				}
			case "missing-log":
				if len(attempt.Failures) != 1 || attempt.Failures[0].Problem == "" {
					t.Fatalf("missing log became proof: %+v", attempt)
				}
			case "unstarted-cancellation":
				if len(attempt.UnstartedJobs) != 1 || len(attempt.CancelledJobs) != 1 || len(attempt.Failures) != 1 || attempt.Failures[0].Conclusion != "cancelled" {
					t.Fatalf("unstarted cancellation hidden by API timestamp: %+v", attempt)
				}
			default:
				if len(attempt.Problems) == 0 {
					t.Fatalf("%s became complete evidence: %+v", mode, attempt)
				}
			}
		})
	}
}

func installFailureObservationAPI(t *testing.T, run failureRunMetadata, mode string) {
	t.Helper()
	dir := t.TempDir()
	policy, weights, packages := writePlannerFixtures(t, dir)
	plan, err := readPlan(writeSyntheticPlan(t, dir, policy, weights, packages, strings.Repeat("b", 40)))
	if err != nil {
		t.Fatal(err)
	}
	report, err := plan.CISelectionReport(123, 2)
	if err != nil {
		t.Fatal(err)
	}
	archive := artifactPlanArchive(t, plan, report, nil)
	job := testtiming.ActionJob{ID: 11, RunID: 123, RunAttempt: 2, HeadSHA: run.HeadSHA, Name: "Go proof owned", Conclusion: "failure", StartedAt: time.Now()}
	if mode == "foreign-attempt" {
		run.RunAttempt = 1
	}
	if mode == "foreign-job" {
		job.HeadSHA = strings.Repeat("c", 40)
	}
	if mode == "unstarted-cancellation" {
		job.Conclusion = "cancelled"
	}
	jobs := []testtiming.ActionJob{job}
	if mode == "duplicate-job" {
		jobs = append(jobs, job)
	}
	list := `{"total_count":1,"artifacts":[{"id":7,"name":"ci-plan-123-2"}]}`
	if mode == "missing-plan" {
		list = `{"total_count":0,"artifacts":[]}`
	}
	rawRun, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	rawJobs, err := json.Marshal(struct {
		Total int                    `json:"total_count"`
		Jobs  []testtiming.ActionJob `json:"jobs"`
	}{len(jobs), jobs})
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{"run.json": rawRun, "jobs.json": rawJobs, "list.json": []byte(list), "plan.zip": archive,
		"log": []byte("2026-10-13T00:00:00Z {\"Action\":\"output\",\"Package\":\"proof\",\"Test\":\"TestReal/sqlite\",\"Output\":\"precise assertion\"}\n2026-10-13T00:00:00Z {\"Action\":\"fail\",\"Package\":\"proof\",\"Test\":\"TestReal/sqlite\"}\n"),
		"gh": []byte(`#!/bin/sh
set -eu
test "$1" = api
shift
if test "$1" = --allow-escape-sequences; then shift; fi
case "$1" in
  'repos/division-sh/swarm/actions/runs/123/attempts/2') cat "$FAILURE_API/run.json" ;;
  'repos/division-sh/swarm/actions/runs/123/attempts/2/jobs?per_page=100&page=1') test "$FAILURE_API_MODE" != missing-jobs; cat "$FAILURE_API/jobs.json" ;;
  'repos/division-sh/swarm/actions/runs/123/artifacts?name=ci-plan-123-2&per_page=100') cat "$FAILURE_API/list.json" ;;
  'repos/division-sh/swarm/actions/artifacts/7/zip') cat "$FAILURE_API/plan.zip" ;;
  'repos/division-sh/swarm/actions/jobs/11/logs') test "$FAILURE_API_MODE" != missing-log; cat "$FAILURE_API/log" ;;
  *) exit 1 ;;
esac
`)}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("FAILURE_API", dir)
	t.Setenv("FAILURE_API_MODE", mode)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}
