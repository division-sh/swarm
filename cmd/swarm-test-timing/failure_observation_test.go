package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
	checkFailureRejectedPlan(t)
	checkFailureAggregates(t)
	checkFailureParentEvidence(t)
	checkFailureJobPlatforms(t)
	checkFailureAPIAttemptCount(t)
}

func checkFailureRejectedPlan(t *testing.T) {
	t.Run("rejected-plan-cannot-match-reviewed-observation", func(t *testing.T) {
		run := failureObservationRun()
		installFailureObservationAPI(t, run, "valid")
		dir := os.Getenv("FAILURE_API")
		raw, err := os.ReadFile(filepath.Join(dir, "plan.zip"))
		if err != nil {
			t.Fatal(err)
		}
		plan, report, err := mergedPlanArchive(raw)
		if err != nil {
			t.Fatal(err)
		}
		report.WorkflowAttempt = 1
		writeFailureFixture(t, "plan.zip", artifactPlanArchive(t, plan, report, nil))
		got := observeFailureAttempt(context.Background(), run, 2)
		if len(got.Problems) == 0 || len(got.Failures) != 1 {
			t.Fatal(got)
		}
		f := got.Failures[0]
		if f.Source != "" || f.GOOS != "" || f.Problem == "" {
			t.Fatalf("rejected authority survived: %+v", got)
		}
		ledger := failureObservationLedger(f)
		f.Source, f.GOOS, f.Problem = strings.Repeat("b", 40), "linux", ""
		matched, err := ledger.Match(f)
		if err != nil || matched.Attribution != "non-PR-proven" {
			t.Fatalf("control did not match: %+v / %v", matched, err)
		}
		matched, err = ledger.Match(got.Failures[0])
		if err != nil || matched.Record != "" || matched.Attribution != "unknown" {
			t.Fatalf("rejected plan matched: %+v / %v", matched, err)
		}
		summary, err := ledger.Summarize([]testtiming.FailureRun{{ID: run.ID, RunAttempt: 2, Event: run.Event, HeadSHA: run.HeadSHA, Status: "completed", Conclusion: "failure", Attempts: []testtiming.FailureAttempt{{Number: 1, Conclusion: "success"}, got}}})
		if err != nil || summary.Incomplete != 1 || summary.NonPROnly != 0 || summary.UnknownOnly != 1 {
			t.Fatalf("rejected summary: %+v / %v", summary, err)
		}
	})
}

func checkFailureAggregates(t *testing.T) {
	for _, mode := range []string{"independent", "propagation", "unavailable"} {
		t.Run("aggregate-"+mode, func(t *testing.T) {
			run := failureObservationRun()
			installFailureObservationAPI(t, run, "valid")
			jobs := []testtiming.ActionJob{
				{ID: 11, RunID: run.ID, RunAttempt: 2, HeadSHA: run.HeadSHA, Name: failureFixtureJobName(t), Conclusion: "failure", StartedAt: time.Now()},
				{ID: 12, RunID: run.ID, RunAttempt: 2, HeadSHA: run.HeadSHA, Name: "CI timing budget", Conclusion: "failure", StartedAt: time.Now()},
				{ID: 13, RunID: run.ID, RunAttempt: 2, HeadSHA: run.HeadSHA, Name: "Required test summary", Conclusion: "failure", StartedAt: time.Now()},
			}
			writeFailureJobs(t, jobs)
			if mode != "unavailable" {
				detail := "independent policy contract failure"
				if mode == "propagation" {
					detail = "Go proof owned failed; required dependency is not success"
				}
				writeFailureFixture(t, "log-12", []byte(detail))
				writeFailureFixture(t, "log-13", []byte(detail))
			}
			got := observeFailureAttempt(context.Background(), run, 2)
			if len(got.Failures) != 3 {
				t.Fatalf("failed jobs disappeared: %+v", got)
			}
			for _, f := range got.Failures[1:] {
				if f.JobID != 12 && f.JobID != 13 {
					t.Fatal(f)
				}
				if mode == "unavailable" && f.Problem == "" {
					t.Fatalf("missing aggregate log trusted: %+v", f)
				}
				if mode != "unavailable" && f.Detail == "" {
					t.Fatalf("aggregate provenance lost: %+v", f)
				}
			}
			ledger := failureObservationLedger(got.Failures[0])
			summary, err := ledger.Summarize([]testtiming.FailureRun{{ID: run.ID, RunAttempt: 2, Event: run.Event, HeadSHA: run.HeadSHA, Status: "completed", Conclusion: "failure", Attempts: []testtiming.FailureAttempt{{Number: 1, Conclusion: "success"}, got}}})
			if err != nil || summary.Mixed != 1 || summary.NonPROnly != 0 || summary.ByPhenomenon["unknown-variance"].UnknownObservations != 2 {
				t.Fatalf("aggregate evidence falsely exonerated run: %+v / %v", summary, err)
			}
		})
	}
}

func checkFailureParentEvidence(t *testing.T) {
	for _, mode := range []string{"cleanup", "package-build", "package-panic"} {
		t.Run(mode, func(t *testing.T) {
			parent, detail := "TestParent", "parent_test.go:99: independent cleanup corruption"
			if mode != "cleanup" {
				parent, detail = "", "build or panic evidence"
			}
			raw := []byte(fmt.Sprintf("{\"Action\":\"fail\",\"Package\":\"p\",\"Test\":\"TestParent/child\"}\n{\"Action\":\"output\",\"Package\":\"p\",\"Test\":%q,\"Output\":%q}\n{\"Action\":\"fail\",\"Package\":\"p\",\"Test\":%q}\n", parent, detail, parent))
			got, err := nativeFailureEvents(raw, testtiming.ObservedFailure{})
			if err != nil || len(got) != 2 {
				t.Fatalf("independent root evidence lost: %+v / %v", got, err)
			}
			found := false
			for _, f := range got {
				found = found || f.Test == parent && strings.Contains(f.Detail, detail)
			}
			if !found {
				t.Fatalf("independent assertion lost: %+v", got)
			}
		})
	}
}

func failureObservationRun() failureRunMetadata {
	return failureRunMetadata{QualifiedRun: testplanning.QualifiedRun{ID: 123, RunAttempt: 2, HeadSHA: strings.Repeat("a", 40), Status: "completed", Conclusion: "failure"}, Event: "pull_request"}
}

func failureObservationLedger(f testtiming.ObservedFailure) testtiming.FailureLedger {
	url := "https://github.com/division-sh/swarm/issues/2353#issuecomment-1"
	return testtiming.FailureLedger{Version: 1, Sources: []testtiming.FailureSource{{ID: "source", URL: url, SHA256: strings.Repeat("c", 64)}}, Records: []testtiming.FailureRecord{{ID: "exact", Job: f.Job, Package: f.Package, Test: f.Test, Signature: "precise assertion", GOOS: "linux", Phenomenon: "unknown-variance", Attribution: "unknown", Status: "open", Repair: "unclassified", FirstSeen: "2026-10-13", LastSeen: "2026-10-13", Tracking: url, Review: url, Sources: []string{"source"}, Observations: []testtiming.FailureObservation{{RunID: f.RunID, Attempt: f.Attempt, JobID: f.JobID, Source: strings.Repeat("b", 40), Attribution: "non-PR-proven", Review: url}}}}}
}

func writeFailureFixture(t *testing.T, name string, raw []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(os.Getenv("FAILURE_API"), name), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func writeFailureJobs(t *testing.T, jobs []testtiming.ActionJob) {
	t.Helper()
	raw, err := json.Marshal(struct {
		Total int                    `json:"total_count"`
		Jobs  []testtiming.ActionJob `json:"jobs"`
	}{len(jobs), jobs})
	if err != nil {
		t.Fatal(err)
	}
	writeFailureFixture(t, "jobs.json", raw)
}

func failureFixtureJobName(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(os.Getenv("FAILURE_API"), "plan.zip"))
	if err != nil {
		t.Fatal(err)
	}
	plan, _, err := mergedPlanArchive(raw)
	if err != nil {
		t.Fatal(err)
	}
	return "Go proof " + plan.Batches[0].ID
}

func failureProofArchive(t *testing.T, plan testplanning.RunPlan) []byte {
	t.Helper()
	var entries []planArchiveEntry
	for _, id := range plan.Batches[0].Units {
		unit, err := plan.Unit(id)
		if err != nil {
			t.Fatal(err)
		}
		e := testtiming.CommandEvidence{Version: testtiming.CommandEvidenceVersion, WorkflowRunID: 123, WorkflowAttempt: 2, PlanDigest: plan.Digest, Profile: plan.Profile, WorkloadProfile: unit.WorkloadProfile, ExecutionTier: unit.ExecutionTier, BuildContext: plan.BuildContext, HeadSHA: plan.HeadSHA, UnitID: id, Surface: id, Attempt: testtiming.AttemptPrimary, ExitCode: 1, Packages: unit.Packages, EnvironmentID: unit.EnvironmentID, CountMode: unit.CountMode, Run: unit.Run, Skip: unit.Skip, GoTimeout: unit.GoTimeout}
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, planArchiveEntry{id + "-primary-evidence.json", raw})
	}
	return artifactPlanArchive(t, plan, testplanning.CISelectionReport{}, func([]planArchiveEntry) []planArchiveEntry { return entries })
}

func checkFailureJobPlatforms(t *testing.T) {
	for _, mode := range []string{"linux", "darwin", "contradictory", "unavailable", "corrupt-result", "foreign-source"} {
		t.Run("job-platform-"+mode, func(t *testing.T) {
			run := failureObservationRun()
			installFailureObservationAPI(t, run, "valid")
			platform, title := "darwin", "Darwin"
			if mode == "linux" {
				platform, title = "linux", "Linux"
			}
			writeFailureJobs(t, []testtiming.ActionJob{{ID: 11, RunID: run.ID, RunAttempt: 2, HeadSHA: run.HeadSHA, Name: "Native " + title + " unused analysis", Conclusion: "failure", StartedAt: time.Now()}})
			if mode != "unavailable" {
				result := []byte("native diagnostic bytes")
				sum := sha256.Sum256(result)
				e := testtiming.UnusedEvidence{Schema: 1, State: "complete", SourceSHA: strings.Repeat("b", 40), Config: testtiming.UnusedConfiguration{Toolchain: "go1.26.8"}, Native: testtiming.UnusedNative{GOOS: platform, GOHOSTOS: platform, GOARCH: "amd64", GOHOSTARCH: "amd64", CGO_ENABLED: "1", GOVERSION: "go1.26.8"}, ResultSHA256: fmt.Sprintf("%x", sum), ResultBytes: int64(len(result))}
				if mode == "contradictory" {
					e.Native.GOOS = "linux"
				}
				if mode == "foreign-source" {
					e.SourceSHA = strings.Repeat("c", 40)
				}
				if mode == "corrupt-result" {
					result[0] = 'X'
				}
				metadata, err := json.Marshal(e)
				if err != nil {
					t.Fatal(err)
				}
				writeFailureFixture(t, "native-list.json", []byte(fmt.Sprintf(`{"total_count":1,"artifacts":[{"id":9,"name":"unused-%s-123-2"}]}`, platform)))
				archive := artifactPlanArchive(t, testplanning.RunPlan{}, testplanning.CISelectionReport{}, func([]planArchiveEntry) []planArchiveEntry {
					return []planArchiveEntry{{"metadata.json", metadata}, {"staticcheck.bin", result}}
				})
				writeFailureFixture(t, "native.zip", archive)
			}
			got := observeFailureAttempt(context.Background(), run, 2)
			if len(got.Failures) != 1 {
				t.Fatal(got)
			}
			f := got.Failures[0]
			if mode == "linux" || mode == "darwin" {
				if f.GOOS != platform || f.Source != strings.Repeat("b", 40) || f.Problem != "" {
					t.Fatalf("job-native authority not read: %+v", f)
				}
			} else if f.GOOS != "" || f.Source != "" || f.Problem == "" {
				t.Fatalf("job label or planner invented authority: %+v", f)
			}
			if mode == "foreign-source" && !strings.Contains(f.Problem, "stale evidence") {
				t.Fatalf("lost stale-source readback classification: %+v", f)
			}
		})
	}
}

func checkFailureAPIAttemptCount(t *testing.T) {
	t.Run("api-and-offline-retain-declared-attempt-count", func(t *testing.T) {
		run := failureObservationRun()
		installFailureObservationAPI(t, run, "valid")
		first := run
		first.RunAttempt = 1
		first.Conclusion = "success"
		raw, err := json.Marshal(first)
		if err != nil {
			t.Fatal(err)
		}
		writeFailureFixture(t, "run-first.json", raw)
		runs, err := readFailurePopulation(context.Background(), failureOptions{run: run.ID})
		if err != nil || len(runs) != 1 || runs[0].RunAttempt != 2 || len(runs[0].Attempts) != 2 {
			t.Fatalf("API history: %+v / %v", runs, err)
		}
		raw, err = json.Marshal(runs)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "population.json")
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		offline, err := readFailurePopulation(context.Background(), failureOptions{population: path})
		if err != nil || !reflect.DeepEqual(runs, offline) {
			t.Fatalf("offline history lost declared count: %+v / %v", offline, err)
		}
	})
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
	job := testtiming.ActionJob{ID: 11, RunID: 123, RunAttempt: 2, HeadSHA: run.HeadSHA, Name: "Go proof " + plan.Batches[0].ID, Conclusion: "failure", StartedAt: time.Now()}
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
	proofName := fmt.Sprintf("proof-123-2-%s-timing", plan.Batches[0].ID)
	proofList := fmt.Sprintf(`{"total_count":1,"artifacts":[{"id":8,"name":%q}]}`, proofName)
	files := map[string][]byte{"run.json": rawRun, "jobs.json": rawJobs, "list.json": []byte(list), "plan.zip": archive,
		"proof-list.json": []byte(proofList), "proof.zip": failureProofArchive(t, plan),
		"log": []byte("2026-10-13T00:00:00Z {\"Action\":\"output\",\"Package\":\"proof\",\"Test\":\"TestReal/sqlite\",\"Output\":\"precise assertion\"}\n2026-10-13T00:00:00Z {\"Action\":\"fail\",\"Package\":\"proof\",\"Test\":\"TestReal/sqlite\"}\n"),
		"gh": []byte(`#!/bin/sh
set -eu
test "$1" = api
shift
if test "$1" = --allow-escape-sequences; then shift; fi
case "$1" in
  'repos/division-sh/swarm/actions/runs/123/attempts/2') cat "$FAILURE_API/run.json" ;;
  'repos/division-sh/swarm/actions/runs/123/attempts/1') cat "$FAILURE_API/run-first.json" ;;
  'repos/division-sh/swarm/actions/runs/123') cat "$FAILURE_API/run.json" ;;
  'repos/division-sh/swarm/actions/runs/123/attempts/2/jobs?per_page=100&page=1') test "$FAILURE_API_MODE" != missing-jobs; cat "$FAILURE_API/jobs.json" ;;
  'repos/division-sh/swarm/actions/runs/123/artifacts?name=ci-plan-123-2&per_page=100') cat "$FAILURE_API/list.json" ;;
  'repos/division-sh/swarm/actions/artifacts/7/zip') cat "$FAILURE_API/plan.zip" ;;
  'repos/division-sh/swarm/actions/runs/123/artifacts?name=proof-'*) cat "$FAILURE_API/proof-list.json" ;;
  'repos/division-sh/swarm/actions/artifacts/8/zip') cat "$FAILURE_API/proof.zip" ;;
  'repos/division-sh/swarm/actions/runs/123/artifacts?name=unused-'*) cat "$FAILURE_API/native-list.json" ;;
  'repos/division-sh/swarm/actions/artifacts/9/zip') cat "$FAILURE_API/native.zip" ;;
  'repos/division-sh/swarm/actions/jobs/11/logs') test "$FAILURE_API_MODE" != missing-log; cat "$FAILURE_API/log" ;;
  'repos/division-sh/swarm/actions/jobs/12/logs') cat "$FAILURE_API/log-12" ;;
  'repos/division-sh/swarm/actions/jobs/13/logs') cat "$FAILURE_API/log-13" ;;
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
