package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func artifactPlanFixture(t *testing.T) (testplanning.RunPlan, testplanning.CISelectionReport) {
	t.Helper()
	dir := t.TempDir()
	policy, weights, packages := writePlannerFixtures(t, dir)
	plan, err := readPlan(writeSyntheticPlan(t, dir, policy, weights, packages, "execution"))
	if err != nil {
		t.Fatal(err)
	}
	report, err := plan.CISelectionReport(123, 2)
	if err != nil {
		t.Fatal(err)
	}
	return plan, report
}

type planArchiveEntry struct {
	name string
	data []byte
}

func artifactPlanArchive(t *testing.T, plan testplanning.RunPlan, report testplanning.CISelectionReport, edit func([]planArchiveEntry) []planArchiveEntry) []byte {
	t.Helper()
	p, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	r, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	entries := []planArchiveEntry{{"proof-plan.json", p}, {"ci-selection.json", r}}
	if edit != nil {
		entries = edit(entries)
	}
	var data bytes.Buffer
	archive := zip.NewWriter(&data)
	for _, entry := range entries {
		file, err := archive.Create(entry.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(entry.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}

func installPlanArtifactAPI(t *testing.T, list string, archive []byte) string {
	t.Helper()
	dir := t.TempDir()
	for name, data := range map[string][]byte{
		"list.json": []byte(list), "plan.zip": archive,
		"old.json": []byte(`{"total_count":1,"artifacts":[{"id":999999,"name":"ci-plan","created_at":"2099-01-01T00:00:00Z"}]}`),
		"gh": []byte(`#!/bin/sh
set -eu
test "$1" = api
printf '%s\n' "$2" >> "$ARTIFACT_API/requests"
case "$2" in
  'repos/division-sh/swarm/actions/runs/123/artifacts?name=ci-plan-123-2&per_page=100') cat "$ARTIFACT_API/list.json" ;;
  'repos/division-sh/swarm/actions/runs/123/artifacts?per_page=100') cat "$ARTIFACT_API/old.json" ;;
  'repos/division-sh/swarm/actions/artifacts/7/zip') cat "$ARTIFACT_API/plan.zip" ;;
  *) exit 1 ;;
esac
`),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("ARTIFACT_API", dir)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return filepath.Join(dir, "requests")
}

func TestMergedRunPlanAcquiresObservedAttemptNotTimestamp(t *testing.T) {
	plan, report := artifactPlanFixture(t)
	requests := installPlanArtifactAPI(t, `{"total_count":1,"artifacts":[{"id":7,"name":"ci-plan-123-2","created_at":"2000-01-01T00:00:00Z"}]}`, artifactPlanArchive(t, plan, report, nil))
	got, err := mergedRunPlan(context.Background(), "division-sh/swarm", testplanning.QualifiedRun{ID: 123, RunAttempt: 2})
	if err != nil || got.Digest != plan.Digest {
		t.Fatalf("A18 observed-attempt plan: %v %+v", err, got)
	}
	raw, err := os.ReadFile(requests)
	if err != nil || string(raw) != "repos/division-sh/swarm/actions/runs/123/artifacts?name=ci-plan-123-2&per_page=100\nrepos/division-sh/swarm/actions/artifacts/7/zip\n" {
		t.Fatalf("not exact observed-run acquisition: %s %v", raw, err)
	}
}

func TestMergedRunPlanRefusesMissingDuplicateForeignExpiredAndIncomplete(t *testing.T) {
	plan, report := artifactPlanFixture(t)
	archive := artifactPlanArchive(t, plan, report, nil)
	for name, list := range map[string]string{
		"missing_current_old_success_retained": `{"total_count":0,"artifacts":[]}`,
		"old_name_newer_timestamp_larger_id":   `{"total_count":1,"artifacts":[{"id":999999,"name":"ci-plan","created_at":"2099-01-01T00:00:00Z"}]}`,
		"wrong_attempt":                        `{"total_count":1,"artifacts":[{"id":7,"name":"ci-plan-123-1"}]}`,
		"wrong_run":                            `{"total_count":1,"artifacts":[{"id":7,"name":"ci-plan-124-2"}]}`,
		"duplicate_current":                    `{"total_count":2,"artifacts":[{"id":7,"name":"ci-plan-123-2"},{"id":8,"name":"ci-plan-123-2"}]}`,
		"expired":                              `{"total_count":1,"artifacts":[{"id":7,"name":"ci-plan-123-2","expired":true}]}`,
		"zero_id":                              `{"total_count":1,"artifacts":[{"id":0,"name":"ci-plan-123-2"}]}`,
		"incomplete_page":                      `{"total_count":2,"artifacts":[{"id":7,"name":"ci-plan-123-2"}]}`,
		"malformed":                            `{`,
	} {
		t.Run(name, func(t *testing.T) {
			requests := installPlanArtifactAPI(t, list, archive)
			if _, err := mergedRunPlan(context.Background(), "division-sh/swarm", testplanning.QualifiedRun{ID: 123, RunAttempt: 2}); err == nil {
				t.Fatal("A19 invalid current artifact accepted")
			}
			raw, _ := os.ReadFile(requests)
			if strings.Contains(string(raw), "/zip") {
				t.Fatal("invalid listing reached archive download")
			}
		})
	}
}

func TestMergedRunPlanRefusesForeignSelectionAndMalformedArchive(t *testing.T) {
	plan, original := artifactPlanFixture(t)
	for name, edit := range map[string]func(*testplanning.CISelectionReport){
		"run":       func(r *testplanning.CISelectionReport) { r.WorkflowRunID++ },
		"attempt":   func(r *testplanning.CISelectionReport) { r.WorkflowAttempt-- },
		"source":    func(r *testplanning.CISelectionReport) { r.ExecutionSHA = "foreign" },
		"plan":      func(r *testplanning.CISelectionReport) { r.PlanDigest = "foreign" },
		"tier":      func(r *testplanning.CISelectionReport) { r.Tier = "core" },
		"selection": func(r *testplanning.CISelectionReport) { r.SelectionDigest = "foreign" },
		"check":     func(r *testplanning.CISelectionReport) { r.CheckName = "foreign" },
		"extras":    func(r *testplanning.CISelectionReport) { r.ExtraUnits = []string{"foreign"} },
		"version":   func(r *testplanning.CISelectionReport) { r.Version++ },
	} {
		t.Run(name, func(t *testing.T) {
			report := original
			edit(&report)
			installPlanArtifactAPI(t, `{"total_count":1,"artifacts":[{"id":7,"name":"ci-plan-123-2"}]}`, artifactPlanArchive(t, plan, report, nil))
			if _, err := mergedRunPlan(context.Background(), "division-sh/swarm", testplanning.QualifiedRun{ID: 123, RunAttempt: 2}); err == nil {
				t.Fatal("A20 foreign selection accepted")
			}
		})
	}
	for name, edit := range map[string]func([]planArchiveEntry) []planArchiveEntry{
		"missing_plan":     func(e []planArchiveEntry) []planArchiveEntry { return e[1:] },
		"missing_report":   func(e []planArchiveEntry) []planArchiveEntry { return e[:1] },
		"duplicate_plan":   func(e []planArchiveEntry) []planArchiveEntry { return append(e, e[0]) },
		"duplicate_report": func(e []planArchiveEntry) []planArchiveEntry { return append(e, e[1]) },
		"malformed_plan":   func(e []planArchiveEntry) []planArchiveEntry { e[0].data = []byte("{"); return e },
		"malformed_report": func(e []planArchiveEntry) []planArchiveEntry { e[1].data = []byte("{"); return e },
		"trailing_report": func(e []planArchiveEntry) []planArchiveEntry {
			e[1].data = append(e[1].data, []byte(" {}")...)
			return e
		},
		"unknown_report": func(e []planArchiveEntry) []planArchiveEntry { e[1].data = []byte(`{"unknown":true}`); return e },
		"invalid_plan_digest": func(e []planArchiveEntry) []planArchiveEntry {
			e[0].data = bytes.ReplaceAll(e[0].data, []byte(plan.Digest), []byte(strings.Repeat("a", 64)))
			return e
		},
	} {
		t.Run(name, func(t *testing.T) {
			installPlanArtifactAPI(t, `{"total_count":1,"artifacts":[{"id":7,"name":"ci-plan-123-2"}]}`, artifactPlanArchive(t, plan, original, edit))
			if _, err := mergedRunPlan(context.Background(), "division-sh/swarm", testplanning.QualifiedRun{ID: 123, RunAttempt: 2}); err == nil {
				t.Fatal("A19 malformed archive accepted")
			}
		})
	}
}

func TestMergedRunPlanRefusesUnboundObservationAndArchiveAPIFailure(t *testing.T) {
	plan, report := artifactPlanFixture(t)
	for _, tc := range []struct {
		name, repository string
		run              testplanning.QualifiedRun
	}{
		{"foreign_repository", "foreign/swarm", testplanning.QualifiedRun{ID: 123, RunAttempt: 2}},
		{"absent_run", "division-sh/swarm", testplanning.QualifiedRun{RunAttempt: 2}},
		{"absent_attempt", "division-sh/swarm", testplanning.QualifiedRun{ID: 123}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := installPlanArtifactAPI(t, `{"total_count":1,"artifacts":[{"id":7,"name":"ci-plan-123-2"}]}`, artifactPlanArchive(t, plan, report, nil))
			if _, err := mergedRunPlan(context.Background(), tc.repository, tc.run); err == nil {
				t.Fatal("unbound observation accepted")
			}
			if _, err := os.Stat(requests); !os.IsNotExist(err) {
				t.Fatal("unbound observation reached GitHub")
			}
		})
	}
	for _, endpoint := range []string{"listing", "archive"} {
		t.Run(endpoint, func(t *testing.T) {
			requests := installPlanArtifactAPI(t, `{"total_count":1,"artifacts":[{"id":7,"name":"ci-plan-123-2"}]}`, artifactPlanArchive(t, plan, report, nil))
			path := filepath.Join(filepath.Dir(requests), "gh")
			script := "#!/bin/sh\nexit 1\n"
			if endpoint == "archive" {
				script = "#!/bin/sh\ncase \"$2\" in */zip) exit 1 ;; *) cat \"$ARTIFACT_API/list.json\" ;; esac\n"
			}
			if err := os.WriteFile(path, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			if _, err := mergedRunPlan(context.Background(), "division-sh/swarm", testplanning.QualifiedRun{ID: 123, RunAttempt: 2}); err == nil {
				t.Fatal("failed GitHub read accepted")
			}
		})
	}
}

func TestMergedObservationRevalidationRefusesAttemptAndSelectionChange(t *testing.T) {
	plan, _ := artifactPlanFixture(t)
	var original testplanning.MergedPR
	original.Number, original.Merged, original.State, original.MergeCommitSHA = 7, true, "closed", strings.Repeat("a", 40)
	original.Body, original.Base.Ref = "CI-Tier: full", "master"
	original.Head.SHA, original.Head.Repo.FullName = "branch", "division-sh/swarm"
	expected := testplanning.QualifiedRun{ID: 123, RunAttempt: 2, Status: "completed", Conclusion: "success"}
	for _, cut := range []string{"unchanged", "attempt", "running", "failed", "head", "tier", "extras"} {
		t.Run(cut, func(t *testing.T) {
			pr, run := original, expected
			switch cut {
			case "attempt":
				run.RunAttempt++
			case "running":
				run.Status = "in_progress"
			case "failed":
				run.Conclusion = "failure"
			case "head":
				pr.Head.SHA = "foreign"
			case "tier":
				pr.Body = "CI-Tier: core"
			case "extras":
				pr.Body += "\nCI-Units: foreign"
			}
			dir := t.TempDir()
			if err := writeJSON(filepath.Join(dir, "pr.json"), pr); err != nil {
				t.Fatal(err)
			}
			if err := writeJSON(filepath.Join(dir, "runs.json"), map[string]any{"workflow_runs": []testplanning.QualifiedRun{run}}); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(`#!/bin/sh
set -eu
case "$2" in
  'repos/division-sh/swarm/actions/workflows/ci.yml/runs?event=pull_request&head_sha=branch&per_page=100') cat "$MERGED_REVALIDATION/runs.json" ;;
  'repos/division-sh/swarm/pulls/7') cat "$MERGED_REVALIDATION/pr.json" ;;
  *) exit 1 ;;
esac
`), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("MERGED_REVALIDATION", dir)
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			err := revalidateMergedObservation(context.Background(), config{repository: "division-sh/swarm", branchRef: "refs/heads/master", headSHA: original.MergeCommitSHA}, original, expected, plan)
			if (err == nil) != (cut == "unchanged") {
				t.Fatalf("A21 %s: %v", cut, err)
			}
		})
	}
}
