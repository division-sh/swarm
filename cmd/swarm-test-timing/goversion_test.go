package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestMasterReplayCompilerVersionRefusalUsesNativeReader(t *testing.T) {
	dir := t.TempDir()
	policy, weights, packages := productionPlannerInputs(t, dir)
	path := filepath.Join(dir, "plan.json")
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	cfg := config{proofPolicyPath: policy, weightModelPath: weights, packagesPath: packages, planPath: path,
		matrixPath: filepath.Join(dir, "matrix.json"), markdownPath: filepath.Join(dir, "plan.md"), event: "push", headSHA: strings.TrimSpace(string(head))}
	if err := planCI(cfg); err != nil {
		t.Fatal(err)
	}
	plan, err := readPlan(path)
	if err != nil || validateCurrentPlan(plan, cfg) != nil {
		t.Fatal("native current plan refused equal compiler", err)
	}
	for _, cut := range []string{"equal", "missing", "foreign"} {
		t.Run(cut, func(t *testing.T) {
			candidate := plan
			if cut == "missing" {
				candidate.BuildContext.GOVERSION = ""
			} else if cut == "foreign" {
				candidate.BuildContext.GOVERSION = plan.BuildContext.GOVERSION + "-foreign"
			}
			candidate.Digest = ""
			raw, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			candidate.Digest = hex.EncodeToString(digest[:])
			installCompilerReplayAPI(t, candidate, plan)
			cfg.repository, cfg.branchRef = "division-sh/swarm", "refs/heads/master"
			cfg.eventPath, cfg.resultJSONPath = filepath.Join(t.TempDir(), "event.json"), filepath.Join(t.TempDir(), "result.json")
			if err := writeJSON(cfg.eventPath, map[string]any{"ref": cfg.branchRef, "before": strings.Repeat("b", 40), "after": cfg.headSHA, "forced": false, "repository": map[string]string{"full_name": cfg.repository}}); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("untouched"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := verifyMergedProof(cfg); err != nil {
				t.Fatal(err)
			}
			var result struct {
				Verified   bool
				Reason     string
				RunID      int64  `json:"run_id"`
				Attempt    int    `json:"attempt"`
				PlanDigest string `json:"plan_digest"`
			}
			if err := readJSON(cfg.resultJSONPath, &result); err != nil {
				t.Fatal(err)
			}
			if cut == "equal" {
				loaded, err := readPlan(path)
				if !result.Verified || err != nil || loaded.BuildContext.GOVERSION != plan.BuildContext.GOVERSION {
					t.Fatalf("equal compiler replay refused: %+v %v", result, err)
				}
				return
			}
			if result.Verified || !strings.Contains(result.Reason, "build context") {
				t.Fatalf("native replay did not require full for %s compiler: %+v", cut, result)
			}
			if raw, err := os.ReadFile(path); err != nil || string(raw) != "untouched" {
				t.Fatal("refused replay replaced the executable plan", err)
			}
		})
	}
}

func installCompilerReplayAPI(t *testing.T, plan, admitted testplanning.RunPlan) {
	t.Helper()
	const root = "repos/division-sh/swarm"
	const runPath = root + "/actions/runs/123"
	dir := t.TempDir()
	var pr testplanning.MergedPR
	pr.Number, pr.Merged, pr.State, pr.MergeCommitSHA, pr.Body = 7, true, "closed", plan.HeadSHA, "CI-Tier: full"
	pr.Base.Ref, pr.Head.SHA, pr.Head.Repo.FullName = "master", plan.HeadSHA, "division-sh/swarm"
	run := testplanning.QualifiedRun{ID: 123, RunAttempt: 2, HeadSHA: plan.HeadSHA, Event: "pull_request", Name: "CI", Status: "completed", Conclusion: "success"}
	started := time.Date(2026, 10, 11, 1, 0, 0, 0, time.UTC)
	selection, err := admitted.CISelectionReport(run.ID, run.RunAttempt)
	if err != nil {
		t.Fatal(err)
	}
	if plan.BuildContext.GOVERSION != "" {
		selection, err = plan.CISelectionReport(run.ID, run.RunAttempt)
		if err != nil {
			t.Fatal(err)
		}
	}
	responses := map[string]any{
		root + "/commits/" + plan.HeadSHA + "/pulls": []testplanning.MergedPR{pr},
		root + "/pulls/7": pr,
		root + "/actions/workflows/ci.yml/runs?event=pull_request&head_sha=" + plan.HeadSHA + "&per_page=100": map[string]any{"workflow_runs": []testplanning.QualifiedRun{run}},
		runPath: struct {
			testplanning.QualifiedRun
			Started time.Time `json:"run_started_at"`
		}{run, started},
		runPath + "/artifacts?name=ci-plan-123-2&per_page=100": map[string]any{"total_count": 1, "artifacts": []map[string]any{{"id": 5, "name": "ci-plan-123-2", "expired": false, "created_at": started}}},
	}
	var jobs []testplanning.MergeJob
	var protection []testplanning.ProtectedCheck
	for i, name := range []string{"Required test summary", "SQLite local smoke", selection.CheckName} {
		id := int64(40 + i)
		apiPath := root + "/check-runs/" + strconv.FormatInt(id, 10)
		jobs = append(jobs, testplanning.MergeJob{ID: id, RunID: run.ID, RunAttempt: run.RunAttempt, HeadSHA: run.HeadSHA, Name: name, Status: "completed", Conclusion: "success", CheckRunURL: "https://api.github.com/" + apiPath})
		check := testplanning.MergeCheck{Name: name, HeadSHA: run.HeadSHA, Status: "completed", Conclusion: "success", DetailsURL: fmt.Sprintf("https://github.com/division-sh/swarm/actions/runs/123/job/%d", id)}
		check.App.ID, check.CheckSuite.ID = 15368, 70
		responses[apiPath] = check
		if i < 2 {
			protection = append(protection, testplanning.ProtectedCheck{Context: name, AppID: 15368})
		}
	}
	responses[runPath+"/attempts/2/jobs?per_page=100"] = map[string]any{"total_count": len(jobs), "jobs": jobs}
	responses[root+"/branches/master"] = map[string]any{"protection": map[string]any{"required_status_checks": map[string]any{"checks": protection}}}
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	for name, value := range map[string]any{"proof-plan.json": plan, "ci-selection.json": selection} {
		entry, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(entry).Encode(value); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	zipPath := filepath.Join(dir, "plan.zip")
	if err := os.WriteFile(zipPath, archive.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nset -eu\ncase \"$2\" in\n"
	for route, value := range responses {
		path := filepath.Join(dir, fmt.Sprintf("response-%d.json", len(script)))
		if err := writeJSON(path, value); err != nil {
			t.Fatal(err)
		}
		script += fmt.Sprintf("%q) cat %q;;\n", route, path)
	}
	script += fmt.Sprintf("%q) cat %q;;\n*) exit 1;;\nesac\n", root+"/actions/artifacts/5/zip", zipPath)
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestEvidenceProducerRefusesForeignCompilerBeforePublication(t *testing.T) {
	dir := t.TempDir()
	policy, weights, packages := writePlannerFixtures(t, dir)
	path := writeSyntheticPlan(t, dir, policy, weights, packages, "head")
	plan, err := readPlan(path)
	if err != nil {
		t.Fatal(err)
	}
	plan.BuildContext.GOVERSION += "-foreign"
	plan.Digest = ""
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	plan.Digest = hex.EncodeToString(digest[:])
	if err := writeJSON(path, plan); err != nil {
		t.Fatal(err)
	}
	err = recordEvidence(config{planPath: path, unitID: plan.Units[0].ID, evidencePath: filepath.Join(dir, "refused.json"),
		inputPath: filepath.Join(dir, "must-not-read.json"), workflowRunID: 1, workflowAttempt: 1})
	if err == nil || !strings.Contains(err.Error(), "build context differs") {
		t.Fatalf("foreign compiler reached input/receipt publication: %v", err)
	}
	if plan.BuildContext == (testplanning.BuildContext{}) {
		t.Fatal("counterexample lost its otherwise valid execution context")
	}
}
