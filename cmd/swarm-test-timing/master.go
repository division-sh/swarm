package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
)

func verifyMergedProof(cfg config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	plan, run, err := observeMergedProof(ctx, cfg)
	result := struct {
		Verified   bool   `json:"verified"`
		Reason     string `json:"reason"`
		RunID      int64  `json:"run_id,omitempty"`
		Attempt    int    `json:"attempt,omitempty"`
		PlanDigest string `json:"plan_digest,omitempty"`
	}{Reason: "exact merged tree and current successful PR evidence", RunID: run.ID, Attempt: run.RunAttempt}
	if err != nil {
		result.Reason = err.Error()
	} else {
		result.Verified, result.PlanDigest = true, plan.Digest
		if err := writeJSON(cfg.planPath, plan); err != nil {
			return err
		}
		if err := os.WriteFile(cfg.matrixPath, []byte("{\"include\":[]}\n"), 0600); err != nil {
			return err
		}
	}
	if err := writeJSON(cfg.resultJSONPath, result); err != nil {
		return err
	}
	if result.Verified {
		fmt.Println("verified")
	} else {
		fmt.Fprintf(os.Stderr, "master replay refused: %s; full qualification required\n", result.Reason)
		fmt.Println("full")
	}
	return nil
}

func observeMergedProof(ctx context.Context, cfg config) (testplanning.RunPlan, testplanning.QualifiedRun, error) {
	var plan testplanning.RunPlan
	var run testplanning.QualifiedRun
	if cfg.repository != "division-sh/swarm" || cfg.branchRef != "refs/heads/master" {
		return plan, run, fmt.Errorf("not the trusted master push")
	}
	event, err := os.ReadFile(cfg.eventPath)
	if err != nil {
		return plan, run, err
	}
	if err := testplanning.CheckMasterPush(event, cfg.repository, cfg.branchRef, cfg.headSHA); err != nil {
		return plan, run, err
	}
	plan, run, err = observeMergedQualification(ctx, cfg)
	if err == nil {
		err = validateCurrentPlan(plan, cfg)
	}
	return plan, run, err
}

func observeMergedQualification(ctx context.Context, cfg config) (testplanning.RunPlan, testplanning.QualifiedRun, error) {
	var plan testplanning.RunPlan
	var run testplanning.QualifiedRun
	if cfg.repository != "division-sh/swarm" || cfg.branchRef != "refs/heads/master" {
		return plan, run, fmt.Errorf("not the trusted master qualification")
	}
	if decoded, err := hex.DecodeString(cfg.headSHA); err != nil || len(decoded) != 20 {
		return plan, run, fmt.Errorf("invalid master source SHA")
	}
	api := func(path string, value any) error {
		raw, err := ghRead(ctx, path)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, value)
	}
	root := "repos/" + cfg.repository
	var associated []testplanning.MergedPR
	if err := api(root+"/commits/"+cfg.headSHA+"/pulls", &associated); err != nil {
		return plan, run, err
	}
	if len(associated) != 1 {
		return plan, run, fmt.Errorf("ambiguous or absent merged PR association")
	}
	var pr testplanning.MergedPR
	if err := api(root+"/pulls/"+strconv.Itoa(associated[0].Number), &pr); err != nil {
		return plan, run, err
	}
	if _, err := testplanning.MergedAssociation(cfg.repository, cfg.branchRef, cfg.headSHA, []testplanning.MergedPR{pr}); err != nil {
		return plan, run, err
	}
	latest, err := latestPRRun(ctx, root, pr.Head.SHA)
	if err != nil {
		return plan, run, err
	}
	runPath := root + "/actions/runs/" + strconv.FormatInt(latest.ID, 10)
	var details struct {
		testplanning.QualifiedRun
		Started time.Time `json:"run_started_at"`
	}
	if err := api(runPath, &details); err != nil {
		return plan, run, err
	}
	run = details.QualifiedRun
	if run.Status != "completed" || run.Conclusion != "success" || details.Started.IsZero() {
		return plan, run, fmt.Errorf("latest PR run is not successful")
	}
	var jobs struct {
		Total int                     `json:"total_count"`
		Jobs  []testplanning.MergeJob `json:"jobs"`
	}
	if err := api(runPath+"/attempts/"+strconv.Itoa(run.RunAttempt)+"/jobs?per_page=100", &jobs); err != nil {
		return plan, run, err
	}
	if jobs.Total != len(jobs.Jobs) {
		return plan, run, fmt.Errorf("incomplete attempt-specific jobs page")
	}
	run.Jobs = jobs.Jobs
	checks, err := mergedRequiredChecks(ctx, cfg.repository, run.Jobs)
	if err != nil {
		return plan, run, err
	}
	var branch struct {
		Protection struct {
			Required struct {
				Checks []testplanning.ProtectedCheck `json:"checks"`
			} `json:"required_status_checks"`
		} `json:"protection"`
	}
	if err := api(root+"/branches/master", &branch); err != nil {
		return plan, run, err
	}
	plan, err = mergedRunPlan(ctx, runPath, details.Started)
	if err != nil {
		return plan, run, err
	}
	masterTree, err := gitTree(ctx, cfg.headSHA)
	if err != nil {
		return plan, run, err
	}
	planTree, err := gitTree(ctx, plan.HeadSHA)
	if err != nil {
		return plan, run, err
	}
	if err := testplanning.ValidateMasterReplay(cfg.repository, cfg.branchRef, cfg.headSHA, masterTree, planTree, []testplanning.MergedPR{pr}, run, checks, branch.Protection.Required.Checks, plan); err != nil {
		return plan, run, err
	}
	if err := revalidateMergedObservation(ctx, cfg, pr, run, plan); err != nil {
		return plan, run, err
	}
	return plan, run, nil
}

func latestPRRun(ctx context.Context, root, sha string) (testplanning.QualifiedRun, error) {
	raw, err := ghRead(ctx, root+"/actions/workflows/ci.yml/runs?event=pull_request&head_sha="+sha+"&per_page=100")
	if err != nil {
		return testplanning.QualifiedRun{}, err
	}
	var runs struct {
		WorkflowRuns []testplanning.QualifiedRun `json:"workflow_runs"`
	}
	if err := json.Unmarshal(raw, &runs); err != nil {
		return testplanning.QualifiedRun{}, err
	}
	if len(runs.WorkflowRuns) == 0 {
		return testplanning.QualifiedRun{}, fmt.Errorf("no PR qualification")
	}
	sort.Slice(runs.WorkflowRuns, func(i, j int) bool { return runs.WorkflowRuns[i].ID > runs.WorkflowRuns[j].ID })
	return runs.WorkflowRuns[0], nil
}

func revalidateMergedObservation(ctx context.Context, cfg config, pr testplanning.MergedPR, run testplanning.QualifiedRun, plan testplanning.RunPlan) error {
	root := "repos/" + cfg.repository
	current, err := latestPRRun(ctx, root, pr.Head.SHA)
	if err != nil {
		return err
	}
	if current.ID != run.ID || current.RunAttempt != run.RunAttempt || current.Status != "completed" || current.Conclusion != "success" {
		return fmt.Errorf("qualification changed during observation")
	}
	raw, err := ghRead(ctx, root+"/pulls/"+strconv.Itoa(pr.Number))
	if err != nil {
		return err
	}
	var currentPR testplanning.MergedPR
	if err := json.Unmarshal(raw, &currentPR); err != nil {
		return err
	}
	if _, err := testplanning.MergedAssociation(cfg.repository, cfg.branchRef, cfg.headSHA, []testplanning.MergedPR{currentPR}); err != nil {
		return err
	}
	if currentPR.Head.SHA != pr.Head.SHA {
		return fmt.Errorf("merged PR source changed during observation")
	}
	return testplanning.CheckCurrentCISelection(plan, currentPR.Body)
}

func mergedRequiredChecks(ctx context.Context, repository string, jobs []testplanning.MergeJob) ([]testplanning.MergeCheck, error) {
	var checks []testplanning.MergeCheck
	for _, job := range jobs {
		if job.Name != "Required test summary" && job.Name != "SQLite local smoke" && !strings.HasPrefix(job.Name, "CI tier: ") {
			continue
		}
		prefix := "https://api.github.com/repos/" + repository + "/check-runs/"
		if !strings.HasPrefix(job.CheckRunURL, prefix) {
			return nil, fmt.Errorf("foreign required-check API URL")
		}
		id := strings.TrimPrefix(job.CheckRunURL, prefix)
		if _, err := strconv.ParseInt(id, 10, 64); err != nil {
			return nil, err
		}
		raw, err := ghRead(ctx, "repos/"+repository+"/check-runs/"+id)
		if err != nil {
			return nil, err
		}
		var check testplanning.MergeCheck
		if err := json.Unmarshal(raw, &check); err != nil {
			return nil, err
		}
		checks = append(checks, check)
	}
	return checks, nil
}

func mergedRunPlan(ctx context.Context, runPath string, started time.Time) (testplanning.RunPlan, error) {
	var plan testplanning.RunPlan
	raw, err := ghRead(ctx, runPath+"/artifacts?per_page=100")
	if err != nil {
		return plan, err
	}
	var list struct {
		Total     int `json:"total_count"`
		Artifacts []struct {
			ID      int64     `json:"id"`
			Name    string    `json:"name"`
			Expired bool      `json:"expired"`
			Created time.Time `json:"created_at"`
		} `json:"artifacts"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return plan, err
	}
	if list.Total != len(list.Artifacts) {
		return plan, fmt.Errorf("incomplete artifacts page")
	}
	var id int64
	for _, artifact := range list.Artifacts {
		if artifact.Name != "ci-plan" || artifact.Created.Before(started) {
			continue
		}
		if id != 0 || artifact.Expired {
			return plan, fmt.Errorf("ambiguous or expired plan artifact")
		}
		id = artifact.ID
	}
	if id <= 0 {
		return plan, fmt.Errorf("no current-attempt plan artifact")
	}
	parts := strings.Split(runPath, "/")
	raw, err = ghRead(ctx, strings.Join(parts[:3], "/")+"/actions/artifacts/"+strconv.FormatInt(id, 10)+"/zip")
	if err != nil {
		return plan, err
	}
	archive, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return plan, err
	}
	count := 0
	for _, entry := range archive.File {
		if entry.Name != "proof-plan.json" {
			continue
		}
		count++
		reader, err := entry.Open()
		if err != nil {
			return plan, err
		}
		data, err := io.ReadAll(io.LimitReader(reader, 32<<20))
		_ = reader.Close()
		if err != nil {
			return plan, err
		}
		if err := json.Unmarshal(data, &plan); err != nil {
			return plan, err
		}
	}
	if count != 1 {
		return plan, fmt.Errorf("missing/duplicate plan in artifact")
	}
	return plan, plan.Validate()
}

func validateCurrentPlan(plan testplanning.RunPlan, cfg config) error {
	policy, err := readProofPolicy(cfg.proofPolicyPath)
	if err != nil {
		return err
	}
	model, err := readWeightModel(cfg.weightModelPath)
	if err != nil {
		return err
	}
	inventory, err := testplanning.DiscoverRootInventory(context.Background(), ".")
	if err != nil {
		return err
	}
	var packages []string
	for name := range inventory.Packages {
		packages = append(packages, name)
	}
	current, err := testplanning.BuildPlan(policy, model, packages, plan.Profile, plan.Reason, plan.HeadSHA, testplanning.BuildOptions{ExtraUnits: plan.ExtraUnits})
	if err != nil {
		return err
	}
	proofs, err := testplanning.LoadParityProofs("internal/apiv1/testdata/public_surface_backend_matrix.yaml")
	if err != nil {
		return err
	}
	if err := testplanning.BindExecution(&current, inventory, proofs, policy); err != nil {
		return err
	}
	if plan.Digest != current.Digest {
		return fmt.Errorf("qualified plan differs from current policy/root census/build context")
	}
	return nil
}

func ghRead(ctx context.Context, path string) ([]byte, error) {
	return exec.CommandContext(ctx, "gh", "api", path).Output()
}

func gitTree(ctx context.Context, sha string) (string, error) {
	if decoded, err := hex.DecodeString(sha); err != nil || len(decoded) != 20 {
		return "", fmt.Errorf("invalid execution commit")
	}
	command := func(arguments ...string) ([]byte, error) {
		return exec.CommandContext(ctx, "git", arguments...).Output()
	}
	if _, err := command("cat-file", "-e", sha+"^{commit}"); err != nil {
		if _, err := command("fetch", "origin", sha); err != nil {
			return "", err
		}
	}
	raw, err := command("rev-parse", sha+"^{tree}")
	return strings.TrimSpace(string(raw)), err
}
