package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testtiming"
	"gopkg.in/yaml.v3"
)

func TestEvaluateBudgetPRRetainedDriftIsAdvisory(t *testing.T) {
	for _, profile := range []string{"core", "lifecycle", "full"} {
		t.Run(profile, func(t *testing.T) {
			cfg := ratchetCLIConfig(t, profile, 2000, false)
			cfg.event = "pull_request"
			if err := run(cfg); err != nil {
				t.Fatalf("complete retained-only PR drift blocked: %v", err)
			}
			result := readRatchetCLIResult(t, cfg)
			if result.Status != testtiming.BudgetWarn || result.TestTime == nil || result.TestTime.Status != testtiming.BudgetWarn || result.ExitCode() != 0 {
				t.Fatalf("advisory result lost: %+v", result)
			}
		})
	}
}

func TestEvaluateBudgetCadenceRejectsForgedTier(t *testing.T) {
	for _, event := range []string{"schedule", "workflow_dispatch", "push", "", "unknown"} {
		for _, profile := range []string{"core", "lifecycle"} {
			t.Run(event+"/"+profile, func(t *testing.T) {
				cfg := ratchetCLIConfig(t, profile, 0, false)
				cfg.event = event
				if err := run(cfg); err == nil || !strings.Contains(err.Error(), "INCOMPLETE") {
					t.Fatalf("non-full cadence plan admitted: %v", err)
				}
				result := readRatchetCLIResult(t, cfg)
				if result.Status != testtiming.BudgetIncomplete || result.TestTime == nil || !strings.Contains(strings.Join(result.TestTime.Problems, " "), "full") {
					t.Fatalf("cadence refusal missing: %+v", result)
				}
			})
		}
	}
}

func TestEvaluateBudgetIncompleteDominatesRatchetFailure(t *testing.T) {
	cfg := ratchetCLIConfig(t, "core", 2000, true)
	cfg.event = "pull_request"
	if err := os.WriteFile(filepath.Join(cfg.evidenceRoot, "broken-evidence.json"), []byte("{broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(cfg); err == nil {
		t.Fatal("invalid proof earned success")
	}
	result := readRatchetCLIResult(t, cfg)
	if result.Status != testtiming.BudgetIncomplete || result.TestTime == nil || result.TestTime.Status != testtiming.BudgetFail {
		t.Fatalf("ratchet failure downgraded incomplete command evidence: %+v", result)
	}
}

func readRatchetCLIResult(t *testing.T, cfg config) testtiming.BudgetResult {
	t.Helper()
	var result testtiming.BudgetResult
	if err := readJSON(cfg.resultJSONPath, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func ratchetCLIConfig(t *testing.T, profile string, growth float64, newRoot bool) config {
	t.Helper()
	dir := t.TempDir()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	referencePath := filepath.Join(root, ".github/test-time-reference.json")
	raw, err := os.ReadFile(referencePath)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := testtiming.LoadTestTimeReference(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(reference.Roots, func(cell testtiming.ReferenceRootTime) bool {
		return cell.Backend == "" && cell.Count == "count-1" && slices.Contains(cell.Tiers, "core")
	})
	if index < 0 {
		t.Fatal("reference lacks an ordinary core cell")
	}
	cell := reference.Roots[index]
	policy := testplanning.Policy{
		Version: testplanning.PolicyVersion, Module: "github.com/division-sh/swarm",
		Planning: testplanning.PlanningPolicy{TargetSeconds: 120, MaxShards: 1, UnknownPackageSeconds: 1},
		Profiles: map[string]testplanning.ProfilePolicy{},
	}
	for _, tier := range []string{"core", "lifecycle", "full"} {
		policy.Profiles[tier] = testplanning.ProfilePolicy{CountMode: cell.Count, EnvironmentID: cell.Environment}
	}
	model := testplanning.WeightModel{Version: testplanning.WeightModelVersion, SourceRunID: "synthetic", Packages: map[string]float64{cell.Package: 1}}
	plan, err := testplanning.BuildPlan(policy, model, []string{cell.Package}, profile, "synthetic ratchet proof", "execution-head")
	if err != nil {
		t.Fatal(err)
	}
	name := cell.Root
	if newRoot {
		name = "TestUnapprovedNewTimingCell"
	}
	unit := &plan.Units[0]
	unit.SelectedRoots = []testplanning.TestRoot{{Package: cell.Package, Name: name}}
	unit.RequiredTests = []testplanning.RequiredTest{{TestRoot: unit.SelectedRoots[0]}}
	unit.TestBearingPackages = []string{cell.Package}
	plan.BuildContext = reference.BuildContext
	plan.Digest = ""
	raw, err = json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	plan.Digest = hex.EncodeToString(digest[:])
	cfg := config{
		evaluateBudget: true, workflowRunID: 42, workflowAttempt: 1, workflowHeadSHA: "workflow-head",
		planPath: filepath.Join(dir, "plan.json"), proofPolicyPath: filepath.Join(dir, "proof.yaml"),
		weightModelPath: filepath.Join(dir, "weights.json"), budgetPath: filepath.Join(dir, "budget.yaml"),
		testTimeReferencePath: referencePath, evidenceRoot: filepath.Join(dir, "evidence"),
		jobsPath: filepath.Join(dir, "jobs.json"), resultJSONPath: filepath.Join(dir, "result.json"),
		markdownPath: filepath.Join(dir, "result.md"),
	}
	writeRatchetCLIInputs(t, cfg, plan, policy, model)
	writeRatchetCLIProof(t, cfg, plan, cell.Package, name, cell.Seconds+growth)
	return cfg
}

func writeRatchetCLIInputs(t *testing.T, cfg config, plan testplanning.RunPlan, policy testplanning.Policy, model testplanning.WeightModel) {
	t.Helper()
	if err := writeJSON(cfg.planPath, plan); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(cfg.weightModelPath, model); err != nil {
		t.Fatal(err)
	}
	policyBytes, err := yaml.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.proofPolicyPath, policyBytes, 0600); err != nil {
		t.Fatal(err)
	}
	budget := "version: 1\nhard:\n  max_shard_command_seconds: {limit_seconds: 100000, justification: synthetic fixture}\n  full_conformance_command_seconds: {limit_seconds: 100000, justification: synthetic fixture}\n"
	if err := os.WriteFile(cfg.budgetPath, []byte(budget), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeRatchetCLIProof(t *testing.T, cfg config, plan testplanning.RunPlan, pkg, name string, seconds float64) {
	t.Helper()
	unit := plan.Units[0]
	if err := os.Mkdir(cfg.evidenceRoot, 0700); err != nil {
		t.Fatal(err)
	}
	report := testtiming.Report{
		Tests:    []testtiming.TestTiming{{Package: pkg, Test: name, Result: "pass", Elapsed: seconds}},
		Packages: []testtiming.PackageTiming{{Package: pkg, Result: "pass", Elapsed: seconds}},
		Summary:  testtiming.Summary{Events: 2, Tests: 1, Packages: 1, PackageElapsedSec: seconds},
	}
	evidence := testtiming.CommandEvidence{
		Version: testtiming.CommandEvidenceVersion, WorkflowRunID: 42, WorkflowAttempt: 1,
		PlanDigest: plan.Digest, Profile: plan.Profile, HeadSHA: plan.HeadSHA, BuildContext: plan.BuildContext,
		UnitID: unit.ID, Surface: unit.ID, Attempt: testtiming.AttemptPrimary, ElapsedSeconds: seconds,
		Packages: unit.Packages, CountMode: unit.CountMode, EnvironmentID: unit.EnvironmentID,
		WorkloadProfile: unit.WorkloadProfile, ExecutionTier: unit.ExecutionTier, Report: report,
	}
	if err := writeJSON(filepath.Join(cfg.evidenceRoot, "command-primary-evidence.json"), evidence); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Duration(seconds*float64(time.Second)) + 30*time.Second)
	job := testtiming.ActionJob{
		ID: 1, RunID: 42, RunAttempt: 1, HeadSHA: cfg.workflowHeadSHA, Name: "Go proof " + plan.Batches[0].ID,
		Status: "completed", Conclusion: "success", CreatedAt: start.Add(-time.Second), StartedAt: start, CompletedAt: end,
		Steps: []testtiming.ActionStep{
			{Name: "Run exact planned proof unit", Status: "completed", Conclusion: "success", StartedAt: start, CompletedAt: end.Add(-2 * time.Second)},
			{Name: "Upload proof evidence", Status: "completed", Conclusion: "success", StartedAt: end.Add(-2 * time.Second), CompletedAt: end.Add(-time.Second)},
		},
	}
	if err := writeJSON(cfg.jobsPath, []any{map[string]any{"jobs": []testtiming.ActionJob{job}}}); err != nil {
		t.Fatal(err)
	}
}

func TestEvaluateBudgetTrustedEventAndReportContract(t *testing.T) {
	for _, event := range []string{"pull_request", "schedule", "workflow_dispatch", "push", "", "PULL_REQUEST", "pull_request "} {
		t.Run(event, func(t *testing.T) {
			cfg := ratchetCLIConfig(t, "full", 2000, false)
			cfg.event = event
			err := run(cfg)
			result := readRatchetCLIResult(t, cfg)
			want, mode := testtiming.BudgetFail, testtiming.TestTimeStrict
			if event == "pull_request" {
				want, mode = testtiming.BudgetWarn, testtiming.TestTimePR
			}
			if result.Status != want || result.TestTime.Mode != mode || (err == nil) != (want == testtiming.BudgetWarn) {
				t.Fatalf("event %q: %v %+v", event, err, result)
			}
			markdown, err := os.ReadFile(cfg.markdownPath)
			if err != nil {
				t.Fatal(err)
			}
			for _, text := range []string{string(mode), "Strict counterfactual", "tiers[].retained_cells"} {
				if !strings.Contains(string(markdown), text) {
					t.Fatalf("CLI report lost %q", text)
				}
			}
			for _, tier := range result.TestTime.Tiers {
				if tier.AddedSeconds != 0 || tier.Retained != len(tier.RetainedCells) || tier.RetainedDelta != 2000 || tier.StrictStatus != testtiming.BudgetFail {
					t.Fatalf("CLI JSON lost exact decomposition: %+v", tier)
				}
			}
		})
	}
}

func TestEvaluateBudgetFailedJobCannotBecomeAdvisory(t *testing.T) {
	for _, event := range []string{"pull_request", "schedule"} {
		t.Run(event, func(t *testing.T) {
			cfg := ratchetCLIConfig(t, "full", 2000, false)
			cfg.event = event
			raw, err := os.ReadFile(cfg.jobsPath)
			if err != nil {
				t.Fatal(err)
			}
			var pages []struct {
				Jobs []testtiming.ActionJob `json:"jobs"`
			}
			if err := json.Unmarshal(raw, &pages); err != nil {
				t.Fatal(err)
			}
			pages[0].Jobs[0].Conclusion = "cancelled"
			if err := writeJSON(cfg.jobsPath, pages); err != nil {
				t.Fatal(err)
			}
			if err := run(cfg); err == nil {
				t.Fatal("cancelled job earned success")
			}
			result := readRatchetCLIResult(t, cfg)
			if result.Status != testtiming.BudgetIncomplete || result.ExitCode() == 0 {
				t.Fatalf("cancelled proof became advisory: %+v", result)
			}
		})
	}
}
