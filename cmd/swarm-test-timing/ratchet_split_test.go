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
	for _, event := range []string{"schedule", "workflow_dispatch", "push"} {
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

func TestEvaluateBudgetIncompleteDominatesSnapshotAdvisory(t *testing.T) {
	cfg := ratchetCLIConfig(t, "core", 2000, true)
	cfg.event = "pull_request"
	if err := os.WriteFile(filepath.Join(cfg.evidenceRoot, "broken-evidence.json"), []byte("{broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(cfg); err == nil {
		t.Fatal("invalid proof earned success")
	}
	result := readRatchetCLIResult(t, cfg)
	if result.Status != testtiming.BudgetIncomplete || result.TestTime == nil || result.TestTime.Status != testtiming.BudgetWarn {
		t.Fatalf("snapshot advisory downgraded incomplete command evidence: %+v", result)
	}
}

func TestEvaluateBudgetSnapshotCostAndPlacementAreAdvisory(t *testing.T) {
	for _, event := range []string{"pull_request", "push", "schedule", "workflow_dispatch"} {
		t.Run(event, func(t *testing.T) {
			cfg := ratchetCLIConfig(t, "full", 2000, true)
			cfg.event = event
			if err := run(cfg); err != nil {
				t.Fatalf("complete snapshot-only growth blocked: %v", err)
			}
			result := readRatchetCLIResult(t, cfg)
			if result.Status != testtiming.BudgetWarn || result.ExitCode() != 0 || result.TestTime == nil || result.TestTime.Status != testtiming.BudgetWarn || len(result.TestTime.Problems) != 0 || len(result.TestTime.Tiers) != 3 {
				t.Fatalf("snapshot cost earned a hard failure or lost evidence: %+v", result)
			}
			for _, row := range result.TestTime.Tiers {
				if len(row.Added) != 1 || row.AddedSeconds < 2000 || row.AddedSeconds <= row.Allowance || row.StrictStatus != testtiming.BudgetFail || !strings.Contains(strings.Join(row.Warnings, " "), ">30s") {
					t.Fatalf("snapshot advisory hid placement, charge or counterfactual: %+v", row)
				}
			}
		})
	}
}

func TestEvaluateBudgetCurrentScopeDoesNotRequireHistoricalMatch(t *testing.T) {
	for _, event := range []string{"pull_request", "push", "schedule", "workflow_dispatch"} {
		for _, scope := range []string{"build", "environment", "count"} {
			t.Run(event+"/"+scope, func(t *testing.T) {
				cfg := ratchetCLIConfig(t, "full", 0, false)
				cfg.event = event
				plan, err := readPlan(cfg.planPath)
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(cfg.evidenceRoot, "command-primary-evidence.json")
				receipt, err := readEvidence(path)
				if err != nil {
					t.Fatal(err)
				}
				original := receipt
				switch scope {
				case "build":
					plan.BuildContext.CGOEnabled = "0"
					receipt.BuildContext = plan.BuildContext
				case "environment":
					plan.Units[0].EnvironmentID = "ci-new-gateway-v2"
					receipt.EnvironmentID = plan.Units[0].EnvironmentID
				case "count":
					plan.Units[0].CountMode = testtiming.CountModeCacheDefault
					receipt.CountMode = plan.Units[0].CountMode
				}
				policy, err := readProofPolicy(cfg.proofPolicyPath)
				if err != nil {
					t.Fatal(err)
				}
				for tier, recipe := range policy.Profiles {
					recipe.CountMode, recipe.EnvironmentID = plan.Units[0].CountMode, plan.Units[0].EnvironmentID
					policy.Profiles[tier] = recipe
				}
				raw, err := yaml.Marshal(policy)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(cfg.proofPolicyPath, raw, 0600); err != nil {
					t.Fatal(err)
				}
				plan.Digest = ""
				raw, err = json.Marshal(plan)
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(raw)
				plan.Digest = hex.EncodeToString(digest[:])
				receipt.PlanDigest = plan.Digest
				if err := writeJSON(cfg.planPath, plan); err != nil {
					t.Fatal(err)
				}
				if err := writeJSON(path, receipt); err != nil {
					t.Fatal(err)
				}
				if err := run(cfg); err != nil {
					t.Fatalf("valid current %s scope refused: %v", scope, err)
				}
				result := readRatchetCLIResult(t, cfg)
				if result.Status != testtiming.BudgetWarn || result.ExitCode() != 0 || result.TestTime == nil || len(result.TestTime.Problems) != 0 || len(result.TestTime.Warnings) == 0 {
					t.Fatalf("historical scope mismatch did not remain advisory: %+v", result)
				}
				for _, row := range result.TestTime.Tiers {
					if row.Retained != 0 || len(row.RetainedCells) != 0 || len(row.Added) != 1 || row.AddedSeconds != receipt.Report.Tests[0].Elapsed {
						t.Fatalf("uncomparable current scope earned retained credit: %+v", row)
					}
				}
				receipt.BuildContext, receipt.EnvironmentID, receipt.CountMode = original.BuildContext, original.EnvironmentID, original.CountMode
				if err := writeJSON(path, receipt); err != nil {
					t.Fatal(err)
				}
				if err := run(cfg); err == nil {
					t.Fatal("receipt-versus-plan mismatch earned advisory success")
				}
				result = readRatchetCLIResult(t, cfg)
				if result.Status != testtiming.BudgetIncomplete || result.ExitCode() == 0 || result.TestTime == nil || len(result.TestTime.Tiers) != 0 {
					t.Fatalf("invalid current receipt softened: %+v", result)
				}
			})
		}
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
	for _, event := range []string{"pull_request", "schedule", "workflow_dispatch", "push"} {
		t.Run(event, func(t *testing.T) {
			cfg := ratchetCLIConfig(t, "full", 2000, false)
			cfg.event = event
			err := run(cfg)
			result := readRatchetCLIResult(t, cfg)
			want, mode := testtiming.BudgetWarn, testtiming.TestTimeStrict
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
			for _, text := range []string{string(mode), "Strict counterfactual", "tiers[].retained_cells", "cadence reports aggregate growth as advisory"} {
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

func TestEvaluateBudgetRejectsUnknownEventsBeforePolicySelection(t *testing.T) {
	for _, event := range []string{"", "unknown", "PULL_REQUEST", "pull_request ", " pull_request", "push ", "schedule\n", "workflow_dispatch\t", "pull_request_target"} {
		t.Run(event, func(t *testing.T) {
			for _, growth := range []float64{0, 2000} {
				cfg := ratchetCLIConfig(t, "full", growth, false)
				cfg.event = event
				if err := run(cfg); err == nil || !strings.Contains(err.Error(), "INCOMPLETE") {
					t.Fatalf("invalid event %q selected a policy: %v", event, err)
				}
				result := readRatchetCLIResult(t, cfg)
				if result.Status != testtiming.BudgetIncomplete || result.ExitCode() == 0 || result.TestTime != nil || len(result.Problems) != 1 || !strings.Contains(result.Problems[0], "unsupported workflow event") {
					t.Fatalf("invalid event earned a timing comparison: %+v", result)
				}
			}
		})
	}
}

func TestEvaluateBudgetCadenceCommandCeilingStaysHard(t *testing.T) {
	for _, event := range []string{"push", "schedule", "workflow_dispatch"} {
		t.Run(event, func(t *testing.T) {
			cfg := ratchetCLIConfig(t, "full", 2000, false)
			cfg.event = event
			receipt, err := readEvidence(filepath.Join(cfg.evidenceRoot, "command-primary-evidence.json"))
			if err != nil {
				t.Fatal(err)
			}
			policy, err := readBudgetPolicy(cfg.budgetPath)
			if err != nil {
				t.Fatal(err)
			}
			policy.Hard.MaxShardCommandSeconds.LimitSeconds = receipt.ElapsedSeconds / 2
			raw, err := yaml.Marshal(map[string]any{
				"version": policy.Version,
				"hard": map[string]testtiming.CommandBudget{
					"max_shard_command_seconds":        policy.Hard.MaxShardCommandSeconds,
					"full_conformance_command_seconds": policy.Hard.FullConformanceCommandSeconds,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(cfg.budgetPath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if err := run(cfg); err == nil {
				t.Fatal("over-ceiling command earned success")
			}
			result := readRatchetCLIResult(t, cfg)
			if result.Status != testtiming.BudgetFail || result.ExitCode() == 0 || result.TestTime == nil || result.TestTime.Status != testtiming.BudgetWarn || len(result.Surfaces) != 1 || result.Surfaces[0].Status != testtiming.BudgetFail {
				t.Fatalf("aggregate advisory softened the command ceiling: %+v", result)
			}
		})
	}
}

func TestEvaluateBudgetPRBufferedCommandCeilingIsAdvisory(t *testing.T) {
	for _, tc := range []struct {
		name    string
		seconds float64
		want    testtiming.BudgetStatus
	}{
		{"within", 10, testtiming.BudgetPass},
		{"buffered-boundary", 13, testtiming.BudgetPass},
		{"over-buffered-ceiling", 13.001, testtiming.BudgetWarn},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ratchetCLIConfig(t, "full", 0, false)
			cfg.event = "pull_request"
			plan, err := readPlan(cfg.planPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(cfg.evidenceRoot); err != nil {
				t.Fatal(err)
			}
			root := plan.Units[0].SelectedRoots[0]
			writeRatchetCLIProof(t, cfg, plan, root.Package, root.Name, tc.seconds)
			budget := "version: 1\nhard:\n  max_shard_command_seconds: {limit_seconds: 10, justification: synthetic boundary proof}\n  full_conformance_command_seconds: {limit_seconds: 10, justification: synthetic boundary proof}\n"
			if err := os.WriteFile(cfg.budgetPath, []byte(budget), 0600); err != nil {
				t.Fatal(err)
			}
			if err := run(cfg); err != nil {
				t.Fatalf("successful PR command was refused: %v", err)
			}
			result := readRatchetCLIResult(t, cfg)
			if result.Status != tc.want || result.ExitCode() != 0 || len(result.Surfaces) != 1 || result.Surfaces[0].Status != tc.want || result.Surfaces[0].LimitSeconds != 10 || result.Surfaces[0].BufferedCeilingSeconds != 13 || len(result.Surfaces[0].Problems) != 0 {
				t.Fatalf("PR buffered ceiling was mislabeled or changed: %+v", result)
			}
			if (len(result.Surfaces[0].Warnings) != 0) != (tc.seconds > 10) {
				t.Fatalf("PR ceiling warning lost: %+v", result.Surfaces[0])
			}
			if (strings.Contains(strings.Join(result.Surfaces[0].Warnings, " "), "buffered timing ceiling")) != (tc.want == testtiming.BudgetWarn) {
				t.Fatalf("baseline warning confused with buffered overrun: %+v", result.Surfaces[0])
			}
			path := filepath.Join(cfg.evidenceRoot, "command-primary-evidence.json")
			receipt, err := readEvidence(path)
			if err != nil {
				t.Fatal(err)
			}
			receipt.ExitCode = 1
			if err := writeJSON(path, receipt); err != nil {
				t.Fatal(err)
			}
			if err := run(cfg); err == nil {
				t.Fatal("failed PR command gained advisory success")
			}
			if result := readRatchetCLIResult(t, cfg); result.Status != testtiming.BudgetIncomplete || result.ExitCode() == 0 {
				t.Fatalf("PR advisory softened actual command failure: %+v", result)
			}
		})
	}
}

func TestEvaluateBudgetCadenceInvalidEvidenceStaysFatal(t *testing.T) {
	for _, defect := range []string{"missing", "duplicate", "wrong-run", "wrong-attempt", "wrong-head", "wrong-plan"} {
		t.Run(defect, func(t *testing.T) {
			cfg := ratchetCLIConfig(t, "full", 2000, false)
			cfg.event = "schedule"
			path := filepath.Join(cfg.evidenceRoot, "command-primary-evidence.json")
			receipt, err := readEvidence(path)
			if err != nil {
				t.Fatal(err)
			}
			switch defect {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "duplicate":
				path = filepath.Join(cfg.evidenceRoot, "duplicate-evidence.json")
			case "wrong-run":
				receipt.WorkflowRunID++
			case "wrong-attempt":
				receipt.WorkflowAttempt++
			case "wrong-head":
				receipt.HeadSHA = "foreign"
			case "wrong-plan":
				receipt.PlanDigest = "foreign"
			}
			if defect != "missing" {
				if err := writeJSON(path, receipt); err != nil {
					t.Fatal(err)
				}
			}
			if err := run(cfg); err == nil {
				t.Fatal("invalid cadence evidence earned success")
			}
			result := readRatchetCLIResult(t, cfg)
			if result.Status != testtiming.BudgetIncomplete || result.ExitCode() == 0 {
				t.Fatalf("invalid proof became advisory: %+v", result)
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
