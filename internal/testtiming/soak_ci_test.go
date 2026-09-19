package testtiming

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
	"gopkg.in/yaml.v3"
)

func soakCIPolicy(t *testing.T) testplanning.Policy {
	t.Helper()
	f, err := os.Open(filepath.Join(testTimingRepoRoot(t), ".github/test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	p, err := testplanning.LoadPolicy(f)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func soakCIPlan(t *testing.T, policy testplanning.Policy, profile string) testplanning.RunPlan {
	t.Helper()
	plan, err := testplanning.BuildPlan(policy, testplanning.WeightModel{Version: testplanning.WeightModelVersion, SourceRunID: "guard", Packages: map[string]float64{}}, policy.SpecialPackages, profile, "soak guard", "exact-head")
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestMandatorySoakCompleteDisjointPartitionAllProfiles(t *testing.T) {
	p := soakCIPolicy(t)
	dir := filepath.Join(testTimingRepoRoot(t), "internal/runtime/conformance")
	for profile := range p.Profiles {
		t.Run(profile, func(t *testing.T) {
			plan := soakCIPlan(t, p, profile)
			var units []testplanning.ProofUnit
			for _, u := range plan.Units {
				if slices.Contains(u.Packages, testplanning.SoakPackage) {
					units = append(units, u)
				}
			}
			if err := testplanning.ValidateConformanceProofPartition(dir, units); err != nil {
				t.Fatal(err)
			}
			for _, mutation := range []string{"missing sqlite", "missing postgres", "duplicate", "ordinary overlap", "broad skip", "partial cell", "wrong timeout", "cached cell"} {
				t.Run(mutation, func(t *testing.T) {
					changed := slices.Clone(units)
					for i, u := range changed {
						backend, soak := testplanning.SoakBackend(u.Run)
						switch {
						case mutation == "missing "+backend && soak:
							changed = append(changed[:i], changed[i+1:]...)
						case mutation == "duplicate" && soak:
							changed = append(changed, u)
						case mutation == "ordinary overlap" && u.Skip != "":
							changed[i].Skip = ""
						case mutation == "broad skip" && u.Skip != "":
							changed[i].Skip = "^TestIssue2394.*$"
						case mutation == "partial cell" && soak:
							changed[i].Run += "/^partial$"
						case mutation == "wrong timeout" && soak:
							changed[i].GoTimeout = "30m"
						case mutation == "cached cell" && soak:
							changed[i].CountMode = "cache-default"
						default:
							continue
						}
						break
					}
					if err := testplanning.ValidateConformanceProofPartition(dir, changed); err == nil {
						t.Fatal("accepted incomplete/overlapping partition")
					}
				})
			}
			matrix, err := testplanning.MatrixJSON(plan)
			if err != nil {
				t.Fatal(err)
			}
			var entries struct {
				Include []struct {
					Unit string
				}
			}
			if err := json.Unmarshal(matrix, &entries); err != nil {
				t.Fatal(err)
			}
			var cells []string
			for _, entry := range entries.Include {
				unit, err := plan.Unit(entry.Unit)
				if err != nil {
					t.Fatal(err)
				}
				if unit.BudgetClass == "soak" {
					cells = append(cells, entry.Unit)
				}
			}
			slices.Sort(cells)
			if !slices.Equal(cells, []string{"conformance-soak-postgres", "conformance-soak-sqlite"}) {
				t.Fatalf("soak matrix: %v", cells)
			}
		})
	}
	// Verify the original test's finite backend declaration and workload remain
	// aligned with the approved cells; no inferred arbitrary subtest partition.
	raw, err := os.ReadFile(filepath.Join(dir, "fan_out_staged_soak_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`func ` + testplanning.SoakTest + `(t *testing.T)`, `range []string{"sqlite", "postgres"}`, `t.Run(backend, func(t *testing.T) { proveSupplementalPressureSoak(t, backend, 22, 15*time.Minute, time.Second) })`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("original soak declaration changed: %s", want)
		}
	}
}

func TestMandatorySoakWorkflowRequiredExactHeadAndBudgets(t *testing.T) {
	root := testTimingRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github/workflows/ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]ciWorkflowJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	job := workflow.Jobs["mandatory-soak"]
	if job.If != "" || job.TimeoutMinutes != 30 || job.RunsOn != "ubuntu-latest" || job.Strategy.FailFast == nil || *job.Strategy.FailFast || job.Strategy.Matrix != "${{ fromJson(needs.ci-plan.outputs.soak_matrix) }}" || !slices.Equal(job.Needs, []string{"ci-plan"}) || job.Name != "Go proof ${{ matrix.unit }}" {
		t.Fatalf("mandatory isolated soak job changed: %+v", job)
	}
	for _, name := range []string{"timing-budget", "required-tests", "publish-timing-model"} {
		if !slices.Contains(workflow.Jobs[name].Needs, "mandatory-soak") {
			t.Fatalf("%s does not await mandatory soak", name)
		}
	}
	planner := findWorkflowStep(workflow.Jobs["ci-plan"].Steps, "Plan proof topology")
	for _, selection := range []string{`.budget_class != "soak"`, `.budget_class == "soak"`, `--slurpfile plan test-results/proof-plan.json`, `select(.id == $id)`} {
		if planner == nil || !strings.Contains(planner.Run, selection) {
			t.Fatalf("matrix partition missing %s", selection)
		}
	}
	if workflow.Jobs["proof-unit"].Strategy.Matrix != "${{ fromJson(needs.ci-plan.outputs.proof_matrix) }}" {
		t.Fatal("ordinary matrix changed")
	}
	ordinary := findWorkflowStep(workflow.Jobs["proof-unit"].Steps, "Run exact planned proof unit")
	if ordinary == nil || !strings.Contains(ordinary.Run, `run_args+=(-skip "$skip_pattern")`) || !strings.Contains(ordinary.Run, `.skip // empty`) {
		t.Fatal("ordinary consumer drops exact exclusion")
	}
	proof := findWorkflowStep(job.Steps, "Run exact planned proof unit")
	if proof == nil || proof.If != "" || proof.ContinueOnError {
		t.Fatal("soak proof may be skipped/ignored")
	}
	for _, want := range []string{"-assert-execution-sha", `git rev-parse HEAD`, `.go_timeout`, `1500s`, `--kill-after=30s`, `-count=1 -timeout "$go_timeout" -json`, `status=${PIPESTATUS[0]}`, `-record-evidence`, `-attempt primary`, `test "$status" -eq 0`} {
		if !strings.Contains(proof.Run, want) {
			t.Fatalf("soak proof missing %s", want)
		}
	}
	if job.Steps[0].With["ref"] != "${{ needs.ci-plan.outputs.execution_sha }}" {
		t.Fatal("soak checkout is not exact-head")
	}
	upload := findWorkflowStep(job.Steps, "Upload proof evidence")
	if upload == nil || upload.If != "always()" || upload.With["name"] != "proof-${{ matrix.unit }}-timing" || upload.With["if-no-files-found"] != "error" {
		t.Fatal("soak evidence upload missing/fail-open")
	}
	summary := findWorkflowStep(workflow.Jobs["required-tests"].Steps, "Summarize required checks")
	if summary == nil || !strings.Contains(summary.Run, `if [ "${{ needs.mandatory-soak.result }}" != success ]; then failed=1; fi`) {
		t.Fatal("skipped soak may pass required aggregation")
	}
	f, err := os.Open(filepath.Join(root, ".github/test-timing-budgets.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	budget, err := LoadBudgetPolicy(f)
	if err != nil {
		t.Fatal(err)
	}
	if budget.Hard.MaxShardCommandSeconds.LimitSeconds != 240 || budget.Hard.FullConformanceCommandSeconds.LimitSeconds != 540 || budget.Hard.MandatorySoakCommandSeconds.LimitSeconds != 1500 {
		t.Fatal("approved command envelopes changed")
	}
}

func TestMandatorySoakWorkflowMatrixExecutionAndShellSyntax(t *testing.T) {
	root := testTimingRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github/workflows/ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]ciWorkflowJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ci-plan", "proof-unit", "mandatory-soak", "required-tests"} {
		for _, step := range workflow.Jobs[name].Steps {
			if step.Run == "" {
				continue
			}
			command := exec.Command("bash", "-n")
			command.Stdin = strings.NewReader(regexp.MustCompile(`\$\{\{[^}]*\}\}`).ReplaceAllString(step.Run, "expression"))
			if out, err := command.CombinedOutput(); err != nil {
				t.Fatalf("%s/%s shell: %s: %v", name, step.Name, out, err)
			}
		}
	}
	p := soakCIPolicy(t)
	for profile := range p.Profiles {
		t.Run(profile, func(t *testing.T) {
			plan := soakCIPlan(t, p, profile)
			dir := t.TempDir()
			if err := os.Mkdir(filepath.Join(dir, "test-results"), 0700); err != nil {
				t.Fatal(err)
			}
			planJSON, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			matrix, err := testplanning.MatrixJSON(plan)
			if err != nil {
				t.Fatal(err)
			}
			for path, data := range map[string][]byte{"proof-plan.json": planJSON, "proof-matrix.json": matrix} {
				if err := os.WriteFile(filepath.Join(dir, "test-results", path), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			var script strings.Builder
			for _, line := range strings.Split(findWorkflowStep(workflow.Jobs["ci-plan"].Steps, "Plan proof topology").Run, "\n") {
				if strings.Contains(line, `echo "proof_matrix=`) || strings.Contains(line, `echo "soak_matrix=`) {
					script.WriteString(line + "\n")
				}
			}
			output := filepath.Join(dir, "output")
			command := exec.Command("bash", "-euo", "pipefail", "-c", script.String())
			command.Dir = dir
			command.Env = append(os.Environ(), "GITHUB_OUTPUT="+output)
			if out, err := command.CombinedOutput(); err != nil {
				t.Fatalf("matrix shell: %s: %v", out, err)
			}
			data, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			soaks := 0
			for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				lane, raw, ok := strings.Cut(line, "=")
				if !ok || (lane != "soak_matrix" && lane != "proof_matrix") {
					t.Fatalf("unexpected matrix output %q", line)
				}
				var matrix struct{ Include []struct{ Unit string } }
				if err := json.Unmarshal([]byte(raw), &matrix); err != nil {
					t.Fatal(err)
				}
				for _, entry := range matrix.Include {
					u, err := plan.Unit(entry.Unit)
					if err != nil {
						t.Fatal(err)
					}
					if seen[u.ID] || (lane == "soak_matrix") != (u.BudgetClass == "soak") {
						t.Fatalf("duplicated/misrouted unit %s", u.ID)
					}
					seen[u.ID] = true
					if u.BudgetClass == "soak" {
						soaks++
					}
				}
			}
			if len(seen) != len(plan.Units) || soaks != 2 {
				t.Fatalf("incomplete matrices: %d/%d units, %d soak cells", len(seen), len(plan.Units), soaks)
			}
		})
	}
}

func TestMandatorySoakEvidenceRequiresBothFullBackendReceipts(t *testing.T) {
	policy := soakCIPolicy(t)
	// Exercise the normal complete-plan evaluator with a minimal two-cell plan.
	for name, profile := range policy.Profiles {
		profile.Units = []string{"conformance-soak-sqlite", "conformance-soak-postgres"}
		policy.Profiles[name] = profile
	}
	policy.SpecialPackages = []string{testplanning.SoakPackage}
	plan := soakCIPlan(t, policy, testplanning.ProfileFull)
	budget := timingTestPolicy()
	budget.Hard.MandatorySoakCommandSeconds = CommandBudget{LimitSeconds: 1500, Justification: "approved soak"}
	var evidence []CommandEvidence
	for _, u := range plan.Units {
		e := timingTestEvidence(plan, u.ID, AttemptPrimary, 1000)
		e.Report.Packages[0].Elapsed = 950
		e.Report.Summary.PackageElapsedSec = 950
		backend, _ := testplanning.SoakBackend(u.Run)
		e.Report.Tests = []TestTiming{{Package: testplanning.SoakPackage, Test: testplanning.SoakTest, Result: "pass", Elapsed: 950}, {Package: testplanning.SoakPackage, Test: testplanning.SoakTest + "/" + backend, Result: "pass", Elapsed: 950}}
		e.Report.Summary.Tests = 2
		evidence = append(evidence, e)
	}
	opts := EvaluationOptions{Plan: plan}
	if got := EvaluateBudget(budget, opts, evidence); got.Status != BudgetPass {
		t.Fatalf("complete synthetic evidence rejected: %+v", got)
	}
	for _, name := range []string{"missing", "duplicate", "wrong backend", "missing parent", "missing cell", "short", "skip", "wrong head", "wrong selection", "overrun", "failed"} {
		t.Run(name, func(t *testing.T) {
			changed := slices.Clone(evidence)
			changed[0].Report.Tests = slices.Clone(changed[0].Report.Tests)
			switch name {
			case "missing":
				changed = changed[:1]
			case "duplicate":
				changed = append(changed, changed[0])
			case "wrong backend":
				changed[0].Report.Tests[1].Test = testplanning.SoakTest + "/postgres"
			case "missing parent":
				changed[0].Report.Tests = changed[0].Report.Tests[1:]
				changed[0].Report.Summary.Tests = 1
			case "missing cell":
				changed[0].Report.Tests = changed[0].Report.Tests[:1]
				changed[0].Report.Summary.Tests = 1
			case "short":
				changed[0].Report.Tests[1].Elapsed = 899
			case "skip":
				changed[0].Report.Tests[1].Result = "skip"
				changed[0].Report.Summary.SkippedTests = 1
			case "wrong head":
				changed[0].HeadSHA = "stale"
			case "wrong selection":
				changed[0].GoTimeout = "30m"
			case "overrun":
				changed[0].ElapsedSeconds = 1501
			case "failed":
				changed[0].ExitCode = 1
			}
			if got := EvaluateBudget(budget, opts, changed); got.Status == BudgetPass || got.Status == BudgetWarn {
				t.Fatalf("accepted %s: %+v", name, got)
			}
		})
	}
}
